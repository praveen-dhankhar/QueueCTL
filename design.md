# queuectl Design

## SQLite

SQLite keeps the assignment self-contained while still providing durable storage, transactions, constraints, and portable files. `modernc.org/sqlite` is used so the project does not depend on CGO. WAL mode improves read/write behavior for the CLI plus worker process model, and `busy_timeout` reduces transient lock failures.

## Schema

`jobs` stores the durable queue state. `config` stores runtime tuning values. `workers` stores supervisor goroutine heartbeats for status. `job_runs` records every completed execution attempt with stdout, stderr, exit code, and timestamps.

The job state check constraint allows only:

- `pending`
- `processing`
- `completed`
- `failed`
- `dead`

## Job Lifecycle

New jobs start as `pending` with `attempts = 0`. Workers claim `pending` or retryable `failed` jobs and move them to `processing`. A successful command increments `attempts` and moves the job to `completed`. A failed command increments `attempts`; if attempts remain, the job moves to `failed` with `next_retry_at`, otherwise it moves to `dead`.

Dead jobs are never retried automatically. `queuectl dlq retry JOB_ID` preserves the ID, command, and max retry value, resets attempts and locks, and moves the job back to `pending`.

## Atomic Claim

Workers never do a separate select and update. Claiming uses one `BEGIN IMMEDIATE` transaction and an `UPDATE ... RETURNING` statement:

```sql
UPDATE jobs
SET
state = 'processing',
locked_by = ?,
locked_at = CURRENT_TIMESTAMP,
updated_at = CURRENT_TIMESTAMP
WHERE id = (
SELECT id
FROM jobs
WHERE state IN ('pending', 'failed')
AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
ORDER BY created_at ASC
LIMIT 1
)
RETURNING *;
```

This prevents two workers from claiming the same job. Failed jobs are included only after `next_retry_at` so retry backoff is enforced by storage, not by worker memory.

## Worker Pool

`queuectl worker start --count N` starts one foreground supervisor process and N goroutines. Each goroutine registers a worker row using `hostname:pid:worker-number`, heartbeats every 5 seconds, claims at most one job at a time, executes it to completion, and then repeats.

Only one supervisor process per database path is supported. The default database uses `.queuectl/worker.pid`; custom database paths use a hashed PID file under `.queuectl/` so separate queues do not collide. Before sending stop signals, queuectl verifies that the PID file points to a `queuectl worker start` process.

## Execution

Jobs are executed with:

```go
exec.CommandContext(ctx, "sh", "-c", job.Command)
```

The command string is not split manually. POSIX shell behavior is intentional: users can use quoting, pipes, redirects, and environment variables. The trade-off is that commands are trusted input and not sandboxed.

## Retries And Backoff

`max_retries` is maximum total attempts. With `max_retries = 3`, the job can run at most three times.

Backoff is:

```text
delay_seconds = backoff_base ^ attempts
```

With `backoff-base = 2`, failure after attempt 1 retries after 2 seconds, and failure after attempt 2 retries after 4 seconds. Tests can set `backoff-base = 1` for fast retries.

## DLQ

A job moves to `dead` when `attempts >= max_retries`. Dead jobs are shown by `queuectl dlq list`. Retrying from DLQ is explicit and resets attempts to zero while preserving the original job ID and command.

## Heartbeat And Status

Each worker updates `workers.last_heartbeat` every 5 seconds. `queuectl status` counts active workers using:

```text
last_heartbeat > now - worker-stale-seconds
```

This avoids treating stale worker rows from crashed processes as active workers.

## Job Lease Fencing

Claimed jobs store `locked_by` and `locked_at`. While a command is running, the worker renews `locked_at` before the lock timeout can expire. Completion and failure updates are fenced by both `locked_by` and the latest `locked_at`; if the reaper or another worker has reclaimed the row, the old worker cannot mark that newer claim as completed or failed.

## Stop Design

`queuectl worker stop` reads `.queuectl/worker.pid`, sends SIGTERM, and waits up to `stop-timeout-seconds`. On SIGTERM, workers stop claiming new jobs, finish in-flight jobs, delete worker rows, remove the PID file, and exit. If the process does not exit in time, `worker stop` sends SIGKILL.

Before signaling, `worker stop` verifies the PID actually belongs to a `queuectl worker start` process (via `/proc/<pid>/cmdline` on Linux, falling back to `ps -p <pid> -o command=` elsewhere), so it never signals an unrelated process that happens to reuse a recycled PID. In sandboxed environments where that verification itself cannot run (for example, `ps` blocked by macOS sandboxing), pass `--force` to skip verification and signal the PID anyway; a definitive "this PID is not queuectl" result still refuses to signal regardless of `--force`.

## Crash Recovery Reaper

If a worker process dies after claiming a job, that job can remain `processing`. The reaper runs once on worker startup and then every 30 seconds. It finds processing jobs whose `locked_at` is older than `lock-timeout-seconds`, increments attempts, and moves them to either `failed` with backoff or `dead`.

Workers renew job leases during execution, but `lock-timeout-seconds` should still be greater than expected scheduling stalls and database pauses. Long-running production jobs would also benefit from explicit command timeouts and richer lease observability.

## Known Trade-Offs

- SQLite is excellent for a local CLI queue, but distributed multi-host queues would need a server database or broker.
- `sh -c` is flexible but assumes trusted input.
- A forced SIGKILL can interrupt an in-flight command before a `job_runs` row is recorded; the reaper still advances the job so it does not stay stuck forever.
- Only one supervisor process per database path is allowed because PID-file process management is intentionally simple.
- Worker config is read from SQLite rather than hardcoded, but some loop constants such as heartbeat and reaper cadence are fixed by the assignment.
