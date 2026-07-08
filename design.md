# queuectl Design

## SQLite

SQLite keeps the assignment self-contained while still providing durable storage, transactions, constraints, and portable files. `modernc.org/sqlite` is used so the project does not depend on CGO. WAL mode improves read/write behavior for the CLI plus worker process model, and `busy_timeout` reduces transient lock failures.

Schema setup (`CREATE TABLE IF NOT EXISTS`, additive `ALTER TABLE` for columns added after the initial schema, default config rows) runs inside a single `BEGIN IMMEDIATE` transaction on every `Store.Open`. This matters beyond atomicity: the additive-column check is a read-then-`ALTER TABLE`, and SQLite has no `ADD COLUMN IF NOT EXISTS`. Without the transaction, two `queuectl` processes racing to open the same pre-existing database for the first time after an upgrade could both see the column missing and both try to add it, and the loser would fail with "duplicate column name". Wrapping the whole migration in `BEGIN IMMEDIATE` means the loser blocks on SQLite's reserved lock until the winner commits, then re-checks and finds the column already there.

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
ORDER BY created_at ASC, rowid ASC
LIMIT 1
)
RETURNING *;
```

This prevents two workers from claiming the same job. Failed jobs are included only after `next_retry_at` so retry backoff is enforced by storage, not by worker memory.

`created_at` is stored with one-second resolution, so jobs enqueued within the same wall-clock second tie on it. `rowid` — SQLite's implicit, monotonically increasing insertion-order column, present on any table not declared `WITHOUT ROWID` — breaks that tie in true insertion order. Job `id` itself is not a safe tiebreaker: an auto-generated id is random hex, unrelated to enqueue order. `queuectl list` orders the same way for the same reason.

## Worker Pool

`queuectl worker start --count N` starts one foreground supervisor process and N goroutines. Each goroutine registers a worker row using `hostname:pid:worker-number`, heartbeats every 5 seconds, claims at most one job at a time, executes it to completion, and then repeats.

Only one supervisor process per database path is supported. The default database uses `.queuectl/worker.pid`; custom database paths use a hashed PID file under `.queuectl/` so separate queues do not collide. Before sending stop signals, queuectl verifies that the PID file points to a `queuectl worker start` process.

Each goroutine's job-execution step recovers from any panic raised while running or recording a job, logging it instead of letting it propagate. Without this, a single bad job (a bug surfaced by a particular command, a store call hitting an unexpected state) would crash the whole supervisor process and take every other in-flight job down with it. Recovery cancels that job's lease-renewal loop before returning, so its lock still ages out normally and the reaper picks the abandoned claim back up exactly as it would for a worker that crashed outright - the panic is contained to one job, not silently swallowed.

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

This avoids treating stale worker rows from crashed processes as active workers. `worker-stale-seconds`'s minimum is enforced at twice the heartbeat cadence (10s): anything smaller and a perfectly healthy worker would periodically - and incorrectly - read as inactive in the ordinary gap between two heartbeats, which would be a self-inflicted flicker rather than a real staleness signal.

A worker that exits gracefully deletes its own row as part of shutdown. One that is killed (SIGKILL, a crash) cannot, so its row would otherwise sit in `workers` forever. The reaper garbage-collects rows whose heartbeat is older than a much larger threshold (one hour) on every pass - large enough that it can only ever catch a row from a process that is unambiguously gone, never a worker that is merely slow to heartbeat.

## Job Lease Fencing

Claimed jobs store `locked_by` and `locked_at`. While a command is running, the worker renews `locked_at` before the lock timeout can expire. Completion and failure updates are fenced by both `locked_by` and the latest `locked_at`; if the reaper or another worker has reclaimed the row, the old worker cannot mark that newer claim as completed or failed.

Jobs also store `locked_pgid`: the OS process-group ID leading the job's `sh -c` command, set once the worker actually starts it (`Store.SetJobLockPGID`, itself fenced on `locked_by`). The command runs via `SysProcAttr{Setpgid: true}`, making it (and any children it spawns, e.g. in a pipeline) the sole members of a fresh process group rather than sharing queuectl's own. This is what lets the reaper do more than just update the row — see Crash Recovery Reaper below.

## Stop Design

`queuectl worker stop` reads `.queuectl/worker.pid`, sends SIGTERM, and waits up to `stop-timeout-seconds`. On SIGTERM, workers stop claiming new jobs, finish in-flight jobs, delete worker rows, remove the PID file, and exit. If the process does not exit in time, `worker stop` sends SIGKILL.

Before signaling, `worker stop` verifies the PID actually belongs to a `queuectl worker start` process (via `/proc/<pid>/cmdline` on Linux, falling back to `ps -p <pid> -o command=` elsewhere), so it never signals an unrelated process that happens to reuse a recycled PID. In sandboxed environments where that verification itself cannot run (for example, `ps` blocked by macOS sandboxing), pass `--force` to skip verification and signal the PID anyway; a definitive "this PID is not queuectl" result still refuses to signal regardless of `--force`.

## Crash Recovery Reaper

If a worker process dies after claiming a job, that job can remain `processing`. The reaper runs once on worker startup and then every 30 seconds. It finds processing jobs whose `locked_at` is older than `lock-timeout-seconds`, increments attempts, and moves them to either `failed` with backoff or `dead`.

Recovering the row is not the end of it: the reaper also sends `SIGKILL` to the job's process group (`-locked_pgid`), reading that column from the very row it just fenced away from its previous owner. This matters because reclaiming the *database* row does not, by itself, stop the *OS process* — a worker that stopped renewing its lease because it's wedged (not dead) can still be running the command, and a supervisor killed via `worker stop`'s SIGKILL escalation leaves its in-flight job's process group alive and orphaned (parent death does not kill children on Unix, and killing a single PID does not kill its process group). Group-killing on recovery closes that gap: once the reaper has moved a job on, its old execution is guaranteed to be gone too, so a subsequent re-claim of the same job can't run concurrently with a leftover copy of itself. This works even across process boundaries — the reaper that eventually recovers an orphan may be a freshly restarted supervisor, not the one that started the command, since `locked_pgid` is read from the database, not from any in-memory state. Sending the kill to a group whose leader has already exited naturally is a harmless no-op (`ESRCH`, swallowed).

The residual gap: this cleanup only happens while a reaper is actually running, i.e. inside an active `queuectl worker start` process. If `worker stop` has to escalate to SIGKILL, the orphaned job's process group survives until the *next* `worker start`'s startup reaper pass, not instantly at stop time.

Workers renew job leases during execution, but `lock-timeout-seconds` should still be greater than expected scheduling stalls and database pauses. Long-running production jobs would also benefit from explicit command timeouts and richer lease observability.

## Job Output Logging

Every execution attempt's stdout, stderr, exit code, and timestamps are persisted to `job_runs` regardless of outcome. `queuectl logs JOB_ID` lists them oldest-first, one block per attempt.

## Known Trade-Offs

- SQLite is excellent for a local CLI queue, but distributed multi-host queues would need a server database or broker.
- `sh -c` is flexible but assumes trusted input.
- A forced SIGKILL can interrupt an in-flight command before a `job_runs` row is recorded; the reaper still advances the job (and now also kills its process group, see Crash Recovery Reaper) so it does not stay stuck forever.
- Only one supervisor process per database path is allowed because PID-file process management is intentionally simple.
- Worker config is read from SQLite rather than hardcoded, but some loop constants such as heartbeat and reaper cadence are fixed by the assignment.
- Process-group kill is a coarse tool: it terminates the whole group with SIGKILL rather than attempting a graceful SIGTERM-then-wait against an orphaned job's own process tree, since by the time the reaper acts, that process is already considered lost and there is no one left to negotiate a graceful exit with.
