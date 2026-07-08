# queuectl

`queuectl` is a CLI-based background job queue for trusted shell commands. Jobs are stored in SQLite, processed by worker goroutines, retried with exponential backoff, and moved to a Dead Letter Queue when they exhaust their allowed attempts.

Demo video: <add Google Drive / Loom link here before submission>

## Setup

Requirements:

- Go 1.22+
- macOS or Linux
- POSIX `sh`

Install dependencies and build:

```bash
go mod tidy
go build -o queuectl ./cmd/queuectl
```

The default database is `.queuectl/queuectl.db`. The `.queuectl/` directory is created automatically.

Database path precedence:

1. `--db ./custom.db`
2. `QUEUECTL_DB_PATH=./custom.db`
3. `.queuectl/queuectl.db`

## Usage

Enqueue a job:

```bash
./queuectl enqueue '{"id":"job1","command":"echo hello"}'
```

Start workers in the foreground:

```bash
./queuectl worker start --count 3
```

Stop the worker supervisor:

```bash
./queuectl worker stop
```

If process verification can't run (e.g. `ps` is blocked in a sandboxed environment), pass `--force` to skip verification and signal the PID anyway:

```bash
./queuectl worker stop --force
```

Show queue status:

```bash
./queuectl status
```

Sample output:

```text
Jobs:
pending: 3
processing: 1
completed: 10
failed: 2
dead: 1

Workers:
active: 3
```

List jobs by state:

```bash
./queuectl list --state pending
```

Inspect and retry DLQ jobs:

```bash
./queuectl dlq list
./queuectl dlq retry job1
```

Update configuration:

```bash
./queuectl config set max-retries 3
./queuectl config set backoff-base 2
./queuectl config set poll-interval-ms 500
./queuectl config set lock-timeout-seconds 120
./queuectl config set worker-stale-seconds 15
./queuectl config set stop-timeout-seconds 30
```

Use a custom database:

```bash
./queuectl --db ./custom.db enqueue '{"id":"job1","command":"echo hello"}'
QUEUECTL_DB_PATH=./custom.db ./queuectl status
```

## Architecture

The code is split into small internal packages:

- `internal/cli`: Cobra commands and CLI output.
- `internal/config`: DB path resolution, default paths, and config validation.
- `internal/job`: job model, valid states, and state transitions.
- `internal/storage`: SQLite connection setup, migrations, config storage, atomic claiming, status, DLQ updates, worker rows, and job run records.
- `internal/worker`: worker pool, command execution, exponential backoff, heartbeat, job lease renewal, supervisor PID handling, and crash recovery reaper.

SQLite is the source of truth. On open, queuectl applies WAL mode, a busy timeout, normal synchronous mode, and foreign keys. Tables and default config values are created automatically.

## Configuration

| Key | Default | Validation | Meaning |
| --- | ---: | --- | --- |
| `max-retries` | `3` | `>= 1` | Default maximum total execution attempts for newly enqueued jobs. |
| `backoff-base` | `2` | `>= 1` | Base used by `backoff_base ^ attempts`. |
| `poll-interval-ms` | `500` | `>= 50` | Worker sleep duration when no job is available. |
| `lock-timeout-seconds` | `120` | `>= 1` | Age after which a processing job is considered stale. |
| `worker-stale-seconds` | `15` | `>= 1` | Heartbeat cutoff used by `queuectl status`. |
| `stop-timeout-seconds` | `30` | `>= 1` | Graceful stop wait before SIGKILL. |

`max_retries` in enqueue JSON overrides the current `max-retries` config for that job only. Existing jobs keep their stored `max_retries` even if config changes later.

## Testing

Run unit tests:

```bash
go test ./...
```

Run the integration script:

```bash
bash scripts/test.sh
```

The script builds `./queuectl`, creates a temporary DB, applies fast test config, starts multiple workers, verifies completion, DLQ movement, invalid command handling, no overlap across workers, persistence after worker stop, and DLQ retry.

## Assumptions And Trade-Offs

- `max_retries` means maximum total execution attempts, not retries after the first attempt.
- Commands run through `sh -c`, so they can use quotes, pipes, redirects, and environment variables.
- Commands are trusted input. `queuectl` is not a sandbox.
- The target OS is macOS/Linux.
- Only one worker supervisor process per database path is supported at a time.
- SIGTERM is graceful: workers stop claiming new jobs and finish current jobs.
- SIGKILL is forced and may interrupt in-flight work; the reaper later recovers stale processing jobs.
- `lock-timeout-seconds` must exceed normal job runtime. Long-running production jobs would need job heartbeat extension or explicit timeouts.
- Workers renew processing job leases while commands run, and completion/failure updates are fenced by the current job lock owner.
- The default database uses `.queuectl/worker.pid`; custom database paths use `.queuectl/worker-<hash>.pid` to avoid cross-database PID-file collisions.
