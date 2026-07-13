# queuectl

`queuectl` is a small CLI job queue for running shell commands in the background. Enqueue a job, spin up a few workers, and they'll pick jobs off the queue, run them, retry the ones that fail with exponential backoff, and park anything that keeps failing in a Dead Letter Queue. Everything is backed by SQLite, so the queue survives restarts.

I built this as a backend internship assignment, so the scope is deliberately focused: a single-host queue you run from the terminal, not a distributed job system.

Demo video: [watch here](https://drive.google.com/file/d/1awshL5vxTgYVtXDZ0W_F99IeD1xWiJfa/view?usp=sharing)

See [DECISIONS.md](DECISIONS.md) for the specific design trade-offs (atomic claiming across processes, crash recovery timing, DLQ retry semantics, `worker stop` signaling, and what breaks if priorities were added).

## Setup

You'll need:

- Go 1.22+
- macOS or Linux (jobs run through `sh -c`, so Windows isn't supported)

Build it:

```bash
go mod tidy
go build -o queuectl ./cmd/queuectl
```

That's it, no external database to install. By default `queuectl` writes to `.queuectl/queuectl.db` and creates the `.queuectl/` directory itself the first time you run it.

If you want a different database file, `queuectl` checks in this order:

1. `--db ./custom.db` flag
2. `QUEUECTL_DB_PATH` environment variable
3. `.queuectl/queuectl.db` (the default, if neither of the above is set)

## Usage

Enqueue a job. If you don't give it an `id`, one gets generated for you:

```bash
./queuectl enqueue '{"id":"job1","command":"echo hello"}'
```

Give a job a wall-clock limit with `timeout_seconds` — if the command outruns it, its whole process group is killed and the attempt is charged as a failure, so it backs off and retries like any other:

```bash
./queuectl enqueue '{"id":"job2","command":"curl -s slow.example.com","timeout_seconds":30}'
```

Leave it out and the job inherits the `job-timeout-seconds` config default (`0`, meaning no timeout). Pass an explicit `"timeout_seconds": 0` to opt one job out of that default and let it run as long as it needs.

Start a few workers. This runs in the foreground and blocks, so you'll want a separate terminal (or run it with `&`, or under something like `tmux`/`nohup`):

```bash
./queuectl worker start --count 3
```

You can run `worker start` more than once against the same database — from as many separate terminals as you like. Each invocation is its own OS process with its own goroutine pool; they all claim from the same queue, and no job is ever claimed by two workers at once (see [Concurrency](#concurrency)):

```bash
# terminal 2, same database
./queuectl worker start --count 2
```

When you're done, stop them all gracefully — in-flight jobs get to finish before each process exits — from any other terminal:

```bash
./queuectl worker stop
```

`worker stop` discovers every running supervisor for the database (see [Concurrency](#concurrency)) and double-checks each PID actually belongs to a `queuectl` process before signaling it. In a sandboxed environment where that check itself can't run (e.g. `ps` is blocked), skip it with `--force`:

```bash
./queuectl worker stop --force
```

Check on things:

```bash
./queuectl status
```

which prints something like:

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

Look at jobs in a given state:

```bash
./queuectl list --state pending
```

Add `--json` for a machine-readable array of job objects on stdout (and nothing else on stdout), for scripting:

```bash
./queuectl list --state pending --json
# [{"id":"job1","command":"echo hello","state":"pending","attempts":0,"max_retries":3,"created_at":"2025-11-04T10:30:00Z","updated_at":"2025-11-04T10:30:00Z"}]
```

Jobs that ran out of retries land in the DLQ — list them, and retry the ones worth retrying:

```bash
./queuectl dlq list
./queuectl dlq retry job1
```

Every execution attempt (stdout, stderr, exit code) is recorded, whether it succeeded or failed — look at a job's history with:

```bash
./queuectl logs job1
```

An attempt whose worker was killed mid-command shows up here too, as an attempt with no exit code:

```text
attempt 1  worker=host:61530:1  exit_code=interrupted (worker died; job requeued)  started=...  finished=...
```

A crash isn't the job's own failure, so an interrupted attempt is recorded but *not* charged against the job's retry budget — it goes back to `pending` with its attempts untouched and runs again. See [DECISIONS.md](DECISIONS.md) question 2.

Each stream is captured up to 64KiB. Job commands are arbitrary shell, so their output is arbitrary in size — an uncapped buffer means a command that prints in a loop grows the worker's memory until the OS kills it, taking every other in-flight job on that worker with it. Anything past the cap is dropped, visibly:

```text
[queuectl: output truncated at 65536 bytes; 1934469 further bytes dropped]
```

Aggregate stats across every recorded attempt (`--json` works here too):

```bash
./queuectl metrics
```

```text
Runs:
total: 2
succeeded: 1
failed: 0
interrupted: 1
success rate: 100.0%

Duration (seconds, completed attempts only):
avg: 6.00
p95: 6.00
max: 6.00

Throughput (jobs completed):
last 1m: 1
last 5m: 1
```

Interrupted attempts are excluded from the success rate and the durations: a command cut short by a SIGKILL says nothing about how long it takes or whether it works, so averaging its truncated duration in would just skew the numbers.

Tune the defaults (see the [Configuration](#configuration) table below for what each key does):

```bash
./queuectl config set max-retries 3
./queuectl config set backoff-base 2
./queuectl config set poll-interval-ms 500
./queuectl config set lock-timeout-seconds 20
./queuectl config set worker-stale-seconds 15
./queuectl config set stop-timeout-seconds 30
```

Check what's currently set, either one key at a time or all of them:

```bash
./queuectl config get max-retries
./queuectl config list
```

Running more than one queue side by side? Point each at its own database:

```bash
./queuectl --db ./custom.db enqueue '{"id":"job1","command":"echo hello"}'
QUEUECTL_DB_PATH=./custom.db ./queuectl status
```

## Architecture

I split the code into a few small packages rather than one flat `main.go`, mostly so the state machine and the SQL don't end up tangled with CLI parsing:

- `internal/cli` — the Cobra commands themselves and how they format output.
- `internal/config` — where the database lives, PID directory naming, config validation.
- `internal/job` — the `Job` struct, its valid states, and the transitions between them.
- `internal/storage` — everything SQLite: migrations, atomic claiming, DLQ updates, worker heartbeats, job run history.
- `internal/worker` — the actual worker loop: claiming jobs, running commands, backoff math, lease renewal, the supervisor's PID handling, and the crash-recovery reaper.

SQLite is the single source of truth for job state — there's no in-memory queue sitting in front of it. On startup `queuectl` turns on WAL mode, sets a busy timeout, uses `synchronous = NORMAL`, and enables foreign keys. Tables and the default config rows get created automatically the first time you touch the database, so there's no separate migration step to remember.

### Job lifecycle

A job starts `pending`. A worker claims it (`processing`), runs the command, and depending on the exit code either marks it `completed` or increments its attempt count and decides between `failed` (there's backoff time to wait out before it's eligible again) and `dead` (out of retries, sitting in the DLQ until someone runs `dlq retry`).

### Concurrency

Claiming a job is a single `UPDATE ... RETURNING` inside a `BEGIN IMMEDIATE` transaction, so two workers racing for the same row serialize at the database rather than in application code — there's no separate "read the queue, then update" step that could race. This holds across processes, not just goroutines: any number of `queuectl worker start` invocations — including ones started from separate terminals — can run against the same database at once, and claiming still serializes at SQLite. Each claimed job is stamped with the worker's ID and a lock timestamp that gets renewed while the command runs; if a worker dies mid-job, a background reaper notices the stale lock after `lock-timeout-seconds` and requeues (or kills) the job on its behalf. See [DECISIONS.md](DECISIONS.md) for the exact lines and why the mechanism is atomic across OS processes.

Job commands run as the leader of their own OS process group (not queuectl's own group), and that group's ID is persisted alongside the lock. When the reaper reclaims a stale job, it also sends `SIGKILL` to that whole process group, not just the DB row — so a job whose worker went silent (wedged, crashed, or killed via `worker stop`'s forced-shutdown path) doesn't keep running as an untracked orphan that could still be doing work (or racing a fresh execution of the same command) after the queue has already moved on from it.

Each `worker start` process registers its own PID file (named after its own PID) in a shared directory under `.queuectl/`, rather than one process claiming a single exclusive PID file. `worker stop` scans that directory, verifies each live PID is actually a `queuectl worker start` process, and signals every one it finds — so it can stop any number of supervisors from a single invocation, from a different terminal than any of them.

### Delivery guarantee: at-least-once

Worth being precise about, because it's the one guarantee people assume is stronger than it is.

**Claiming is exactly-once.** Two workers can never hold the same job at the same time — that's the `BEGIN IMMEDIATE` claim above, and it holds across separate OS processes. Under normal operation (including graceful shutdown, retries, and DLQ round-trips) every job's command runs exactly once.

**Execution is at-least-once, and only a crash can make it more than once.** There is a window between the moment a command finishes and the moment the worker writes that outcome to the database. If the worker is `SIGKILL`ed inside that window, the command has already had its side effects, but nothing recorded them: the row is still `processing`, and the database has no way to distinguish "the command succeeded and the worker died before saying so" from "the command never ran at all". The reaper takes the only safe option and requeues it, so the command runs a second time.

Closing that window for real needs the job's side effects and the queue's bookkeeping to commit together — a transactional outbox, or an idempotency key the command itself checks. Both push work onto the job author, and neither is in scope here. Every durable queue that runs arbitrary shell commands (SQS, Sidekiq, Celery) has the same seam in the same place.

The practical consequence: **job commands should be idempotent** if a duplicate run would be harmful. `echo`, a rebuild, an `INSERT ... ON CONFLICT` are all fine. Charging a credit card is not.

## Configuration

Everything here is stored in SQLite and changed with `queuectl config set <key> <value>` — nothing is hardcoded. `queuectl config get <key>` and `queuectl config list` read it back.

| Key | Default | Must be | What it controls |
| --- | ---: | --- | --- |
| `max-retries` | `3` | `>= 1` | How many total attempts a new job gets before it's declared dead. |
| `backoff-base` | `2` | `>= 1` | The base in `backoff_base ^ attempts` — how fast retry delays grow. The delay is capped at 1 hour, so a large base (or a job with a large `max_retries`) can't overflow the calculation into a nonsense delay. |
| `poll-interval-ms` | `500` | `>= 50` | How long an idle worker sleeps between checks for new work. |
| `lock-timeout-seconds` | `20` | `>= 1` | How long a `processing` job can go without a heartbeat before the reaper assumes its worker died. |
| `worker-stale-seconds` | `15` | `>= 10` | How recent a worker's heartbeat needs to be for `status` to count it as active. The minimum is tied to the worker heartbeat cadence (every 5s): anything lower and a perfectly healthy worker would periodically show as inactive in the gap between two ordinary heartbeats. |
| `stop-timeout-seconds` | `30` | `>= 1` | How long `worker stop` waits for a graceful shutdown before escalating to SIGKILL. |
| `job-timeout-seconds` | `0` | `>= 0` | Default wall-clock limit for a job whose enqueue JSON has no `timeout_seconds`. `0` means no timeout. |

Note that `max_retries` set in the enqueue JSON only affects that one job — it doesn't touch the `max-retries` config, and existing jobs don't retroactively pick up a config change either. Each job keeps whatever `max_retries` it was created with, since it's stored on the job row at enqueue time. `timeout_seconds` works exactly the same way: read from config at enqueue time if the JSON omits it, then baked into the job row.

`backoff-base` behaves differently: it is *not* stored per job, so it's read fresh from config every time a retry delay is computed. Changing it with `config set backoff-base <n>` immediately changes the delay calculation for every job's *next* failure — including jobs that were already enqueued, already failed once and waiting on `next_retry_at`, or mid-execution when the change happens — not just jobs enqueued afterward. The same applies to `poll-interval-ms`, `lock-timeout-seconds`, `worker-stale-seconds`, and `stop-timeout-seconds`: all of them are read from config at the point of use rather than captured once. `max-retries` and `job-timeout-seconds` are the two exceptions, since both are baked into the job row at enqueue time.

## Testing

The unit tests cover the state machine, config validation, and the trickier storage-layer stuff (atomic claiming, lock fencing, stale-job recovery):

```bash
go test ./...
```

For an end-to-end check, `scripts/test.sh` builds the binary, spins up a temporary database and three real workers, and walks through the scenarios the assignment asks for: a job that just completes, a job that fails its way into the DLQ with backoff in between, an invalid command failing gracefully, ten jobs fanned out across workers with no duplicate execution, a completed job still being there after the workers are stopped and restarted, two independent `worker start` processes (simulating separate terminals) sharing one queue with no duplicate execution, and a worker being `SIGKILL`ed mid-job with the job recovering and completing after restart. It's not mocked — it actually shells out and runs `queuectl`.

```bash
bash scripts/test.sh
```

## Assumptions And Trade-Offs

A few decisions worth calling out, in case they matter for how this gets evaluated:

- **The queue is at-least-once, not exactly-once.** Claiming a job is exactly-once — two workers never hold the same row — but a worker `SIGKILL`ed in the window between its command finishing and the outcome being written will have the job requeued and run again, side effects and all. That's a property of crash recovery, not a bug, and it's why job commands should be idempotent. See [Delivery guarantee](#delivery-guarantee-at-least-once) for the full reasoning and what it would take to close.
- **`max_retries` counts total attempts, not retries after the first failure.** So `max_retries: 3` means the job runs at most 3 times total, not 1 try + 3 retries. I went with this because it's what the field name in the assignment's job JSON implies, but it's worth flagging since "retries" is genuinely ambiguous.
- **Commands run through `sh -c`**, not split and exec'd directly. This means pipes, quoting, redirects, and env vars all work like you'd expect from a shell — but it also means `queuectl` trusts whatever command it's given. There's no sandboxing here; don't point this at untrusted input.
- Built and tested for **macOS/Linux**. No Windows support — `sh -c` doesn't exist there.
- **A job that backgrounds a process (`something &`) completes as soon as the command itself exits.** The background process keeps running — that's what `&` means — but `queuectl` stops capturing its output after a few seconds and notes so in the job's stderr. Without that cutoff the worker would wait on the output pipe forever, since the backgrounded process inherits it and outlives the shell.
- **Any number of worker supervisors can run against the same database at once**, including ones started from separate terminals — each registers its own PID file rather than contending for one. `worker stop` discovers and signals all of them. Multiple reapers ticking concurrently is safe (each stale-job recovery is fenced) but is redundant work; harmless at this scale.
- Shutdown is two-tiered: **SIGTERM is graceful** (workers stop picking up new jobs and let whatever they're running finish), **SIGKILL is not** (it can cut a job off mid-execution). Either way, the job's command runs in its own OS process group, and the reaper kills that group (not just the DB row) once it notices the orphaned `processing` row on a later pass — so a forced shutdown doesn't leave the command running unsupervised in the background. That cleanup only happens once a `queuectl worker start` process is running its reaper again, so a job killed by `worker stop`'s SIGKILL escalation can stay orphaned until the next `worker start`, not instantly.
- `lock-timeout-seconds` is *not* a job timeout, and the two are independent. It's how long a `processing` job can go without a lease renewal before the reaper assumes the **worker** died; workers renew their lease on a timer while a command runs, so a legitimately long job holds its lock indefinitely and is never reaped out from under itself. The default (20s, with a reaper sweep every 10s) is tuned to keep worst-case crash recovery under the assignment's 60-second bound — see [DECISIONS.md](DECISIONS.md). If you want to bound how long a **job** may run, that's `timeout_seconds` / `job-timeout-seconds`.
- **A timeout is charged as a failed attempt; a dead worker is not.** A command that ran past its own deadline is the job failing, so it backs off, retries, and eventually lands in the DLQ like any other failure. A worker that got SIGKILLed says nothing about whether the command would have succeeded, so the reaper requeues that job with its retry budget untouched. Those are deliberately different, and it's the line that decides whether a poison job can exhaust its retries.
- PID files: the default database uses `.queuectl/workers/`; anything opened with a custom `--db` gets its own hashed directory (`.queuectl/workers-<hash>/`) so two differently-named queues don't stomp on each other's PID files. Each running supervisor's own PID names its file within that directory.


## Walkthrough

A full run-through touching every command, using two terminals side by side.

Build and check the CLI is there:

```bash
go build -o queuectl ./cmd/queuectl
./queuectl --help
```

Enqueue a job and check status:

```bash
./queuectl enqueue '{"id":"job1","command":"echo hello","max_retries":1}'
./queuectl status
```

**Terminal 1** — start the workers (this blocks):

```bash
./queuectl worker start --count 3
```

**Terminal 2** — watch it get picked up and check the logs:

```bash
./queuectl status
./queuectl logs job1
```

**Terminal 3** — start a second, independent supervisor against the same database, proving workers really are separate OS processes rather than just goroutines under one process:

```bash
./queuectl worker start --count 2
```

**Terminal 2** — `status` now reports 5 active workers (3 + 2) across two supervisor processes:

```bash
./queuectl status
./queuectl list --state pending --json
```

Enqueue a job that's guaranteed to fail and watch it land in the DLQ:

```bash
./queuectl enqueue '{"id":"job-fail","command":"exit 7","max_retries":2}'
./queuectl status
./queuectl dlq list
./queuectl logs job-fail
```

Retry it from the DLQ:

```bash
./queuectl dlq retry job-fail
./queuectl status
```

Fan ten jobs out across the three workers:

```bash
for i in $(seq 1 10); do
  ./queuectl enqueue "{\"id\":\"multi-$i\",\"command\":\"echo processed-$i\",\"max_retries\":1}"
done
./queuectl status
sleep 2
./queuectl status
```
## Crash recovery (the SIGKILL test)

**[T2]** Type:
 
```bash
./queuectl enqueue '{"id":"crash-job","command":"sleep 30 && echo survived","max_retries":3}'
./queuectl status
```
**[T2]** Point at the still-running worker pane (T1), then hard-kill the supervisor
from the driver pane with a SIGKILL — no graceful shutdown, no cleanup:
 
```bash
pkill -9 -f "queuectl worker start"
./queuectl status
```

**[T1]** Restart workers:
 
```bash
./queuectl worker start --count 3
```
 
**[T2]** Wait, then:
 
```bash
./queuectl status
./queuectl logs crash-job
```

**Terminal 2**:

```bash
./queuectl status
```

Tune config on the fly:

```bash
./queuectl config list
./queuectl config set max-retries 5
./queuectl config get max-retries
```

Clean shutdown:

```bash
./queuectl worker stop
```