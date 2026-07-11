# queuectl

`queuectl` is a small CLI job queue for running shell commands in the background. Enqueue a job, spin up a few workers, and they'll pick jobs off the queue, run them, retry the ones that fail with exponential backoff, and park anything that keeps failing in a Dead Letter Queue. Everything is backed by SQLite, so the queue survives restarts.

I built this as a backend internship assignment, so the scope is deliberately focused: a single-host queue you run from the terminal, not a distributed job system.

Demo video: [watch here](https://drive.google.com/file/d/1A1UNLaXX3Np2dR4Gjmx-bNcFtxluOHnr/view?usp=sharing)

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

Start a few workers. This runs in the foreground and blocks, so you'll want a separate terminal (or run it with `&`, or under something like `tmux`/`nohup`):

```bash
./queuectl worker start --count 3
```

You can run `worker start` more than once against the same database — from as many separate terminals as you like. Each invocation is its own OS process with its own goroutine pool; they all claim from the same queue and never duplicate work (see [Concurrency](#concurrency)):

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

Tune the defaults (see the [Configuration](#configuration) table below for what each key does):

```bash
./queuectl config set max-retries 3
./queuectl config set backoff-base 2
./queuectl config set poll-interval-ms 500
./queuectl config set lock-timeout-seconds 120
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

## Configuration

Everything here is stored in SQLite and changed with `queuectl config set <key> <value>` — nothing is hardcoded. `queuectl config get <key>` and `queuectl config list` read it back.

| Key | Default | Must be | What it controls |
| --- | ---: | --- | --- |
| `max-retries` | `3` | `>= 1` | How many total attempts a new job gets before it's declared dead. |
| `backoff-base` | `2` | `>= 1` | The base in `backoff_base ^ attempts` — how fast retry delays grow. |
| `poll-interval-ms` | `500` | `>= 50` | How long an idle worker sleeps between checks for new work. |
| `lock-timeout-seconds` | `20` | `>= 1` | How long a `processing` job can go without a heartbeat before the reaper assumes its worker died. |
| `worker-stale-seconds` | `15` | `>= 10` | How recent a worker's heartbeat needs to be for `status` to count it as active. The minimum is tied to the worker heartbeat cadence (every 5s): anything lower and a perfectly healthy worker would periodically show as inactive in the gap between two ordinary heartbeats. |
| `stop-timeout-seconds` | `30` | `>= 1` | How long `worker stop` waits for a graceful shutdown before escalating to SIGKILL. |

Note that `max_retries` set in the enqueue JSON only affects that one job — it doesn't touch the `max-retries` config, and existing jobs don't retroactively pick up a config change either. Each job keeps whatever `max_retries` it was created with, since it's stored on the job row at enqueue time.

`backoff-base` behaves differently: it is *not* stored per job, so it's read fresh from config every time a retry delay is computed. Changing it with `config set backoff-base <n>` immediately changes the delay calculation for every job's *next* failure — including jobs that were already enqueued, already failed once and waiting on `next_retry_at`, or mid-execution when the change happens — not just jobs enqueued afterward. The same applies to `poll-interval-ms`, `lock-timeout-seconds`, `worker-stale-seconds`, and `stop-timeout-seconds`: all of them are read from config at the point of use rather than captured once, so `max-retries` (baked into the job row) is the one exception, not the rule.

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

- **`max_retries` counts total attempts, not retries after the first failure.** So `max_retries: 3` means the job runs at most 3 times total, not 1 try + 3 retries. I went with this because it's what the field name in the assignment's job JSON implies, but it's worth flagging since "retries" is genuinely ambiguous.
- **Commands run through `sh -c`**, not split and exec'd directly. This means pipes, quoting, redirects, and env vars all work like you'd expect from a shell — but it also means `queuectl` trusts whatever command it's given. There's no sandboxing here; don't point this at untrusted input.
- Built and tested for **macOS/Linux**. No Windows support — `sh -c` doesn't exist there.
- **Any number of worker supervisors can run against the same database at once**, including ones started from separate terminals — each registers its own PID file rather than contending for one. `worker stop` discovers and signals all of them. Multiple reapers ticking concurrently is safe (each stale-job recovery is fenced) but is redundant work; harmless at this scale.
- Shutdown is two-tiered: **SIGTERM is graceful** (workers stop picking up new jobs and let whatever they're running finish), **SIGKILL is not** (it can cut a job off mid-execution). Either way, the job's command runs in its own OS process group, and the reaper kills that group (not just the DB row) once it notices the orphaned `processing` row on a later pass — so a forced shutdown doesn't leave the command running unsupervised in the background. That cleanup only happens once a `queuectl worker start` process is running its reaper again, so a job killed by `worker stop`'s SIGKILL escalation can stay orphaned until the next `worker start`, not instantly.
- `lock-timeout-seconds` needs to comfortably exceed how long your jobs actually take to run — if a legitimate job runs past that window, the reaper can't tell it apart from a genuinely stuck one and will recover it out from under the worker still running it. Workers do renew their lease periodically while a command runs to push this out, but very long-running jobs would eventually want explicit timeout handling instead. The default (20s, with a reaper sweep every 10s) is tuned to keep worst-case crash recovery under the assignment's 60-second bound — see [DECISIONS.md](DECISIONS.md).
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

Stop every supervisor (both Terminal 1's and Terminal 3's) with a single `worker stop`, then confirm a completed job survives the restart:

```bash
./queuectl worker stop
./queuectl status
```

**Terminal 1** — start them back up:

```bash
./queuectl worker start --count 3
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