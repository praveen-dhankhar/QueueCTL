#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

rm -f queuectl
rm -rf .queuectl
go build -o queuectl ./cmd/queuectl

TMP_DIR="$(mktemp -d)"
DB_PATH="$TMP_DIR/queuectl.db"
WORKER_LOG="$TMP_DIR/worker.log"
WORKER2_LOG="$TMP_DIR/worker2.log"
CRASH_LOG="$TMP_DIR/crash-worker.log"
RECOVER_LOG="$TMP_DIR/recover-worker.log"
MULTI_OUT="$TMP_DIR/multi.out"
MULTI_PROC_OUT="$TMP_DIR/multi-proc.out"

cleanup() {
  set +e
  ./queuectl --db "$DB_PATH" worker stop >/dev/null 2>&1
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run() {
  ./queuectl --db "$DB_PATH" "$@"
}

wait_for_state() {
  local id="$1"
  local state="$2"
  local timeout_seconds="${3:-20}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if run list --state "$state" | awk 'NR > 1 {print $1}' | grep -Fxq "$id"; then
      return 0
    fi
    sleep 0.2
  done
  echo "timed out waiting for job $id to reach state $state" >&2
  echo "worker log:" >&2
  cat "$WORKER_LOG" >&2 || true
  run status >&2 || true
  return 1
}

wait_for_active_workers() {
  local expected="$1"
  local timeout_seconds="${2:-10}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if run status | awk '/active:/ {print $2}' | grep -Fxq "$expected"; then
      return 0
    fi
    sleep 0.1
  done
  echo "workers did not reach active=$expected" >&2
  run status >&2 || true
  return 1
}

run config set backoff-base 1
run config set poll-interval-ms 100
run config set stop-timeout-seconds 5

run worker start --count 3 >"$WORKER_LOG" 2>&1 &
WORKER_SHELL_PID=$!
wait_for_active_workers 3

run enqueue '{"id":"success","command":"echo hello","max_retries":1}'
wait_for_state success completed

if ! run logs success | grep -q "exit_code=0"; then
  echo "logs command did not report a recorded successful attempt" >&2
  run logs success >&2
  exit 1
fi
if ! run logs success | grep -q "hello"; then
  echo "logs command did not include the job's stdout" >&2
  run logs success >&2
  exit 1
fi

if ! run list --state completed --json | python3 -c "
import json, sys
jobs = json.load(sys.stdin)
assert isinstance(jobs, list), 'list --json must print a JSON array'
assert any(j['id'] == 'success' for j in jobs), 'success job missing from list --json'
assert jobs[0]['created_at'].endswith('Z'), 'created_at must be RFC3339 with a Z suffix'
"; then
  echo "list --json output failed validation" >&2
  run list --state completed --json >&2
  exit 1
fi

run enqueue '{"id":"fail-dlq","command":"exit 7","max_retries":2}'
wait_for_state fail-dlq dead

if [[ "$(run logs fail-dlq | grep -c 'attempt ')" != "2" ]]; then
  echo "logs command did not report both recorded attempts for fail-dlq" >&2
  run logs fail-dlq >&2
  exit 1
fi

run enqueue '{"id":"invalid-command","command":"definitely-not-a-queuectl-command","max_retries":1}'
wait_for_state invalid-command dead

run enqueue '{"id":"after-invalid","command":"echo still-running","max_retries":1}'
wait_for_state after-invalid completed

for i in {1..10}; do
  run enqueue "{\"id\":\"multi-$i\",\"command\":\"printf 'multi-$i\\n' >> '$MULTI_OUT'\",\"max_retries\":1}"
done
for i in {1..10}; do
  wait_for_state "multi-$i" completed
done
total_lines="$(wc -l < "$MULTI_OUT" | tr -d ' ')"
unique_lines="$(sort "$MULTI_OUT" | uniq | wc -l | tr -d ' ')"
if [[ "$total_lines" != "10" || "$unique_lines" != "10" ]]; then
  echo "multiple worker overlap check failed: total=$total_lines unique=$unique_lines" >&2
  cat "$MULTI_OUT" >&2
  exit 1
fi

# A second, independent "queuectl worker start" process against the same
# database, simulating a second terminal - proves workers are real OS
# processes that can be started separately (not just goroutines under one
# supervisor), and that claiming still never duplicates work across them.
run worker start --count 2 >"$WORKER2_LOG" 2>&1 &
WORKER2_SHELL_PID=$!
wait_for_active_workers 5

for i in {1..10}; do
  run enqueue "{\"id\":\"multiproc-$i\",\"command\":\"printf 'multiproc-$i\\n' >> '$MULTI_PROC_OUT'\",\"max_retries\":1}"
done
for i in {1..10}; do
  wait_for_state "multiproc-$i" completed
done
total_lines="$(wc -l < "$MULTI_PROC_OUT" | tr -d ' ')"
unique_lines="$(sort "$MULTI_PROC_OUT" | uniq | wc -l | tr -d ' ')"
if [[ "$total_lines" != "10" || "$unique_lines" != "10" ]]; then
  echo "cross-process overlap check failed: total=$total_lines unique=$unique_lines" >&2
  cat "$MULTI_PROC_OUT" >&2
  exit 1
fi

run status
run worker stop
wait "$WORKER_SHELL_PID"
wait "$WORKER2_SHELL_PID"

if ! run list --state completed | awk 'NR > 1 {print $1}' | grep -Fxq success; then
  echo "completed job was not persisted after worker restart boundary" >&2
  exit 1
fi

run dlq retry fail-dlq
wait_for_state fail-dlq pending 2

# SIGKILL crash recovery, at the shipped defaults - lock-timeout-seconds is
# deliberately NOT overridden here, so this validates the actual worst-case
# recovery time (<60s) the assignment requires with default settings.
#
# Invoked directly (not through the run() function) so that $! is the real
# "queuectl worker start" process PID: backgrounding a shell function call
# instead backgrounds an extra subshell wrapping it, and $! would capture
# that wrapper's PID rather than the queuectl process actually running the
# job - SIGKILLing the wrapper wouldn't touch the real worker at all.
run enqueue '{"id":"crash-job","command":"sleep 2","max_retries":3}'
./queuectl --db "$DB_PATH" worker start --count 1 >"$CRASH_LOG" 2>&1 &
CRASH_WORKER_PID=$!
wait_for_state crash-job processing 10

echo "SIGKILL-ing worker PID $CRASH_WORKER_PID mid-job"
CRASH_START=$SECONDS
kill -9 "$CRASH_WORKER_PID"
wait "$CRASH_WORKER_PID" 2>/dev/null || true

# A fresh worker, as if an operator restarted after the crash, is what
# actually recovers the stale lock and completes the job on retry.
run worker start --count 1 >"$RECOVER_LOG" 2>&1 &
RECOVER_WORKER_PID=$!

wait_for_state crash-job completed 45
CRASH_ELAPSED=$((SECONDS - CRASH_START))
echo "crash recovery completed in ${CRASH_ELAPSED}s"
if (( CRASH_ELAPSED >= 60 )); then
  echo "crash recovery exceeded the 60-second requirement: ${CRASH_ELAPSED}s" >&2
  exit 1
fi

STUCK_COUNT="$(run list --state processing --json | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")"
if [[ "$STUCK_COUNT" != "0" ]]; then
  echo "a job is still stuck in processing after crash recovery" >&2
  run status >&2
  exit 1
fi

run worker stop
wait "$RECOVER_WORKER_PID" 2>/dev/null || true

echo "scripts/test.sh passed"
