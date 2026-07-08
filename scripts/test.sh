#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

rm -f queuectl .queuectl/worker.pid
go build -o queuectl ./cmd/queuectl

TMP_DIR="$(mktemp -d)"
DB_PATH="$TMP_DIR/queuectl.db"
WORKER_LOG="$TMP_DIR/worker.log"
MULTI_OUT="$TMP_DIR/multi.out"

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

run config set backoff-base 1
run config set poll-interval-ms 100
run config set stop-timeout-seconds 5

run worker start --count 3 >"$WORKER_LOG" 2>&1 &
WORKER_SHELL_PID=$!

for _ in {1..50}; do
  if run status | awk '/active:/ {print $2}' | grep -Fxq "3"; then
    break
  fi
  sleep 0.1
done
if ! run status | awk '/active:/ {print $2}' | grep -Fxq "3"; then
  echo "workers did not become active" >&2
  cat "$WORKER_LOG" >&2 || true
  exit 1
fi

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

run status
run worker stop
wait "$WORKER_SHELL_PID"

if ! run list --state completed | awk 'NR > 1 {print $1}' | grep -Fxq success; then
  echo "completed job was not persisted after worker restart boundary" >&2
  exit 1
fi

run dlq retry fail-dlq
wait_for_state fail-dlq pending 2

echo "scripts/test.sh passed"
