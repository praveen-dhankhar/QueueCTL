package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	DefaultDBPath = ".queuectl/queuectl.db"

	// PIDDirName is the subdirectory (under .queuectl) that holds one PID
	// file per running "queuectl worker start" process, letting multiple
	// supervisors run concurrently against the same database (see
	// WorkerPIDDir). Each file is named after its own process's PID.
	PIDDirName = "workers"

	KeyMaxRetries         = "max-retries"
	KeyBackoffBase        = "backoff-base"
	KeyPollIntervalMS     = "poll-interval-ms"
	KeyLockTimeoutSeconds = "lock-timeout-seconds"
	KeyWorkerStaleSeconds = "worker-stale-seconds"
	KeyStopTimeoutSeconds = "stop-timeout-seconds"

	// KeyJobTimeoutSeconds is the default wall-clock limit applied to a job
	// whose enqueue JSON does not carry its own "timeout_seconds". Zero - the
	// default - means no timeout, so an existing queue behaves exactly as it
	// did before this key existed. Like max-retries, it is read once at
	// enqueue time and baked into the job row, so changing it does not
	// retroactively bound jobs that are already queued.
	KeyJobTimeoutSeconds = "job-timeout-seconds"

	// HeartbeatInterval is how often a running worker refreshes its
	// workers.last_heartbeat row (internal/worker.Pool uses this constant
	// directly rather than defining its own, so it can't drift out of sync
	// with the worker-stale-seconds minimum derived from it below).
	HeartbeatInterval = 5 * time.Second

	// ReaperInterval is how often the crash-recovery reaper sweeps for
	// stale processing jobs (internal/worker.RunReaperLoop uses this
	// constant directly). Combined with the default lock-timeout-seconds
	// below, this sets the worst-case crash-recovery delay: a job can sit
	// stale for up to lock-timeout-seconds before the reaper even
	// considers it, plus up to one more ReaperInterval before the next
	// sweep actually runs. With the defaults below (20s + 10s = 30s) that
	// stays comfortably under the assignment's 60-second requirement, with
	// margin for scheduling jitter on a loaded machine. See DECISIONS.md.
	ReaperInterval = 10 * time.Second
)

// minWorkerStaleSeconds requires worker-stale-seconds to cover at least two
// heartbeat intervals. status counts a worker active if its heartbeat is
// newer than worker-stale-seconds ago; if that were allowed to be smaller
// than (or close to) HeartbeatInterval, a perfectly healthy worker would
// periodically - and incorrectly - be reported inactive in the gap between
// two ordinary heartbeats, purely from configuring this value too small
// rather than from any real staleness.
var minWorkerStaleSeconds = int(2 * HeartbeatInterval / time.Second)

var Defaults = map[string]int{
	KeyMaxRetries:         3,
	KeyBackoffBase:        2,
	KeyPollIntervalMS:     500,
	KeyLockTimeoutSeconds: 20,
	KeyWorkerStaleSeconds: 15,
	KeyStopTimeoutSeconds: 30,
	KeyJobTimeoutSeconds:  0,
}

// ResolveDBPath picks the database path with precedence --db flag >
// QUEUECTL_DB_PATH env var > DefaultDBPath.
func ResolveDBPath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if envValue := os.Getenv("QUEUECTL_DB_PATH"); envValue != "" {
		return envValue
	}
	return DefaultDBPath
}

// EnsureParentDir creates path's parent directory (and any missing
// ancestors) if it doesn't already exist.
func EnsureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

// WorkerPIDDir derives the PID directory for a given database path: the
// well-known ".queuectl/workers" directory for the default database, or a
// sibling of the database file derived from a hash of its absolute path
// otherwise. This lets multiple worker supervisors run concurrently against
// different --db targets without colliding on one another's PID files, while
// every supervisor sharing a database path registers into the same directory
// (see worker.RegisterSupervisor / worker.StopAllSupervisors) so any number
// of "queuectl worker start" processes - including ones started from separate
// terminals - can coexist and all be discovered by "queuectl worker stop".
//
// The returned path is anchored to the database file's own directory, not to
// the process's working directory. That matters because "worker start" and
// "worker stop" are separate invocations that need not share a working
// directory: with a CWD-relative PID directory, "queuectl worker stop --db
// /abs/path/queue.db" run from anywhere other than where the supervisor was
// started would look in an empty (or wrong) directory and report no workers
// to stop, even though the same --db target was named in both commands.
func WorkerPIDDir(dbPath string) string {
	absDB, err := filepath.Abs(dbPath)
	if err != nil {
		absDB = dbPath
	}
	dbDir := filepath.Dir(absDB)

	absDefault, err := filepath.Abs(DefaultDBPath)
	if err == nil && absDB == absDefault {
		return filepath.Join(dbDir, PIDDirName)
	}

	sum := sha256.Sum256([]byte(absDB))
	return filepath.Join(dbDir, PIDDirName+"-"+hex.EncodeToString(sum[:6]))
}

// ValidateConfigValue parses raw as an integer and checks it against the
// per-key minimum, returning an error for unknown keys, non-integer input,
// or values below the minimum.
func ValidateConfigValue(key string, raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}

	// job-timeout-seconds is the one key whose minimum is 0, because 0 is a
	// meaningful value for it (no timeout) rather than a misconfiguration.
	minimum, ok := map[string]int{
		KeyMaxRetries:         1,
		KeyBackoffBase:        1,
		KeyPollIntervalMS:     50,
		KeyLockTimeoutSeconds: 1,
		KeyWorkerStaleSeconds: minWorkerStaleSeconds,
		KeyStopTimeoutSeconds: 1,
		KeyJobTimeoutSeconds:  0,
	}[key]
	if !ok {
		return 0, fmt.Errorf("unknown config key %q", key)
	}
	if value < minimum {
		return 0, fmt.Errorf("%s must be >= %d", key, minimum)
	}
	return value, nil
}
