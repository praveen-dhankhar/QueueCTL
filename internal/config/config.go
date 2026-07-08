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
	PIDFilePath   = ".queuectl/worker.pid"

	KeyMaxRetries         = "max-retries"
	KeyBackoffBase        = "backoff-base"
	KeyPollIntervalMS     = "poll-interval-ms"
	KeyLockTimeoutSeconds = "lock-timeout-seconds"
	KeyWorkerStaleSeconds = "worker-stale-seconds"
	KeyStopTimeoutSeconds = "stop-timeout-seconds"

	// HeartbeatInterval is how often a running worker refreshes its
	// workers.last_heartbeat row (internal/worker.Pool uses this constant
	// directly rather than defining its own, so it can't drift out of sync
	// with the worker-stale-seconds minimum derived from it below).
	HeartbeatInterval = 5 * time.Second
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
	KeyLockTimeoutSeconds: 120,
	KeyWorkerStaleSeconds: 15,
	KeyStopTimeoutSeconds: 30,
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

// EnsureQueueDir creates the .queuectl directory used for the default
// database and worker PID files.
func EnsureQueueDir() error {
	return os.MkdirAll(".queuectl", 0o755)
}

// WorkerPIDPath derives the PID file path for a given database path: the
// well-known PIDFilePath for the default database, or a path derived from
// a hash of dbPath's absolute form otherwise. This lets multiple worker
// supervisors run concurrently against different --db targets without
// colliding on one PID file.
func WorkerPIDPath(dbPath string) string {
	absDB, err := filepath.Abs(dbPath)
	if err != nil {
		absDB = dbPath
	}
	absDefault, err := filepath.Abs(DefaultDBPath)
	if err == nil && absDB == absDefault {
		return PIDFilePath
	}

	sum := sha256.Sum256([]byte(absDB))
	return filepath.Join(".queuectl", "worker-"+hex.EncodeToString(sum[:6])+".pid")
}

// ValidateConfigValue parses raw as an integer and checks it against the
// per-key minimum, returning an error for unknown keys, non-integer input,
// or values below the minimum.
func ValidateConfigValue(key string, raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}

	minimum, ok := map[string]int{
		KeyMaxRetries:         1,
		KeyBackoffBase:        1,
		KeyPollIntervalMS:     50,
		KeyLockTimeoutSeconds: 1,
		KeyWorkerStaleSeconds: minWorkerStaleSeconds,
		KeyStopTimeoutSeconds: 1,
	}[key]
	if !ok {
		return 0, fmt.Errorf("unknown config key %q", key)
	}
	if value < minimum {
		return 0, fmt.Errorf("%s must be >= %d", key, minimum)
	}
	return value, nil
}
