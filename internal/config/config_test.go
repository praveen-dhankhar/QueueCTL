package config_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appconfig "queuectl/internal/config"
)

func TestValidateConfigValueAcceptsBoundaryMinimums(t *testing.T) {
	minimums := map[string]int{
		appconfig.KeyMaxRetries:         1,
		appconfig.KeyBackoffBase:        1,
		appconfig.KeyPollIntervalMS:     50,
		appconfig.KeyLockTimeoutSeconds: 1,
		appconfig.KeyWorkerStaleSeconds: int(2 * appconfig.HeartbeatInterval / time.Second),
		appconfig.KeyStopTimeoutSeconds: 1,
	}
	for key, minimum := range minimums {
		value, err := appconfig.ValidateConfigValue(key, strconv.Itoa(minimum))
		require.NoError(t, err, "key %s at its minimum should be valid", key)
		require.Equal(t, minimum, value)
	}
}

func TestValidateConfigValueRejectsBelowMinimum(t *testing.T) {
	_, err := appconfig.ValidateConfigValue(appconfig.KeyPollIntervalMS, "49")
	require.Error(t, err)
	require.Contains(t, err.Error(), "poll-interval-ms")

	_, err = appconfig.ValidateConfigValue(appconfig.KeyMaxRetries, "0")
	require.Error(t, err)

	_, err = appconfig.ValidateConfigValue(appconfig.KeyLockTimeoutSeconds, "-1")
	require.Error(t, err)
}

// TestValidateConfigValueRejectsWorkerStaleSecondsBelowHeartbeatMargin
// guards the fix for a real status-flicker bug: worker-stale-seconds used
// to accept any value >= 1, but the heartbeat that keeps a worker looking
// "active" only fires every appconfig.HeartbeatInterval. A worker-stale-
// seconds smaller than that would make `queuectl status` intermittently
// report a perfectly healthy worker as inactive, purely from the gap
// between two ordinary heartbeats - not from any actual staleness.
func TestValidateConfigValueRejectsWorkerStaleSecondsBelowHeartbeatMargin(t *testing.T) {
	tooSmall := int(2*appconfig.HeartbeatInterval/time.Second) - 1
	_, err := appconfig.ValidateConfigValue(appconfig.KeyWorkerStaleSeconds, strconv.Itoa(tooSmall))
	require.Error(t, err)
	require.Contains(t, err.Error(), "worker-stale-seconds")
}

func TestValidateConfigValueRejectsNonInteger(t *testing.T) {
	_, err := appconfig.ValidateConfigValue(appconfig.KeyMaxRetries, "not-a-number")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be an integer")

	_, err = appconfig.ValidateConfigValue(appconfig.KeyMaxRetries, "3.5")
	require.Error(t, err)
}

func TestValidateConfigValueRejectsUnknownKey(t *testing.T) {
	_, err := appconfig.ValidateConfigValue("not-a-real-key", "5")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown config key")
}

func TestValidateConfigValueAcceptsAboveMinimum(t *testing.T) {
	value, err := appconfig.ValidateConfigValue(appconfig.KeyBackoffBase, "10")
	require.NoError(t, err)
	require.Equal(t, 10, value)
}

// TestWorkerPIDDirDoesNotDependOnWorkingDirectory guards the fix for a real
// "worker stop finds nothing to stop" bug: the PID directory used to be a
// CWD-relative path, so "worker start --db /abs/queue.db" in one directory
// and "worker stop --db /abs/queue.db" in another - same database, same
// flag - resolved to two different PID directories, and stop reported no
// running workers while the supervisor kept running.
func TestWorkerPIDDirDoesNotDependOnWorkingDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	fromHere := appconfig.WorkerPIDDir(dbPath)

	// Same --db target, resolved from a different working directory.
	chdir(t, t.TempDir())
	fromElsewhere := appconfig.WorkerPIDDir(dbPath)

	require.Equal(t, fromHere, fromElsewhere, "the same --db target must map to one PID directory from any working directory")
	require.True(t, filepath.IsAbs(fromElsewhere), "PID directory must be absolute, not relative to the caller's CWD")
	require.Equal(t, filepath.Dir(dbPath), filepath.Dir(fromElsewhere), "PID directory must live alongside the database file it belongs to")
}

// TestWorkerPIDDirSeparatesDistinctDatabases keeps the property that made the
// directory hashed in the first place: two databases must never share a PID
// directory, or "worker stop --db a.db" would signal b.db's supervisors too.
func TestWorkerPIDDirSeparatesDistinctDatabases(t *testing.T) {
	dir := t.TempDir()
	require.NotEqual(t,
		appconfig.WorkerPIDDir(filepath.Join(dir, "a.db")),
		appconfig.WorkerPIDDir(filepath.Join(dir, "b.db")),
	)
}

// TestWorkerPIDDirResolvesRelativeAndAbsoluteAlike covers the exact shape of
// the original bug report: the same database named relatively at start and
// absolutely at stop.
func TestWorkerPIDDirResolvesRelativeAndAbsoluteAlike(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)

	// t.TempDir() can hand back a symlinked path (/var/... -> /private/var/...
	// on macOS); resolve it so this compares path derivation, not symlinks.
	resolved, err := filepath.EvalSymlinks(dir)
	require.NoError(t, err)

	require.Equal(t,
		appconfig.WorkerPIDDir(filepath.Join(resolved, "queue.db")),
		appconfig.WorkerPIDDir("queue.db"),
	)
}

func chdir(t *testing.T, dir string) {
	t.Helper()
	original, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(original))
	})
}
