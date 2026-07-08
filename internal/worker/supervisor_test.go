package worker

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadPID(t *testing.T) {
	dir := t.TempDir()

	_, err := readPID(filepath.Join(dir, "missing.pid"))
	require.True(t, errors.Is(err, os.ErrNotExist))

	invalid := filepath.Join(dir, "invalid.pid")
	require.NoError(t, os.WriteFile(invalid, []byte("not-a-number"), 0o644))
	_, err = readPID(invalid)
	require.Error(t, err)

	zero := filepath.Join(dir, "zero.pid")
	require.NoError(t, os.WriteFile(zero, []byte("0"), 0o644))
	_, err = readPID(zero)
	require.Error(t, err)

	negative := filepath.Join(dir, "negative.pid")
	require.NoError(t, os.WriteFile(negative, []byte("-5"), 0o644))
	_, err = readPID(negative)
	require.Error(t, err)

	valid := filepath.Join(dir, "valid.pid")
	require.NoError(t, os.WriteFile(valid, []byte(" 1234 \n"), 0o644))
	pid, err := readPID(valid)
	require.NoError(t, err)
	require.Equal(t, 1234, pid)
}

func TestIsProcessAlive(t *testing.T) {
	require.True(t, isProcessAlive(os.Getpid()))

	exitedPID := spawnAndWaitExited(t)
	require.False(t, isProcessAlive(exitedPID))
}

func TestEnsureNoLiveSupervisorNoPIDFile(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "worker.pid")
	require.NoError(t, EnsureNoLiveSupervisor(pidPath))
}

func TestEnsureNoLiveSupervisorRemovesStalePIDFile(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "worker.pid")
	exitedPID := spawnAndWaitExited(t)
	require.NoError(t, os.WriteFile(pidPath, []byte(strconv.Itoa(exitedPID)), 0o644))

	require.NoError(t, EnsureNoLiveSupervisor(pidPath))
	_, err := os.Stat(pidPath)
	require.True(t, os.IsNotExist(err), "stale PID file should have been removed")
}

func TestEnsureNoLiveSupervisorRefusesLiveNonQueueCTLProcess(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "worker.pid")
	require.NoError(t, os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o644))

	err := EnsureNoLiveSupervisor(pidPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-queuectl process")
}

func TestClaimSupervisorPIDFileConcurrentOnlyOneWins(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "worker.pid")

	const racers = 20
	var wg sync.WaitGroup
	var successes int32
	var mu sync.Mutex
	var errs []error

	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := ClaimSupervisorPIDFile(pidPath)
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
				return
			}
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	require.EqualValues(t, 1, successes, "exactly one concurrent claim should win the PID file")
	require.Len(t, errs, racers-1, "every other claim should fail rather than silently succeed")
	for _, err := range errs {
		// All racers share this test process's own PID, so the losing
		// side's verification won't recognize it as "queuectl worker
		// start" and will report a non-queuectl refusal rather than
		// "already running" - either way, the point proven here is that
		// it refuses instead of silently claiming the file too.
		require.Contains(t, err.Error(), pidPath)
	}
}

func TestClaimSupervisorPIDFileRemovesStaleAndClaims(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "worker.pid")
	exitedPID := spawnAndWaitExited(t)
	require.NoError(t, os.WriteFile(pidPath, []byte(strconv.Itoa(exitedPID)), 0o644))

	require.NoError(t, ClaimSupervisorPIDFile(pidPath))

	raw, err := os.ReadFile(pidPath)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(os.Getpid()), string(raw))
}

func TestStopSupervisorMissingPIDFile(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "worker.pid")
	err := StopSupervisor(pidPath, time.Second, &bytes.Buffer{}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

func TestStopSupervisorRemovesStalePIDFile(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "worker.pid")
	exitedPID := spawnAndWaitExited(t)
	require.NoError(t, os.WriteFile(pidPath, []byte(strconv.Itoa(exitedPID)), 0o644))

	err := StopSupervisor(pidPath, time.Second, &bytes.Buffer{}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not running")
	_, statErr := os.Stat(pidPath)
	require.True(t, os.IsNotExist(statErr), "stale PID file should have been removed")
}

func TestStopSupervisorRefusesLiveNonQueueCTLProcessEvenWithForce(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "worker.pid")
	require.NoError(t, os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o644))

	// A definitive "this is not queuectl" verdict must refuse to signal
	// the process regardless of --force; force only relaxes the case
	// where verification itself could not be performed.
	err := StopSupervisor(pidPath, time.Second, &bytes.Buffer{}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-queuectl process")
}

// spawnAndWaitExited starts and waits for a short-lived child process,
// returning its PID after it has exited so isProcessAlive/isQueueCTLSupervisor
// checks against that PID observe a definitely-dead process.
func spawnAndWaitExited(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	require.NoError(t, cmd.Wait())
	return pid
}
