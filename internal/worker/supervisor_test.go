package worker

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsProcessAlive(t *testing.T) {
	require.True(t, isProcessAlive(os.Getpid()))

	exitedPID := spawnAndWaitExited(t)
	require.False(t, isProcessAlive(exitedPID))
}

func TestParsePIDFilename(t *testing.T) {
	pid, ok := parsePIDFilename("1234.pid")
	require.True(t, ok)
	require.Equal(t, 1234, pid)

	_, ok = parsePIDFilename("not-a-pid.pid")
	require.False(t, ok)

	_, ok = parsePIDFilename("1234.txt")
	require.False(t, ok)

	_, ok = parsePIDFilename("0.pid")
	require.False(t, ok)

	_, ok = parsePIDFilename("-5.pid")
	require.False(t, ok)
}

func TestRegisterSupervisorCreatesOwnPIDFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "workers")
	path, err := RegisterSupervisor(dir)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, strconv.Itoa(os.Getpid())+".pid"), path)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(os.Getpid()), string(raw))
}

// TestRegisterSupervisorCoexistsWithOtherPIDFiles is the core of the
// multi-process fix: registering must never refuse just because other
// supervisors' PID files already exist in the same directory - that's what
// lets "queuectl worker start" run from more than one terminal at once
// against the same database, instead of the second invocation being
// refused the way a single shared PID file would refuse it.
func TestRegisterSupervisorCoexistsWithOtherPIDFiles(t *testing.T) {
	dir := t.TempDir()
	otherPath := filepath.Join(dir, "999999.pid")
	require.NoError(t, os.WriteFile(otherPath, []byte("999999"), 0o644))

	path, err := RegisterSupervisor(dir)
	require.NoError(t, err)
	require.NotEqual(t, otherPath, path)

	_, err = os.Stat(otherPath)
	require.NoError(t, err, "an unrelated supervisor's PID file must be left alone")
	_, err = os.Stat(path)
	require.NoError(t, err, "this process's own PID file must also exist")
}

func TestRegisterSupervisorReclaimsStaleFileForSamePID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, strconv.Itoa(os.Getpid())+".pid")
	require.NoError(t, os.WriteFile(path, []byte("garbage-from-a-recycled-pid"), 0o644))

	got, err := RegisterSupervisor(dir)
	require.NoError(t, err)
	require.Equal(t, path, got)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(os.Getpid()), string(raw))
}

// Having no supervisors to stop is a satisfied request, not a failure: a
// teardown script running under "set -e" must not abort just because the
// workers it is stopping had already exited. Each of the four "nothing to
// stop" shapes below must therefore return nil and say so on out.
func TestStopAllSupervisorsMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	var out bytes.Buffer
	require.NoError(t, StopAllSupervisors(dir, time.Second, &out, false))
	require.Contains(t, out.String(), "nothing to stop")
}

func TestStopAllSupervisorsEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	require.NoError(t, StopAllSupervisors(dir, time.Second, &out, false))
	require.Contains(t, out.String(), "no worker supervisors found")
}

func TestStopAllSupervisorsRemovesStalePIDFiles(t *testing.T) {
	dir := t.TempDir()
	exitedPID := spawnAndWaitExited(t)
	path := filepath.Join(dir, strconv.Itoa(exitedPID)+".pid")
	require.NoError(t, os.WriteFile(path, []byte(strconv.Itoa(exitedPID)), 0o644))

	var out bytes.Buffer
	require.NoError(t, StopAllSupervisors(dir, time.Second, &out, false))
	require.Contains(t, out.String(), "no live queuectl worker supervisors")

	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "stale PID file should have been removed")
}

func TestStopAllSupervisorsSkipsLiveNonQueueCTLProcessEvenWithForce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, strconv.Itoa(os.Getpid())+".pid")
	require.NoError(t, os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o644))

	// A definitive "this is not queuectl" verdict must refuse to signal
	// the process regardless of --force; force only relaxes the case
	// where verification itself could not be performed.
	var out bytes.Buffer
	require.NoError(t, StopAllSupervisors(dir, time.Second, &out, true))
	require.Contains(t, out.String(), "no live queuectl worker supervisors")

	_, statErr := os.Stat(path)
	require.NoError(t, statErr, "a live non-queuectl process's PID file must not be touched")
}

func TestStopAllSupervisorsIgnoresUnrelatedFilenames(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.txt"), []byte("not a pid file"), 0o644))

	var out bytes.Buffer
	require.NoError(t, StopAllSupervisors(dir, time.Second, &out, false))
	require.Contains(t, out.String(), "no worker supervisors found")
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
