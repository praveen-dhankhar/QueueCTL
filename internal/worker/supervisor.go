package worker

import (
	"errors"
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// EnsureNoLiveSupervisor guards against starting a second worker
// supervisor over the same database. It reads the PID file at pidPath and:
//   - returns nil if no PID file exists;
//   - returns nil (after deleting the file) if the recorded PID is not
//     alive, i.e. the previous supervisor crashed without cleaning up;
//   - returns an error if the PID is alive and verified (via
//     processCommand) to be a "queuectl worker start" process, since that
//     is an already-running supervisor;
//   - returns an error if the PID is alive but does not look like
//     queuectl, since signaling or overwriting that PID file would be
//     unsafe;
//   - returns an error if the process command could not be verified at
//     all (e.g. /proc and ps are both unavailable), since it's safer to
//     block a start than risk running two supervisors against one
//     database.
func EnsureNoLiveSupervisor(pidPath string) error {
	pid, err := readPID(pidPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if isProcessAlive(pid) {
		ok, command, err := isQueueCTLSupervisor(pid)
		if err != nil {
			return fmt.Errorf("worker PID file %s points to live PID %d, but queuectl could not verify the process: %w", pidPath, pid, err)
		}
		if !ok {
			return fmt.Errorf("worker PID file %s points to live non-queuectl process PID %d (%q); remove the stale PID file manually", pidPath, pid, command)
		}
		return fmt.Errorf("worker supervisor already running with PID %d", pid)
	}
	if err := os.Remove(pidPath); err != nil {
		return fmt.Errorf("remove stale worker PID file: %w", err)
	}
	return nil
}

// ClaimSupervisorPIDFile atomically claims pidPath for the calling process.
// Callers must use this instead of calling EnsureNoLiveSupervisor followed
// by a separate write: that two-step "check, then act" pattern lets two
// "queuectl worker start" processes launched at nearly the same time both
// pass the check before either has written the file, so both end up
// believing they are the sole supervisor. Here, the exclusive create
// (O_CREATE|O_EXCL) is itself the arbiter - the OS guarantees only one
// caller can win it - and EnsureNoLiveSupervisor's checks are only
// consulted on the losing side, to produce the right error (or to clear a
// stale file left by a crashed supervisor and retry the claim once).
func ClaimSupervisorPIDFile(pidPath string) error {
	for attempt := 0; attempt < 2; attempt++ {
		err := writePIDFileExclusive(pidPath, os.Getpid())
		if err == nil {
			return nil
		}
		if !os.IsExist(err) {
			return fmt.Errorf("write worker PID file: %w", err)
		}
		if err := EnsureNoLiveSupervisor(pidPath); err != nil {
			return err
		}
	}
	return fmt.Errorf("worker PID file %s could not be claimed", pidPath)
}

// writePIDFileExclusive creates pidPath only if it does not already exist,
// so two concurrent callers can never both believe they created it.
func writePIDFileExclusive(pidPath string, pid int) error {
	f, err := os.OpenFile(pidPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(strconv.Itoa(pid))
	return err
}

// StopSupervisor reads the PID file at pidPath, verifies it points at a
// live "queuectl worker start" process, and sends SIGTERM. It waits up to
// timeout for the process to exit before escalating to SIGKILL.
//
// Process verification (via processCommand) shells out to `ps` on
// platforms without /proc (e.g. macOS). In sandboxed environments where
// exec is blocked, verification itself fails with an error rather than a
// definitive yes/no, which would otherwise leave `worker stop` unable to
// stop a supervisor it started. If force is true, a verification failure
// is downgraded to a warning on out and the stop proceeds; a definitive
// "this PID is not queuectl" result still refuses to signal the process
// regardless of force.
func StopSupervisor(pidPath string, timeout time.Duration, out io.Writer, force bool) error {
	pid, err := readPID(pidPath)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("worker PID file %s does not exist", pidPath)
	}
	if err != nil {
		return err
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find worker supervisor PID %d: %w", pid, err)
	}
	if !isProcessAlive(pid) {
		_ = os.Remove(pidPath)
		return fmt.Errorf("worker supervisor PID %d is not running; removed stale PID file", pid)
	}
	ok, command, err := isQueueCTLSupervisor(pid)
	if err != nil {
		if !force {
			return fmt.Errorf("worker PID file %s points to live PID %d, but queuectl could not verify the process: %w (retry with --force to skip verification)", pidPath, pid, err)
		}
		if _, printErr := fmt.Fprintf(out, "warning: could not verify process PID %d (%v); proceeding because --force was set\n", pid, err); printErr != nil {
			return printErr
		}
		ok = true
	}
	if !ok {
		return fmt.Errorf("worker PID file %s points to live non-queuectl process PID %d (%q); refusing to signal it", pidPath, pid, command)
	}

	if _, err := fmt.Fprintf(out, "stopping worker supervisor PID %d\n", pid); err != nil {
		return err
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("send SIGTERM to worker supervisor PID %d: %w", pid, err)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !isProcessAlive(pid) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	if _, err := fmt.Fprintf(out, "worker supervisor PID %d did not exit within %s; sending SIGKILL\n", pid, timeout); err != nil {
		return err
	}
	if err := process.Signal(syscall.SIGKILL); err != nil {
		return fmt.Errorf("send SIGKILL to worker supervisor PID %d: %w", pid, err)
	}
	return nil
}

// readPID parses the PID file at pidPath, returning os.ErrNotExist if it
// is missing so callers can distinguish "no supervisor" from a read error.
func readPID(pidPath string) (int, error) {
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, fmt.Errorf("invalid worker PID file %s: %w", pidPath, err)
	}
	if pid <= 0 {
		return 0, fmt.Errorf("invalid worker PID %d in %s", pid, pidPath)
	}
	return pid, nil
}

// isProcessAlive reports whether pid identifies a running process, using
// signal 0 which the OS treats as an existence check without actually
// signaling the process.
func isProcessAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// isQueueCTLSupervisor reports whether pid's command line looks like a
// "queuectl worker start" invocation, alongside the raw command for
// diagnostics.
func isQueueCTLSupervisor(pid int) (bool, string, error) {
	command, err := processCommand(pid)
	if err != nil {
		return false, "", err
	}
	return strings.Contains(command, "queuectl") && strings.Contains(command, "worker start"), command, nil
}

// processCommand returns pid's full command line, read from /proc on Linux
// or via `ps` elsewhere (e.g. macOS, which has no /proc). In sandboxed
// environments where both are unavailable, this returns an error rather
// than guessing; see StopSupervisor's force parameter for how callers can
// choose to proceed anyway.
func processCommand(pid int) (string, error) {
	if raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil && len(raw) > 0 {
		command := strings.ReplaceAll(strings.Trim(string(raw), "\x00"), "\x00", " ")
		if strings.TrimSpace(command) != "" {
			return command, nil
		}
	}

	out, err := osexec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if err != nil {
		return "", err
	}
	command := strings.TrimSpace(string(out))
	if command == "" {
		return "", fmt.Errorf("empty process command for PID %d", pid)
	}
	return command, nil
}
