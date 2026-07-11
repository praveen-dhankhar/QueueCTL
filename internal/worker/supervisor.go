package worker

import (
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// RegisterSupervisor claims this process's own PID file inside pidDir,
// creating the directory if needed, and returns the file's path for the
// caller to remove on graceful shutdown.
//
// Unlike a single shared PID file, this never refuses a second supervisor:
// any number of "queuectl worker start" processes - including ones started
// from separate terminals - can each register their own file in the same
// directory, which is what lets "queuectl worker stop" (see
// StopAllSupervisors) discover and signal every one of them. The filename
// is the calling process's own PID, which the OS guarantees is not held by
// any other currently-running process, so two live supervisors can never
// contend for the same path. If a file already exists at that exact path,
// it can only be a leftover from an earlier, now-dead process that
// happened to reuse this PID (the OS would not have handed out a PID that
// was already in use), so it is safe to remove and reclaim without an
// aliveness check.
func RegisterSupervisor(pidDir string) (string, error) {
	if err := os.MkdirAll(pidDir, 0o755); err != nil {
		return "", fmt.Errorf("create worker pid directory: %w", err)
	}
	path := pidFilePath(pidDir, os.Getpid())
	for attempt := 0; attempt < 2; attempt++ {
		err := writePIDFileExclusive(path, os.Getpid())
		if err == nil {
			return path, nil
		}
		if !os.IsExist(err) {
			return "", fmt.Errorf("write worker pid file: %w", err)
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("remove stale worker pid file: %w", err)
		}
	}
	return "", fmt.Errorf("worker pid file %s could not be claimed", path)
}

func pidFilePath(pidDir string, pid int) string {
	return filepath.Join(pidDir, strconv.Itoa(pid)+".pid")
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

// StopAllSupervisors discovers every "queuectl worker start" process
// registered under pidDir (one PID file per supervisor, written by
// RegisterSupervisor), sends each a graceful SIGTERM, and waits up to
// timeout total for all of them to exit before escalating any stragglers to
// SIGKILL. Every discovered PID is reported to out as it is handled, so a
// caller running "queuectl worker stop" from a different terminal than any
// of the workers can see exactly what was signaled.
//
// A PID file whose process is no longer alive is treated as a stale
// leftover (from a graceful exit that raced this call, or a prior SIGKILL
// escalation that couldn't clean up after itself) and removed rather than
// signaled. A PID file whose process is alive but does not look like
// queuectl is left untouched and never signaled, with or without force;
// see isQueueCTLSupervisor for what "looks like queuectl" means and why
// verification can fail outright in sandboxed environments.
//
// It returns an error only if there was nothing to stop at all: no PID
// directory, no PID files in it, or every PID file present turned out to be
// stale or unverifiable/non-queuectl. That mirrors the single-supervisor
// version's behavior of failing when there's no running worker to stop,
// while still succeeding as long as at least one real supervisor was
// signaled.
func StopAllSupervisors(pidDir string, timeout time.Duration, out io.Writer, force bool) error {
	entries, err := os.ReadDir(pidDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no worker supervisors running (pid directory %s does not exist)", pidDir)
		}
		return fmt.Errorf("read worker pid directory %s: %w", pidDir, err)
	}

	var signaled []int
	found := false
	for _, entry := range entries {
		pid, ok := parsePIDFilename(entry.Name())
		if !ok {
			continue
		}
		found = true
		path := filepath.Join(pidDir, entry.Name())

		if !isProcessAlive(pid) {
			_ = os.Remove(path)
			continue
		}

		ok, command, err := isQueueCTLSupervisor(pid)
		if err != nil {
			if !force {
				if _, printErr := fmt.Fprintf(out, "warning: could not verify PID %d (%v); skipping (retry with --force to signal anyway)\n", pid, err); printErr != nil {
					return printErr
				}
				continue
			}
			if _, printErr := fmt.Fprintf(out, "warning: could not verify PID %d (%v); proceeding because --force was set\n", pid, err); printErr != nil {
				return printErr
			}
			ok = true
		}
		if !ok {
			if _, printErr := fmt.Fprintf(out, "skipping live non-queuectl process PID %d (%q)\n", pid, command); printErr != nil {
				return printErr
			}
			continue
		}

		if _, printErr := fmt.Fprintf(out, "stopping worker supervisor PID %d\n", pid); printErr != nil {
			return printErr
		}
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
			if _, printErr := fmt.Fprintf(out, "warning: failed to signal PID %d: %v\n", pid, err); printErr != nil {
				return printErr
			}
			continue
		}
		signaled = append(signaled, pid)
	}

	if !found {
		return fmt.Errorf("no worker supervisors found in %s", pidDir)
	}
	if len(signaled) == 0 {
		return fmt.Errorf("no live queuectl worker supervisors found to stop in %s", pidDir)
	}

	deadline := time.Now().Add(timeout)
	remaining := signaled
	for time.Now().Before(deadline) && len(remaining) > 0 {
		remaining = filterAlive(remaining)
		if len(remaining) == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	remaining = filterAlive(remaining)

	for _, pid := range remaining {
		if _, printErr := fmt.Fprintf(out, "worker supervisor PID %d did not exit within %s; sending SIGKILL\n", pid, timeout); printErr != nil {
			return printErr
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			if _, printErr := fmt.Fprintf(out, "warning: failed to SIGKILL PID %d: %v\n", pid, err); printErr != nil {
				return printErr
			}
		}
	}
	return nil
}

func filterAlive(pids []int) []int {
	var alive []int
	for _, pid := range pids {
		if isProcessAlive(pid) {
			alive = append(alive, pid)
		}
	}
	return alive
}

// parsePIDFilename extracts the PID from a "<pid>.pid" filename as written
// by RegisterSupervisor, ignoring anything else that might be in the
// directory.
func parsePIDFilename(name string) (int, bool) {
	if !strings.HasSuffix(name, ".pid") {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSuffix(name, ".pid"))
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// isProcessAlive reports whether pid identifies a running process, using
// signal 0 which the OS treats as an existence check without actually
// signaling the process.
func isProcessAlive(pid int) bool {
	err := syscall.Kill(pid, syscall.Signal(0))
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
// than guessing; see StopAllSupervisors' force parameter for how callers
// can choose to proceed anyway.
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
