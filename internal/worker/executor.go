package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

// MaxOutputBytes caps how much of each of a job's stdout and stderr is kept.
// Job commands are arbitrary shell, so their output is arbitrary in size: an
// uncapped buffer means `yes` (or any command that logs in a loop) grows the
// worker's heap until the OS kills it, and takes every other in-flight job on
// that worker down with it. Anything past the cap is dropped, with a count of
// the dropped bytes appended so the truncation is visible in "queuectl logs"
// rather than silent.
//
// 64KiB is far more than enough to diagnose a failure from the tail of a
// command's output, and small enough that the worst case - every worker
// running a maximally chatty job - stays trivially bounded.
const MaxOutputBytes = 64 << 10

// outputDrainGrace bounds how long cmd.Wait keeps waiting for a command's
// stdout/stderr to reach EOF after the command itself has already exited.
//
// It has to exist because Stdout/Stderr here are buffers, not *os.File: os/exec
// therefore connects the command to an OS pipe and copies from it in a
// goroutine, and cmd.Wait waits for that pipe to close - not for "sh" to exit.
// A command that backgrounds anything ("make &", "cmd | tee &", a daemon that
// forks) hands the write end of that pipe to a process that outlives the shell,
// so the pipe never closes and cmd.Wait blocks *forever*. That wedges the
// worker goroutine permanently, and it is worse than one lost job: the lease
// renewer keeps renewing, so the job's lock never goes stale and the reaper -
// the one mechanism that recovers stuck jobs - is fenced out by design. The
// worker slot is gone until the process restarts.
//
// With a WaitDelay set, Wait force-closes the pipes once the command has exited
// and returns exec.ErrWaitDelay instead of hanging. The grace period only ever
// applies after the command is already dead, and draining a pipe from a dead
// process is instantaneous, so this is generous rather than tight: it is a
// deadlock breaker, not a performance knob. A var, not a const, only so tests
// don't have to sleep for it.
var outputDrainGrace = 5 * time.Second

// ExecutionResult captures the outcome of running one job command: its
// exit code, captured stdout/stderr (each truncated to MaxOutputBytes), and
// start/finish timestamps, all of which are persisted to the job_runs table.
type ExecutionResult struct {
	ExitCode   int
	Stdout     string
	Stderr     string
	StartedAt  time.Time
	FinishedAt time.Time
}

// cappedBuffer is an io.Writer that keeps at most limit bytes and counts the
// rest. It always reports a full write to its caller: os/exec treats a short
// write as an I/O error and would kill the command over it, and dropping
// output is not a reason to fail a job that is otherwise running fine.
type cappedBuffer struct {
	buf     bytes.Buffer
	limit   int
	dropped int64
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if remaining := c.limit - c.buf.Len(); remaining > 0 {
		keep := len(p)
		if keep > remaining {
			keep = remaining
		}
		c.buf.Write(p[:keep])
		c.dropped += int64(len(p) - keep)
	} else {
		c.dropped += int64(len(p))
	}
	return len(p), nil
}

// String returns the captured output, with a truncation note appended if any
// was dropped.
func (c *cappedBuffer) String() string {
	if c.dropped == 0 {
		return c.buf.String()
	}
	return fmt.Sprintf("%s\n[queuectl: output truncated at %d bytes; %d further bytes dropped]",
		c.buf.String(), c.limit, c.dropped)
}

// ExecuteCommand runs command through "sh -c", capturing stdout/stderr
// separately. A non-zero exit is reported via ExitCode rather than an
// error return; only a failure to start/run the shell itself (command not
// found, permission denied, etc.) produces ExitCode -1 with the error text
// appended to Stderr, since job execution failures are expected/normal
// outcomes, not programming errors.
//
// The command is started as the leader of a new process group (Setpgid),
// separate from queuectl's own group, so that the group as a whole -
// including any children the shell command itself spawns (pipelines,
// subshells) - can be killed together by PID if the job's lock is later
// reclaimed as stale. Without this, killing only the worker or supervisor
// process would leave the shell command (and its children) running as
// orphans. If cmd.Start succeeds, onStart is invoked with the new process's
// PID (which is also its process group ID) before ExecuteCommand blocks on
// the command's completion; onStart may be nil.
//
// Canceling ctx kills the command. exec.CommandContext's default cancel only
// signals cmd.Process itself, which is precisely the wrong thing here: the
// command runs as its own process-group leader (Setpgid above), so killing
// just the leader would leave any children it spawned - the pipeline stages
// and subshells of an arbitrary "sh -c" string - running as orphans. cmd.Cancel
// is overridden to kill the whole group instead, so cancellation has the same
// reach as the reaper's own cleanup (see killProcessGroup).
func ExecuteCommand(ctx context.Context, command string, onStart func(pid int)) ExecutionResult {
	startedAt := time.Now().UTC()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return killProcessGroup(cmd.Process.Pid)
	}
	cmd.WaitDelay = outputDrainGrace

	stdout := cappedBuffer{limit: MaxOutputBytes}
	stderr := cappedBuffer{limit: MaxOutputBytes}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Start()
	if err == nil {
		if onStart != nil {
			onStart(cmd.Process.Pid)
		}
		err = cmd.Wait()
	}
	finishedAt := time.Now().UTC()

	exitCode := 0
	stderrText := stderr.String()
	if err != nil {
		var exitErr *exec.ExitError
		switch {
		case errors.As(err, &exitErr):
			exitCode = exitErr.ExitCode()

		case errors.Is(err, exec.ErrWaitDelay):
			// The command itself exited successfully - Wait only reports
			// ErrWaitDelay when it has nothing worse to report, since a
			// non-zero exit would have produced an ExitError above. All that
			// timed out was the wait for its output pipe to close, because
			// something the command backgrounded inherited that pipe and
			// outlived it (see outputDrainGrace). That is a completed job, and
			// reporting it as a -1 failure would fail jobs that did exactly
			// what they were asked to do. The background process is left
			// running, which is what "&" means; only our capture of it stops.
			exitCode = 0
			if stderrText != "" {
				stderrText += "\n"
			}
			stderrText += fmt.Sprintf(
				"queuectl: command exited 0 but left a background process holding its output open; stopped capturing after %s", outputDrainGrace)

		default:
			// The shell itself failed to start or run (command not found,
			// permission denied). This is appended to the rendered text
			// rather than written into the capped buffer: it explains why the
			// job never ran at all, so it must not be what gets truncated
			// away by a command that had already filled the buffer.
			exitCode = -1
			if stderrText != "" {
				stderrText += "\n"
			}
			stderrText += err.Error()
		}
	}

	return ExecutionResult{
		ExitCode:   exitCode,
		Stdout:     stdout.String(),
		Stderr:     stderrText,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}
}
