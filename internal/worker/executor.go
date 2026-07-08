package worker

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"
)

// ExecutionResult captures the outcome of running one job command: its
// exit code, captured stdout/stderr, and start/finish timestamps, all of
// which are persisted to the job_runs table.
type ExecutionResult struct {
	ExitCode   int
	Stdout     string
	Stderr     string
	StartedAt  time.Time
	FinishedAt time.Time
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
func ExecuteCommand(ctx context.Context, command string, onStart func(pid int)) ExecutionResult {
	startedAt := time.Now().UTC()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
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
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
			if stderr.Len() > 0 {
				stderr.WriteByte('\n')
			}
			stderr.WriteString(err.Error())
		}
	}

	return ExecutionResult{
		ExitCode:   exitCode,
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}
}
