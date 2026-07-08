package worker

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
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
func ExecuteCommand(ctx context.Context, command string) ExecutionResult {
	startedAt := time.Now().UTC()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
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
