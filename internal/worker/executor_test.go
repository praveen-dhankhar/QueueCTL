package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A job's output used to be buffered without limit, so a command that writes
// in a loop (`yes`, a chatty log) grew the worker's heap until the OS killed
// it - taking every other in-flight job on that worker with it. Output past
// the cap must be dropped, and the drop must be visible rather than silent.
// Stdout/Stderr are buffers, so os/exec hands the command an OS pipe and
// cmd.Wait waits for that pipe to hit EOF - not for the command to exit. A
// command that backgrounds anything gives the write end of that pipe to a
// process that outlives the shell, so before WaitDelay was set, cmd.Wait would
// block forever here: the worker goroutine wedged permanently, its lease still
// being renewed, so even the reaper could never recover the job.
//
// The command below exits 0 immediately and leaves a 60s sleeper holding the
// pipe. It must come back promptly, and it must come back reporting the success
// the command actually had - ErrWaitDelay is not an ExitError, so it would
// otherwise land in the "shell failed to run" branch and fail a job that
// worked.
func TestExecuteCommandDoesNotHangOnBackgroundedChild(t *testing.T) {
	original := outputDrainGrace
	outputDrainGrace = 300 * time.Millisecond
	defer func() { outputDrainGrace = original }()

	type outcome struct{ result ExecutionResult }
	done := make(chan outcome, 1)
	go func() {
		done <- outcome{ExecuteCommand(context.Background(), "echo hi; (sleep 60 &); exit 0", nil)}
	}()

	select {
	case got := <-done:
		require.Equal(t, 0, got.result.ExitCode,
			"the command exited 0; only its backgrounded child held the pipe open")
		require.Contains(t, got.result.Stdout, "hi")
		require.Contains(t, got.result.Stderr, "background process")
	case <-time.After(10 * time.Second):
		t.Fatal("ExecuteCommand never returned: cmd.Wait is blocked on an output pipe held open by a process the command backgrounded")
	}
}

func TestExecuteCommandCapsOutput(t *testing.T) {
	// 1MiB of stdout, well past the 64KiB cap.
	result := ExecuteCommand(context.Background(), "head -c 1048576 /dev/zero | tr '\\0' 'a'", nil)

	require.Equal(t, 0, result.ExitCode, "capping output must not fail the job: %s", result.Stderr)
	require.Less(t, len(result.Stdout), MaxOutputBytes+512,
		"stdout must be bounded by the cap (plus the truncation note), not by what the command chose to print")
	require.Contains(t, result.Stdout, "output truncated", "the truncation must be visible in the logs")
	require.Contains(t, result.Stdout, strings.Repeat("a", 1024), "the output kept must be the command's real output")
}

// Output under the cap must be passed through untouched - no note, no
// truncation, byte-for-byte what the command printed.
func TestExecuteCommandLeavesSmallOutputIntact(t *testing.T) {
	result := ExecuteCommand(context.Background(), "echo hello", nil)

	require.Equal(t, 0, result.ExitCode)
	require.Equal(t, "hello\n", result.Stdout)
	require.NotContains(t, result.Stdout, "truncated")
}

// A command that never starts reports why. That message is appended after the
// captured output rather than written into the capped buffer, so a command
// that had already flooded its buffer cannot truncate away the explanation
// for its own failure.
func TestExecuteCommandReportsStartFailure(t *testing.T) {
	result := ExecuteCommand(context.Background(), "this-command-does-not-exist", nil)

	require.NotEqual(t, 0, result.ExitCode)
	require.NotEmpty(t, result.Stderr)
}
