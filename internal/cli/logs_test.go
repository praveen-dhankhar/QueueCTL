package cli_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"queuectl/internal/cli"
	"queuectl/internal/job"
	"queuectl/internal/storage"
)

func TestLogsCommandShowsRecordedRuns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	ctx := context.Background()

	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)

	j, err := job.New("job1", "echo hi", 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)

	exitCode := 0
	run := storage.JobRun{
		WorkerID:   "worker1",
		ExitCode:   &exitCode,
		Stdout:     "hi\n",
		Stderr:     "",
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
	}
	require.NoError(t, store.RecordJobSuccess(ctx, claimed, run))
	require.NoError(t, store.Close())

	out := runCLI(t, dbPath, "logs", "job1")
	require.Contains(t, out, "attempt 1")
	require.Contains(t, out, "worker=worker1")
	require.Contains(t, out, "exit_code=0")
	require.Contains(t, out, "hi")
	require.Contains(t, out, "(empty)") // empty stderr
}

func TestLogsCommandUnknownJob(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(context.Background(), dbPath)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	root := cli.NewRootCommand()
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(out)
	root.SetArgs([]string{"--db", dbPath, "logs", "missing"})
	err = root.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestLogsCommandNoRunsYet(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	ctx := context.Background()
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	j, err := job.New("pending-job", "echo hi", 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))
	require.NoError(t, store.Close())

	out := runCLI(t, dbPath, "logs", "pending-job")
	require.Contains(t, out, "no recorded execution attempts")
}

func runCLI(t *testing.T, dbPath string, args ...string) string {
	t.Helper()
	root := cli.NewRootCommand()
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(out)
	root.SetArgs(append([]string{"--db", dbPath}, args...))
	require.NoError(t, root.Execute())
	return out.String()
}
