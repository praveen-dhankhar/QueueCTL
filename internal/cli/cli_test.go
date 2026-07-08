package cli_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"queuectl/internal/cli"
	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
	"queuectl/internal/storage"
)

// runCLIExpectError runs the root command expecting a non-nil error,
// returning that error. Unlike runCLI (in logs_test.go), which asserts
// success, most of the tests below are specifically about the CLI
// surfacing a store/validation error correctly.
func runCLIExpectError(t *testing.T, dbPath string, args ...string) error {
	t.Helper()
	root := cli.NewRootCommand()
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(out)
	root.SetArgs(append([]string{"--db", dbPath}, args...))
	return root.Execute()
}

func TestEnqueueCommandGeneratesIDAndUsesConfigDefaultMaxRetries(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	out := runCLI(t, dbPath, "enqueue", `{"command":"echo hi"}`)
	require.Contains(t, out, "enqueued job ")

	ctx := context.Background()
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()

	jobs, err := store.ListJobs(ctx, job.StatePending)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, appconfig.Defaults[appconfig.KeyMaxRetries], jobs[0].MaxRetries)
}

func TestEnqueueCommandRejectsBlankCommand(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	err := runCLIExpectError(t, dbPath, "enqueue", `{"command":"   "}`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "command is required")
}

func TestEnqueueCommandRejectsDuplicateID(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	runCLI(t, dbPath, "enqueue", `{"id":"dup","command":"echo hi"}`)

	err := runCLIExpectError(t, dbPath, "enqueue", `{"id":"dup","command":"echo again"}`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
}

func TestEnqueueCommandRejectsMaxRetriesBelowOne(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	err := runCLIExpectError(t, dbPath, "enqueue", `{"command":"echo hi","max_retries":0}`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "max_retries must be >= 1")
}

func TestStatusCommandReportsJobCountsAndActiveWorkers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	ctx := context.Background()
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	j, err := job.New("job1", "echo hi", 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))
	require.NoError(t, store.Close())

	out := runCLI(t, dbPath, "status")
	require.Contains(t, out, "pending: 1")
	require.Contains(t, out, "active: 0")
}

func TestListCommandFiltersByState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	ctx := context.Background()
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)

	pending, err := job.New("pending-job", "echo hi", 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, pending))

	completed, err := job.New("completed-job", "echo hi", 3, time.Now())
	require.NoError(t, err)
	completed.State = job.StateCompleted
	require.NoError(t, store.InsertJob(ctx, completed))
	require.NoError(t, store.Close())

	out := runCLI(t, dbPath, "list", "--state", "pending")
	require.Contains(t, out, "pending-job")
	require.NotContains(t, out, "completed-job")
}

func TestListCommandRejectsInvalidState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	err := runCLIExpectError(t, dbPath, "list", "--state", "not-a-state")
	require.Error(t, err)
}

func TestDLQListAndRetry(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	ctx := context.Background()
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)

	dead, err := job.New("dead-job", "false", 1, time.Now())
	require.NoError(t, err)
	dead.State = job.StateDead
	dead.Attempts = 1
	require.NoError(t, store.InsertJob(ctx, dead))
	require.NoError(t, store.Close())

	listOut := runCLI(t, dbPath, "dlq", "list")
	require.Contains(t, listOut, "dead-job")

	retryOut := runCLI(t, dbPath, "dlq", "retry", "dead-job")
	require.Contains(t, retryOut, "moved job dead-job from dead to pending")

	store, err = storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()
	got, err := store.GetJob(ctx, "dead-job")
	require.NoError(t, err)
	require.Equal(t, job.StatePending, got.State)
}

func TestDLQRetryRejectsNonDeadJob(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	ctx := context.Background()
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	pending, err := job.New("pending-job", "echo hi", 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, pending))
	require.NoError(t, store.Close())

	err = runCLIExpectError(t, dbPath, "dlq", "retry", "pending-job")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not dead")
}

func TestConfigSetGetAndList(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")

	setOut := runCLI(t, dbPath, "config", "set", "max-retries", "7")
	require.Contains(t, setOut, "max-retries=7")

	getOut := runCLI(t, dbPath, "config", "get", "max-retries")
	require.Contains(t, getOut, "max-retries=7")

	listOut := runCLI(t, dbPath, "config", "list")
	require.Contains(t, listOut, "max-retries=7")
	require.Contains(t, listOut, appconfig.KeyBackoffBase)
	require.Contains(t, listOut, appconfig.KeyWorkerStaleSeconds)
}

func TestConfigSetRejectsInvalidValue(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")

	err := runCLIExpectError(t, dbPath, "config", "set", "max-retries", "0")
	require.Error(t, err)

	err = runCLIExpectError(t, dbPath, "config", "set", "worker-stale-seconds", "1")
	require.Error(t, err, "worker-stale-seconds below the heartbeat-derived minimum must be rejected")
}

func TestConfigGetUnknownKey(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	err := runCLIExpectError(t, dbPath, "config", "get", "not-a-key")
	require.Error(t, err)
}
