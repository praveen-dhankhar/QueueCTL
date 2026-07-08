package worker

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
	"queuectl/internal/storage"
)

// TestExecuteJobRecoversPanicAndLeavesJobForReaper reproduces the scenario
// the panic-recovery fix targets: something inside job execution (a bug in
// ExecuteCommand, a store call, anything under executeJob) panics. Before
// the fix, that panic would propagate out of the worker goroutine and crash
// the entire supervisor process, taking every other worker down with it.
// This asserts two things: the panic doesn't escape executeJob, and the
// lease-renewal goroutine it started is actually stopped (not leaked
// renewing the lock forever), so the job's lock still goes stale and the
// reaper can recover it just like it would for a worker that crashed
// outright.
func TestExecuteJobRecoversPanicAndLeavesJobForReaper(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds, 1))
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyBackoffBase, 1))

	j, err := job.New("panicky", "echo hi", 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool := NewPool(store, 1, appconfig.WorkerPIDPath(dbPath), logger)

	original := executeCommandFn
	executeCommandFn = func(ctx context.Context, command string, onStart func(pid int)) ExecutionResult {
		panic("boom: simulated execution bug")
	}
	defer func() { executeCommandFn = original }()

	require.NotPanics(t, func() {
		pool.executeJob("worker1", claimed)
	})

	// Give the lock time to age past lock-timeout-seconds, with enough
	// margin to absorb locked_at's whole-second storage precision (a
	// locked_at of, say, 14.9s truncates to 14s, so a cutoff computed a
	// bare 1s later can land exactly on it rather than strictly past it).
	// If the panic had leaked the lease-renewal goroutine, it would keep
	// bumping locked_at and the job would never look stale to the reaper.
	time.Sleep(2500 * time.Millisecond)
	recovered, err := RunReaperOnce(ctx, store, nil)
	require.NoError(t, err)
	require.Equal(t, 1, recovered, "reaper should recover the abandoned job rather than finding its lock still fresh")

	got, err := store.GetJob(ctx, "panicky")
	require.NoError(t, err)
	require.Equal(t, job.StateFailed, got.State)
}
