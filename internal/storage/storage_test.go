package storage_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
	"queuectl/internal/storage"
	"queuectl/internal/worker"
)

func TestStorageInsertListAndClaim(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	now := time.Now().UTC()
	pending, err := job.New("pending", "echo pending", 3, now)
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, pending))

	jobs, err := store.ListJobs(ctx, job.StatePending)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "pending", jobs[0].ID)

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "pending", claimed.ID)
	require.Equal(t, job.StateProcessing, claimed.State)
	require.NotNil(t, claimed.LockedBy)
	require.Equal(t, "worker1", *claimed.LockedBy)
}

func TestClaimRules(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	now := time.Now().UTC()
	future := now.Add(time.Hour)
	failedFuture, err := job.New("failed-future", "false", 3, now)
	require.NoError(t, err)
	failedFuture.State = job.StateFailed
	failedFuture.Attempts = 1
	failedFuture.NextRetryAt = &future
	require.NoError(t, store.InsertJob(ctx, failedFuture))

	completed, err := job.New("completed", "echo done", 3, now)
	require.NoError(t, err)
	completed.State = job.StateCompleted
	completed.Attempts = 1
	require.NoError(t, store.InsertJob(ctx, completed))

	dead, err := job.New("dead", "false", 1, now)
	require.NoError(t, err)
	dead.State = job.StateDead
	dead.Attempts = 1
	require.NoError(t, store.InsertJob(ctx, dead))

	_, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.False(t, ok)

	past := now.Add(-time.Second)
	failedPast, err := job.New("failed-past", "false", 3, now.Add(time.Second))
	require.NoError(t, err)
	failedPast.State = job.StateFailed
	failedPast.Attempts = 1
	failedPast.NextRetryAt = &past
	require.NoError(t, store.InsertJob(ctx, failedPast))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "failed-past", claimed.ID)
}

func TestInsertJobDuplicateIDFails(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	now := time.Now().UTC()
	original, err := job.New("dup", "echo original", 3, now)
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, original))

	duplicate, err := job.New("dup", "echo duplicate", 3, now)
	require.NoError(t, err)
	err = store.InsertJob(ctx, duplicate)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "constraint")

	jobs, err := store.ListJobs(ctx, job.StatePending)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "echo original", jobs[0].Command)
}

func TestLockFencingPreventsStaleCompletion(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	now := time.Now().UTC()
	j, err := job.New("stale-complete", "echo hi", 3, now)
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)

	// Simulate the reaper recovering the job out from under worker1, e.g.
	// because worker1's lease renewal stalled on a network blip.
	recovered, err := store.RecoverStaleJob(ctx, claimed, claimed.Attempts+1, job.StateFailed, nil)
	require.NoError(t, err)
	require.True(t, recovered)

	got, err := store.GetJob(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, job.StateFailed, got.State)

	// worker1 now finishes the command it was still executing and tries to
	// record success against its stale lock. The fenced UPDATE must affect
	// zero rows and return ErrJobLockLost rather than clobbering the
	// reaper's recovery.
	exitCode := 0
	run := storage.JobRun{
		WorkerID:   "worker1",
		ExitCode:   &exitCode,
		StartedAt:  now,
		FinishedAt: now,
	}
	err = store.RecordJobSuccess(ctx, claimed, run)
	require.ErrorIs(t, err, storage.ErrJobLockLost)

	got, err = store.GetJob(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, job.StateFailed, got.State, "reaper's recovery must survive the stale completion attempt")
}

func TestWorkerConcurrencyCompletesJobsOnce(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	chdirForTest(t, tmp)

	dbPath := filepath.Join(tmp, "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyPollIntervalMS, 50))
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyBackoffBase, 1))

	for i := 0; i < 50; i++ {
		j, err := job.New(fmt.Sprintf("job-%02d", i), "echo ok", 3, time.Now())
		require.NoError(t, err)
		require.NoError(t, store.InsertJob(ctx, j))
	}

	poolCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool := worker.NewPool(store, 5, appconfig.WorkerPIDPath(dbPath), logger)
	go func() {
		errCh <- pool.Start(poolCtx)
	}()

	require.Eventually(t, func() bool {
		counts, err := store.CountJobsByState(ctx)
		require.NoError(t, err)
		return counts[job.StateCompleted] == 50
	}, 10*time.Second, 50*time.Millisecond)

	cancel()
	require.NoError(t, <-errCh)

	runCounts, err := store.JobRunCounts(ctx)
	require.NoError(t, err)
	require.Len(t, runCounts, 50)
	for i := 0; i < 50; i++ {
		require.Equal(t, 1, runCounts[fmt.Sprintf("job-%02d", i)])
	}
}

func TestLeaseRenewalPreventsStaleRecoveryDuringLongJob(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	chdirForTest(t, tmp)

	dbPath := filepath.Join(tmp, "queue.db")
	outPath := filepath.Join(tmp, "runs.out")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyPollIntervalMS, 50))
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds, 1))

	j, err := job.New("slow", fmt.Sprintf("sleep 3; printf 'run\\n' >> %q", outPath), 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))

	poolCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool := worker.NewPool(store, 1, appconfig.WorkerPIDPath(dbPath), logger)
	go func() {
		errCh <- pool.Start(poolCtx)
	}()

	require.Eventually(t, func() bool {
		got, err := store.GetJob(ctx, "slow")
		require.NoError(t, err)
		return got.State == job.StateProcessing
	}, 3*time.Second, 50*time.Millisecond)

	time.Sleep(2 * time.Second)
	recovered, err := worker.RunReaperOnce(ctx, store, nil)
	require.NoError(t, err)
	require.Equal(t, 0, recovered)

	require.Eventually(t, func() bool {
		got, err := store.GetJob(ctx, "slow")
		require.NoError(t, err)
		return got.State == job.StateCompleted
	}, 6*time.Second, 50*time.Millisecond)

	cancel()
	require.NoError(t, <-errCh)

	raw, err := os.ReadFile(outPath)
	require.NoError(t, err)
	require.Equal(t, "run\n", string(raw))
}

func TestReaperRecoversStaleProcessingJob(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds, 1))
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyBackoffBase, 1))

	now := time.Now().UTC()
	lockedBy := "worker1"
	lockedAt := now.Add(-5 * time.Second)
	stale, err := job.New("stale", "sleep 10", 3, now.Add(-10*time.Second))
	require.NoError(t, err)
	stale.State = job.StateProcessing
	stale.LockedBy = &lockedBy
	stale.LockedAt = &lockedAt
	require.NoError(t, store.InsertJob(ctx, stale))

	recovered, err := worker.RunReaperOnce(ctx, store, nil)
	require.NoError(t, err)
	require.Equal(t, 1, recovered)

	got, err := store.GetJob(ctx, "stale")
	require.NoError(t, err)
	require.Equal(t, job.StateFailed, got.State)
	require.Equal(t, 1, got.Attempts)
	require.NotNil(t, got.NextRetryAt)
	require.Nil(t, got.LockedBy)
	require.Nil(t, got.LockedAt)
}

func TestReaperMovesFinalAttemptToDead(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds, 1))

	now := time.Now().UTC()
	lockedBy := "worker1"
	lockedAt := now.Add(-5 * time.Second)
	stale, err := job.New("stale-dead", "false", 2, now.Add(-10*time.Second))
	require.NoError(t, err)
	stale.State = job.StateProcessing
	stale.Attempts = 1
	stale.LockedBy = &lockedBy
	stale.LockedAt = &lockedAt
	require.NoError(t, store.InsertJob(ctx, stale))

	recovered, err := worker.RunReaperOnce(ctx, store, nil)
	require.NoError(t, err)
	require.Equal(t, 1, recovered)

	got, err := store.GetJob(ctx, "stale-dead")
	require.NoError(t, err)
	require.Equal(t, job.StateDead, got.State)
	require.Equal(t, 2, got.Attempts)
	require.Nil(t, got.NextRetryAt)
}

func newTestStore(t *testing.T) *storage.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(context.Background(), path)
	require.NoError(t, err)
	return store
}

func chdirForTest(t *testing.T, dir string) {
	t.Helper()
	previous, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(previous))
	})
}
