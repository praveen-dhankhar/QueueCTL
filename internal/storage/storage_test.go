package storage_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
	"queuectl/internal/storage"
	"queuectl/internal/worker"

	_ "modernc.org/sqlite"
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

// TestClaimOrdersBySameSecondInsertionOrderNotID guards against a real
// ordering gap: created_at has one-second resolution (sqliteTimeLayout has
// no fractional seconds), so jobs enqueued within the same second tie on
// the primary sort key. Before the rowid tiebreaker, that tie broke on
// whatever order SQLite happened to return matching rows in - not
// necessarily insertion order. IDs here are deliberately reverse-alphabetical
// versus insertion order, so a test that passed only because it happened to
// also sort correctly by id would fail here.
func TestClaimOrdersBySameSecondInsertionOrderNotID(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	now := time.Now().UTC()
	ids := []string{"zzz-first", "mmm-second", "aaa-third"}
	for _, id := range ids {
		j, err := job.New(id, "echo "+id, 3, now)
		require.NoError(t, err)
		require.NoError(t, store.InsertJob(ctx, j))
	}

	for _, want := range ids {
		claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, want, claimed.ID, "claim order should follow insertion order, not id or a coincidental row order")
	}
}

// TestListJobsOrdersBySameSecondInsertionOrderNotID is ListJobs' counterpart
// to TestClaimOrdersBySameSecondInsertionOrderNotID: "queuectl list" should
// show same-second jobs in the order they were enqueued, not alphabetically
// by (possibly random, auto-generated) id.
func TestListJobsOrdersBySameSecondInsertionOrderNotID(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	now := time.Now().UTC()
	ids := []string{"zzz-first", "mmm-second", "aaa-third"}
	for _, id := range ids {
		j, err := job.New(id, "echo "+id, 3, now)
		require.NoError(t, err)
		require.NoError(t, store.InsertJob(ctx, j))
	}

	jobs, err := store.ListJobs(ctx, job.StatePending)
	require.NoError(t, err)
	require.Len(t, jobs, 3)
	for i, want := range ids {
		require.Equal(t, want, jobs[i].ID, "list order should follow insertion order, not id")
	}
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
	recovered, err := store.RecoverStaleJob(ctx, claimed, storage.JobRun{
		WorkerID:   "worker1",
		StartedAt:  now,
		FinishedAt: now,
	})
	require.NoError(t, err)
	require.True(t, recovered)

	got, err := store.GetJob(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, job.StatePending, got.State)

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
	require.Equal(t, job.StatePending, got.State, "reaper's recovery must survive the stale completion attempt")
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
	pool := worker.NewPool(store, 5, appconfig.WorkerPIDDir(dbPath), logger)
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
	pool := worker.NewPool(store, 1, appconfig.WorkerPIDDir(dbPath), logger)
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
	require.Equal(t, job.StatePending, got.State)
	require.Equal(t, 0, got.Attempts, "the interrupted run was not the job's own failure")
	require.Nil(t, got.NextRetryAt)
	require.Nil(t, got.LockedBy)
	require.Nil(t, got.LockedAt)
}

// A worker dying mid-run is not the job's failure, so the reaper must not
// charge it an attempt: with max_retries = 1 (attempts already at its last
// allowed value) the old behavior sent a perfectly healthy job straight to
// dead on the first SIGKILL, instead of running it again. The job must come
// back as pending, immediately claimable, with its attempts count untouched.
func TestReaperRequeuesInterruptedJobWithoutChargingAnAttempt(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds, 1))

	now := time.Now().UTC()
	lockedBy := "worker1"
	lockedAt := now.Add(-5 * time.Second)
	stale, err := job.New("stale-interrupted", "echo hi", 1, now.Add(-10*time.Second))
	require.NoError(t, err)
	stale.State = job.StateProcessing
	stale.LockedBy = &lockedBy
	stale.LockedAt = &lockedAt
	require.NoError(t, store.InsertJob(ctx, stale))

	recovered, err := worker.RunReaperOnce(ctx, store, nil)
	require.NoError(t, err)
	require.Equal(t, 1, recovered)

	got, err := store.GetJob(ctx, "stale-interrupted")
	require.NoError(t, err)
	require.Equal(t, job.StatePending, got.State, "an interrupted job must be runnable again, not dead")
	require.Equal(t, 0, got.Attempts, "a dead worker must not consume the job's retry budget")
	require.Nil(t, got.NextRetryAt, "an interrupted job is not being punished, so it must be claimable at once")

	// The interrupted attempt is still visible in "queuectl logs": a run row
	// with no exit code, which is what marks it as interrupted rather than
	// completed.
	runs, err := store.ListJobRuns(ctx, "stale-interrupted")
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Nil(t, runs[0].ExitCode)
	require.Equal(t, "worker1", runs[0].WorkerID)

	// And it can actually be claimed again, which is the whole point.
	claimed, ok, err := store.ClaimNextJob(ctx, "worker2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "stale-interrupted", claimed.ID)
}

func TestSetJobLockPGIDFencing(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	now := time.Now().UTC()
	j, err := job.New("pgid-job", "true", 3, now)
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Nil(t, claimed.LockedPGID, "pgid is unknown until the command actually starts")

	require.NoError(t, store.SetJobLockPGID(ctx, claimed.ID, "worker1", 4242))
	got, err := store.GetJob(ctx, claimed.ID)
	require.NoError(t, err)
	require.NotNil(t, got.LockedPGID)
	require.Equal(t, 4242, *got.LockedPGID)

	// A worker id that doesn't hold the current lock must not be able to
	// overwrite the pgid recorded by whoever actually claimed the job.
	err = store.SetJobLockPGID(ctx, claimed.ID, "worker-imposter", 9999)
	require.ErrorIs(t, err, storage.ErrJobLockLost)
}

func TestListJobRuns(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	now := time.Now().UTC()
	j, err := job.New("runs-job", "false", 2, now)
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)

	exitCode := 1
	run := storage.JobRun{
		WorkerID:   "worker1",
		ExitCode:   &exitCode,
		Stdout:     "out1",
		Stderr:     "err1",
		StartedAt:  now,
		FinishedAt: now,
	}
	retryAt := now.Add(time.Second)
	require.NoError(t, store.RecordJobFailure(ctx, claimed, run, job.StateFailed, &retryAt))

	runs, err := store.ListJobRuns(ctx, "runs-job")
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, 1, runs[0].Attempt)
	require.Equal(t, "worker1", runs[0].WorkerID)
	require.Equal(t, "out1", runs[0].Stdout)
	require.Equal(t, "err1", runs[0].Stderr)
	require.NotNil(t, runs[0].ExitCode)
	require.Equal(t, 1, *runs[0].ExitCode)

	noRuns, err := store.ListJobRuns(ctx, "no-such-job")
	require.NoError(t, err)
	require.Empty(t, noRuns)
}

// TestDeleteStaleWorkers guards the worker-row garbage collection fix: a
// worker that exits gracefully deletes its own row (DeleteWorker), but one
// that is SIGKILLed leaves its row behind forever unless something purges
// it. last_heartbeat can't be backdated through the public API (RegisterWorker
// always stamps "now"), so this reaches around it with a raw connection to
// the same database file - the same technique
// TestOpenAddsLockedPGIDColumnToPreExistingDatabase uses below.
func TestDeleteStaleWorkers(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.RegisterWorker(ctx, "fresh-worker", 111, "host"))
	require.NoError(t, store.RegisterWorker(ctx, "abandoned-worker", 222, "host"))

	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = raw.Exec(`UPDATE workers SET last_heartbeat = datetime('now', '-2 hours') WHERE worker_id = 'abandoned-worker';`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	purged, err := store.DeleteStaleWorkers(ctx, 3600)
	require.NoError(t, err)
	require.EqualValues(t, 1, purged)

	remaining, err := store.CountActiveWorkers(ctx, 999999)
	require.NoError(t, err)
	require.Equal(t, 1, remaining, "only the fresh worker's row should remain")
}

// TestReaperPurgesAbandonedWorkerRows is the reaper-integration counterpart
// to TestDeleteStaleWorkers: RunReaperOnce must actually invoke the purge as
// part of its normal pass, not just leave DeleteStaleWorkers as dead code.
func TestReaperPurgesAbandonedWorkerRows(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.RegisterWorker(ctx, "fresh-worker", 111, "host"))
	require.NoError(t, store.RegisterWorker(ctx, "abandoned-worker", 222, "host"))

	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = raw.Exec(`UPDATE workers SET last_heartbeat = datetime('now', '-2 hours') WHERE worker_id = 'abandoned-worker';`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	_, err = worker.RunReaperOnce(ctx, store, nil)
	require.NoError(t, err)

	remaining, err := store.CountActiveWorkers(ctx, 999999)
	require.NoError(t, err)
	require.Equal(t, 1, remaining, "reaper pass should have purged the abandoned worker row")
}

// TestReaperKillsOrphanedProcessGroupOnRecovery reproduces the scenario the
// process-group fix targets: a job's command is still actually running as
// an OS process (e.g. the worker that started it is wedged and stopped
// renewing its lease, or a previous supervisor was killed and a new one is
// now recovering the row on startup) when the reaper decides its lock is
// stale. Recovering the DB row must also kill the command's process group,
// not just abandon it to keep running unsupervised.
func TestReaperKillsOrphanedProcessGroupOnRecovery(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds, 1))

	markerPath := filepath.Join(t.TempDir(), "marker")
	cmd := exec.Command("sh", "-c", fmt.Sprintf("sleep 5; touch %q", markerPath))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	pgid := cmd.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		_ = cmd.Wait()
	})

	now := time.Now().UTC()
	lockedBy := "worker1"
	lockedAt := now.Add(-5 * time.Second)
	stale, err := job.New("orphan", "sleep 5", 3, now.Add(-10*time.Second))
	require.NoError(t, err)
	stale.State = job.StateProcessing
	stale.LockedBy = &lockedBy
	stale.LockedAt = &lockedAt
	stale.LockedPGID = &pgid
	require.NoError(t, store.InsertJob(ctx, stale))

	recovered, err := worker.RunReaperOnce(ctx, store, nil)
	require.NoError(t, err)
	require.Equal(t, 1, recovered)

	waitErr := cmd.Wait()
	require.Error(t, waitErr, "the orphaned process should have been killed, not exited cleanly")

	_, statErr := os.Stat(markerPath)
	require.True(t, os.IsNotExist(statErr), "process should have been killed before it could write the marker file")
}

// TestOpenAddsLockedPGIDColumnToPreExistingDatabase simulates upgrading
// queuectl against a database created before the locked_pgid column
// existed (e.g. jobs.sql from before the process-group fix): CREATE TABLE
// IF NOT EXISTS is a no-op against an already-existing jobs table, so
// Store.Open must add the column explicitly rather than silently losing
// the ability to persist it - and must do so without disturbing rows
// already in the table, since persistence across upgrades is a hard
// requirement.
func TestOpenAddsLockedPGIDColumnToPreExistingDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")

	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = raw.Exec(`CREATE TABLE jobs (
id TEXT PRIMARY KEY,
command TEXT NOT NULL,
state TEXT NOT NULL,
attempts INTEGER NOT NULL DEFAULT 0,
max_retries INTEGER NOT NULL,
next_retry_at DATETIME,
locked_by TEXT,
locked_at DATETIME,
created_at DATETIME NOT NULL,
updated_at DATETIME NOT NULL
);`)
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at)
VALUES ('legacy1', 'echo hi', 'pending', 0, 3, '2026-01-01 00:00:00', '2026-01-01 00:00:00');`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	ctx := context.Background()
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()

	got, err := store.GetJob(ctx, "legacy1")
	require.NoError(t, err)
	require.Equal(t, "echo hi", got.Command)
	require.Nil(t, got.LockedPGID)

	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds, 1))
	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, store.SetJobLockPGID(ctx, claimed.ID, "worker1", 777))

	got, err = store.GetJob(ctx, "legacy1")
	require.NoError(t, err)
	require.NotNil(t, got.LockedPGID)
	require.Equal(t, 777, *got.LockedPGID)
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
