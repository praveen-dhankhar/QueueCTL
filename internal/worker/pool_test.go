package worker

import (
	"context"
	"errors"
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
	pool := NewPool(store, 1, appconfig.WorkerPIDDir(dbPath), logger)

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

	// The panic abandoned the claim mid-run, which the reaper treats like any
	// other interrupted attempt: requeued as pending to be run again, not
	// charged a failure the command never actually reported.
	got, err := store.GetJob(ctx, "panicky")
	require.NoError(t, err)
	require.Equal(t, job.StatePending, got.State)
	require.Equal(t, 0, got.Attempts)
}

// If registering a worker fails partway through Pool.Start's launch loop, the
// workers already launched must be stopped and waited for before Start returns.
// Otherwise they keep claiming jobs, running commands and holding locks after
// the call that owns them has returned an error - answering to nobody, and
// invisible to every caller. It survived only because main happens to exit
// immediately on this error.
//
// The assertion hangs off the workers table: runWorker deletes its own row on
// the way out, so a row still present after Start returned means that
// goroutine is still alive.
func TestStartStopsAlreadyLaunchedWorkersWhenRegistrationFails(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()

	// Fail the second registration; the first worker is by then already
	// launched and looping.
	calls := 0
	original := registerWorkerFn
	registerWorkerFn = func(s *storage.Store, ctx context.Context, id string, pid int, host string) error {
		calls++
		if calls == 2 {
			return errors.New("simulated registration failure")
		}
		return s.RegisterWorker(ctx, id, pid, host)
	}
	defer func() { registerWorkerFn = original }()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool := NewPool(store, 3, filepath.Join(t.TempDir(), "pids"), logger)

	// Note ctx is never cancelled: Start must return on the error alone, and
	// clean up on the error alone.
	done := make(chan error, 1)
	go func() { done <- pool.Start(ctx) }()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Pool.Start never returned after a failed worker registration")
	}

	// The first worker registered a row. If its goroutine had been left running,
	// its deferred DeleteWorker would never have run and the row would still be
	// here.
	active, err := store.CountActiveWorkers(ctx, 3600)
	require.NoError(t, err)
	require.Equal(t, 0, active,
		"a worker goroutine outlived the failed Pool.Start that launched it: still claiming and running jobs with nobody to stop it")
}

// A job that outruns its timeout_seconds must have its command killed, be
// charged the attempt, and carry the reason into job_runs. The marker file is
// the load-bearing assertion: without the deadline on execCtx the command runs
// to completion regardless of what the job row ends up saying, which is the
// failure mode a timeout exists to prevent (a wedged command holding a worker
// slot forever).
//
// The kill must also reach the command's *children*, not just the "sh" leader -
// hence the backgrounded subshell writing the marker. Killing only the leader
// would leave that child alive to touch the marker after sh is long gone.
func TestJobTimeoutKillsCommandAndChargesTheAttempt(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()

	marker := filepath.Join(t.TempDir(), "survived")
	command := "(sleep 10; touch " + marker + ") & wait"

	// max_retries 1: the timed-out attempt is the job's only one, so a job that
	// lands in dead proves the timeout was charged as a real failure rather
	// than treated like an interrupted attempt (which the reaper leaves
	// uncharged - see storage.RecoverStaleJob).
	j, err := job.New("slowpoke", command, 1, time.Now())
	require.NoError(t, err)
	j.TimeoutSeconds = 1
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, claimed.TimeoutSeconds, "timeout_seconds must survive the round trip through the claim")

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool := NewPool(store, 1, appconfig.WorkerPIDDir(dbPath), logger)

	done := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(done)
		pool.executeJob("worker1", claimed)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("executeJob outlived its job's 1s timeout by 5s: the command was never killed")
	}
	require.Less(t, time.Since(started), 5*time.Second)

	// The command needed 10s. If it is still alive, it reaches the marker.
	time.Sleep(1500 * time.Millisecond)
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "the timed-out command survived and ran to completion")

	got, err := store.GetJob(ctx, "slowpoke")
	require.NoError(t, err)
	require.Equal(t, job.StateDead, got.State)
	require.Equal(t, 1, got.Attempts, "a timeout is the job failing, so it must be charged an attempt")

	runs, err := store.ListJobRuns(ctx, "slowpoke")
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.NotNil(t, runs[0].ExitCode)
	require.NotEqual(t, 0, *runs[0].ExitCode)
	require.Contains(t, runs[0].Stderr, "timed out after 1s")
}

// A job with timeout_seconds 0 (the default) has no deadline at all: it must
// run to completion however long it takes. This is the other half of the
// contract - a botched deadline that fires on every job would be caught here,
// not by the timeout test above.
func TestZeroTimeoutMeansNoDeadline(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()

	j, err := job.New("unbounded", "sleep 1; echo done", 1, time.Now())
	require.NoError(t, err)
	require.Equal(t, 0, j.TimeoutSeconds)
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool := NewPool(store, 1, appconfig.WorkerPIDDir(dbPath), logger)
	pool.executeJob("worker1", claimed)

	got, err := store.GetJob(ctx, "unbounded")
	require.NoError(t, err)
	require.Equal(t, job.StateCompleted, got.State)
}

// A worker that loses its lock mid-run has had its job reclaimed by the
// reaper and requeued for someone else. The command it started must die with
// the claim: if it keeps running, the job's side effects happen twice - once
// here, once in whichever worker picks up the requeued row.
//
// Killing it used to be left entirely to the reaper, via the process group
// recorded in jobs.locked_pgid. That is not enough on its own: SetJobLockPGID
// is fenced on the very lock being lost here, so on exactly this path it can
// fail and leave locked_pgid NULL, giving the reaper nothing to kill. The
// worker must kill the command itself when its lease renewal reports the lock
// gone.
func TestLostLockKillsTheRunningCommand(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()
	// Renewal interval is lock-timeout/3, floored at 250ms, so this makes the
	// worker notice the stolen lock within ~333ms instead of seconds.
	require.NoError(t, store.SetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds, 1))

	// The marker is written only if the command survives its sleep - i.e. only
	// if killing it failed.
	marker := filepath.Join(t.TempDir(), "survived")
	command := "sleep 10; touch " + marker

	j, err := job.New("stolen", command, 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool := NewPool(store, 1, appconfig.WorkerPIDDir(dbPath), logger)

	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.executeJob("worker1", claimed)
	}()

	// Steal the lock out from under the running worker, exactly as the reaper
	// does when it decides the job is stale. This has to land inside the first
	// renewal interval (~333ms above): a renewal would bump locked_at, and the
	// recovery below is fenced on the locked_at that ClaimNextJob returned.
	time.Sleep(100 * time.Millisecond)
	recovered, err := store.RecoverStaleJob(ctx, claimed, storage.JobRun{
		WorkerID:   "worker1",
		StartedAt:  time.Now().UTC(),
		FinishedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.True(t, recovered, "test setup: the reaper's recovery must win the lock")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("executeJob still running 5s after losing its lock: the command was not killed and is running to completion alongside the requeued job")
	}

	// The command slept for 10s; it cannot have reached its marker in the ~1s
	// this test allowed it before the kill.
	time.Sleep(500 * time.Millisecond)
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "the orphaned command survived the lost lock and ran to completion")

	// Losing the lock must not let the worker write an outcome over the
	// reaper's requeue: the job stays pending, uncharged, for another worker.
	got, err := store.GetJob(ctx, "stolen")
	require.NoError(t, err)
	require.Equal(t, job.StatePending, got.State)
	require.Equal(t, 0, got.Attempts)
}
