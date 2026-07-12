package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"queuectl/internal/job"
	"queuectl/internal/storage"
)

// runJob enqueues a job, claims it, and records one completed attempt lasting
// exactly duration, so a test can pin the numbers GetMetrics is supposed to
// report instead of racing a real command's wall clock.
func runJob(t *testing.T, store *storage.Store, id string, duration time.Duration, exitCode int) {
	t.Helper()
	ctx := context.Background()

	j, err := job.New(id, "true", 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))

	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, id, claimed.ID)

	startedAt := time.Now().UTC()
	run := storage.JobRun{
		WorkerID:   "worker1",
		ExitCode:   &exitCode,
		StartedAt:  startedAt,
		FinishedAt: startedAt.Add(duration),
	}
	if exitCode == 0 {
		require.NoError(t, store.RecordJobSuccess(ctx, claimed, run))
		return
	}
	require.NoError(t, store.RecordJobFailure(ctx, claimed, run, job.StateFailed, nil))
}

// job_runs timestamps used to be stored at whole-second resolution, so the
// duration of anything shorter than a second collapsed to 0s or 1s depending
// on nothing but whether the run happened to straddle a second boundary - and
// avg/p95/max, the entire point of the metrics command, were noise for the
// sub-second jobs that make up most of a queue. Durations must survive the
// round-trip through SQLite with at least millisecond precision.
func TestMetricsPreservesSubSecondDurations(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	runJob(t, store, "quick", 250*time.Millisecond, 0)

	m, err := store.GetMetrics(ctx)
	require.NoError(t, err)

	require.Equal(t, 1, m.TotalRuns)
	require.Equal(t, 1, m.Succeeded)
	require.InDelta(t, 0.250, m.AvgSeconds, 0.002, "a 250ms run must not be recorded as 0s or 1s")
	require.InDelta(t, 0.250, m.MaxSeconds, 0.002)
	require.InDelta(t, 0.250, m.P95Seconds, 0.002)
}

// p95 used to floor the rank (n*95/100) rather than take the nearest rank
// ceil(0.95*n). At small n that lands below the mean: four runs of
// 1s/1s/1s/5s reported a p95 of 1s, which is not a 95th percentile of
// anything. p95 must be >= avg, always, and must pick the slowest run here.
func TestMetricsP95UsesNearestRank(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	runJob(t, store, "fast1", 1*time.Second, 0)
	runJob(t, store, "fast2", 1*time.Second, 0)
	runJob(t, store, "fast3", 1*time.Second, 0)
	runJob(t, store, "slow", 5*time.Second, 0)

	m, err := store.GetMetrics(ctx)
	require.NoError(t, err)

	require.InDelta(t, 2.0, m.AvgSeconds, 0.01)
	require.InDelta(t, 5.0, m.MaxSeconds, 0.01)
	require.InDelta(t, 5.0, m.P95Seconds, 0.01, "nearest-rank p95 of 4 runs is the 4th, i.e. the slowest")
	require.GreaterOrEqual(t, m.P95Seconds, m.AvgSeconds, "a p95 below the mean is never a percentile")
}

// A run cut short by its worker dying (no exit code - see RecoverStaleJob) is
// counted, but must not pollute the success rate or the durations: its
// truncated wall-clock time says nothing about how long the command takes.
func TestMetricsExcludesInterruptedRunsFromRatesAndDurations(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	runJob(t, store, "ok", 2*time.Second, 0)
	runJob(t, store, "bad", 2*time.Second, 7)

	// An interrupted attempt: claimed, then recovered by the reaper with no
	// exit code ever reported.
	j, err := job.New("killed", "sleep 60", 3, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.InsertJob(ctx, j))
	claimed, ok, err := store.ClaimNextJob(ctx, "worker1")
	require.NoError(t, err)
	require.True(t, ok)
	startedAt := time.Now().UTC()
	recovered, err := store.RecoverStaleJob(ctx, claimed, storage.JobRun{
		WorkerID:   "worker1",
		StartedAt:  startedAt,
		FinishedAt: startedAt.Add(90 * time.Second),
	})
	require.NoError(t, err)
	require.True(t, recovered)

	m, err := store.GetMetrics(ctx)
	require.NoError(t, err)

	require.Equal(t, 3, m.TotalRuns)
	require.Equal(t, 1, m.Succeeded)
	require.Equal(t, 1, m.Failed)
	require.Equal(t, 1, m.Interrupted)
	require.InDelta(t, 50.0, m.SuccessRate, 0.01, "success rate is over completed runs only, not the interrupted one")
	require.InDelta(t, 2.0, m.AvgSeconds, 0.01, "the 90s interrupted run must not skew the average")
	require.InDelta(t, 2.0, m.MaxSeconds, 0.01, "the 90s interrupted run must not become the max")
}

// An empty queue must report zeroes rather than failing on the NULL
// aggregates every duration query returns when there are no rows at all.
func TestMetricsOnEmptyQueue(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	m, err := store.GetMetrics(context.Background())
	require.NoError(t, err)
	require.Equal(t, storage.Metrics{}, m)
}

// Throughput counts successful runs finished inside the trailing window, so a
// run that succeeded just now lands in both buckets and a failure lands in
// neither.
func TestMetricsThroughputCountsRecentSuccesses(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	defer store.Close()

	runJob(t, store, "ok", 100*time.Millisecond, 0)
	runJob(t, store, "bad", 100*time.Millisecond, 1)

	m, err := store.GetMetrics(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Last1m)
	require.Equal(t, 1, m.Last5m)
}
