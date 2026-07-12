package worker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"queuectl/internal/job"
	"queuectl/internal/storage"
)

// One stale job that cannot be recovered must not park every other stale job
// behind it. ListStaleProcessingJobs orders by locked_at ASC, so the oldest
// unrecoverable row leads every single sweep - aborting the loop on it meant
// crash recovery stopped for the whole queue, permanently, not just for that
// one job.
//
// The poison row here is reachable without touching the DB by hand: a
// processing job with locked_at set but locked_by NULL matches the staleness
// query (which only requires locked_at) but fails RecoverStaleJob's lock fence,
// which needs both halves of the lock to fence against.
func TestReaperSweepContinuesPastAnUnrecoverableJob(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := storage.Open(ctx, dbPath)
	require.NoError(t, err)
	defer store.Close()

	stale := time.Now().UTC().Add(-1 * time.Hour)
	worker := "worker1"

	// Sorts first (older locked_at), and has no locked_by to fence on.
	poison, err := job.New("poison", "echo poison", 3, stale)
	require.NoError(t, err)
	poison.State = job.StateProcessing
	poison.LockedAt = &stale
	require.NoError(t, store.InsertJob(ctx, poison))

	// Sorts second, and is a perfectly ordinary stale job: it is the one the
	// old code never got to.
	healthyLockedAt := stale.Add(time.Minute)
	healthy, err := job.New("healthy", "echo healthy", 3, stale)
	require.NoError(t, err)
	healthy.State = job.StateProcessing
	healthy.LockedAt = &healthyLockedAt
	healthy.LockedBy = &worker
	require.NoError(t, store.InsertJob(ctx, healthy))

	found, err := store.ListStaleProcessingJobs(ctx, 60)
	require.NoError(t, err)
	require.Len(t, found, 2, "test setup: both jobs must look stale to the reaper")
	require.Equal(t, "poison", found[0].ID, "test setup: the unrecoverable job must lead the sweep")

	recovered, err := RunReaperOnce(ctx, store, nil)

	// The sweep still reports what went wrong...
	require.Error(t, err)
	require.Contains(t, err.Error(), "poison")

	// ...but it did not give up on the rest of the queue because of it.
	require.Equal(t, 1, recovered)
	got, err := store.GetJob(ctx, "healthy")
	require.NoError(t, err)
	require.Equal(t, job.StatePending, got.State,
		"a job behind an unrecoverable one in the sweep was never recovered")
}
