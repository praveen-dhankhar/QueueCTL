package job

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJobStateTransitions(t *testing.T) {
	now := time.Now().UTC()
	j, err := New("job1", "echo hello", 3, now)
	require.NoError(t, err)
	require.Equal(t, StatePending, j.State)

	require.NoError(t, j.MarkProcessing("worker1", now))
	require.Equal(t, StateProcessing, j.State)
	require.Equal(t, "worker1", *j.LockedBy)

	require.NoError(t, j.MarkCompleted(now))
	require.Equal(t, StateCompleted, j.State)
	require.Equal(t, 1, j.Attempts)
	require.Nil(t, j.LockedBy)
	require.Nil(t, j.LockedAt)
}

func TestJobFailureTransitions(t *testing.T) {
	now := time.Now().UTC()
	retryAt := now.Add(2 * time.Second)

	j, err := New("job1", "false", 2, now)
	require.NoError(t, err)

	require.NoError(t, j.MarkProcessing("worker1", now))
	require.NoError(t, j.MarkFailedOrDead(&retryAt, now))
	require.Equal(t, StateFailed, j.State)
	require.Equal(t, 1, j.Attempts)
	require.Equal(t, retryAt, *j.NextRetryAt)

	require.NoError(t, j.MarkProcessing("worker1", now))
	require.NoError(t, j.MarkFailedOrDead(nil, now))
	require.Equal(t, StateDead, j.State)
	require.Equal(t, 2, j.Attempts)
	require.Nil(t, j.NextRetryAt)
}

// TestNextAttemptState guards the single source of truth for the
// retry-vs-dead decision that Job.MarkFailedOrDead, worker.Pool.executeJob,
// and worker.RunReaperOnce all defer to, so the three failure paths
// (normal execution, panic recovery, and reaper recovery) can't drift out
// of sync with each other.
func TestNextAttemptState(t *testing.T) {
	require.Equal(t, StateFailed, NextAttemptState(1, 3), "attempts below max_retries should retry")
	require.Equal(t, StateFailed, NextAttemptState(2, 3), "attempts still below max_retries should retry")
	require.Equal(t, StateDead, NextAttemptState(3, 3), "attempts reaching max_retries should be dead")
	require.Equal(t, StateDead, NextAttemptState(4, 3), "attempts exceeding max_retries should be dead")
	require.Equal(t, StateDead, NextAttemptState(1, 1), "a single-attempt job dies on its first failure")
}

func TestRetryFromDLQ(t *testing.T) {
	now := time.Now().UTC()
	j, err := New("job1", "false", 1, now)
	require.NoError(t, err)

	require.NoError(t, j.MarkProcessing("worker1", now))
	require.NoError(t, j.MarkFailedOrDead(nil, now))
	require.Equal(t, StateDead, j.State)

	require.NoError(t, j.RetryFromDLQ(now))
	require.Equal(t, StatePending, j.State)
	require.Equal(t, 0, j.Attempts)
	require.Nil(t, j.NextRetryAt)
	require.Nil(t, j.LockedBy)
	require.Nil(t, j.LockedAt)
}
