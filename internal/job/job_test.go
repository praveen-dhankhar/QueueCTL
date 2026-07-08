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
