package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoffDelay(t *testing.T) {
	require.Equal(t, 2*time.Second, BackoffDelay(2, 1))
	require.Equal(t, 4*time.Second, BackoffDelay(2, 2))
	require.Equal(t, 1*time.Second, BackoffDelay(1, 3))
}
