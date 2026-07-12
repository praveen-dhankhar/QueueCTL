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

// A delay must never come back negative. base^attempts overflowed int64 and
// wrapped negative at 34 attempts with base 2, and a negative delay puts
// next_retry_at in the past - deleting the backoff and turning a failing job
// into a hot retry loop. Reachable from "enqueue" alone, via max_retries.
func TestBackoffDelayNeverNegativeOrUnbounded(t *testing.T) {
	for _, base := range []int{1, 2, 3, 10, 1000} {
		for attempts := 0; attempts <= 200; attempts++ {
			delay := BackoffDelay(base, attempts)
			require.Positive(t, delay, "base=%d attempts=%d produced a non-positive delay", base, attempts)
			require.LessOrEqual(t, delay, MaxBackoffDelay, "base=%d attempts=%d exceeded the cap", base, attempts)
		}
	}
}

// The clamp must not disturb the delays below it: growth stays exponential
// right up to the cap, then flattens.
func TestBackoffDelayClampsOnlyAtTheCeiling(t *testing.T) {
	require.Equal(t, 1024*time.Second, BackoffDelay(2, 10))
	require.Equal(t, 2048*time.Second, BackoffDelay(2, 11))
	require.Equal(t, MaxBackoffDelay, BackoffDelay(2, 12), "2^12s = 4096s is past the 1h cap")
	require.Equal(t, MaxBackoffDelay, BackoffDelay(2, 34), "the old overflow point")
	require.Equal(t, MaxBackoffDelay, BackoffDelay(10, 20))
}
