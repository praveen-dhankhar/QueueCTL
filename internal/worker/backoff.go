package worker

import "time"

// MaxBackoffDelay caps the retry delay. Without a cap, base^attempts is not
// just impractically large at high attempt counts - it is wrong: the
// exponentiation silently overflows int64 and wraps negative (base 2 does it
// at 34 attempts), and a negative delay puts next_retry_at in the *past*,
// which deletes the backoff entirely and turns a failing job into a hot
// retry loop running at poll speed. Any caller of "enqueue" can reach that
// today by asking for a large max_retries, so this is a reachable bug, not a
// theoretical one.
//
// An hour is chosen as the ceiling because a job still failing after an hour
// of backoff is failing for a reason that more waiting won't fix; the point
// of the backoff is to stop hammering a struggling dependency, and an hourly
// retry already does that.
const MaxBackoffDelay = time.Hour

// BackoffDelay computes the retry delay as base^attempts seconds (e.g. with
// base 2: 2s, 4s, 8s, ... for attempts 1, 2, 3, ...), clamped to
// MaxBackoffDelay. base is floored at 1 and attempts at 0 so a misconfigured
// or zero-attempt call still returns a sane 1-second delay instead of a
// nonsensical or zero duration.
func BackoffDelay(base int, attempts int) time.Duration {
	if base < 1 {
		base = 1
	}
	if attempts < 0 {
		attempts = 0
	}

	maxSeconds := int64(MaxBackoffDelay / time.Second)
	seconds := int64(1)
	for i := 0; i < attempts; i++ {
		// Check before multiplying, not after: testing the result for
		// overflow once it has already wrapped is testing a value that no
		// longer means anything. base >= 1 here, so the division is safe.
		if seconds > maxSeconds/int64(base) {
			return MaxBackoffDelay
		}
		seconds *= int64(base)
	}
	if seconds > maxSeconds {
		return MaxBackoffDelay
	}
	return time.Duration(seconds) * time.Second
}
