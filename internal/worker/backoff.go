package worker

import "time"

// BackoffDelay computes the retry delay as base^attempts seconds (e.g. with
// base 2: 2s, 4s, 8s, ... for attempts 1, 2, 3, ...). base is floored at 1
// and attempts at 0 so a misconfigured or zero-attempt call still returns a
// sane 1-second delay instead of a nonsensical or zero duration.
func BackoffDelay(base int, attempts int) time.Duration {
	if base < 1 {
		base = 1
	}
	if attempts < 0 {
		attempts = 0
	}
	seconds := 1
	for i := 0; i < attempts; i++ {
		seconds *= base
	}
	return time.Duration(seconds) * time.Second
}
