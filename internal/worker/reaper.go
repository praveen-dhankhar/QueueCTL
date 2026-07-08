package worker

import (
	"context"
	"log/slog"
	"time"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
	"queuectl/internal/storage"
)

const reaperInterval = 30 * time.Second

// RunReaperOnce recovers processing jobs whose lock has gone stale
// (locked_at older than the configured lock-timeout-seconds), moving each
// either back to failed with a backoff or to dead if retries are
// exhausted. logger may be nil to suppress per-job warnings. It returns
// the number of jobs actually recovered, which can be less than the
// number found stale if a worker's lease renewal or completion raced with
// the reaper (see RecoverStaleJob's lock fencing).
func RunReaperOnce(ctx context.Context, store *storage.Store, logger *slog.Logger) (int, error) {
	lockTimeout, err := store.GetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds)
	if err != nil {
		return 0, err
	}
	backoffBase, err := store.GetConfigInt(ctx, appconfig.KeyBackoffBase)
	if err != nil {
		return 0, err
	}

	staleJobs, err := store.ListStaleProcessingJobs(ctx, lockTimeout)
	if err != nil {
		return 0, err
	}

	recovered := 0
	for _, stale := range staleJobs {
		attempts := stale.Attempts + 1
		nextState := job.StateDead
		var nextRetryAt *time.Time
		if attempts < stale.MaxRetries {
			nextState = job.StateFailed
			retryAt := time.Now().UTC().Add(BackoffDelay(backoffBase, attempts))
			nextRetryAt = &retryAt
		}
		ok, err := store.RecoverStaleJob(ctx, stale, attempts, nextState, nextRetryAt)
		if err != nil {
			return recovered, err
		}
		if ok {
			recovered++
			if logger != nil {
				logger.Warn("recovered stale processing job", "job_id", stale.ID, "state", nextState, "attempts", attempts)
			}
		}
	}
	return recovered, nil
}

// RunReaperLoop calls RunReaperOnce every reaperInterval until ctx is
// canceled. It is meant to run in its own goroutine alongside a worker
// pool, in addition to the single startup call in Pool.Start.
func RunReaperLoop(ctx context.Context, store *storage.Store, logger *slog.Logger) {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := RunReaperOnce(ctx, store, logger); err != nil && logger != nil {
				logger.Error("reaper failed", "error", err)
			}
		}
	}
}
