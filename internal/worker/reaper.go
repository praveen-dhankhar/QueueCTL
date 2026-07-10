package worker

import (
	"context"
	"errors"
	"log/slog"
	"syscall"
	"time"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
	"queuectl/internal/storage"
)

const (
	// staleWorkerRowTTL is how old a worker row's last_heartbeat must be
	// before the reaper garbage-collects it. This is deliberately much
	// larger than worker-stale-seconds (which only controls whether
	// `status` displays a worker as active, and can legitimately be
	// configured tight - see appconfig.HeartbeatInterval): a worker that
	// exits gracefully already deletes its own row, so any row surviving
	// this long can only belong to a process that was SIGKILLed, crashed,
	// or otherwise vanished without cleaning up after itself.
	staleWorkerRowTTL = 1 * time.Hour
)

// RunReaperOnce recovers processing jobs whose lock has gone stale
// (locked_at older than the configured lock-timeout-seconds), moving each
// either back to failed with a backoff or to dead if retries are
// exhausted, and also garbage-collects worker rows abandoned by a
// non-graceful exit (see staleWorkerRowTTL). logger may be nil to suppress
// per-job warnings. It returns the number of jobs actually recovered, which
// can be less than the number found stale if a worker's lease renewal or
// completion raced with the reaper (see RecoverStaleJob's lock fencing).
// A failure to purge stale worker rows is logged but does not fail the
// call or affect the returned count, since job recovery is this function's
// primary responsibility and worker-row cleanup is best-effort.
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
			// The job's lock is fenced away from whatever worker held it,
			// but the "sh -c" command it started may still be running (the
			// worker could be wedged rather than dead, or this reaper could
			// be a freshly restarted supervisor recovering a job orphaned
			// by a killed one). Kill its process group so recovering the DB
			// row doesn't leave a duplicate execution running unsupervised
			// in the background.
			if stale.LockedPGID != nil {
				if err := killProcessGroup(*stale.LockedPGID); err != nil && logger != nil {
					logger.Warn("failed to kill process group of recovered job", "job_id", stale.ID, "pgid", *stale.LockedPGID, "error", err)
				}
			}
		}
	}

	if purged, err := store.DeleteStaleWorkers(ctx, int(staleWorkerRowTTL/time.Second)); err != nil {
		if logger != nil {
			logger.Error("delete stale worker rows failed", "error", err)
		}
	} else if purged > 0 && logger != nil {
		logger.Warn("purged worker rows abandoned without a graceful shutdown", "count", purged)
	}

	return recovered, nil
}

// killProcessGroup sends SIGKILL to every process in the group led by
// pgid (a negative PID targets the whole group rather than one process).
// ESRCH - no such process/group - is not an error here: it just means the
// command had already exited naturally before the reaper got to it.
func killProcessGroup(pgid int) error {
	if pgid <= 0 {
		return nil
	}
	err := syscall.Kill(-pgid, syscall.SIGKILL)
	if err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// RunReaperLoop calls RunReaperOnce every appconfig.ReaperInterval until
// ctx is canceled. It is meant to run in its own goroutine alongside a
// worker pool, in addition to the single startup call in Pool.Start.
func RunReaperLoop(ctx context.Context, store *storage.Store, logger *slog.Logger) {
	ticker := time.NewTicker(appconfig.ReaperInterval)
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
