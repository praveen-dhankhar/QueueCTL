package worker

import (
	"context"
	"errors"
	"fmt"
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

// interruptedRunNote is the stderr body recorded in job_runs for an attempt
// that was cut short by its worker dying rather than by the command itself
// exiting, so "queuectl logs" shows the interrupted attempt instead of
// silently skipping from one complete attempt to the next.
const interruptedRunNote = "queuectl: attempt interrupted - the worker holding this job stopped renewing its lease (killed, crashed, or wedged) and the job was requeued by the reaper"

// RunReaperOnce recovers processing jobs whose lock has gone stale
// (locked_at older than the configured lock-timeout-seconds), returning each
// to pending to be claimed and run again, and also garbage-collects worker
// rows abandoned by a non-graceful exit (see staleWorkerRowTTL). logger may
// be nil to suppress per-job warnings. It returns the number of jobs actually
// recovered, which can be less than the number found stale if a worker's
// lease renewal or completion raced with the reaper (see RecoverStaleJob's
// lock fencing). A failure to purge stale worker rows is logged but does not
// fail the call or affect the returned count, since job recovery is this
// function's primary responsibility and worker-row cleanup is best-effort.
//
// Every stale job is attempted even if an earlier one fails; the returned
// error joins whatever went wrong, and the returned count still reports the
// jobs that were recovered alongside it. A non-nil error therefore does not
// mean nothing was recovered.
//
// A recovered job keeps its attempts count: a dead worker is not a failed
// command, so the interrupted attempt is recorded in job_runs but not
// charged against the job's retry budget. Interruptions have their own
// budget instead - a job recovered storage.MaxJobInterrupts times is
// dead-lettered rather than requeued. See storage.RecoverStaleJob.
func RunReaperOnce(ctx context.Context, store *storage.Store, logger *slog.Logger) (int, error) {
	lockTimeout, err := store.GetConfigInt(ctx, appconfig.KeyLockTimeoutSeconds)
	if err != nil {
		return 0, err
	}

	staleJobs, err := store.ListStaleProcessingJobs(ctx, lockTimeout)
	if err != nil {
		return 0, err
	}

	// A job that fails to recover must not stop the sweep. ListStaleProcessingJobs
	// orders by locked_at ASC, so the same row leads every pass: aborting on it
	// would park every *other* stale job behind one poison row, on this pass and
	// on all of them, and crash recovery is the one thing that has to keep
	// working when things are already going wrong. Errors are collected and
	// returned together once every job has had its turn.
	recovered := 0
	var errs []error
	for _, stale := range staleJobs {
		run := storage.JobRun{
			Stderr:     interruptedRunNote,
			StartedAt:  staleRunStartedAt(stale),
			FinishedAt: time.Now().UTC(),
		}
		if stale.LockedBy != nil {
			run.WorkerID = *stale.LockedBy
		}
		ok, err := store.RecoverStaleJob(ctx, stale, run)
		if err != nil {
			errs = append(errs, fmt.Errorf("recover stale job %s: %w", stale.ID, err))
			if logger != nil {
				logger.Error("failed to recover stale job; continuing the sweep", "job_id", stale.ID, "error", err)
			}
			continue
		}
		if ok {
			recovered++
			if logger != nil {
				logger.Warn("recovered stale processing job; requeued as pending without charging an attempt",
					"job_id", stale.ID, "attempts", stale.Attempts)
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

	return recovered, errors.Join(errs...)
}

// staleRunStartedAt is the best available start time for an interrupted
// attempt. The jobs row does not persist when the command actually started -
// only locked_at, which the worker overwrites on every lease renewal - so
// this is the last moment the dead worker was known to still hold the job,
// not the true start. It is an approximation, recorded so the job_runs row
// for the interrupted attempt has a timestamp at all (started_at is NOT
// NULL); a job_runs row with no exit code is what marks the attempt as
// interrupted, not this value.
func staleRunStartedAt(stale job.Job) time.Time {
	if stale.LockedAt != nil {
		return *stale.LockedAt
	}
	return stale.UpdatedAt
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
