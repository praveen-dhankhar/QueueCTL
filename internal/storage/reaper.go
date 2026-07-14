package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"queuectl/internal/job"
)

// ListStaleProcessingJobs returns processing jobs whose locked_at is older
// than lockTimeoutSeconds, i.e. candidates for the reaper to recover
// because their worker likely crashed or stopped renewing its lease.
func (s *Store) ListStaleProcessingJobs(ctx context.Context, lockTimeoutSeconds int) ([]job.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT `+jobColumns+`
FROM jobs
WHERE state = 'processing'
AND locked_at < datetime('now', '-' || ? || ' seconds')
ORDER BY locked_at ASC;`, lockTimeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("list stale processing jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows)
}

// MaxJobInterrupts caps how many times a job may be recovered from a stale
// processing lock before it is dead-lettered instead of requeued. Interrupted
// attempts deliberately don't consume the job's retry budget (see
// RecoverStaleJob), which left one unbounded loop: a job whose command kills
// its own worker every run (an OOM big enough to take the supervisor with it,
// say) would be reclaimed and re-run forever. Five worker deaths in a row on
// the same job is no longer bad operational luck - it is evidence the job
// itself is the killer, and the DLQ is where it can be inspected without
// taking more workers down. A deliberate "queuectl dlq retry" resets the
// count (see RetryDeadJob).
const MaxJobInterrupts = 5

// errRecoveryFenceLost is an internal sentinel used to roll back
// RecoverStaleJob's transaction when the lock fence doesn't match, so the
// job_runs row it would have written for the interrupted attempt is not
// persisted for a job it turned out not to have recovered.
var errRecoveryFenceLost = errors.New("recovery fence lost")

// RecoverStaleJob returns a stale processing job to pending and records the
// interrupted attempt in job_runs, in one BEGIN IMMEDIATE transaction. It is
// fenced on the job still being processing and locked by j's own
// locked_by/locked_at: a worker that renewed its lease (or already completed
// the job) between ListStaleProcessingJobs and this call wins the race, the
// update affects zero rows, and the second return value is false with no
// job_runs row written. Callers must not assume recovery happened just
// because the job looked stale a moment earlier.
//
// attempts is deliberately left unchanged. A stale lock means the worker
// died mid-run (SIGKILL, crash, wedge) - it says nothing about whether the
// job's own command would have succeeded, so charging the job a retry for it
// would let an operator-side event exhaust a job's retry budget. With
// max_retries = 1 that is fatal: the very first SIGKILL of a worker would
// send an otherwise healthy job straight to dead instead of running it
// again, which is exactly the crash-recovery guarantee this queue is
// supposed to make. next_retry_at is cleared for the same reason: the job
// isn't being punished for a failure, so it becomes claimable immediately
// rather than after a backoff.
//
// Interruptions are still bounded, just by their own budget: each recovery
// increments the jobs.interrupts column (a separate counter, not `attempts`,
// which keeps meaning "times this command actually ran and failed"), and a
// job reaching MaxJobInterrupts goes to dead instead of pending. The CASE in
// the UPDATE makes the count-and-decide atomic with the recovery itself.
func (s *Store) RecoverStaleJob(ctx context.Context, j job.Job, run JobRun) (bool, error) {
	run.JobID = j.ID
	run.Attempt = j.Attempts + 1
	lockedBy, lockedAt, err := jobLockFence(j)
	if err != nil {
		return false, err
	}

	err = s.withImmediateTx(ctx, func(ctx context.Context, conn *sql.Conn) error {
		result, err := conn.ExecContext(ctx, `
UPDATE jobs
SET state = CASE WHEN interrupts + 1 >= ? THEN 'dead' ELSE 'pending' END,
interrupts = interrupts + 1,
next_retry_at = NULL, locked_by = NULL, locked_at = NULL, locked_pgid = NULL, updated_at = ?
WHERE id = ? AND state = 'processing' AND locked_by = ? AND locked_at = ?;`,
			MaxJobInterrupts,
			formatTime(time.Now()),
			j.ID,
			lockedBy,
			lockedAt,
		)
		if err != nil {
			return fmt.Errorf("recover stale job %s: %w", j.ID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("recover stale job rows affected: %w", err)
		}
		if rows == 0 {
			return errRecoveryFenceLost
		}
		return insertJobRun(ctx, conn, run)
	})
	if errors.Is(err, errRecoveryFenceLost) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// RetryDeadJob moves a dead job back to pending with attempts, interrupts
// and locks reset, fenced on state = 'dead' so retrying a job that isn't
// currently dead returns an error instead of silently reviving it.
//
// locked_pgid is cleared along with the rest of the lock. It is always already
// NULL on a dead job today (the only path into dead, RecordJobFailure, nulls
// it), but a stale pgid surviving on a claimable row is not a bug worth being
// one refactor away from: the reaper SIGKILLs whatever process group
// locked_pgid names, and a PID the OS has since recycled names somebody else's
// processes.
func (s *Store) RetryDeadJob(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET state = 'pending',
attempts = 0,
interrupts = 0,
next_retry_at = NULL,
locked_by = NULL,
locked_at = NULL,
locked_pgid = NULL,
updated_at = ?
WHERE id = ? AND state = 'dead';`, formatTime(time.Now()), id)
	if err != nil {
		return fmt.Errorf("retry dead job %s: %w", id, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("retry dead job rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("job %q does not exist or is not dead", id)
	}
	return nil
}
