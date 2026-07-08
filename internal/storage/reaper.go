package storage

import (
	"context"
	"fmt"
	"time"

	"queuectl/internal/job"
)

// ListStaleProcessingJobs returns processing jobs whose locked_at is older
// than lockTimeoutSeconds, i.e. candidates for the reaper to recover
// because their worker likely crashed or stopped renewing its lease.
func (s *Store) ListStaleProcessingJobs(ctx context.Context, lockTimeoutSeconds int) ([]job.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, command, state, attempts, max_retries, next_retry_at, locked_by, locked_at, created_at, updated_at
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

// RecoverStaleJob moves a stale processing job to state (failed or dead),
// fenced on the job still being processing and locked by j's own
// locked_by/locked_at. The fence means a worker that renewed its lease (or
// already completed the job) between ListStaleProcessingJobs and this call
// wins the race: the update affects zero rows and the second return value
// is false. Callers must not assume recovery happened just because the job
// looked stale a moment earlier.
func (s *Store) RecoverStaleJob(ctx context.Context, j job.Job, attempts int, state job.State, nextRetryAt *time.Time) (bool, error) {
	lockedBy, lockedAt, err := jobLockFence(j)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET attempts = ?, state = ?, next_retry_at = ?, locked_by = NULL, locked_at = NULL, updated_at = ?
WHERE id = ? AND state = 'processing' AND locked_by = ? AND locked_at = ?;`,
		attempts,
		string(state),
		nullableTime(nextRetryAt),
		formatTime(time.Now()),
		j.ID,
		lockedBy,
		lockedAt,
	)
	if err != nil {
		return false, fmt.Errorf("recover stale job %s: %w", j.ID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("recover stale job rows affected: %w", err)
	}
	return rows > 0, nil
}

// RetryDeadJob moves a dead job back to pending with attempts and locks
// reset, fenced on state = 'dead' so retrying a job that isn't currently
// dead returns an error instead of silently reviving it.
func (s *Store) RetryDeadJob(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET state = 'pending',
attempts = 0,
next_retry_at = NULL,
locked_by = NULL,
locked_at = NULL,
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
