package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"queuectl/internal/job"
)

// ClaimNextJob atomically claims the oldest eligible job (pending, or failed
// with an elapsed backoff) for workerID and marks it processing.
//
// Ordering is "ORDER BY created_at ASC, rowid ASC": created_at is stored
// with one-second resolution (see sqliteTimeLayout), so two jobs enqueued
// within the same wall-clock second would otherwise tie and fall back to
// whatever order SQLite happens to return them in. rowid is SQLite's
// implicit, monotonically increasing insertion-order column (present on any
// table, like jobs, that isn't declared WITHOUT ROWID) - ordering by it as a
// tiebreaker makes same-second claims deterministic and actually
// chronological, which job id itself is not: enqueue generates a random hex
// id when none is given, so sorting by id would be arbitrary rather than
// insertion-ordered.
//
// It uses a raw "BEGIN IMMEDIATE" transaction instead of db.BeginTx because
// Go's database/sql package has no way to request SQLite's IMMEDIATE
// transaction mode: BeginTx's sql.TxOptions only configures isolation level
// and read-only mode, neither of which maps onto SQLite's DEFERRED /
// IMMEDIATE / EXCLUSIVE distinction. BEGIN IMMEDIATE acquires the reserved
// write lock up front, so two workers racing to claim a job serialize at
// BEGIN rather than at the UPDATE, which is what makes the claim atomic
// under SetMaxOpenConns(1) and concurrent goroutines. See withImmediateTx.
func (s *Store) ClaimNextJob(ctx context.Context, workerID string) (job.Job, bool, error) {
	var claimed job.Job
	var ok bool

	err := s.withImmediateTx(ctx, func(ctx context.Context, conn *sql.Conn) error {
		row := conn.QueryRowContext(ctx, `
UPDATE jobs
SET
state = 'processing',
locked_by = ?,
locked_at = CURRENT_TIMESTAMP,
locked_pgid = NULL,
updated_at = CURRENT_TIMESTAMP
WHERE id = (
SELECT id
FROM jobs
WHERE state IN ('pending', 'failed')
AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
ORDER BY created_at ASC, rowid ASC
LIMIT 1
)
RETURNING `+jobColumns+`;`, workerID)

		j, err := scanJob(row)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("claim job: %w", err)
		}
		claimed = j
		ok = true
		return nil
	})
	if err != nil {
		return job.Job{}, false, err
	}
	return claimed, ok, nil
}

// withImmediateTx runs fn inside a raw "BEGIN IMMEDIATE" / COMMIT transaction
// on a dedicated connection, rolling back automatically if fn returns an
// error or panics. See ClaimNextJob for why IMMEDIATE is requested via raw
// SQL instead of db.BeginTx.
func (s *Store) withImmediateTx(ctx context.Context, fn func(ctx context.Context, conn *sql.Conn) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get sqlite connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return fmt.Errorf("begin immediate transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK;")
		}
	}()

	if err := fn(ctx, conn); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return fmt.Errorf("commit immediate transaction: %w", err)
	}
	committed = true
	return nil
}
