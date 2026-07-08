package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"

	_ "modernc.org/sqlite"
)

const sqliteTimeLayout = "2006-01-02 15:04:05"

// ErrJobLockLost is returned by RecordJobSuccess/RecordJobFailure when the
// lock-fenced UPDATE affects zero rows, meaning another actor (typically
// the reaper recovering a stale lock) already changed the job's state or
// lock ownership since it was claimed.
var ErrJobLockLost = errors.New("job lock lost")

// Store wraps a SQLite database holding the jobs, job_runs, workers, and
// config tables. The underlying pool is intentionally limited to a single
// connection (SetMaxOpenConns(1)/SetMaxIdleConns(1) in Open) so that every
// caller shares one physical connection; SQLite serializes writers anyway,
// and this avoids relying on the CGo-free modernc.org/sqlite driver's
// behavior under concurrent connections.
type Store struct {
	db   *sql.DB
	path string
}

// JobRun is one execution attempt of a job, persisted to job_runs for
// auditing (stdout/stderr/exit code) regardless of whether the attempt
// succeeded or failed.
type JobRun struct {
	JobID      string
	WorkerID   string
	Attempt    int
	ExitCode   *int
	Stdout     string
	Stderr     string
	StartedAt  time.Time
	FinishedAt time.Time
}

// Open creates the database file and parent directory if needed, applies
// the production SQLite pragmas (WAL, busy_timeout, synchronous, foreign
// keys), and runs migrations. The connection pool is capped at a single
// connection (see the Store field docs) since modernc.org/sqlite is a
// CGo-free driver and SQLite serializes writes regardless.
func Open(ctx context.Context, path string) (*Store, error) {
	if err := appconfig.EnsureParentDir(path); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &Store{db: db, path: path}
	if err := store.applyPragmas(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

// Close closes the underlying database connection pool.
func (s *Store) Close() error {
	return s.db.Close()
}

// Path returns the filesystem path the Store was opened with.
func (s *Store) Path() string {
	return s.path
}

func (s *Store) applyPragmas(ctx context.Context) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA foreign_keys = ON;",
	}
	for _, pragma := range pragmas {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("apply %s: %w", strings.TrimSuffix(pragma, ";"), err)
		}
	}
	return nil
}

// InsertJob persists a new job row. Callers should treat a "constraint"
// error as a duplicate ID (jobs.id is the primary key).
func (s *Store) InsertJob(ctx context.Context, j job.Job) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO jobs(id, command, state, attempts, max_retries, next_retry_at, locked_by, locked_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		j.ID,
		j.Command,
		string(j.State),
		j.Attempts,
		j.MaxRetries,
		nullableTime(j.NextRetryAt),
		nullableString(j.LockedBy),
		nullableTime(j.LockedAt),
		formatTime(j.CreatedAt),
		formatTime(j.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// GetJob fetches a single job by ID.
func (s *Store) GetJob(ctx context.Context, id string) (job.Job, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, command, state, attempts, max_retries, next_retry_at, locked_by, locked_at, created_at, updated_at
FROM jobs WHERE id = ?;`, id)
	return scanJob(row)
}

// ListJobs returns every job in the given state, oldest first.
func (s *Store) ListJobs(ctx context.Context, state job.State) ([]job.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, command, state, attempts, max_retries, next_retry_at, locked_by, locked_at, created_at, updated_at
FROM jobs
WHERE state = ?
ORDER BY created_at ASC, id ASC;`, string(state))
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows)
}

// CountJobsByState returns the number of jobs in each state, including
// zero counts for states with no jobs.
func (s *Store) CountJobsByState(ctx context.Context) (map[job.State]int, error) {
	counts := make(map[job.State]int)
	for _, state := range job.AllStates() {
		counts[state] = 0
	}

	rows, err := s.db.QueryContext(ctx, `SELECT state, COUNT(*) FROM jobs GROUP BY state;`)
	if err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var raw string
		var count int
		if err := rows.Scan(&raw, &count); err != nil {
			return nil, fmt.Errorf("scan job count: %w", err)
		}
		state, err := job.ParseState(raw)
		if err != nil {
			return nil, err
		}
		counts[state] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job counts: %w", err)
	}
	return counts, nil
}

// GetConfigInt reads a single config value. It returns an error if key is
// not a known config key (all keys are seeded with defaults by migrate).
func (s *Store) GetConfigInt(ctx context.Context, key string) (int, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM config WHERE key = ?;`, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("missing config key %q", key)
	}
	if err != nil {
		return 0, fmt.Errorf("read config %s: %w", key, err)
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config %s is not an integer: %w", key, err)
	}
	return value, nil
}

// SetConfigInt updates a config value. It returns an error if key does not
// already exist in the config table, so callers must validate the key
// (see appconfig.ValidateConfigValue) before calling this.
func (s *Store) SetConfigInt(ctx context.Context, key string, value int) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE config
SET value = ?, updated_at = ?
WHERE key = ?;`, strconv.Itoa(value), formatTime(time.Now()), key)
	if err != nil {
		return fmt.Errorf("set config %s: %w", key, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check config update: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("unknown config key %q", key)
	}
	return nil
}

// CountActiveWorkers returns the number of registered workers whose last
// heartbeat is newer than staleSeconds ago.
func (s *Store) CountActiveWorkers(ctx context.Context, staleSeconds int) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM workers
WHERE last_heartbeat > datetime('now', '-' || ? || ' seconds');`, staleSeconds).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active workers: %w", err)
	}
	return count, nil
}

// RegisterWorker upserts a worker row with a fresh heartbeat timestamp.
func (s *Store) RegisterWorker(ctx context.Context, workerID string, pid int, hostname string) error {
	now := formatTime(time.Now())
	_, err := s.db.ExecContext(ctx, `
INSERT OR REPLACE INTO workers(worker_id, pid, hostname, started_at, last_heartbeat)
VALUES (?, ?, ?, ?, ?);`, workerID, pid, hostname, now, now)
	if err != nil {
		return fmt.Errorf("register worker %s: %w", workerID, err)
	}
	return nil
}

// HeartbeatWorker refreshes last_heartbeat for workerID.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE workers
SET last_heartbeat = CURRENT_TIMESTAMP
WHERE worker_id = ?;`, workerID)
	if err != nil {
		return fmt.Errorf("heartbeat worker %s: %w", workerID, err)
	}
	return nil
}

// DeleteWorker removes a worker row, called on graceful worker shutdown.
func (s *Store) DeleteWorker(ctx context.Context, workerID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM workers WHERE worker_id = ?;`, workerID)
	if err != nil {
		return fmt.Errorf("delete worker %s: %w", workerID, err)
	}
	return nil
}

// RenewJobLock extends a held lock's locked_at to now, fenced on the job
// still being processing and locked by workerID. The second return value
// is false (with no error) if the fence didn't match, meaning the lock was
// lost to the reaper; callers must stop treating the job as owned.
func (s *Store) RenewJobLock(ctx context.Context, jobID string, workerID string) (time.Time, bool, error) {
	var rawLockedAt string
	err := s.db.QueryRowContext(ctx, `
UPDATE jobs
SET locked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
WHERE id = ? AND state = 'processing' AND locked_by = ?
RETURNING locked_at;`, jobID, workerID).Scan(&rawLockedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("renew job lock %s: %w", jobID, err)
	}
	lockedAt, err := parseTime(rawLockedAt)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse renewed lock for job %s: %w", jobID, err)
	}
	return lockedAt, true, nil
}

// RecordJobSuccess inserts the job_runs row for a completed attempt and
// marks the job completed in a single BEGIN IMMEDIATE transaction. The
// UPDATE is fenced on state = 'processing' AND locked_by/locked_at matching
// j's lock, so if the reaper already reclaimed the job out from under a
// slow worker, the update affects zero rows and this returns
// ErrJobLockLost instead of clobbering the newer claim.
func (s *Store) RecordJobSuccess(ctx context.Context, j job.Job, run JobRun) error {
	attempt := j.Attempts + 1
	run.Attempt = attempt
	run.JobID = j.ID
	lockedBy, lockedAt, err := jobLockFence(j)
	if err != nil {
		return err
	}

	return s.withImmediateTx(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := insertJobRun(ctx, conn, run); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `
UPDATE jobs
SET attempts = ?, state = 'completed', next_retry_at = NULL, locked_by = NULL, locked_at = NULL, updated_at = ?
WHERE id = ? AND state = 'processing' AND locked_by = ? AND locked_at = ?;`,
			attempt, formatTime(time.Now()), j.ID, lockedBy, lockedAt)
		if err != nil {
			return fmt.Errorf("mark job completed: %w", err)
		}
		return requireRowsAffected(result, "mark job completed")
	})
}

// RecordJobFailure inserts the job_runs row for a failed attempt and moves
// the job to nextState (failed with a backoff, or dead once retries are
// exhausted) in a single BEGIN IMMEDIATE transaction. Like
// RecordJobSuccess, the UPDATE is fenced on j's lock and returns
// ErrJobLockLost if the lock was lost to the reaper before this call.
func (s *Store) RecordJobFailure(ctx context.Context, j job.Job, run JobRun, nextState job.State, nextRetryAt *time.Time) error {
	attempt := j.Attempts + 1
	run.Attempt = attempt
	run.JobID = j.ID
	lockedBy, lockedAt, err := jobLockFence(j)
	if err != nil {
		return err
	}

	return s.withImmediateTx(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := insertJobRun(ctx, conn, run); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `
UPDATE jobs
SET attempts = ?, state = ?, next_retry_at = ?, locked_by = NULL, locked_at = NULL, updated_at = ?
WHERE id = ? AND state = 'processing' AND locked_by = ? AND locked_at = ?;`,
			attempt, string(nextState), nullableTime(nextRetryAt), formatTime(time.Now()), j.ID, lockedBy, lockedAt)
		if err != nil {
			return fmt.Errorf("mark job failed: %w", err)
		}
		return requireRowsAffected(result, "mark job failed")
	})
}

// JobRunCounts returns the number of job_runs rows per job ID.
func (s *Store) JobRunCounts(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT job_id, COUNT(*) FROM job_runs GROUP BY job_id;`)
	if err != nil {
		return nil, fmt.Errorf("count job runs: %w", err)
	}
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var jobID string
		var count int
		if err := rows.Scan(&jobID, &count); err != nil {
			return nil, fmt.Errorf("scan job run count: %w", err)
		}
		counts[jobID] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job run counts: %w", err)
	}
	return counts, nil
}

func jobLockFence(j job.Job) (string, string, error) {
	if j.LockedBy == nil || j.LockedAt == nil {
		return "", "", fmt.Errorf("job %s has no active lock fence", j.ID)
	}
	return *j.LockedBy, formatTime(*j.LockedAt), nil
}

// execer is satisfied by both *sql.Tx and *sql.Conn, letting insertJobRun
// run inside either a database/sql transaction or a raw BEGIN IMMEDIATE
// connection (see withImmediateTx in claim.go).
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertJobRun(ctx context.Context, tx execer, run JobRun) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO job_runs(job_id, worker_id, attempt, exit_code, stdout, stderr, started_at, finished_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		run.JobID,
		run.WorkerID,
		run.Attempt,
		nullableInt(run.ExitCode),
		run.Stdout,
		run.Stderr,
		formatTime(run.StartedAt),
		formatTime(run.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("insert job run: %w", err)
	}
	return nil
}

func requireRowsAffected(result sql.Result, action string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s rows affected: %w", action, err)
	}
	if rows == 0 {
		return fmt.Errorf("%s: %w", action, ErrJobLockLost)
	}
	return nil
}

func scanJobs(rows *sql.Rows) ([]job.Job, error) {
	var jobs []job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}
	return jobs, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (job.Job, error) {
	var j job.Job
	var state string
	var nextRetryAt sql.NullString
	var lockedBy sql.NullString
	var lockedAt sql.NullString
	var createdAt string
	var updatedAt string

	if err := row.Scan(
		&j.ID,
		&j.Command,
		&state,
		&j.Attempts,
		&j.MaxRetries,
		&nextRetryAt,
		&lockedBy,
		&lockedAt,
		&createdAt,
		&updatedAt,
	); err != nil {
		return job.Job{}, err
	}

	parsedState, err := job.ParseState(state)
	if err != nil {
		return job.Job{}, err
	}
	j.State = parsedState
	if nextRetryAt.Valid {
		t, err := parseTime(nextRetryAt.String)
		if err != nil {
			return job.Job{}, fmt.Errorf("parse next_retry_at for job %s: %w", j.ID, err)
		}
		j.NextRetryAt = &t
	}
	if lockedBy.Valid {
		j.LockedBy = &lockedBy.String
	}
	if lockedAt.Valid {
		t, err := parseTime(lockedAt.String)
		if err != nil {
			return job.Job{}, fmt.Errorf("parse locked_at for job %s: %w", j.ID, err)
		}
		j.LockedAt = &t
	}
	j.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return job.Job{}, fmt.Errorf("parse created_at for job %s: %w", j.ID, err)
	}
	j.UpdatedAt, err = parseTime(updatedAt)
	if err != nil {
		return job.Job{}, fmt.Errorf("parse updated_at for job %s: %w", j.ID, err)
	}
	return j, nil
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(sqliteTimeLayout)
}

func parseTime(raw string) (time.Time, error) {
	layouts := []string{
		sqliteTimeLayout,
		time.RFC3339,
		time.RFC3339Nano,
	}
	var lastErr error
	for _, layout := range layouts {
		if layout == sqliteTimeLayout {
			t, err := time.ParseInLocation(layout, raw, time.UTC)
			if err == nil {
				return t.UTC(), nil
			}
			lastErr = err
			continue
		}
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}
