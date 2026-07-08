package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	appconfig "queuectl/internal/config"
)

func migrate(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
id TEXT PRIMARY KEY,
command TEXT NOT NULL,
state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'completed', 'failed', 'dead')),
attempts INTEGER NOT NULL DEFAULT 0,
max_retries INTEGER NOT NULL,
next_retry_at DATETIME,
locked_by TEXT,
locked_at DATETIME,
locked_pgid INTEGER,
created_at DATETIME NOT NULL,
updated_at DATETIME NOT NULL
);`,
		`CREATE TABLE IF NOT EXISTS config (
key TEXT PRIMARY KEY,
value TEXT NOT NULL,
updated_at DATETIME NOT NULL
);`,
		`CREATE TABLE IF NOT EXISTS workers (
worker_id TEXT PRIMARY KEY,
pid INTEGER NOT NULL,
hostname TEXT NOT NULL,
started_at DATETIME NOT NULL,
last_heartbeat DATETIME NOT NULL
);`,
		`CREATE TABLE IF NOT EXISTS job_runs (
id INTEGER PRIMARY KEY AUTOINCREMENT,
job_id TEXT NOT NULL,
worker_id TEXT NOT NULL,
attempt INTEGER NOT NULL,
exit_code INTEGER,
stdout TEXT,
stderr TEXT,
started_at DATETIME NOT NULL,
finished_at DATETIME,
FOREIGN KEY (job_id) REFERENCES jobs(id)
);`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON jobs(state, next_retry_at, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers(last_heartbeat);`,
		`CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);`,
	}

	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("run migration: %w", err)
		}
	}

	// jobs.locked_pgid was added after the initial schema; CREATE TABLE IF
	// NOT EXISTS above is a no-op against a database created before this
	// column existed, so it needs an explicit additive migration.
	if err := ensureColumn(ctx, db, "jobs", "locked_pgid", "INTEGER"); err != nil {
		return err
	}

	now := formatTime(time.Now())
	for key, value := range appconfig.Defaults {
		if _, err := db.ExecContext(ctx, `
INSERT OR IGNORE INTO config(key, value, updated_at)
VALUES (?, ?, ?);`, key, strconv.Itoa(value), now); err != nil {
			return fmt.Errorf("insert default config %s: %w", key, err)
		}
	}
	return nil
}

// ensureColumn adds column to table if it is not already present, using
// PRAGMA table_info to check first since SQLite has no "ADD COLUMN IF NOT
// EXISTS". table and column are always package-internal constants, never
// user input, so building the DDL by string formatting is safe here (SQLite
// does not support binding identifiers as query parameters).
func ensureColumn(ctx context.Context, db *sql.DB, table string, column string, columnType string) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &pk); err != nil {
			return fmt.Errorf("scan %s column info: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s column info: %w", table, err)
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, columnType)); err != nil {
		return fmt.Errorf("add column %s.%s: %w", table, column, err)
	}
	return nil
}
