package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	appconfig "queuectl/internal/config"
)

// migrate applies the schema and default config rows inside a single BEGIN
// IMMEDIATE transaction. This matters beyond atomicity: ensureColumn below
// does a check-then-ALTER TABLE that isn't safe if two separate queuectl
// processes race to open the same pre-existing (pre-locked_pgid-column)
// database for the first time after an upgrade. Running the whole migration
// under BEGIN IMMEDIATE means the loser of that race blocks on SQLite's
// reserved lock until the winner commits, then re-reads PRAGMA table_info
// and sees the column already added - so it skips the ALTER instead of
// duplicating it and failing.
func migrate(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get sqlite connection for migration: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK;")
		}
	}()

	if err := runMigrationStatements(ctx, conn); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return fmt.Errorf("commit migration transaction: %w", err)
	}
	committed = true
	return nil
}

func runMigrationStatements(ctx context.Context, conn *sql.Conn) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
id TEXT PRIMARY KEY,
command TEXT NOT NULL,
state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'completed', 'failed', 'dead')),
attempts INTEGER NOT NULL DEFAULT 0,
max_retries INTEGER NOT NULL,
timeout_seconds INTEGER NOT NULL DEFAULT 0,
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
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("run migration: %w", err)
		}
	}

	// jobs.locked_pgid and jobs.timeout_seconds were both added after the
	// initial schema; CREATE TABLE IF NOT EXISTS above is a no-op against a
	// database created before these columns existed, so each needs an explicit
	// additive migration. timeout_seconds defaults to 0 (no timeout), so jobs
	// already queued in an older database keep their existing behavior.
	if err := ensureColumn(ctx, conn, "jobs", "locked_pgid", "INTEGER"); err != nil {
		return err
	}
	if err := ensureColumn(ctx, conn, "jobs", "timeout_seconds", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}

	now := formatTime(time.Now())
	for key, value := range appconfig.Defaults {
		if _, err := conn.ExecContext(ctx, `
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
// does not support binding identifiers as query parameters). Callers must
// run this inside the same BEGIN IMMEDIATE transaction as the rest of
// migrate so the check-then-ALTER isn't racy across processes (see migrate).
func ensureColumn(ctx context.Context, conn *sql.Conn, table string, column string, columnType string) error {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s);", table))
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

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, columnType)); err != nil {
		return fmt.Errorf("add column %s.%s: %w", table, column, err)
	}
	return nil
}
