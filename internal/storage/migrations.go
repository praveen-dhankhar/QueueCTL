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
