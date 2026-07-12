package storage

import (
	"context"
	"database/sql"
	"fmt"
)

// Metrics summarizes execution history across every recorded attempt in
// job_runs. Durations are in seconds.
//
// Interrupted attempts (a worker died mid-command, so the run has no exit
// code - see RecoverStaleJob) are counted in TotalRuns and Interrupted, but
// deliberately excluded from Succeeded, Failed, and every duration figure: a
// run that was cut short by a SIGKILL says nothing about how long the command
// takes or whether it works, and averaging its truncated duration in would
// quietly skew the numbers this command exists to report.
type Metrics struct {
	TotalRuns   int     `json:"total_runs"`
	Succeeded   int     `json:"succeeded"`
	Failed      int     `json:"failed"`
	Interrupted int     `json:"interrupted"`
	SuccessRate float64 `json:"success_rate"`
	AvgSeconds  float64 `json:"avg_seconds"`
	P95Seconds  float64 `json:"p95_seconds"`
	MaxSeconds  float64 `json:"max_seconds"`
	Last1m      int     `json:"completed_last_1m"`
	Last5m      int     `json:"completed_last_5m"`
}

// runDurationSeconds is the SQL expression for a completed run's wall-clock
// duration. julianday() returns fractional days, so the multiplier converts
// to seconds; SQLite has no native duration type to subtract directly.
const runDurationSeconds = `(julianday(finished_at) - julianday(started_at)) * 86400.0`

// GetMetrics computes queue-wide execution metrics from job_runs.
func (s *Store) GetMetrics(ctx context.Context) (Metrics, error) {
	var m Metrics
	var avg sql.NullFloat64
	var max sql.NullFloat64

	err := s.db.QueryRowContext(ctx, `
SELECT
COUNT(*),
COUNT(CASE WHEN exit_code = 0 THEN 1 END),
COUNT(CASE WHEN exit_code IS NOT NULL AND exit_code != 0 THEN 1 END),
COUNT(CASE WHEN exit_code IS NULL THEN 1 END),
AVG(CASE WHEN exit_code IS NOT NULL THEN `+runDurationSeconds+` END),
MAX(CASE WHEN exit_code IS NOT NULL THEN `+runDurationSeconds+` END),
COUNT(CASE WHEN exit_code = 0 AND finished_at > datetime('now', '-60 seconds') THEN 1 END),
COUNT(CASE WHEN exit_code = 0 AND finished_at > datetime('now', '-300 seconds') THEN 1 END)
FROM job_runs;`).Scan(&m.TotalRuns, &m.Succeeded, &m.Failed, &m.Interrupted, &avg, &max, &m.Last1m, &m.Last5m)
	if err != nil {
		return Metrics{}, fmt.Errorf("read metrics: %w", err)
	}
	m.AvgSeconds = avg.Float64
	m.MaxSeconds = max.Float64

	if completed := m.Succeeded + m.Failed; completed > 0 {
		m.SuccessRate = float64(m.Succeeded) / float64(completed) * 100
	}

	// p95: order completed runs by duration and take the one at the 95th
	// percentile position. SQLite has no percentile function, so this is an
	// OFFSET into the sorted set - exact, and fine at this scale.
	//
	// The rank is the nearest-rank definition, ceil(0.95 * n), computed in
	// integer arithmetic as (95n + 99) / 100 and then turned into a 0-based
	// OFFSET by subtracting one. Flooring instead (95n / 100) is off by one
	// for every n that isn't a multiple of 20 and, at small n, lands below
	// the mean: with four runs of 0s, 0s, 0s, 1s it selected offset 2 and
	// reported a p95 of 0.00 next to an avg of 0.25, which is not a
	// percentile at all. MAX(0, ...) keeps the offset in range for n = 0.
	//
	// ponytail: O(n log n) sort of every run on each call. Add an index on
	// the duration expression, or keep a rolling aggregate, only if job_runs
	// ever grows big enough for this to show up.
	var p95 sql.NullFloat64
	err = s.db.QueryRowContext(ctx, `
SELECT `+runDurationSeconds+` AS duration
FROM job_runs
WHERE exit_code IS NOT NULL
ORDER BY duration ASC
LIMIT 1
OFFSET MAX(0, (SELECT (COUNT(*) * 95 + 99) / 100 - 1 FROM job_runs WHERE exit_code IS NOT NULL));`).Scan(&p95)
	if err != nil && err != sql.ErrNoRows {
		return Metrics{}, fmt.Errorf("read p95 duration: %w", err)
	}
	m.P95Seconds = p95.Float64

	return m, nil
}
