package storage

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDSNPragmasApplyToEveryConnection is the test that would have caught the
// original bug. busy_timeout and foreign_keys are per-*connection* state, so
// setting them with a "PRAGMA ..." statement after opening configures only
// the one connection that statement ran on. Every other connection the pool
// opens - including a replacement for one database/sql decided was bad - came
// up with busy_timeout=0, which is what turns two queuectl processes hitting
// the same database into hard "database is locked" failures instead of a
// short wait.
//
// The pool is deliberately widened here: Open caps it at one connection,
// which masked the bug rather than fixing it.
func TestDSNPragmasApplyToEveryConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")

	// Create the database through Open first, exactly as any queuectl process
	// would. Opening several connections against a database that does not yet
	// exist is a different (and pre-existing) race: converting a fresh
	// database to WAL takes an exclusive lock, and SQLite does not run the
	// busy handler for that conversion, so simultaneous first-openers can get
	// SQLITE_BUSY no matter how generous busy_timeout is. Store caps its pool
	// at one connection, so it cannot hit that from within a process.
	seed, err := Open(context.Background(), path)
	require.NoError(t, err)
	require.NoError(t, seed.Close())

	db, err := sql.Open("sqlite", buildDSN(path))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(4)

	const connections = 4
	results := make([]map[string]string, connections)
	errs := make([]error, connections)
	var wg sync.WaitGroup
	ready := make(chan struct{}, connections)
	release := make(chan struct{})
	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := db.Conn(context.Background())
			if err != nil {
				errs[i] = err
				ready <- struct{}{}
				return
			}
			defer conn.Close()

			// Hold every connection open at once, so the pool is forced to
			// open four distinct ones rather than handing the same one out
			// four times in sequence.
			ready <- struct{}{}
			<-release

			values := map[string]string{}
			for _, pragma := range []string{"busy_timeout", "foreign_keys", "journal_mode"} {
				var value string
				if err := conn.QueryRowContext(context.Background(), "PRAGMA "+pragma+";").Scan(&value); err != nil {
					errs[i] = err
					return
				}
				values[pragma] = value
			}
			results[i] = values
		}(i)
	}
	for i := 0; i < connections; i++ {
		<-ready
	}
	close(release)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "connection %d", i)
	}

	for i, values := range results {
		require.NotNil(t, values, "connection %d returned no pragma values", i)
		require.Equal(t, "5000", values["busy_timeout"], "connection %d opened without a busy timeout", i)
		require.Equal(t, "1", values["foreign_keys"], "connection %d opened without foreign keys", i)
		require.Equal(t, "wal", values["journal_mode"], "connection %d is not in WAL mode", i)
	}
}

// A database path containing a character that is significant in a URL (a
// space, here) must still open: the DSN is a URL, so the path has to be
// escaped rather than concatenated in raw.
func TestOpenHandlesPathNeedingEscaping(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue ctl data")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	store, err := Open(context.Background(), filepath.Join(dir, "queue.db"))
	require.NoError(t, err)
	defer store.Close()

	value, err := store.GetConfigInt(context.Background(), "max-retries")
	require.NoError(t, err, "a database under a path with a space must be usable, not just openable")
	require.Positive(t, value)
}
