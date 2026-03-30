package store

import (
	"database/sql"
	"testing"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// New — schema error path
// ---------------------------------------------------------------------------

func TestCoverage_New_Bad_SchemaConflict(t *testing.T) {
	// Pre-create a database with an INDEX named "entries". When New() runs
	// CREATE TABLE IF NOT EXISTS entries, SQLite returns an error because the
	// name "entries" is already taken by the index.
	dbPath := testPath(t, "conflict.db")

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE dummy (id INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE INDEX entries ON dummy(id)")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = New(dbPath)
	require.Error(t, err, "New should fail when an index named entries already exists")
	assert.Contains(t, err.Error(), "store.New: schema")
}

// ---------------------------------------------------------------------------
// GetAll — scan error path
// ---------------------------------------------------------------------------

func TestCoverage_GetAll_Bad_ScanError(t *testing.T) {
	// Trigger a scan error by inserting a row with a NULL key. The production
	// code scans into plain strings, which cannot represent NULL.
	s, err := New(":memory:")
	require.NoError(t, err)
	defer s.Close()

	// Insert a normal row first so the query returns results.
	require.NoError(t, s.Set("g", "good", "value"))

	// Restructure the table to allow NULLs, then insert a NULL-key row.
	_, err = s.database.Exec("ALTER TABLE entries RENAME TO entries_backup")
	require.NoError(t, err)
	_, err = s.database.Exec(`CREATE TABLE entries (
		group_name  TEXT,
		entry_key   TEXT,
		entry_value TEXT,
		expires_at  INTEGER
	)`)
	require.NoError(t, err)
	_, err = s.database.Exec("INSERT INTO entries SELECT * FROM entries_backup")
	require.NoError(t, err)
	_, err = s.database.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES ('g', NULL, 'null-key-val')")
	require.NoError(t, err)
	_, err = s.database.Exec("DROP TABLE entries_backup")
	require.NoError(t, err)

	_, err = s.GetAll("g")
	require.Error(t, err, "GetAll should fail when a row contains a NULL key")
	assert.Contains(t, err.Error(), "store.All: scan")
}

// ---------------------------------------------------------------------------
// GetAll — rows iteration error path
// ---------------------------------------------------------------------------

func TestCoverage_GetAll_Bad_RowsError(t *testing.T) {
	// Trigger rows.Err() by corrupting the database file so that iteration
	// starts successfully but encounters a malformed page mid-scan.
	dbPath := testPath(t, "corrupt-getall.db")

	s, err := New(dbPath)
	require.NoError(t, err)

	// Insert enough rows to span multiple database pages.
	const rows = 5000
	for i := range rows {
		require.NoError(t, s.Set("g",
			core.Sprintf("key-%06d", i),
			core.Sprintf("value-with-padding-%06d-xxxxxxxxxxxxxxxxxxxxxxxx", i)))
	}
	s.Close()

	// Force a WAL checkpoint so all data is in the main database file.
	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	_, err = raw.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	// Corrupt data pages in the latter portion of the file (skip the first
	// pages which hold the schema).
	data := requireCoreReadBytes(t, dbPath)
	garbage := make([]byte, 4096)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	require.Greater(t, len(data), len(garbage)*2, "DB should be large enough to corrupt")
	offset := len(data) * 3 / 4
	maxOffset := len(data) - (len(garbage) * 2)
	if offset > maxOffset {
		offset = maxOffset
	}
	copy(data[offset:offset+len(garbage)], garbage)
	copy(data[offset+len(garbage):offset+(len(garbage)*2)], garbage)
	requireCoreWriteBytes(t, dbPath, data)

	// Remove WAL/SHM so the reopened connection reads from the main file.
	_ = testFilesystem().Delete(dbPath + "-wal")
	_ = testFilesystem().Delete(dbPath + "-shm")

	s2, err := New(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	_, err = s2.GetAll("g")
	require.Error(t, err, "GetAll should fail on corrupted database pages")
	assert.Contains(t, err.Error(), "store.All: rows")
}

// ---------------------------------------------------------------------------
// Render — scan error path
// ---------------------------------------------------------------------------

func TestCoverage_Render_Bad_ScanError(t *testing.T) {
	// Same NULL-key technique as TestCoverage_GetAll_Bad_ScanError.
	s, err := New(":memory:")
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Set("g", "good", "value"))

	_, err = s.database.Exec("ALTER TABLE entries RENAME TO entries_backup")
	require.NoError(t, err)
	_, err = s.database.Exec(`CREATE TABLE entries (
		group_name  TEXT,
		entry_key   TEXT,
		entry_value TEXT,
		expires_at  INTEGER
	)`)
	require.NoError(t, err)
	_, err = s.database.Exec("INSERT INTO entries SELECT * FROM entries_backup")
	require.NoError(t, err)
	_, err = s.database.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES ('g', NULL, 'null-key-val')")
	require.NoError(t, err)
	_, err = s.database.Exec("DROP TABLE entries_backup")
	require.NoError(t, err)

	_, err = s.Render("{{ .good }}", "g")
	require.Error(t, err, "Render should fail when a row contains a NULL key")
	assert.Contains(t, err.Error(), "store.All: scan")
}

// ---------------------------------------------------------------------------
// Render — rows iteration error path
// ---------------------------------------------------------------------------

func TestCoverage_Render_Bad_RowsError(t *testing.T) {
	// Same corruption technique as TestCoverage_GetAll_Bad_RowsError.
	dbPath := testPath(t, "corrupt-render.db")

	s, err := New(dbPath)
	require.NoError(t, err)

	const rows = 5000
	for i := range rows {
		require.NoError(t, s.Set("g",
			core.Sprintf("key-%06d", i),
			core.Sprintf("value-with-padding-%06d-xxxxxxxxxxxxxxxxxxxxxxxx", i)))
	}
	s.Close()

	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	_, err = raw.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	data := requireCoreReadBytes(t, dbPath)
	garbage := make([]byte, 4096)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	require.Greater(t, len(data), len(garbage)*2, "DB should be large enough to corrupt")
	offset := len(data) * 3 / 4
	maxOffset := len(data) - (len(garbage) * 2)
	if offset > maxOffset {
		offset = maxOffset
	}
	copy(data[offset:offset+len(garbage)], garbage)
	copy(data[offset+len(garbage):offset+(len(garbage)*2)], garbage)
	requireCoreWriteBytes(t, dbPath, data)

	_ = testFilesystem().Delete(dbPath + "-wal")
	_ = testFilesystem().Delete(dbPath + "-shm")

	s2, err := New(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	_, err = s2.Render("{{ . }}", "g")
	require.Error(t, err, "Render should fail on corrupted database pages")
	assert.Contains(t, err.Error(), "store.All: rows")
}
