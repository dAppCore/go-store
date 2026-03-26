package store

import (
	"database/sql"
	"os"
	"testing"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// New — schema error path
// ---------------------------------------------------------------------------

func TestNew_Bad_SchemaConflict(t *testing.T) {
	// Pre-create a database with an INDEX named "kv". When New() runs
	// CREATE TABLE IF NOT EXISTS kv, SQLite returns an error because the
	// name "kv" is already taken by the index.
	dir := t.TempDir()
	dbPath := core.JoinPath(dir, "conflict.db")

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE dummy (id INTEGER)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE INDEX kv ON dummy(id)")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = New(dbPath)
	require.Error(t, err, "New should fail when an index named kv already exists")
	assert.Contains(t, err.Error(), "store.New: schema")
}

// ---------------------------------------------------------------------------
// GetAll — scan error path
// ---------------------------------------------------------------------------

func TestGetAll_Bad_ScanError(t *testing.T) {
	// Trigger a scan error by inserting a row with a NULL key. The production
	// code scans into plain strings, which cannot represent NULL.
	s, err := New(":memory:")
	require.NoError(t, err)
	defer s.Close()

	// Insert a normal row first so the query returns results.
	require.NoError(t, s.Set("g", "good", "value"))

	// Restructure the table to allow NULLs, then insert a NULL-key row.
	_, err = s.db.Exec("ALTER TABLE kv RENAME TO kv_backup")
	require.NoError(t, err)
	_, err = s.db.Exec(`CREATE TABLE kv (
		grp        TEXT,
		key        TEXT,
		value      TEXT,
		expires_at INTEGER
	)`)
	require.NoError(t, err)
	_, err = s.db.Exec("INSERT INTO kv SELECT * FROM kv_backup")
	require.NoError(t, err)
	_, err = s.db.Exec("INSERT INTO kv (grp, key, value) VALUES ('g', NULL, 'null-key-val')")
	require.NoError(t, err)
	_, err = s.db.Exec("DROP TABLE kv_backup")
	require.NoError(t, err)

	_, err = s.GetAll("g")
	require.Error(t, err, "GetAll should fail when a row contains a NULL key")
	assert.Contains(t, err.Error(), "store.All: scan")
}

// ---------------------------------------------------------------------------
// GetAll — rows iteration error path
// ---------------------------------------------------------------------------

func TestGetAll_Bad_RowsError(t *testing.T) {
	// Trigger rows.Err() by corrupting the database file so that iteration
	// starts successfully but encounters a malformed page mid-scan.
	dir := t.TempDir()
	dbPath := core.JoinPath(dir, "corrupt-getall.db")

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
	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(16384), "DB should be large enough to corrupt")

	f, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
	require.NoError(t, err)
	garbage := make([]byte, 4096)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	offset := info.Size() * 3 / 4
	_, err = f.WriteAt(garbage, offset)
	require.NoError(t, err)
	_, err = f.WriteAt(garbage, offset+4096)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Remove WAL/SHM so the reopened connection reads from the main file.
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")

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

func TestRender_Bad_ScanError(t *testing.T) {
	// Same NULL-key technique as TestGetAll_Bad_ScanError.
	s, err := New(":memory:")
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Set("g", "good", "value"))

	_, err = s.db.Exec("ALTER TABLE kv RENAME TO kv_backup")
	require.NoError(t, err)
	_, err = s.db.Exec(`CREATE TABLE kv (
		grp        TEXT,
		key        TEXT,
		value      TEXT,
		expires_at INTEGER
	)`)
	require.NoError(t, err)
	_, err = s.db.Exec("INSERT INTO kv SELECT * FROM kv_backup")
	require.NoError(t, err)
	_, err = s.db.Exec("INSERT INTO kv (grp, key, value) VALUES ('g', NULL, 'null-key-val')")
	require.NoError(t, err)
	_, err = s.db.Exec("DROP TABLE kv_backup")
	require.NoError(t, err)

	_, err = s.Render("{{ .good }}", "g")
	require.Error(t, err, "Render should fail when a row contains a NULL key")
	assert.Contains(t, err.Error(), "store.All: scan")
}

// ---------------------------------------------------------------------------
// Render — rows iteration error path
// ---------------------------------------------------------------------------

func TestRender_Bad_RowsError(t *testing.T) {
	// Same corruption technique as TestGetAll_Bad_RowsError.
	dir := t.TempDir()
	dbPath := core.JoinPath(dir, "corrupt-render.db")

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

	info, err := os.Stat(dbPath)
	require.NoError(t, err)

	f, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
	require.NoError(t, err)
	garbage := make([]byte, 4096)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	offset := info.Size() * 3 / 4
	_, err = f.WriteAt(garbage, offset)
	require.NoError(t, err)
	_, err = f.WriteAt(garbage, offset+4096)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")

	s2, err := New(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	_, err = s2.Render("{{ . }}", "g")
	require.Error(t, err, "Render should fail on corrupted database pages")
	assert.Contains(t, err.Error(), "store.All: rows")
}
