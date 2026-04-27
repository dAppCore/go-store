package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"
	"testing"

	core "dappco.re/go/core"
)

// ---------------------------------------------------------------------------
// New — schema error path
// ---------------------------------------------------------------------------

func TestCoverage_New_Bad_SchemaConflict(t *testing.T) {
	// Pre-create a database with an INDEX named "entries". When New() runs
	// CREATE TABLE IF NOT EXISTS entries, SQLite returns an error because the
	// name "entries" is already taken by the index.
	databasePath := testPath(t, "conflict.db")

	database, err := sql.Open("sqlite", databasePath)
	assertNoError(t, err)
	database.SetMaxOpenConns(1)
	_, err = database.Exec("PRAGMA journal_mode=WAL")
	assertNoError(t, err)
	_, err = database.Exec("CREATE TABLE dummy (id INTEGER)")
	assertNoError(t, err)
	_, err = database.Exec("CREATE INDEX entries ON dummy(id)")
	assertNoError(t, err)
	assertNoError(t, database.Close())

	_, err = New(databasePath)
	assertError(t, err)
	assertContainsString(t, err.Error(), "store.New: ensure schema")
}

// ---------------------------------------------------------------------------
// GetAll — scan error path
// ---------------------------------------------------------------------------

func TestCoverage_GetAll_Bad_ScanError(t *testing.T) {
	// Trigger a scan error by inserting a row with a NULL key. The production
	// code scans into plain strings, which cannot represent NULL.
	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	// Insert a normal row first so the query returns results.
	assertNoError(t, storeInstance.Set("g", "good", "value"))

	// Restructure the table to allow NULLs, then insert a NULL-key row.
	_, err = storeInstance.sqliteDatabase.Exec("ALTER TABLE entries RENAME TO entries_backup")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(`CREATE TABLE entries (
		group_name  TEXT,
		entry_key   TEXT,
		entry_value TEXT,
		expires_at  INTEGER
	)`)
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("INSERT INTO entries SELECT * FROM entries_backup")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES ('g', NULL, 'null-key-val')")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("DROP TABLE entries_backup")
	assertNoError(t, err)

	_, err = storeInstance.GetAll("g")
	assertError(t, err)
	assertContainsString(t, err.Error(), "store.All: scan")
}

// ---------------------------------------------------------------------------
// GetAll — rows iteration error path
// ---------------------------------------------------------------------------

func TestCoverage_GetAll_Bad_RowsError(t *testing.T) {
	// Trigger rows.Err() by corrupting the database file so that iteration
	// starts successfully but encounters a malformed page mid-scan.
	databasePath := testPath(t, "corrupt-getall.db")

	storeInstance, err := New(databasePath)
	assertNoError(t, err)

	// Insert enough rows to span multiple database pages.
	const rows = 5000
	for i := range rows {
		assertNoError(t, storeInstance.Set("g", core.Sprintf("key-%06d", i), core.Sprintf("value-with-padding-%06d-xxxxxxxxxxxxxxxxxxxxxxxx", i)))
	}
	assertNoError(t, storeInstance.Close())
	// Force a WAL checkpoint so all data is in the main database file.
	rawDatabase, err := sql.Open("sqlite", databasePath)
	assertNoError(t, err)
	rawDatabase.SetMaxOpenConns(1)
	_, err = rawDatabase.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	assertNoError(t, err)
	assertNoError(t, rawDatabase.Close())

	// Corrupt data pages in the latter portion of the file (skip the first
	// pages which hold the schema).
	data := requireCoreReadBytes(t, databasePath)
	garbage := make([]byte, 4096)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	assertGreaterf(t, len(data), len(garbage)*2, "database file should be large enough to corrupt")
	offset := len(data) * 3 / 4
	maxOffset := len(data) - (len(garbage) * 2)
	if offset > maxOffset {
		offset = maxOffset
	}
	copy(data[offset:offset+len(garbage)], garbage)
	copy(data[offset+len(garbage):offset+(len(garbage)*2)], garbage)
	requireCoreWriteBytes(t, databasePath, data)

	// Remove WAL/SHM so the reopened connection reads from the main file.
	_ = testFilesystem().Delete(databasePath + "-wal")
	_ = testFilesystem().Delete(databasePath + "-shm")

	reopenedStore, err := New(databasePath)
	assertNoError(t, err)
	defer func() { _ = reopenedStore.Close() }()

	_, err = reopenedStore.GetAll("g")
	assertError(t, err)
	assertContainsString(t, err.Error(), "store.All: rows")
}

// ---------------------------------------------------------------------------
// Render — scan error path
// ---------------------------------------------------------------------------

func TestCoverage_Render_Bad_ScanError(t *testing.T) {
	// Same NULL-key technique as TestCoverage_GetAll_Bad_ScanError.
	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("g", "good", "value"))

	_, err = storeInstance.sqliteDatabase.Exec("ALTER TABLE entries RENAME TO entries_backup")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(`CREATE TABLE entries (
		group_name  TEXT,
		entry_key   TEXT,
		entry_value TEXT,
		expires_at  INTEGER
	)`)
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("INSERT INTO entries SELECT * FROM entries_backup")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES ('g', NULL, 'null-key-val')")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("DROP TABLE entries_backup")
	assertNoError(t, err)

	_, err = storeInstance.Render("{{ .good }}", "g")
	assertError(t, err)
	assertContainsString(t, err.Error(), "store.All: scan")
}

// ---------------------------------------------------------------------------
// Render — rows iteration error path
// ---------------------------------------------------------------------------

func TestCoverage_Render_Bad_RowsError(t *testing.T) {
	// Same corruption technique as TestCoverage_GetAll_Bad_RowsError.
	databasePath := testPath(t, "corrupt-render.db")

	storeInstance, err := New(databasePath)
	assertNoError(t, err)

	const rows = 5000
	for i := range rows {
		assertNoError(t, storeInstance.Set("g", core.Sprintf("key-%06d", i), core.Sprintf("value-with-padding-%06d-xxxxxxxxxxxxxxxxxxxxxxxx", i)))
	}
	assertNoError(t, storeInstance.Close())
	rawDatabase, err := sql.Open("sqlite", databasePath)
	assertNoError(t, err)
	rawDatabase.SetMaxOpenConns(1)
	_, err = rawDatabase.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	assertNoError(t, err)
	assertNoError(t, rawDatabase.Close())

	data := requireCoreReadBytes(t, databasePath)
	garbage := make([]byte, 4096)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	assertGreaterf(t, len(data), len(garbage)*2, "database file should be large enough to corrupt")
	offset := len(data) * 3 / 4
	maxOffset := len(data) - (len(garbage) * 2)
	if offset > maxOffset {
		offset = maxOffset
	}
	copy(data[offset:offset+len(garbage)], garbage)
	copy(data[offset+len(garbage):offset+(len(garbage)*2)], garbage)
	requireCoreWriteBytes(t, databasePath, data)

	_ = testFilesystem().Delete(databasePath + "-wal")
	_ = testFilesystem().Delete(databasePath + "-shm")

	reopenedStore, err := New(databasePath)
	assertNoError(t, err)
	defer func() { _ = reopenedStore.Close() }()

	_, err = reopenedStore.Render("{{ . }}", "g")
	assertError(t, err)
	assertContainsString(t, err.Error(), "store.All: rows")
}

// ---------------------------------------------------------------------------
// GroupsSeq — defensive error paths
// ---------------------------------------------------------------------------

func TestCoverage_GroupsSeq_Bad_ScanError(t *testing.T) {
	// Trigger a scan error by inserting a row with a NULL group name. The
	// production code scans into a plain string, which cannot represent NULL.
	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	_, err = storeInstance.sqliteDatabase.Exec("ALTER TABLE entries RENAME TO entries_backup")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(`CREATE TABLE entries (
		group_name  TEXT,
		entry_key   TEXT,
		entry_value TEXT,
		expires_at  INTEGER
	)`)
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("INSERT INTO entries SELECT * FROM entries_backup")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES (NULL, 'k', 'v')")
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec("DROP TABLE entries_backup")
	assertNoError(t, err)

	for groupName, iterationErr := range storeInstance.GroupsSeq("") {
		assertError(t, iterationErr)
		assertEmpty(t, groupName)
		break
	}
}

func TestCoverage_GroupsSeq_Bad_RowsError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		groupRows: [][]driver.Value{
			{"group-a"},
		},
		groupRowsErr:      core.E("stubSQLiteScenario", "rows iteration failed", nil),
		groupRowsErrIndex: 0,
	})
	defer func() { _ = database.Close() }()

	storeInstance := &Store{
		sqliteDatabase: database,
		cancelPurge:    func() {},
	}

	for groupName, iterationErr := range storeInstance.GroupsSeq("") {
		assertError(t, iterationErr)
		assertEmpty(t, groupName)
		break
	}
}

// ---------------------------------------------------------------------------
// ScopedStore bulk helpers — defensive error paths
// ---------------------------------------------------------------------------

func TestCoverage_ScopedStore_Bad_GroupsClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	assertNoError(t, storeInstance.Close())

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNotNil(t, scopedStore)

	_, err := scopedStore.Groups("")
	assertError(t, err)
	assertContainsString(t, err.Error(), "store.ScopedStore.Groups")
}

func TestCoverage_ScopedStore_Bad_GroupsSeqRowsError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		groupRows: [][]driver.Value{
			{"tenant-a:config"},
		},
		groupRowsErr:      core.E("stubSQLiteScenario", "rows iteration failed", nil),
		groupRowsErrIndex: 1,
	})
	defer func() { _ = database.Close() }()

	scopedStore := &ScopedStore{
		store: &Store{
			sqliteDatabase: database,
			cancelPurge:    func() {},
		},
		namespace: "tenant-a",
	}

	var seen []string
	for groupName, iterationErr := range scopedStore.GroupsSeq("") {
		if iterationErr != nil {
			assertError(t, iterationErr)
			assertEmpty(t, groupName)
			break
		}
		seen = append(seen, groupName)
	}
	assertEqual(t, []string{"config"}, seen)
}

// ---------------------------------------------------------------------------
// Stubbed SQLite driver coverage
// ---------------------------------------------------------------------------

func TestCoverage_EnsureSchema_Bad_TableExistsQueryError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableExistsErr: core.E("stubSQLiteScenario", "sqlite master query failed", nil),
	})
	defer func() { _ = database.Close() }()

	err := ensureSchema(database)
	assertError(t, err)
	assertContainsString(t, err.Error(), "sqlite master query failed")
}

func TestCoverage_EnsureSchema_Good_ExistingEntriesAndLegacyMigration(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableExistsFound: true,
		tableInfoRows: [][]driver.Value{
			{0, "expires_at", "INTEGER", 0, nil, 0},
		},
	})
	defer func() { _ = database.Close() }()

	assertNoError(t, ensureSchema(database))
}

func TestCoverage_EnsureSchema_Bad_ExpiryColumnQueryError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableExistsFound: true,
		tableInfoErr:     core.E("stubSQLiteScenario", "table_info query failed", nil),
	})
	defer func() { _ = database.Close() }()

	err := ensureSchema(database)
	assertError(t, err)
	assertContainsString(t, err.Error(), "table_info query failed")
}

func TestCoverage_EnsureSchema_Bad_MigrationError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableExistsFound: true,
		tableInfoRows: [][]driver.Value{
			{0, "expires_at", "INTEGER", 0, nil, 0},
		},
		insertErr: core.E("stubSQLiteScenario", "insert failed", nil),
	})
	defer func() { _ = database.Close() }()

	err := ensureSchema(database)
	assertError(t, err)
	assertContainsString(t, err.Error(), "insert failed")
}

func TestCoverage_EnsureSchema_Bad_MigrationCommitError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableExistsFound: true,
		tableInfoRows: [][]driver.Value{
			{0, "expires_at", "INTEGER", 0, nil, 0},
		},
		commitErr: core.E("stubSQLiteScenario", "commit failed", nil),
	})
	defer func() { _ = database.Close() }()

	err := ensureSchema(database)
	assertError(t, err)
	assertContainsString(t, err.Error(), "commit failed")
}

func TestCoverage_TableHasColumn_Bad_QueryError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableInfoErr: core.E("stubSQLiteScenario", "table_info query failed", nil),
	})
	defer func() { _ = database.Close() }()

	_, err := tableHasColumn(database, "entries", "expires_at")
	assertError(t, err)
	assertContainsString(t, err.Error(), "table_info query failed")
}

func TestCoverage_EnsureExpiryColumn_Good_DuplicateColumn(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableInfoRows: [][]driver.Value{
			{0, "entry_key", "TEXT", 1, nil, 0},
		},
		alterTableErr: core.E("stubSQLiteScenario", "duplicate column name: expires_at", nil),
	})
	defer func() { _ = database.Close() }()

	assertNoError(t, ensureExpiryColumn(database))
}

func TestCoverage_EnsureExpiryColumn_Bad_AlterTableError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableInfoRows: [][]driver.Value{
			{0, "entry_key", "TEXT", 1, nil, 0},
		},
		alterTableErr: core.E("stubSQLiteScenario", "permission denied", nil),
	})
	defer func() { _ = database.Close() }()

	err := ensureExpiryColumn(database)
	assertError(t, err)
	assertContainsString(t, err.Error(), "permission denied")
}

func TestCoverage_MigrateLegacyEntriesTable_Bad_InsertError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableInfoRows: [][]driver.Value{
			{0, "grp", "TEXT", 1, nil, 0},
		},
		insertErr: core.E("stubSQLiteScenario", "insert failed", nil),
	})
	defer func() { _ = database.Close() }()

	err := migrateLegacyEntriesTable(database)
	assertError(t, err)
	assertContainsString(t, err.Error(), "insert failed")
}

func TestCoverage_MigrateLegacyEntriesTable_Bad_BeginError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		beginErr: core.E("stubSQLiteScenario", "begin failed", nil),
	})
	defer func() { _ = database.Close() }()

	err := migrateLegacyEntriesTable(database)
	assertError(t, err)
	assertContainsString(t, err.Error(), "begin failed")
}

func TestCoverage_MigrateLegacyEntriesTable_Good_CreatesAndMigratesLegacyRows(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableInfoRows: [][]driver.Value{
			{0, "grp", "TEXT", 1, nil, 0},
		},
	})
	defer func() { _ = database.Close() }()

	assertNoError(t, migrateLegacyEntriesTable(database))
}

func TestCoverage_MigrateLegacyEntriesTable_Bad_TableInfoError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{
		tableInfoErr: core.E("stubSQLiteScenario", "table_info query failed", nil),
	})
	defer func() { _ = database.Close() }()

	err := migrateLegacyEntriesTable(database)
	assertError(t, err)
	assertContainsString(t, err.Error(), "table_info query failed")
}

type stubSQLiteScenario struct {
	tableExistsErr    error
	tableExistsFound  bool
	tableInfoErr      error
	tableInfoRows     [][]driver.Value
	groupRows         [][]driver.Value
	groupRowsErr      error
	groupRowsErrIndex int
	alterTableErr     error
	createTableErr    error
	insertErr         error
	dropTableErr      error
	beginErr          error
	commitErr         error
	rollbackErr       error
}

type stubSQLiteDriver struct{}

type stubSQLiteConn struct {
	scenario *stubSQLiteScenario
}

type stubSQLiteTx struct {
	scenario *stubSQLiteScenario
}

type stubSQLiteRows struct {
	columns      []string
	rows         [][]driver.Value
	index        int
	nextErr      error
	nextErrIndex int
}

type stubSQLiteResult struct{}

var (
	stubSQLiteDriverOnce sync.Once
	stubSQLiteScenarios  sync.Map
)

const stubSQLiteDriverName = "stub-sqlite"

func openStubSQLiteDatabase(t *testing.T, scenario stubSQLiteScenario) (*sql.DB, string) {
	t.Helper()

	stubSQLiteDriverOnce.Do(func() {
		sql.Register(stubSQLiteDriverName, stubSQLiteDriver{})
	})

	databasePath := t.Name()
	stubSQLiteScenarios.Store(databasePath, &scenario)
	t.Cleanup(func() {
		stubSQLiteScenarios.Delete(databasePath)
	})

	database, err := sql.Open(stubSQLiteDriverName, databasePath)
	assertNoError(t, err)
	return database, databasePath
}

func (stubSQLiteDriver) Open(databasePath string) (driver.Conn, error) {
	scenarioValue, ok := stubSQLiteScenarios.Load(databasePath)
	if !ok {
		return nil, core.E("stubSQLiteDriver.Open", "missing scenario", nil)
	}
	return &stubSQLiteConn{scenario: scenarioValue.(*stubSQLiteScenario)}, nil
}

func (conn *stubSQLiteConn) Prepare(query string) (driver.Stmt, error) {
	return nil, core.E("stubSQLiteConn.Prepare", "not implemented", nil)
}

func (conn *stubSQLiteConn) Close() error {
	return nil
}

func (conn *stubSQLiteConn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

func (conn *stubSQLiteConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if conn.scenario.beginErr != nil {
		return nil, conn.scenario.beginErr
	}
	return &stubSQLiteTx{scenario: conn.scenario}, nil
}

func (conn *stubSQLiteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch {
	case core.Contains(query, "ALTER TABLE entries ADD COLUMN expires_at INTEGER"):
		if conn.scenario.alterTableErr != nil {
			return nil, conn.scenario.alterTableErr
		}
	case core.Contains(query, "CREATE TABLE IF NOT EXISTS entries"):
		if conn.scenario.createTableErr != nil {
			return nil, conn.scenario.createTableErr
		}
	case core.Contains(query, "INSERT OR IGNORE INTO entries"):
		if conn.scenario.insertErr != nil {
			return nil, conn.scenario.insertErr
		}
	case core.Contains(query, "DROP TABLE kv"):
		if conn.scenario.dropTableErr != nil {
			return nil, conn.scenario.dropTableErr
		}
	}
	return stubSQLiteResult{}, nil
}

func (conn *stubSQLiteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch {
	case core.Contains(query, "sqlite_master"):
		if conn.scenario.tableExistsErr != nil {
			return nil, conn.scenario.tableExistsErr
		}
		if conn.scenario.tableExistsFound {
			return &stubSQLiteRows{
				columns: []string{"name"},
				rows:    [][]driver.Value{{"entries"}},
			}, nil
		}
		return &stubSQLiteRows{columns: []string{"name"}}, nil
	case core.Contains(query, "SELECT DISTINCT "+entryGroupColumn):
		return &stubSQLiteRows{
			columns:      []string{entryGroupColumn},
			rows:         conn.scenario.groupRows,
			nextErr:      conn.scenario.groupRowsErr,
			nextErrIndex: conn.scenario.groupRowsErrIndex,
		}, nil
	case core.HasPrefix(query, "PRAGMA table_info("):
		if conn.scenario.tableInfoErr != nil {
			return nil, conn.scenario.tableInfoErr
		}
		return &stubSQLiteRows{
			columns: []string{"cid", "name", "type", "notnull", "dflt_value", "pk"},
			rows:    conn.scenario.tableInfoRows,
		}, nil
	}
	return nil, core.E("stubSQLiteConn.QueryContext", "unexpected query", nil)
}

func (transaction *stubSQLiteTx) Commit() error {
	if transaction.scenario.commitErr != nil {
		return transaction.scenario.commitErr
	}
	return nil
}

func (transaction *stubSQLiteTx) Rollback() error {
	if transaction.scenario.rollbackErr != nil {
		return transaction.scenario.rollbackErr
	}
	return nil
}

func (rows *stubSQLiteRows) Columns() []string {
	return rows.columns
}

func (rows *stubSQLiteRows) Close() error {
	return nil
}

func (rows *stubSQLiteRows) Next(dest []driver.Value) error {
	if rows.nextErr != nil && rows.index == rows.nextErrIndex {
		rows.index++
		return rows.nextErr
	}
	if rows.index >= len(rows.rows) {
		return io.EOF
	}
	row := rows.rows[rows.index]
	rows.index++
	for i := range dest {
		dest[i] = nil
	}
	copy(dest, row)
	return nil
}

func (stubSQLiteResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (stubSQLiteResult) RowsAffected() (int64, error) {
	return 0, nil
}
