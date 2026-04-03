package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"syscall"
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

func TestStore_New_Good_Memory(t *testing.T) {
	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	require.NotNil(t, storeInstance)
	defer storeInstance.Close()
}

func TestStore_New_Good_FileBacked(t *testing.T) {
	databasePath := testPath(t, "test.db")
	storeInstance, err := New(databasePath)
	require.NoError(t, err)
	require.NotNil(t, storeInstance)
	defer storeInstance.Close()

	// Verify data persists: write, close, reopen.
	require.NoError(t, storeInstance.Set("g", "k", "v"))
	require.NoError(t, storeInstance.Close())

	reopenedStore, err := New(databasePath)
	require.NoError(t, err)
	defer reopenedStore.Close()

	value, err := reopenedStore.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)
}

func TestStore_New_Bad_InvalidPath(t *testing.T) {
	// A path under a non-existent directory should fail at the WAL pragma step
	// because sql.Open is lazy and only validates on first use.
	_, err := New("/no/such/directory/test.db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.New")
}

func TestStore_New_Bad_CorruptFile(t *testing.T) {
	// A file that exists but is not a valid SQLite database should fail.
	databasePath := testPath(t, "corrupt.db")
	requireCoreOK(t, testFilesystem().Write(databasePath, "not a sqlite database"))

	_, err := New(databasePath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.New")
}

func TestStore_New_Bad_ReadOnlyDir(t *testing.T) {
	// A path in a read-only directory should fail when SQLite tries to create the WAL file.
	dir := t.TempDir()
	databasePath := core.Path(dir, "readonly.db")

	// Create a valid database first, then make the directory read-only.
	storeInstance, err := New(databasePath)
	require.NoError(t, err)
	require.NoError(t, storeInstance.Close())

	// Remove WAL/SHM files and make directory read-only.
	_ = testFilesystem().Delete(databasePath + "-wal")
	_ = testFilesystem().Delete(databasePath + "-shm")
	require.NoError(t, syscall.Chmod(dir, 0555))
	defer func() { _ = syscall.Chmod(dir, 0755) }() // restore for cleanup

	_, err = New(databasePath)
	// May or may not fail depending on OS/filesystem — just exercise the code path.
	if err != nil {
		assert.Contains(t, err.Error(), "store.New")
	}
}

func TestStore_New_Good_WALMode(t *testing.T) {
	databasePath := testPath(t, "wal.db")
	storeInstance, err := New(databasePath)
	require.NoError(t, err)
	defer storeInstance.Close()

	var mode string
	err = storeInstance.database.QueryRow("PRAGMA journal_mode").Scan(&mode)
	require.NoError(t, err)
	assert.Equal(t, "wal", mode, "journal_mode should be WAL")
}

func TestStore_New_Good_WithJournalOption(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	assert.Equal(t, "events", storeInstance.journalConfiguration.bucketName)
	assert.Equal(t, "core", storeInstance.journalConfiguration.organisation)
	assert.Equal(t, "http://127.0.0.1:8086", storeInstance.journalConfiguration.endpointURL)
}

func TestStore_JournalConfiguration_Good(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	config := storeInstance.JournalConfiguration()
	assert.Equal(t, JournalConfiguration{
		EndpointURL:  "http://127.0.0.1:8086",
		Organisation: "core",
		BucketName:   "events",
	}, config)
}

func TestStore_NewConfigured_Good(t *testing.T) {
	storeInstance, err := NewConfigured(StoreConfig{
		DatabasePath: ":memory:",
		Journal: JournalConfiguration{
			EndpointURL:  "http://127.0.0.1:8086",
			Organisation: "core",
			BucketName:   "events",
		},
		PurgeInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	defer storeInstance.Close()

	assert.Equal(t, JournalConfiguration{
		EndpointURL:  "http://127.0.0.1:8086",
		Organisation: "core",
		BucketName:   "events",
	}, storeInstance.JournalConfiguration())
	assert.Equal(t, 20*time.Millisecond, storeInstance.purgeInterval)

	require.NoError(t, storeInstance.Set("g", "k", "v"))
	value, err := storeInstance.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)
}

// ---------------------------------------------------------------------------
// Set / Get — core CRUD
// ---------------------------------------------------------------------------

func TestStore_SetGet_Good(t *testing.T) {
	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	err = storeInstance.Set("config", "theme", "dark")
	require.NoError(t, err)

	value, err := storeInstance.Get("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", value)
}

func TestStore_Set_Good_Upsert(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "k", "v1"))
	require.NoError(t, storeInstance.Set("g", "k", "v2"))

	value, err := storeInstance.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v2", value, "upsert should overwrite the value")

	count, err := storeInstance.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, count, "upsert should not duplicate keys")
}

func TestStore_Get_Bad_NotFound(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := storeInstance.Get("config", "missing")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError), "should wrap NotFoundError")
}

func TestStore_Get_Bad_NonExistentGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := storeInstance.Get("no-such-group", "key")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError))
}

func TestStore_Get_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	_, err := storeInstance.Get("g", "k")
	require.Error(t, err)
}

func TestStore_Set_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	err := storeInstance.Set("g", "k", "v")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestStore_Delete_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_ = storeInstance.Set("config", "key", "val")
	err := storeInstance.Delete("config", "key")
	require.NoError(t, err)

	_, err = storeInstance.Get("config", "key")
	assert.Error(t, err)
}

func TestStore_Delete_Good_NonExistent(t *testing.T) {
	// Deleting a key that does not exist should not error.
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	err := storeInstance.Delete("g", "nope")
	assert.NoError(t, err)
}

func TestStore_Delete_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	err := storeInstance.Delete("g", "k")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Count
// ---------------------------------------------------------------------------

func TestStore_Count_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_ = storeInstance.Set("grp", "a", "1")
	_ = storeInstance.Set("grp", "b", "2")
	_ = storeInstance.Set("other", "c", "3")

	count, err := storeInstance.Count("grp")
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestStore_Count_Good_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	count, err := storeInstance.Count("empty")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestStore_Count_Good_BulkInsert(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	const total = 500
	for i := range total {
		require.NoError(t, storeInstance.Set("bulk", core.Sprintf("key-%04d", i), "v"))
	}
	count, err := storeInstance.Count("bulk")
	require.NoError(t, err)
	assert.Equal(t, total, count)
}

func TestStore_Count_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	_, err := storeInstance.Count("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// DeleteGroup
// ---------------------------------------------------------------------------

func TestStore_DeleteGroup_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_ = storeInstance.Set("grp", "a", "1")
	_ = storeInstance.Set("grp", "b", "2")
	err := storeInstance.DeleteGroup("grp")
	require.NoError(t, err)

	count, _ := storeInstance.Count("grp")
	assert.Equal(t, 0, count)
}

func TestStore_DeleteGroup_Good_ThenGetAllEmpty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_ = storeInstance.Set("grp", "a", "1")
	_ = storeInstance.Set("grp", "b", "2")
	require.NoError(t, storeInstance.DeleteGroup("grp"))

	all, err := storeInstance.GetAll("grp")
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestStore_DeleteGroup_Good_IsolatesOtherGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_ = storeInstance.Set("a", "k", "1")
	_ = storeInstance.Set("b", "k", "2")
	require.NoError(t, storeInstance.DeleteGroup("a"))

	_, err := storeInstance.Get("a", "k")
	assert.Error(t, err)

	value, err := storeInstance.Get("b", "k")
	require.NoError(t, err)
	assert.Equal(t, "2", value, "other group should be untouched")
}

func TestStore_DeleteGroup_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	err := storeInstance.DeleteGroup("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// GetAll
// ---------------------------------------------------------------------------

func TestStore_GetAll_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_ = storeInstance.Set("grp", "a", "1")
	_ = storeInstance.Set("grp", "b", "2")
	_ = storeInstance.Set("other", "c", "3")

	all, err := storeInstance.GetAll("grp")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, all)
}

func TestStore_GetAll_Good_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	all, err := storeInstance.GetAll("empty")
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestStore_GetAll_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	_, err := storeInstance.GetAll("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// All / GroupsSeq
// ---------------------------------------------------------------------------

func TestStore_All_Good_StopsEarly(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "a", "1"))
	require.NoError(t, storeInstance.Set("g", "b", "2"))

	entries := storeInstance.All("g")
	var seen []string
	for entry, err := range entries {
		require.NoError(t, err)
		seen = append(seen, entry.Key)
		break
	}

	assert.Len(t, seen, 1)
}

func TestStore_All_Good_SortedByKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "charlie", "3"))
	require.NoError(t, storeInstance.Set("g", "alpha", "1"))
	require.NoError(t, storeInstance.Set("g", "bravo", "2"))

	var keys []string
	for entry, err := range storeInstance.All("g") {
		require.NoError(t, err)
		keys = append(keys, entry.Key)
	}

	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, keys)
}

func TestStore_AllSeq_Good_SortedByKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "charlie", "3"))
	require.NoError(t, storeInstance.Set("g", "alpha", "1"))
	require.NoError(t, storeInstance.Set("g", "bravo", "2"))

	var keys []string
	for entry, err := range storeInstance.AllSeq("g") {
		require.NoError(t, err)
		keys = append(keys, entry.Key)
	}

	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, keys)
}

func TestStore_All_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	for _, err := range storeInstance.All("g") {
		require.Error(t, err)
	}
}

func TestStore_GroupsSeq_Good_StopsEarly(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("alpha", "a", "1"))
	require.NoError(t, storeInstance.Set("beta", "b", "2"))

	groups := storeInstance.GroupsSeq("")
	var seen []string
	for group, err := range groups {
		require.NoError(t, err)
		seen = append(seen, group)
		break
	}

	assert.Len(t, seen, 1)
}

func TestStore_GroupsSeq_Good_PrefixStopsEarly(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("alpha", "a", "1"))
	require.NoError(t, storeInstance.Set("beta", "b", "2"))

	groups := storeInstance.GroupsSeq("alpha")
	var seen []string
	for group, err := range groups {
		require.NoError(t, err)
		seen = append(seen, group)
		break
	}

	assert.Equal(t, []string{"alpha"}, seen)
}

func TestStore_GroupsSeq_Good_SortedByGroupName(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("charlie", "c", "3"))
	require.NoError(t, storeInstance.Set("alpha", "a", "1"))
	require.NoError(t, storeInstance.Set("bravo", "b", "2"))

	var groups []string
	for group, err := range storeInstance.GroupsSeq("") {
		require.NoError(t, err)
		groups = append(groups, group)
	}

	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, groups)
}

func TestStore_GroupsSeq_Good_DefaultArgument(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("alpha", "a", "1"))
	require.NoError(t, storeInstance.Set("beta", "b", "2"))

	var groups []string
	for group, err := range storeInstance.GroupsSeq() {
		require.NoError(t, err)
		groups = append(groups, group)
	}

	assert.Equal(t, []string{"alpha", "beta"}, groups)
}

func TestStore_GroupsSeq_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	for _, err := range storeInstance.GroupsSeq("") {
		require.Error(t, err)
	}
}

// ---------------------------------------------------------------------------
// GetSplit / GetFields
// ---------------------------------------------------------------------------

func TestStore_GetSplit_Good_SplitsValue(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "comma", "alpha,beta,gamma"))

	parts, err := storeInstance.GetSplit("g", "comma", ",")
	require.NoError(t, err)

	var values []string
	for value := range parts {
		values = append(values, value)
	}

	assert.Equal(t, []string{"alpha", "beta", "gamma"}, values)
}

func TestStore_GetSplit_Good_StopsEarly(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "comma", "alpha,beta,gamma"))

	parts, err := storeInstance.GetSplit("g", "comma", ",")
	require.NoError(t, err)

	var values []string
	for value := range parts {
		values = append(values, value)
		break
	}

	assert.Equal(t, []string{"alpha"}, values)
}

func TestStore_GetSplit_Bad_MissingKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := storeInstance.GetSplit("g", "missing", ",")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError))
}

func TestStore_GetFields_Good_SplitsWhitespace(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "fields", "alpha beta\tgamma\n"))

	fields, err := storeInstance.GetFields("g", "fields")
	require.NoError(t, err)

	var values []string
	for value := range fields {
		values = append(values, value)
	}

	assert.Equal(t, []string{"alpha", "beta", "gamma"}, values)
}

func TestStore_GetFields_Good_StopsEarly(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "fields", "alpha beta\tgamma\n"))

	fields, err := storeInstance.GetFields("g", "fields")
	require.NoError(t, err)

	var values []string
	for value := range fields {
		values = append(values, value)
		break
	}

	assert.Equal(t, []string{"alpha"}, values)
}

func TestStore_GetFields_Bad_MissingKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := storeInstance.GetFields("g", "missing")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError))
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

func TestStore_Render_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_ = storeInstance.Set("user", "pool", "pool.lthn.io:3333")
	_ = storeInstance.Set("user", "wallet", "iz...")

	templateSource := `{"pool":"{{ .pool }}","wallet":"{{ .wallet }}"}`
	renderedTemplate, err := storeInstance.Render(templateSource, "user")
	require.NoError(t, err)
	assert.Contains(t, renderedTemplate, "pool.lthn.io:3333")
	assert.Contains(t, renderedTemplate, "iz...")
}

func TestStore_Render_Good_EmptyGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	// Template that does not reference any variables.
	renderedTemplate, err := storeInstance.Render("static content", "empty")
	require.NoError(t, err)
	assert.Equal(t, "static content", renderedTemplate)
}

func TestStore_Render_Bad_InvalidTemplateSyntax(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := storeInstance.Render("{{ .unclosed", "g")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.Render: parse")
}

func TestStore_Render_Bad_MissingTemplateVar(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	// text/template with a missing key on a map returns <no value>, not an error,
	// unless Option("missingkey=error") is set. The default behaviour is no error.
	renderedTemplate, err := storeInstance.Render("hello {{ .missing }}", "g")
	require.NoError(t, err)
	assert.Contains(t, renderedTemplate, "hello")
}

func TestStore_Render_Bad_ExecError(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_ = storeInstance.Set("g", "name", "hello")

	// Calling a string as a function triggers a template execution error.
	_, err := storeInstance.Render(`{{ call .name }}`, "g")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.Render: exec")
}

func TestStore_Render_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	_, err := storeInstance.Render("{{ .x }}", "g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestStore_Close_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	err := storeInstance.Close()
	require.NoError(t, err)
}

func TestStore_Close_Good_Idempotent(t *testing.T) {
	storeInstance, _ := New(":memory:")

	require.NoError(t, storeInstance.Close())
	require.NoError(t, storeInstance.Close())
}

func TestStore_Close_Good_OperationsFailAfterClose(t *testing.T) {
	storeInstance, _ := New(":memory:")
	require.NoError(t, storeInstance.Close())

	// All operations on a closed store should fail.
	_, err := storeInstance.Get("g", "k")
	assert.Error(t, err, "Get on closed store should fail")

	err = storeInstance.Set("g", "k", "v")
	assert.Error(t, err, "Set on closed store should fail")

	err = storeInstance.Delete("g", "k")
	assert.Error(t, err, "Delete on closed store should fail")

	_, err = storeInstance.Count("g")
	assert.Error(t, err, "Count on closed store should fail")

	err = storeInstance.DeleteGroup("g")
	assert.Error(t, err, "DeleteGroup on closed store should fail")

	_, err = storeInstance.GetAll("g")
	assert.Error(t, err, "GetAll on closed store should fail")

	_, err = storeInstance.Render("{{ .x }}", "g")
	assert.Error(t, err, "Render on closed store should fail")
}

func TestStore_Close_Bad_DriverCloseError(t *testing.T) {
	database := testCloseErrorDatabase(t)
	storeInstance := &Store{
		database:    database,
		cancelPurge: func() {},
	}

	err := storeInstance.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.Close")
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

var testCloseErrorDriverOnce sync.Once

func testCloseErrorDatabase(t *testing.T) *sql.DB {
	t.Helper()

	testCloseErrorDriverOnce.Do(func() {
		sql.Register("test-close-error-driver", testCloseErrorDriver{})
	})

	database, err := sql.Open("test-close-error-driver", "")
	require.NoError(t, err)
	require.NoError(t, database.Ping())
	return database
}

type testCloseErrorDriver struct{}

func (testCloseErrorDriver) Open(name string) (driver.Conn, error) {
	return testCloseErrorConn{}, nil
}

type testCloseErrorConn struct{}

func (testCloseErrorConn) Prepare(query string) (driver.Stmt, error) {
	return nil, core.E("test.CloseDriver", "prepare", nil)
}

func (testCloseErrorConn) Close() error {
	return core.E("test.CloseDriver", "close", nil)
}

func (testCloseErrorConn) Begin() (driver.Tx, error) {
	return nil, core.E("test.CloseDriver", "begin", nil)
}

func (testCloseErrorConn) Ping(ctx context.Context) error {
	return nil
}

var testRowsAffectedErrorDriverOnce sync.Once

func testRowsAffectedErrorDatabase(t *testing.T) *sql.DB {
	t.Helper()

	testRowsAffectedErrorDriverOnce.Do(func() {
		sql.Register("test-rows-affected-error-driver", testRowsAffectedErrorDriver{})
	})

	database, err := sql.Open("test-rows-affected-error-driver", "")
	require.NoError(t, err)
	return database
}

type testRowsAffectedErrorDriver struct{}

func (testRowsAffectedErrorDriver) Open(name string) (driver.Conn, error) {
	return testRowsAffectedErrorConn{}, nil
}

type testRowsAffectedErrorConn struct{}

func (testRowsAffectedErrorConn) Prepare(query string) (driver.Stmt, error) {
	return nil, core.E("test.RowsAffectedDriver", "prepare", nil)
}

func (testRowsAffectedErrorConn) Close() error {
	return nil
}

func (testRowsAffectedErrorConn) Begin() (driver.Tx, error) {
	return nil, core.E("test.RowsAffectedDriver", "begin", nil)
}

func (testRowsAffectedErrorConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return testRowsAffectedErrorResult{}, nil
}

type testRowsAffectedErrorResult struct{}

func (testRowsAffectedErrorResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (testRowsAffectedErrorResult) RowsAffected() (int64, error) {
	return 0, core.E("test.RowsAffectedDriver", "rows affected", nil)
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

func TestStore_SetGet_Good_EdgeCases(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	tests := []struct {
		name  string
		group string
		key   string
		value string
	}{
		{"empty key", "g", "", "val"},
		{"empty value", "g", "k", ""},
		{"empty group", "", "k", "val"},
		{"all empty", "", "", ""},
		{"spaces", "  ", " key ", " val "},
		{"newlines", "g", "line\nbreak", "val\nue"},
		{"tabs", "g", "tab\there", "tab\tval"},
		{"unicode keys", "g", "\u00e9\u00e0\u00fc\u00f6", "accented"},
		{"unicode values", "g", "emoji", "\U0001f600\U0001f680\U0001f30d"},
		{"CJK characters", "g", "\u4f60\u597d", "\u4e16\u754c"},
		{"arabic", "g", "\u0645\u0631\u062d\u0628\u0627", "\u0639\u0627\u0644\u0645"},
		{"null bytes", "g", "null\x00key", "null\x00val"},
		{"special SQL chars", "g", "'; DROP TABLE entries;--", "val"},
		{"backslash", "g", "back\\slash", "val\\ue"},
		{"percent", "g", "100%", "50%"},
		{"long key", "g", repeatString("k", 10000), "val"},
		{"long value", "g", "longval", repeatString("v", 100000)},
		{"long group", repeatString("g", 10000), "k", "val"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := storeInstance.Set(testCase.group, testCase.key, testCase.value)
			require.NoError(t, err, "Set should succeed")

			got, err := storeInstance.Get(testCase.group, testCase.key)
			require.NoError(t, err, "Get should succeed")
			assert.Equal(t, testCase.value, got, "round-trip should preserve value")
		})
	}
}

// ---------------------------------------------------------------------------
// Group isolation
// ---------------------------------------------------------------------------

func TestStore_GroupIsolation_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("alpha", "k", "a-val"))
	require.NoError(t, storeInstance.Set("beta", "k", "b-val"))

	alphaValue, err := storeInstance.Get("alpha", "k")
	require.NoError(t, err)
	assert.Equal(t, "a-val", alphaValue)

	betaValue, err := storeInstance.Get("beta", "k")
	require.NoError(t, err)
	assert.Equal(t, "b-val", betaValue)

	// Delete from alpha should not affect beta.
	require.NoError(t, storeInstance.Delete("alpha", "k"))
	_, err = storeInstance.Get("alpha", "k")
	assert.Error(t, err)

	betaValueAfterDelete, err := storeInstance.Get("beta", "k")
	require.NoError(t, err)
	assert.Equal(t, "b-val", betaValueAfterDelete)
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

func TestStore_Concurrent_Good_ReadWrite(t *testing.T) {
	databasePath := testPath(t, "concurrent.db")
	storeInstance, err := New(databasePath)
	require.NoError(t, err)
	defer storeInstance.Close()

	const goroutines = 10
	const opsPerGoroutine = 100

	var waitGroup sync.WaitGroup
	recordedErrors := make(chan error, goroutines*opsPerGoroutine*2)

	// Writers.
	for g := range goroutines {
		waitGroup.Add(1)
		go func(id int) {
			defer waitGroup.Done()
			group := core.Sprintf("grp-%d", id)
			for i := range opsPerGoroutine {
				key := core.Sprintf("key-%d", i)
				value := core.Sprintf("val-%d-%d", id, i)
				if err := storeInstance.Set(group, key, value); err != nil {
					recordedErrors <- core.E("TestStore_Concurrent_Good_ReadWrite", core.Sprintf("writer %d", id), err)
				}
			}
		}(g)
	}

	// Readers — start immediately alongside writers.
	for g := range goroutines {
		waitGroup.Add(1)
		go func(id int) {
			defer waitGroup.Done()
			group := core.Sprintf("grp-%d", id)
			for i := range opsPerGoroutine {
				key := core.Sprintf("key-%d", i)
				_, err := storeInstance.Get(group, key)
				// NotFoundError is acceptable — the writer may not have written yet.
				if err != nil && !core.Is(err, NotFoundError) {
					recordedErrors <- core.E("TestStore_Concurrent_Good_ReadWrite", core.Sprintf("reader %d", id), err)
				}
			}
		}(g)
	}

	waitGroup.Wait()
	close(recordedErrors)

	for recordedError := range recordedErrors {
		t.Error(recordedError)
	}

	// After all writers finish, every key should be present.
	for g := range goroutines {
		group := core.Sprintf("grp-%d", g)
		count, err := storeInstance.Count(group)
		require.NoError(t, err)
		assert.Equal(t, opsPerGoroutine, count, "group %s should have all keys", group)
	}
}

func TestStore_Concurrent_Good_GetAll(t *testing.T) {
	storeInstance, err := New(testPath(t, "getall.db"))
	require.NoError(t, err)
	defer storeInstance.Close()

	// Seed data.
	for i := range 50 {
		require.NoError(t, storeInstance.Set("shared", core.Sprintf("k%d", i), core.Sprintf("v%d", i)))
	}

	var waitGroup sync.WaitGroup
	for range 10 {
		waitGroup.Go(func() {
			all, err := storeInstance.GetAll("shared")
			if err != nil {
				t.Errorf("GetAll failed: %v", err)
				return
			}
			if len(all) != 50 {
				t.Errorf("expected 50 keys, got %d", len(all))
			}
		})
	}
	waitGroup.Wait()
}

func TestStore_Concurrent_Good_DeleteGroup(t *testing.T) {
	storeInstance, err := New(testPath(t, "delgrp.db"))
	require.NoError(t, err)
	defer storeInstance.Close()

	var waitGroup sync.WaitGroup
	for g := range 10 {
		waitGroup.Add(1)
		go func(id int) {
			defer waitGroup.Done()
			groupName := core.Sprintf("g%d", id)
			for i := range 20 {
				_ = storeInstance.Set(groupName, core.Sprintf("k%d", i), "v")
			}
			_ = storeInstance.DeleteGroup(groupName)
		}(g)
	}
	waitGroup.Wait()
}

// ---------------------------------------------------------------------------
// NotFoundError wrapping verification
// ---------------------------------------------------------------------------

func TestStore_NotFoundError_Good_Is(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := storeInstance.Get("g", "k")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError), "error should be NotFoundError via core.Is")
	assert.Contains(t, err.Error(), "g/k", "error message should include group/key")
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkSet(benchmark *testing.B) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	benchmark.ResetTimer()
	for i := range benchmark.N {
		_ = storeInstance.Set("bench", core.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkGet(benchmark *testing.B) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	// Pre-populate.
	const keys = 10000
	for i := range keys {
		_ = storeInstance.Set("bench", core.Sprintf("key-%d", i), "value")
	}

	benchmark.ResetTimer()
	for i := range benchmark.N {
		_, _ = storeInstance.Get("bench", core.Sprintf("key-%d", i%keys))
	}
}

func BenchmarkGetAll(benchmark *testing.B) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	const keys = 10000
	for i := range keys {
		_ = storeInstance.Set("bench", core.Sprintf("key-%d", i), "value")
	}

	benchmark.ResetTimer()
	for range benchmark.N {
		_, _ = storeInstance.GetAll("bench")
	}
}

func BenchmarkSet_FileBacked(benchmark *testing.B) {
	databasePath := testPath(benchmark, "bench.db")
	storeInstance, _ := New(databasePath)
	defer storeInstance.Close()

	benchmark.ResetTimer()
	for i := range benchmark.N {
		_ = storeInstance.Set("bench", core.Sprintf("key-%d", i), "value")
	}
}

// ---------------------------------------------------------------------------
// TTL support (Phase 1)
// ---------------------------------------------------------------------------

func TestStore_SetWithTTL_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	err := storeInstance.SetWithTTL("g", "k", "v", 5*time.Second)
	require.NoError(t, err)

	value, err := storeInstance.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)
}

func TestStore_SetWithTTL_Good_Upsert(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.SetWithTTL("g", "k", "v1", time.Hour))
	require.NoError(t, storeInstance.SetWithTTL("g", "k", "v2", time.Hour))

	value, err := storeInstance.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v2", value, "upsert should overwrite the value")

	count, err := storeInstance.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, count, "upsert should not duplicate keys")
}

func TestStore_SetWithTTL_Good_ExpiresOnGet(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	// Set a key with a very short TTL.
	require.NoError(t, storeInstance.SetWithTTL("g", "ephemeral", "gone-soon", 1*time.Millisecond))

	// Wait for it to expire.
	time.Sleep(5 * time.Millisecond)

	_, err := storeInstance.Get("g", "ephemeral")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError), "expired key should be NotFoundError")
}

func TestStore_SetWithTTL_Good_ExpiresOnGetEmitsDeleteEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	defer storeInstance.Unwatch("g", events)

	require.NoError(t, storeInstance.SetWithTTL("g", "ephemeral", "gone-soon", 1*time.Millisecond))
	<-events

	time.Sleep(5 * time.Millisecond)

	_, err := storeInstance.Get("g", "ephemeral")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError), "expired key should be NotFoundError")

	select {
	case event := <-events:
		assert.Equal(t, EventDelete, event.Type)
		assert.Equal(t, "g", event.Group)
		assert.Equal(t, "ephemeral", event.Key)
		assert.Empty(t, event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lazy expiry delete event")
	}
}

func TestStore_SetWithTTL_Good_ExcludedFromCount(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "permanent", "stays"))
	require.NoError(t, storeInstance.SetWithTTL("g", "temp", "goes", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	count, err := storeInstance.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, count, "expired key should not be counted")
}

func TestStore_SetWithTTL_Good_ExcludedFromGetAll(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "a", "1"))
	require.NoError(t, storeInstance.SetWithTTL("g", "b", "2", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	all, err := storeInstance.GetAll("g")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1"}, all, "expired key should be excluded")
}

func TestStore_SetWithTTL_Good_ExcludedFromRender(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "name", "Alice"))
	require.NoError(t, storeInstance.SetWithTTL("g", "temp", "gone", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	renderedTemplate, err := storeInstance.Render("Hello {{ .name }}", "g")
	require.NoError(t, err)
	assert.Equal(t, "Hello Alice", renderedTemplate)
}

func TestStore_SetWithTTL_Good_SetClearsTTL(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	// Set with TTL, then overwrite with plain Set — TTL should be cleared.
	require.NoError(t, storeInstance.SetWithTTL("g", "k", "temp", 1*time.Millisecond))
	require.NoError(t, storeInstance.Set("g", "k", "permanent"))
	time.Sleep(5 * time.Millisecond)

	value, err := storeInstance.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "permanent", value, "plain Set should clear TTL")
}

func TestStore_SetWithTTL_Good_FutureTTLAccessible(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.SetWithTTL("g", "k", "v", 1*time.Hour))

	value, err := storeInstance.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value, "far-future TTL should be accessible")

	count, err := storeInstance.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestStore_SetWithTTL_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	err := storeInstance.SetWithTTL("g", "k", "v", time.Hour)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// PurgeExpired
// ---------------------------------------------------------------------------

func TestStore_PurgeExpired_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.SetWithTTL("g", "a", "1", 1*time.Millisecond))
	require.NoError(t, storeInstance.SetWithTTL("g", "b", "2", 1*time.Millisecond))
	require.NoError(t, storeInstance.Set("g", "c", "3"))
	time.Sleep(5 * time.Millisecond)

	removed, err := storeInstance.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(2), removed, "should purge 2 expired keys")

	count, err := storeInstance.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, count, "only non-expiring key should remain")
}

func TestStore_PurgeExpired_Good_NoneExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g", "a", "1"))
	require.NoError(t, storeInstance.SetWithTTL("g", "b", "2", time.Hour))

	removed, err := storeInstance.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed)
}

func TestStore_PurgeExpired_Good_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	removed, err := storeInstance.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed)
}

func TestStore_PurgeExpired_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	_, err := storeInstance.PurgeExpired()
	require.Error(t, err)
}

func TestStore_PurgeExpired_Bad_RowsAffectedError(t *testing.T) {
	database := testRowsAffectedErrorDatabase(t)
	storeInstance := &Store{
		database:    database,
		cancelPurge: func() {},
	}

	_, err := storeInstance.PurgeExpired()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.PurgeExpired")
}

func TestStore_PurgeExpired_Good_BackgroundPurge(t *testing.T) {
	storeInstance, err := New(":memory:", WithPurgeInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer storeInstance.Close()

	require.NoError(t, storeInstance.SetWithTTL("g", "ephemeral", "v", 1*time.Millisecond))
	require.NoError(t, storeInstance.Set("g", "permanent", "stays"))

	// Wait for the background purge to fire.
	time.Sleep(60 * time.Millisecond)

	// The expired key should have been removed by the background goroutine.
	// Use a raw query to check the row is actually gone (not just filtered by Get).
	var count int
	err = storeInstance.database.QueryRow("SELECT COUNT(*) FROM entries WHERE group_name = ?", "g").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "background purge should have deleted the expired row")
}

// ---------------------------------------------------------------------------
// Schema migration — reopening an existing database
// ---------------------------------------------------------------------------

func TestStore_SchemaUpgrade_Good_ExistingDB(t *testing.T) {
	databasePath := testPath(t, "upgrade.db")

	// Open, write, close.
	initialStore, err := New(databasePath)
	require.NoError(t, err)
	require.NoError(t, initialStore.Set("g", "k", "v"))
	require.NoError(t, initialStore.Close())

	// Reopen — the ALTER TABLE ADD COLUMN should be a no-op.
	reopenedStore, err := New(databasePath)
	require.NoError(t, err)
	defer reopenedStore.Close()

	value, err := reopenedStore.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)

	// TTL features should work on the reopened store.
	require.NoError(t, reopenedStore.SetWithTTL("g", "ttl-key", "ttl-val", time.Hour))
	secondValue, err := reopenedStore.Get("g", "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, "ttl-val", secondValue)
}

func TestStore_SchemaUpgrade_Good_EntriesWithoutExpiryColumn(t *testing.T) {
	databasePath := testPath(t, "entries-no-expiry.db")
	database, err := sql.Open("sqlite", databasePath)
	require.NoError(t, err)
	database.SetMaxOpenConns(1)
	_, err = database.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = database.Exec(`CREATE TABLE entries (
		group_name  TEXT NOT NULL,
		entry_key   TEXT NOT NULL,
		entry_value TEXT NOT NULL,
		PRIMARY KEY (group_name, entry_key)
	)`)
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES ('g', 'k', 'v')")
	require.NoError(t, err)
	require.NoError(t, database.Close())

	storeInstance, err := New(databasePath)
	require.NoError(t, err)
	defer storeInstance.Close()

	value, err := storeInstance.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)

	require.NoError(t, storeInstance.SetWithTTL("g", "ttl-key", "ttl-val", time.Hour))
	secondValue, err := storeInstance.Get("g", "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, "ttl-val", secondValue)
}

func TestStore_SchemaUpgrade_Good_LegacyAndCurrentTables(t *testing.T) {
	databasePath := testPath(t, "entries-and-legacy.db")
	database, err := sql.Open("sqlite", databasePath)
	require.NoError(t, err)
	database.SetMaxOpenConns(1)
	_, err = database.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = database.Exec(`CREATE TABLE entries (
		group_name  TEXT NOT NULL,
		entry_key   TEXT NOT NULL,
		entry_value TEXT NOT NULL,
		expires_at  INTEGER,
		PRIMARY KEY (group_name, entry_key)
	)`)
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES ('existing', 'k', 'v')")
	require.NoError(t, err)
	_, err = database.Exec(`CREATE TABLE kv (
		grp   TEXT NOT NULL,
		key   TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY (grp, key)
	)`)
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO kv (grp, key, value) VALUES ('legacy', 'k', 'legacy-v')")
	require.NoError(t, err)
	require.NoError(t, database.Close())

	storeInstance, err := New(databasePath)
	require.NoError(t, err)
	defer storeInstance.Close()

	value, err := storeInstance.Get("existing", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)

	legacyVal, err := storeInstance.Get("legacy", "k")
	require.NoError(t, err)
	assert.Equal(t, "legacy-v", legacyVal)
}

func TestStore_SchemaUpgrade_Good_PreTTLDatabase(t *testing.T) {
	// Simulate a database created before the AX schema rename and TTL support.
	// The legacy key-value table has no expires_at column yet.
	databasePath := testPath(t, "pre-ttl.db")
	database, err := sql.Open("sqlite", databasePath)
	require.NoError(t, err)
	database.SetMaxOpenConns(1)
	_, err = database.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = database.Exec(`CREATE TABLE kv (
		grp   TEXT NOT NULL,
		key   TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY (grp, key)
	)`)
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO kv (grp, key, value) VALUES ('g', 'k', 'v')")
	require.NoError(t, err)
	require.NoError(t, database.Close())

	// Open with New — should migrate the legacy table into the descriptive schema.
	storeInstance, err := New(databasePath)
	require.NoError(t, err)
	defer storeInstance.Close()

	// Existing data should be readable.
	value, err := storeInstance.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)

	// TTL features should work after migration.
	require.NoError(t, storeInstance.SetWithTTL("g", "ttl-key", "ttl-val", time.Hour))
	secondValue, err := storeInstance.Get("g", "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, "ttl-val", secondValue)
}

// ---------------------------------------------------------------------------
// Concurrent TTL access
// ---------------------------------------------------------------------------

func TestStore_Concurrent_Good_TTL(t *testing.T) {
	storeInstance, err := New(testPath(t, "concurrent-ttl.db"))
	require.NoError(t, err)
	defer storeInstance.Close()

	const goroutines = 10
	const ops = 50

	var waitGroup sync.WaitGroup
	for g := range goroutines {
		waitGroup.Add(1)
		go func(id int) {
			defer waitGroup.Done()
			groupName := core.Sprintf("ttl-%d", id)
			for i := range ops {
				key := core.Sprintf("k%d", i)
				if i%2 == 0 {
					_ = storeInstance.SetWithTTL(groupName, key, "v", 50*time.Millisecond)
				} else {
					_ = storeInstance.Set(groupName, key, "v")
				}
			}
		}(g)
	}
	waitGroup.Wait()

	// Give expired keys time to lapse.
	time.Sleep(60 * time.Millisecond)

	for g := range goroutines {
		groupName := core.Sprintf("ttl-%d", g)
		count, err := storeInstance.Count(groupName)
		require.NoError(t, err)
		assert.Equal(t, ops/2, count, "only non-TTL keys should remain in %s", groupName)
	}
}
