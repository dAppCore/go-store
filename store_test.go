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
	s, err := New(":memory:")
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.Close()
}

func TestStore_New_Good_FileBacked(t *testing.T) {
	dbPath := testPath(t, "test.db")
	s, err := New(dbPath)
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.Close()

	// Verify data persists: write, close, reopen.
	require.NoError(t, s.Set("g", "k", "v"))
	require.NoError(t, s.Close())

	s2, err := New(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	val, err := s2.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val)
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
	dbPath := testPath(t, "corrupt.db")
	requireCoreOK(t, testFilesystem().Write(dbPath, "not a sqlite database"))

	_, err := New(dbPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.New")
}

func TestStore_New_Bad_ReadOnlyDir(t *testing.T) {
	// A path in a read-only directory should fail when SQLite tries to create the WAL file.
	dir := t.TempDir()
	dbPath := core.Path(dir, "readonly.db")

	// Create a valid DB first, then make the directory read-only.
	s, err := New(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	// Remove WAL/SHM files and make directory read-only.
	_ = testFilesystem().Delete(dbPath + "-wal")
	_ = testFilesystem().Delete(dbPath + "-shm")
	require.NoError(t, syscall.Chmod(dir, 0555))
	defer func() { _ = syscall.Chmod(dir, 0755) }() // restore for cleanup

	_, err = New(dbPath)
	// May or may not fail depending on OS/filesystem — just exercise the code path.
	if err != nil {
		assert.Contains(t, err.Error(), "store.New")
	}
}

func TestStore_New_Good_WALMode(t *testing.T) {
	dbPath := testPath(t, "wal.db")
	s, err := New(dbPath)
	require.NoError(t, err)
	defer s.Close()

	var mode string
	err = s.database.QueryRow("PRAGMA journal_mode").Scan(&mode)
	require.NoError(t, err)
	assert.Equal(t, "wal", mode, "journal_mode should be WAL")
}

// ---------------------------------------------------------------------------
// Set / Get — core CRUD
// ---------------------------------------------------------------------------

func TestStore_SetGet_Good(t *testing.T) {
	s, err := New(":memory:")
	require.NoError(t, err)
	defer s.Close()

	err = s.Set("config", "theme", "dark")
	require.NoError(t, err)

	val, err := s.Get("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", val)
}

func TestStore_Set_Good_Upsert(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "k", "v1"))
	require.NoError(t, s.Set("g", "k", "v2"))

	val, err := s.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v2", val, "upsert should overwrite the value")

	n, err := s.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, n, "upsert should not duplicate keys")
}

func TestStore_Get_Bad_NotFound(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.Get("config", "missing")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError), "should wrap NotFoundError")
}

func TestStore_Get_Bad_NonExistentGroup(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.Get("no-such-group", "key")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError))
}

func TestStore_Get_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.Get("g", "k")
	require.Error(t, err)
}

func TestStore_Set_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	err := s.Set("g", "k", "v")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestStore_Delete_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("config", "key", "val")
	err := s.Delete("config", "key")
	require.NoError(t, err)

	_, err = s.Get("config", "key")
	assert.Error(t, err)
}

func TestStore_Delete_Good_NonExistent(t *testing.T) {
	// Deleting a key that does not exist should not error.
	s, _ := New(":memory:")
	defer s.Close()

	err := s.Delete("g", "nope")
	assert.NoError(t, err)
}

func TestStore_Delete_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	err := s.Delete("g", "k")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Count
// ---------------------------------------------------------------------------

func TestStore_Count_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("grp", "a", "1")
	_ = s.Set("grp", "b", "2")
	_ = s.Set("other", "c", "3")

	n, err := s.Count("grp")
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestStore_Count_Good_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	n, err := s.Count("empty")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestStore_Count_Good_BulkInsert(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	const total = 500
	for i := range total {
		require.NoError(t, s.Set("bulk", core.Sprintf("key-%04d", i), "v"))
	}
	n, err := s.Count("bulk")
	require.NoError(t, err)
	assert.Equal(t, total, n)
}

func TestStore_Count_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.Count("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// DeleteGroup
// ---------------------------------------------------------------------------

func TestStore_DeleteGroup_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("grp", "a", "1")
	_ = s.Set("grp", "b", "2")
	err := s.DeleteGroup("grp")
	require.NoError(t, err)

	n, _ := s.Count("grp")
	assert.Equal(t, 0, n)
}

func TestStore_DeleteGroup_Good_ThenGetAllEmpty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("grp", "a", "1")
	_ = s.Set("grp", "b", "2")
	require.NoError(t, s.DeleteGroup("grp"))

	all, err := s.GetAll("grp")
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestStore_DeleteGroup_Good_IsolatesOtherGroups(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("a", "k", "1")
	_ = s.Set("b", "k", "2")
	require.NoError(t, s.DeleteGroup("a"))

	_, err := s.Get("a", "k")
	assert.Error(t, err)

	val, err := s.Get("b", "k")
	require.NoError(t, err)
	assert.Equal(t, "2", val, "other group should be untouched")
}

func TestStore_DeleteGroup_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	err := s.DeleteGroup("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// GetAll
// ---------------------------------------------------------------------------

func TestStore_GetAll_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("grp", "a", "1")
	_ = s.Set("grp", "b", "2")
	_ = s.Set("other", "c", "3")

	all, err := s.GetAll("grp")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, all)
}

func TestStore_GetAll_Good_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	all, err := s.GetAll("empty")
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestStore_GetAll_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.GetAll("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// All / GroupsSeq
// ---------------------------------------------------------------------------

func TestStore_All_Good_StopsEarly(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "a", "1"))
	require.NoError(t, s.Set("g", "b", "2"))

	entries := s.All("g")
	var seen []string
	for entry, err := range entries {
		require.NoError(t, err)
		seen = append(seen, entry.Key)
		break
	}

	assert.Len(t, seen, 1)
}

func TestStore_All_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	for _, err := range s.All("g") {
		require.Error(t, err)
	}
}

func TestStore_GroupsSeq_Good_StopsEarly(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("alpha", "a", "1"))
	require.NoError(t, s.Set("beta", "b", "2"))

	groups := s.GroupsSeq("")
	var seen []string
	for group, err := range groups {
		require.NoError(t, err)
		seen = append(seen, group)
		break
	}

	assert.Len(t, seen, 1)
}

func TestStore_GroupsSeq_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	for _, err := range s.GroupsSeq("") {
		require.Error(t, err)
	}
}

// ---------------------------------------------------------------------------
// GetSplit / GetFields
// ---------------------------------------------------------------------------

func TestStore_GetSplit_Good_SplitsValue(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "comma", "alpha,beta,gamma"))

	parts, err := s.GetSplit("g", "comma", ",")
	require.NoError(t, err)

	var values []string
	for value := range parts {
		values = append(values, value)
	}

	assert.Equal(t, []string{"alpha", "beta", "gamma"}, values)
}

func TestStore_GetSplit_Good_StopsEarly(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "comma", "alpha,beta,gamma"))

	parts, err := s.GetSplit("g", "comma", ",")
	require.NoError(t, err)

	var values []string
	for value := range parts {
		values = append(values, value)
		break
	}

	assert.Equal(t, []string{"alpha"}, values)
}

func TestStore_GetSplit_Bad_MissingKey(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.GetSplit("g", "missing", ",")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError))
}

func TestStore_GetFields_Good_SplitsWhitespace(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "fields", "alpha beta\tgamma\n"))

	fields, err := s.GetFields("g", "fields")
	require.NoError(t, err)

	var values []string
	for value := range fields {
		values = append(values, value)
	}

	assert.Equal(t, []string{"alpha", "beta", "gamma"}, values)
}

func TestStore_GetFields_Good_StopsEarly(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "fields", "alpha beta\tgamma\n"))

	fields, err := s.GetFields("g", "fields")
	require.NoError(t, err)

	var values []string
	for value := range fields {
		values = append(values, value)
		break
	}

	assert.Equal(t, []string{"alpha"}, values)
}

func TestStore_GetFields_Bad_MissingKey(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.GetFields("g", "missing")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError))
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

func TestStore_Render_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("user", "pool", "pool.lthn.io:3333")
	_ = s.Set("user", "wallet", "iz...")

	templateSource := `{"pool":"{{ .pool }}","wallet":"{{ .wallet }}"}`
	out, err := s.Render(templateSource, "user")
	require.NoError(t, err)
	assert.Contains(t, out, "pool.lthn.io:3333")
	assert.Contains(t, out, "iz...")
}

func TestStore_Render_Good_EmptyGroup(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// Template that does not reference any variables.
	out, err := s.Render("static content", "empty")
	require.NoError(t, err)
	assert.Equal(t, "static content", out)
}

func TestStore_Render_Bad_InvalidTemplateSyntax(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.Render("{{ .unclosed", "g")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.Render: parse")
}

func TestStore_Render_Bad_MissingTemplateVar(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// text/template with a missing key on a map returns <no value>, not an error,
	// unless Option("missingkey=error") is set. The default behaviour is no error.
	out, err := s.Render("hello {{ .missing }}", "g")
	require.NoError(t, err)
	assert.Contains(t, out, "hello")
}

func TestStore_Render_Bad_ExecError(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("g", "name", "hello")

	// Calling a string as a function triggers a template execution error.
	_, err := s.Render(`{{ call .name }}`, "g")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.Render: exec")
}

func TestStore_Render_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.Render("{{ .x }}", "g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestStore_Close_Good(t *testing.T) {
	s, _ := New(":memory:")
	err := s.Close()
	require.NoError(t, err)
}

func TestStore_Close_Good_OperationsFailAfterClose(t *testing.T) {
	s, _ := New(":memory:")
	require.NoError(t, s.Close())

	// All operations on a closed store should fail.
	_, err := s.Get("g", "k")
	assert.Error(t, err, "Get on closed store should fail")

	err = s.Set("g", "k", "v")
	assert.Error(t, err, "Set on closed store should fail")

	err = s.Delete("g", "k")
	assert.Error(t, err, "Delete on closed store should fail")

	_, err = s.Count("g")
	assert.Error(t, err, "Count on closed store should fail")

	err = s.DeleteGroup("g")
	assert.Error(t, err, "DeleteGroup on closed store should fail")

	_, err = s.GetAll("g")
	assert.Error(t, err, "GetAll on closed store should fail")

	_, err = s.Render("{{ .x }}", "g")
	assert.Error(t, err, "Render on closed store should fail")
}

func TestStore_Close_Bad_DriverCloseError(t *testing.T) {
	db := testCloseErrorDatabase(t)
	s := &Store{
		database:    db,
		cancelPurge: func() {},
	}

	err := s.Close()
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
	s, _ := New(":memory:")
	defer s.Close()

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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := s.Set(tc.group, tc.key, tc.value)
			require.NoError(t, err, "Set should succeed")

			got, err := s.Get(tc.group, tc.key)
			require.NoError(t, err, "Get should succeed")
			assert.Equal(t, tc.value, got, "round-trip should preserve value")
		})
	}
}

// ---------------------------------------------------------------------------
// Group isolation
// ---------------------------------------------------------------------------

func TestStore_GroupIsolation_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("alpha", "k", "a-val"))
	require.NoError(t, s.Set("beta", "k", "b-val"))

	a, err := s.Get("alpha", "k")
	require.NoError(t, err)
	assert.Equal(t, "a-val", a)

	b, err := s.Get("beta", "k")
	require.NoError(t, err)
	assert.Equal(t, "b-val", b)

	// Delete from alpha should not affect beta.
	require.NoError(t, s.Delete("alpha", "k"))
	_, err = s.Get("alpha", "k")
	assert.Error(t, err)

	b2, err := s.Get("beta", "k")
	require.NoError(t, err)
	assert.Equal(t, "b-val", b2)
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

func TestStore_Concurrent_Good_ReadWrite(t *testing.T) {
	dbPath := testPath(t, "concurrent.db")
	s, err := New(dbPath)
	require.NoError(t, err)
	defer s.Close()

	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*opsPerGoroutine*2)

	// Writers.
	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			group := core.Sprintf("grp-%d", id)
			for i := range opsPerGoroutine {
				key := core.Sprintf("key-%d", i)
				val := core.Sprintf("val-%d-%d", id, i)
				if err := s.Set(group, key, val); err != nil {
					errs <- core.E("TestStore_Concurrent_Good_ReadWrite", core.Sprintf("writer %d", id), err)
				}
			}
		}(g)
	}

	// Readers — start immediately alongside writers.
	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			group := core.Sprintf("grp-%d", id)
			for i := range opsPerGoroutine {
				key := core.Sprintf("key-%d", i)
				_, err := s.Get(group, key)
				// NotFoundError is acceptable — the writer may not have written yet.
				if err != nil && !core.Is(err, NotFoundError) {
					errs <- core.E("TestStore_Concurrent_Good_ReadWrite", core.Sprintf("reader %d", id), err)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Error(e)
	}

	// After all writers finish, every key should be present.
	for g := range goroutines {
		group := core.Sprintf("grp-%d", g)
		n, err := s.Count(group)
		require.NoError(t, err)
		assert.Equal(t, opsPerGoroutine, n, "group %s should have all keys", group)
	}
}

func TestStore_Concurrent_Good_GetAll(t *testing.T) {
	s, err := New(testPath(t, "getall.db"))
	require.NoError(t, err)
	defer s.Close()

	// Seed data.
	for i := range 50 {
		require.NoError(t, s.Set("shared", core.Sprintf("k%d", i), core.Sprintf("v%d", i)))
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			all, err := s.GetAll("shared")
			if err != nil {
				t.Errorf("GetAll failed: %v", err)
				return
			}
			if len(all) != 50 {
				t.Errorf("expected 50 keys, got %d", len(all))
			}
		})
	}
	wg.Wait()
}

func TestStore_Concurrent_Good_DeleteGroup(t *testing.T) {
	s, err := New(testPath(t, "delgrp.db"))
	require.NoError(t, err)
	defer s.Close()

	var wg sync.WaitGroup
	for g := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			grp := core.Sprintf("g%d", id)
			for i := range 20 {
				_ = s.Set(grp, core.Sprintf("k%d", i), "v")
			}
			_ = s.DeleteGroup(grp)
		}(g)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// NotFoundError wrapping verification
// ---------------------------------------------------------------------------

func TestStore_NotFoundError_Good_Is(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.Get("g", "k")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError), "error should be NotFoundError via core.Is")
	assert.Contains(t, err.Error(), "g/k", "error message should include group/key")
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkSet(b *testing.B) {
	s, _ := New(":memory:")
	defer s.Close()

	b.ResetTimer()
	for i := range b.N {
		_ = s.Set("bench", core.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkGet(b *testing.B) {
	s, _ := New(":memory:")
	defer s.Close()

	// Pre-populate.
	const keys = 10000
	for i := range keys {
		_ = s.Set("bench", core.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	for i := range b.N {
		_, _ = s.Get("bench", core.Sprintf("key-%d", i%keys))
	}
}

func BenchmarkGetAll(b *testing.B) {
	s, _ := New(":memory:")
	defer s.Close()

	const keys = 10000
	for i := range keys {
		_ = s.Set("bench", core.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	for range b.N {
		_, _ = s.GetAll("bench")
	}
}

func BenchmarkSet_FileBacked(b *testing.B) {
	dbPath := testPath(b, "bench.db")
	s, _ := New(dbPath)
	defer s.Close()

	b.ResetTimer()
	for i := range b.N {
		_ = s.Set("bench", core.Sprintf("key-%d", i), "value")
	}
}

// ---------------------------------------------------------------------------
// TTL support (Phase 1)
// ---------------------------------------------------------------------------

func TestStore_SetWithTTL_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	err := s.SetWithTTL("g", "k", "v", 5*time.Second)
	require.NoError(t, err)

	val, err := s.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val)
}

func TestStore_SetWithTTL_Good_Upsert(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.SetWithTTL("g", "k", "v1", time.Hour))
	require.NoError(t, s.SetWithTTL("g", "k", "v2", time.Hour))

	val, err := s.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v2", val, "upsert should overwrite the value")

	n, err := s.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, n, "upsert should not duplicate keys")
}

func TestStore_SetWithTTL_Good_ExpiresOnGet(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// Set a key with a very short TTL.
	require.NoError(t, s.SetWithTTL("g", "ephemeral", "gone-soon", 1*time.Millisecond))

	// Wait for it to expire.
	time.Sleep(5 * time.Millisecond)

	_, err := s.Get("g", "ephemeral")
	require.Error(t, err)
	assert.True(t, core.Is(err, NotFoundError), "expired key should be NotFoundError")
}

func TestStore_SetWithTTL_Good_ExcludedFromCount(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "permanent", "stays"))
	require.NoError(t, s.SetWithTTL("g", "temp", "goes", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	n, err := s.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, n, "expired key should not be counted")
}

func TestStore_SetWithTTL_Good_ExcludedFromGetAll(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "a", "1"))
	require.NoError(t, s.SetWithTTL("g", "b", "2", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	all, err := s.GetAll("g")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1"}, all, "expired key should be excluded")
}

func TestStore_SetWithTTL_Good_ExcludedFromRender(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "name", "Alice"))
	require.NoError(t, s.SetWithTTL("g", "temp", "gone", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	out, err := s.Render("Hello {{ .name }}", "g")
	require.NoError(t, err)
	assert.Equal(t, "Hello Alice", out)
}

func TestStore_SetWithTTL_Good_SetClearsTTL(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// Set with TTL, then overwrite with plain Set — TTL should be cleared.
	require.NoError(t, s.SetWithTTL("g", "k", "temp", 1*time.Millisecond))
	require.NoError(t, s.Set("g", "k", "permanent"))
	time.Sleep(5 * time.Millisecond)

	val, err := s.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "permanent", val, "plain Set should clear TTL")
}

func TestStore_SetWithTTL_Good_FutureTTLAccessible(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.SetWithTTL("g", "k", "v", 1*time.Hour))

	val, err := s.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val, "far-future TTL should be accessible")

	n, err := s.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestStore_SetWithTTL_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	err := s.SetWithTTL("g", "k", "v", time.Hour)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// PurgeExpired
// ---------------------------------------------------------------------------

func TestStore_PurgeExpired_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.SetWithTTL("g", "a", "1", 1*time.Millisecond))
	require.NoError(t, s.SetWithTTL("g", "b", "2", 1*time.Millisecond))
	require.NoError(t, s.Set("g", "c", "3"))
	time.Sleep(5 * time.Millisecond)

	removed, err := s.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(2), removed, "should purge 2 expired keys")

	n, err := s.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only non-expiring key should remain")
}

func TestStore_PurgeExpired_Good_NoneExpired(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "a", "1"))
	require.NoError(t, s.SetWithTTL("g", "b", "2", time.Hour))

	removed, err := s.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed)
}

func TestStore_PurgeExpired_Good_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	removed, err := s.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed)
}

func TestStore_PurgeExpired_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.PurgeExpired()
	require.Error(t, err)
}

func TestStore_PurgeExpired_Bad_RowsAffectedError(t *testing.T) {
	db := testRowsAffectedErrorDatabase(t)
	s := &Store{
		database:    db,
		cancelPurge: func() {},
	}

	_, err := s.PurgeExpired()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.PurgeExpired")
}

func TestStore_PurgeExpired_Good_BackgroundPurge(t *testing.T) {
	s, _ := New(":memory:")
	// Override purge interval for testing: restart the goroutine with a short interval.
	s.cancelPurge()
	s.purgeWaitGroup.Wait()
	s.purgeInterval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelPurge = cancel
	s.startBackgroundPurge(ctx)
	defer s.Close()

	require.NoError(t, s.SetWithTTL("g", "ephemeral", "v", 1*time.Millisecond))
	require.NoError(t, s.Set("g", "permanent", "stays"))

	// Wait for the background purge to fire.
	time.Sleep(60 * time.Millisecond)

	// The expired key should have been removed by the background goroutine.
	// Use a raw query to check the row is actually gone (not just filtered by Get).
	var count int
	err := s.database.QueryRow("SELECT COUNT(*) FROM entries WHERE group_name = ?", "g").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "background purge should have deleted the expired row")
}

// ---------------------------------------------------------------------------
// Schema migration — reopening an existing database
// ---------------------------------------------------------------------------

func TestStore_SchemaUpgrade_Good_ExistingDB(t *testing.T) {
	dbPath := testPath(t, "upgrade.db")

	// Open, write, close.
	s1, err := New(dbPath)
	require.NoError(t, err)
	require.NoError(t, s1.Set("g", "k", "v"))
	require.NoError(t, s1.Close())

	// Reopen — the ALTER TABLE ADD COLUMN should be a no-op.
	s2, err := New(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	val, err := s2.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val)

	// TTL features should work on the reopened store.
	require.NoError(t, s2.SetWithTTL("g", "ttl-key", "ttl-val", time.Hour))
	val2, err := s2.Get("g", "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, "ttl-val", val2)
}

func TestStore_SchemaUpgrade_Good_EntriesWithoutExpiryColumn(t *testing.T) {
	dbPath := testPath(t, "entries-no-expiry.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE entries (
		group_name  TEXT NOT NULL,
		entry_key   TEXT NOT NULL,
		entry_value TEXT NOT NULL,
		PRIMARY KEY (group_name, entry_key)
	)`)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES ('g', 'k', 'v')")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err := New(dbPath)
	require.NoError(t, err)
	defer s.Close()

	val, err := s.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val)

	require.NoError(t, s.SetWithTTL("g", "ttl-key", "ttl-val", time.Hour))
	val2, err := s.Get("g", "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, "ttl-val", val2)
}

func TestStore_SchemaUpgrade_Good_LegacyAndCurrentTables(t *testing.T) {
	dbPath := testPath(t, "entries-and-legacy.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE entries (
		group_name  TEXT NOT NULL,
		entry_key   TEXT NOT NULL,
		entry_value TEXT NOT NULL,
		expires_at  INTEGER,
		PRIMARY KEY (group_name, entry_key)
	)`)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO entries (group_name, entry_key, entry_value) VALUES ('existing', 'k', 'v')")
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE kv (
		grp   TEXT NOT NULL,
		key   TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY (grp, key)
	)`)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO kv (grp, key, value) VALUES ('legacy', 'k', 'legacy-v')")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err := New(dbPath)
	require.NoError(t, err)
	defer s.Close()

	val, err := s.Get("existing", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val)

	legacyVal, err := s.Get("legacy", "k")
	require.NoError(t, err)
	assert.Equal(t, "legacy-v", legacyVal)
}

func TestStore_SchemaUpgrade_Good_PreTTLDatabase(t *testing.T) {
	// Simulate a database created before the AX schema rename and TTL support.
	// The legacy kv table has no expires_at column yet.
	dbPath := testPath(t, "pre-ttl.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE kv (
		grp   TEXT NOT NULL,
		key   TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY (grp, key)
	)`)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO kv (grp, key, value) VALUES ('g', 'k', 'v')")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Open with New — should migrate the legacy table into the descriptive schema.
	s, err := New(dbPath)
	require.NoError(t, err)
	defer s.Close()

	// Existing data should be readable.
	val, err := s.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val)

	// TTL features should work after migration.
	require.NoError(t, s.SetWithTTL("g", "ttl-key", "ttl-val", time.Hour))
	val2, err := s.Get("g", "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, "ttl-val", val2)
}

// ---------------------------------------------------------------------------
// Concurrent TTL access
// ---------------------------------------------------------------------------

func TestStore_Concurrent_Good_TTL(t *testing.T) {
	s, err := New(testPath(t, "concurrent-ttl.db"))
	require.NoError(t, err)
	defer s.Close()

	const goroutines = 10
	const ops = 50

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			grp := core.Sprintf("ttl-%d", id)
			for i := range ops {
				key := core.Sprintf("k%d", i)
				if i%2 == 0 {
					_ = s.SetWithTTL(grp, key, "v", 50*time.Millisecond)
				} else {
					_ = s.Set(grp, key, "v")
				}
			}
		}(g)
	}
	wg.Wait()

	// Give expired keys time to lapse.
	time.Sleep(60 * time.Millisecond)

	for g := range goroutines {
		grp := core.Sprintf("ttl-%d", g)
		n, err := s.Count(grp)
		require.NoError(t, err)
		assert.Equal(t, ops/2, n, "only non-TTL keys should remain in %s", grp)
	}
}
