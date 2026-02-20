package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

func TestNew_Good_Memory(t *testing.T) {
	s, err := New(":memory:")
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.Close()
}

func TestNew_Good_FileBacked(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
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

func TestNew_Bad_InvalidPath(t *testing.T) {
	// A path under a non-existent directory should fail at the WAL pragma step
	// because sql.Open is lazy and only validates on first use.
	_, err := New("/no/such/directory/test.db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.New")
}

func TestNew_Bad_CorruptFile(t *testing.T) {
	// A file that exists but is not a valid SQLite database should fail.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "corrupt.db")
	require.NoError(t, os.WriteFile(dbPath, []byte("not a sqlite database"), 0644))

	_, err := New(dbPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.New")
}

func TestNew_Bad_ReadOnlyDir(t *testing.T) {
	// A path in a read-only directory should fail when SQLite tries to create the WAL file.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "readonly.db")

	// Create a valid DB first, then make the directory read-only.
	s, err := New(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	// Remove WAL/SHM files and make directory read-only.
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")
	require.NoError(t, os.Chmod(dir, 0555))
	defer os.Chmod(dir, 0755) // restore for cleanup

	_, err = New(dbPath)
	// May or may not fail depending on OS/filesystem — just exercise the code path.
	if err != nil {
		assert.Contains(t, err.Error(), "store.New")
	}
}

func TestNew_Good_WALMode(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "wal.db")
	s, err := New(dbPath)
	require.NoError(t, err)
	defer s.Close()

	var mode string
	err = s.db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	require.NoError(t, err)
	assert.Equal(t, "wal", mode, "journal_mode should be WAL")
}

// ---------------------------------------------------------------------------
// Set / Get — core CRUD
// ---------------------------------------------------------------------------

func TestSetGet_Good(t *testing.T) {
	s, err := New(":memory:")
	require.NoError(t, err)
	defer s.Close()

	err = s.Set("config", "theme", "dark")
	require.NoError(t, err)

	val, err := s.Get("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", val)
}

func TestSet_Good_Upsert(t *testing.T) {
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

func TestGet_Bad_NotFound(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.Get("config", "missing")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound), "should wrap ErrNotFound")
}

func TestGet_Bad_NonExistentGroup(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.Get("no-such-group", "key")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestGet_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.Get("g", "k")
	require.Error(t, err)
}

func TestSet_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	err := s.Set("g", "k", "v")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestDelete_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("config", "key", "val")
	err := s.Delete("config", "key")
	require.NoError(t, err)

	_, err = s.Get("config", "key")
	assert.Error(t, err)
}

func TestDelete_Good_NonExistent(t *testing.T) {
	// Deleting a key that does not exist should not error.
	s, _ := New(":memory:")
	defer s.Close()

	err := s.Delete("g", "nope")
	assert.NoError(t, err)
}

func TestDelete_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	err := s.Delete("g", "k")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Count
// ---------------------------------------------------------------------------

func TestCount_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("grp", "a", "1")
	_ = s.Set("grp", "b", "2")
	_ = s.Set("other", "c", "3")

	n, err := s.Count("grp")
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestCount_Good_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	n, err := s.Count("empty")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestCount_Good_BulkInsert(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	const total = 500
	for i := 0; i < total; i++ {
		require.NoError(t, s.Set("bulk", fmt.Sprintf("key-%04d", i), "v"))
	}
	n, err := s.Count("bulk")
	require.NoError(t, err)
	assert.Equal(t, total, n)
}

func TestCount_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.Count("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// DeleteGroup
// ---------------------------------------------------------------------------

func TestDeleteGroup_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("grp", "a", "1")
	_ = s.Set("grp", "b", "2")
	err := s.DeleteGroup("grp")
	require.NoError(t, err)

	n, _ := s.Count("grp")
	assert.Equal(t, 0, n)
}

func TestDeleteGroup_Good_ThenGetAllEmpty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("grp", "a", "1")
	_ = s.Set("grp", "b", "2")
	require.NoError(t, s.DeleteGroup("grp"))

	all, err := s.GetAll("grp")
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestDeleteGroup_Good_IsolatesOtherGroups(t *testing.T) {
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

func TestDeleteGroup_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	err := s.DeleteGroup("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// GetAll
// ---------------------------------------------------------------------------

func TestGetAll_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("grp", "a", "1")
	_ = s.Set("grp", "b", "2")
	_ = s.Set("other", "c", "3")

	all, err := s.GetAll("grp")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, all)
}

func TestGetAll_Good_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	all, err := s.GetAll("empty")
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestGetAll_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.GetAll("g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

func TestRender_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("user", "pool", "pool.lthn.io:3333")
	_ = s.Set("user", "wallet", "iz...")

	tmpl := `{"pool":"{{ .pool }}","wallet":"{{ .wallet }}"}`
	out, err := s.Render(tmpl, "user")
	require.NoError(t, err)
	assert.Contains(t, out, "pool.lthn.io:3333")
	assert.Contains(t, out, "iz...")
}

func TestRender_Good_EmptyGroup(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// Template that does not reference any variables.
	out, err := s.Render("static content", "empty")
	require.NoError(t, err)
	assert.Equal(t, "static content", out)
}

func TestRender_Bad_InvalidTemplateSyntax(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.Render("{{ .unclosed", "g")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.Render: parse")
}

func TestRender_Bad_MissingTemplateVar(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// text/template with a missing key on a map returns <no value>, not an error,
	// unless Option("missingkey=error") is set. The default behaviour is no error.
	out, err := s.Render("hello {{ .missing }}", "g")
	require.NoError(t, err)
	assert.Contains(t, out, "hello")
}

func TestRender_Bad_ExecError(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_ = s.Set("g", "name", "hello")

	// Calling a string as a function triggers a template execution error.
	_, err := s.Render(`{{ call .name }}`, "g")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.Render: exec")
}

func TestRender_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.Render("{{ .x }}", "g")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestClose_Good(t *testing.T) {
	s, _ := New(":memory:")
	err := s.Close()
	require.NoError(t, err)
}

func TestClose_Good_OperationsFailAfterClose(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

func TestEdgeCases(t *testing.T) {
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
		{"special SQL chars", "g", "'; DROP TABLE kv;--", "val"},
		{"backslash", "g", "back\\slash", "val\\ue"},
		{"percent", "g", "100%", "50%"},
		{"long key", "g", strings.Repeat("k", 10000), "val"},
		{"long value", "g", "longval", strings.Repeat("v", 100000)},
		{"long group", strings.Repeat("g", 10000), "k", "val"},
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

func TestGroupIsolation(t *testing.T) {
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

func TestConcurrent_ReadWrite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent.db")
	s, err := New(dbPath)
	require.NoError(t, err)
	defer s.Close()

	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*opsPerGoroutine*2)

	// Writers.
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			group := fmt.Sprintf("grp-%d", id)
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("key-%d", i)
				val := fmt.Sprintf("val-%d-%d", id, i)
				if err := s.Set(group, key, val); err != nil {
					errs <- fmt.Errorf("writer %d: %w", id, err)
				}
			}
		}(g)
	}

	// Readers — start immediately alongside writers.
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			group := fmt.Sprintf("grp-%d", id)
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("key-%d", i)
				_, err := s.Get(group, key)
				// ErrNotFound is acceptable — the writer may not have written yet.
				if err != nil && !errors.Is(err, ErrNotFound) {
					errs <- fmt.Errorf("reader %d: %w", id, err)
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
	for g := 0; g < goroutines; g++ {
		group := fmt.Sprintf("grp-%d", g)
		n, err := s.Count(group)
		require.NoError(t, err)
		assert.Equal(t, opsPerGoroutine, n, "group %s should have all keys", group)
	}
}

func TestConcurrent_GetAll(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "getall.db"))
	require.NoError(t, err)
	defer s.Close()

	// Seed data.
	for i := 0; i < 50; i++ {
		require.NoError(t, s.Set("shared", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			all, err := s.GetAll("shared")
			if err != nil {
				t.Errorf("GetAll failed: %v", err)
				return
			}
			if len(all) != 50 {
				t.Errorf("expected 50 keys, got %d", len(all))
			}
		}()
	}
	wg.Wait()
}

func TestConcurrent_DeleteGroup(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "delgrp.db"))
	require.NoError(t, err)
	defer s.Close()

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			grp := fmt.Sprintf("g%d", id)
			for i := 0; i < 20; i++ {
				_ = s.Set(grp, fmt.Sprintf("k%d", i), "v")
			}
			_ = s.DeleteGroup(grp)
		}(g)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// ErrNotFound wrapping verification
// ---------------------------------------------------------------------------

func TestErrNotFound_Is(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := s.Get("g", "k")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound), "error should be ErrNotFound via errors.Is")
	assert.Contains(t, err.Error(), "g/k", "error message should include group/key")
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkSet(b *testing.B) {
	s, _ := New(":memory:")
	defer s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set("bench", fmt.Sprintf("key-%d", i), "value")
	}
}

func BenchmarkGet(b *testing.B) {
	s, _ := New(":memory:")
	defer s.Close()

	// Pre-populate.
	const keys = 10000
	for i := 0; i < keys; i++ {
		_ = s.Set("bench", fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get("bench", fmt.Sprintf("key-%d", i%keys))
	}
}

func BenchmarkGetAll(b *testing.B) {
	s, _ := New(":memory:")
	defer s.Close()

	const keys = 10000
	for i := 0; i < keys; i++ {
		_ = s.Set("bench", fmt.Sprintf("key-%d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.GetAll("bench")
	}
}

func BenchmarkSet_FileBacked(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench.db")
	s, _ := New(dbPath)
	defer s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Set("bench", fmt.Sprintf("key-%d", i), "value")
	}
}

// ---------------------------------------------------------------------------
// TTL support (Phase 1)
// ---------------------------------------------------------------------------

func TestSetWithTTL_Good(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	err := s.SetWithTTL("g", "k", "v", 5*time.Second)
	require.NoError(t, err)

	val, err := s.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val)
}

func TestSetWithTTL_Good_Upsert(t *testing.T) {
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

func TestSetWithTTL_Good_ExpiresOnGet(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// Set a key with a very short TTL.
	require.NoError(t, s.SetWithTTL("g", "ephemeral", "gone-soon", 1*time.Millisecond))

	// Wait for it to expire.
	time.Sleep(5 * time.Millisecond)

	_, err := s.Get("g", "ephemeral")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound), "expired key should be ErrNotFound")
}

func TestSetWithTTL_Good_ExcludedFromCount(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "permanent", "stays"))
	require.NoError(t, s.SetWithTTL("g", "temp", "goes", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	n, err := s.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 1, n, "expired key should not be counted")
}

func TestSetWithTTL_Good_ExcludedFromGetAll(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "a", "1"))
	require.NoError(t, s.SetWithTTL("g", "b", "2", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	all, err := s.GetAll("g")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1"}, all, "expired key should be excluded")
}

func TestSetWithTTL_Good_ExcludedFromRender(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "name", "Alice"))
	require.NoError(t, s.SetWithTTL("g", "temp", "gone", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	out, err := s.Render("Hello {{ .name }}", "g")
	require.NoError(t, err)
	assert.Equal(t, "Hello Alice", out)
}

func TestSetWithTTL_Good_SetClearsTTL(t *testing.T) {
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

func TestSetWithTTL_Good_FutureTTLAccessible(t *testing.T) {
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

func TestSetWithTTL_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	err := s.SetWithTTL("g", "k", "v", time.Hour)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// PurgeExpired
// ---------------------------------------------------------------------------

func TestPurgeExpired_Good(t *testing.T) {
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

func TestPurgeExpired_Good_NoneExpired(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g", "a", "1"))
	require.NoError(t, s.SetWithTTL("g", "b", "2", time.Hour))

	removed, err := s.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed)
}

func TestPurgeExpired_Good_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	removed, err := s.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed)
}

func TestPurgeExpired_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.PurgeExpired()
	require.Error(t, err)
}

func TestPurgeExpired_Good_BackgroundPurge(t *testing.T) {
	s, _ := New(":memory:")
	// Override purge interval for testing: restart the goroutine with a short interval.
	s.cancel()
	s.wg.Wait()
	s.purgeInterval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.startPurge(ctx)
	defer s.Close()

	require.NoError(t, s.SetWithTTL("g", "ephemeral", "v", 1*time.Millisecond))
	require.NoError(t, s.Set("g", "permanent", "stays"))

	// Wait for the background purge to fire.
	time.Sleep(60 * time.Millisecond)

	// The expired key should have been removed by the background goroutine.
	// Use a raw query to check the row is actually gone (not just filtered by Get).
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM kv WHERE grp = ?", "g").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "background purge should have deleted the expired row")
}

// ---------------------------------------------------------------------------
// Schema migration — reopening an existing database
// ---------------------------------------------------------------------------

func TestSchemaUpgrade_ExistingDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "upgrade.db")

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

func TestSchemaUpgrade_PreTTLDatabase(t *testing.T) {
	// Simulate a database created before TTL support (no expires_at column).
	dbPath := filepath.Join(t.TempDir(), "pre-ttl.db")
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

	// Open with New — should migrate the schema by adding expires_at.
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

func TestConcurrent_TTL(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "concurrent-ttl.db"))
	require.NoError(t, err)
	defer s.Close()

	const goroutines = 10
	const ops = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			grp := fmt.Sprintf("ttl-%d", id)
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k%d", i)
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

	for g := 0; g < goroutines; g++ {
		grp := fmt.Sprintf("ttl-%d", g)
		n, err := s.Count(grp)
		require.NoError(t, err)
		assert.Equal(t, ops/2, n, "only non-TTL keys should remain in %s", grp)
	}
}
