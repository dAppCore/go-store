# TODO.md -- go-store

Dispatched from core/go orchestration. Pick up tasks in order.

---

## Phase 0: Hardening & Test Coverage

- [x] **Expand test coverage** -- concurrent Set/Get with 10 goroutines (race test), Render() with invalid template syntax, Render() with missing template vars, Get() on non-existent group vs non-existent key, DeleteGroup() then verify GetAll() returns empty, Count() after bulk inserts, :memory: vs file-backed store, WAL mode verification. Coverage: 73.1% -> 90.9%.
- [x] **Edge cases** -- empty key, empty value, empty group, very long key (10K chars), binary-ish value (null bytes), Unicode keys and values, CJK, Arabic, SQL injection attempts, special characters.
- [x] **Benchmark** -- BenchmarkSet, BenchmarkGet, BenchmarkGetAll with 10K keys, BenchmarkSet_FileBacked.
- [x] **`go vet ./...` clean** -- no warnings.
- [x] **Concurrency fix** -- Added `db.SetMaxOpenConns(1)` and `PRAGMA busy_timeout=5000` to prevent SQLITE_BUSY errors under concurrent writes.

## Phase 1: TTL Support

- [x] Add optional expiry timestamp for keys (`expires_at INTEGER` column)
- [x] Background goroutine to purge expired entries (configurable interval, default 60s)
- [x] `SetWithTTL(group, key, value, duration)` API
- [x] Lazy expiry check on `Get` as fallback
- [x] `PurgeExpired()` public method for manual purge
- [x] `Count`, `GetAll`, `Render` exclude expired entries
- [x] Schema migration for pre-TTL databases (ALTER TABLE ADD COLUMN)
- [x] Tests for all TTL functionality including concurrent TTL access

## Phase 2: Namespace Isolation

- [ ] Group-based access control for multi-tenant use
- [ ] Namespace prefixing to prevent key collisions across tenants
- [ ] Per-namespace quota limits (max keys, max total size)

## Phase 3: Event Hooks

- [ ] Notify on `Set` / `Delete` for reactive patterns
- [ ] Channel-based subscription: `Watch(group, key) <-chan Event`
- [ ] Support wildcard watches (`Watch(group, "*")`)
- [ ] Integration hook for go-ws to broadcast store changes via WebSocket

---

## Workflow

1. Virgil in core/go writes tasks here after research
2. This repo's dedicated session picks up tasks in phase order
3. Mark `[x]` when done, note commit hash
