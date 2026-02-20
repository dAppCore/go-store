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

Scoped store wrapper that auto-prefixes groups with a namespace to prevent key collisions across tenants. Pure Go, no new deps.

### 2.1 ScopedStore Wrapper

- [x] **Create `scope.go`** — A lightweight wrapper around `*Store` that auto-prefixes all group names:
  - `type ScopedStore struct { store *Store; namespace string }` — holds reference to underlying store and namespace prefix
  - `func NewScoped(store *Store, namespace string) *ScopedStore` — constructor. Validates namespace is non-empty, alphanumeric + hyphens only.
  - `func (s *ScopedStore) prefix(group string) string` — returns `namespace + ":" + group`
  - Implement all `Store` methods on `ScopedStore` that delegate to the underlying store with prefixed groups:
    - `Get(group, key)`, `Set(group, key, value)`, `SetWithTTL(group, key, value, ttl)`
    - `Delete(group, key)`, `DeleteGroup(group)`
    - `GetAll(group)`, `Count(group)`
    - `Render(group, key, data)`
  - Each method simply calls `s.store.Method(s.prefix(group), key, ...)` — thin delegation, no logic duplication.

### 2.2 Quota Enforcement

- [x] **Add `QuotaConfig` to ScopedStore** — Optional quota limits per namespace:
  - `type QuotaConfig struct { MaxKeys int; MaxGroups int }` — zero means unlimited
  - `func NewScopedWithQuota(store *Store, namespace string, quota QuotaConfig) *ScopedStore`
  - `var ErrQuotaExceeded = errors.New("store: quota exceeded")`
- [x] **Enforce on Set()** — Before inserting, check `Count()` across all groups with the namespace prefix. If `MaxKeys > 0` and current count >= MaxKeys, return `ErrQuotaExceeded`. Only check on new keys (UPSERT existing keys doesn't increase count).
- [x] **Enforce on group creation** — Track distinct groups with the namespace prefix. If `MaxGroups > 0` and adding a new group would exceed the limit, return `ErrQuotaExceeded`.
- [x] **Add `CountAll() (int, error)` to Store** — Returns total key count across ALL groups matching a prefix. SQL: `SELECT COUNT(*) FROM kv WHERE grp LIKE ? AND (expires_at IS NULL OR expires_at > ?)` with `namespace + ":%"`.
- [x] **Add `Groups() ([]string, error)` to Store** — Returns distinct group names. SQL: `SELECT DISTINCT grp FROM kv WHERE (expires_at IS NULL OR expires_at > ?)`. Useful for quota checks and admin tooling.

### 2.3 Tests

- [x] **ScopedStore basic tests** — Set/Get/Delete through ScopedStore, verify underlying store has prefixed groups, two namespaces don't collide, GetAll returns only scoped group's keys
- [x] **Quota tests** — (a) MaxKeys=5, insert 5 keys → OK, insert 6th → ErrQuotaExceeded, (b) UPSERT existing key doesn't count towards quota, (c) Delete + re-insert stays within quota, (d) MaxGroups=3, create 3 groups → OK, 4th → ErrQuotaExceeded, (e) zero quota = unlimited, (f) TTL-expired keys don't count towards quota
- [x] **CountAll/Groups tests** — (a) CountAll with mixed namespaces, (b) Groups returns distinct list, (c) expired keys excluded from both
- [x] **Existing tests still pass** — No changes to Store API, backward compatible. Coverage: 90.9% → 94.7%.

## Phase 3: Event Hooks

Reactive notification system for store mutations. Pure Go, no new deps. The go-ws integration point is via callbacks — go-store does NOT import go-ws.

### 3.1 Event Types (`events.go`)

- [ ] **Create `events.go`** — Define the event model:
  - `type EventType int` with constants: `EventSet`, `EventDelete`, `EventDeleteGroup`
  - `type Event struct { Type EventType; Group string; Key string; Value string; Timestamp time.Time }` — Key is empty for `EventDeleteGroup`, Value is only populated for `EventSet`
  - `func (t EventType) String() string` — returns `"set"`, `"delete"`, `"delete_group"`

### 3.2 Watcher API

- [ ] **Add watcher infrastructure to Store** — New fields on `Store`:
  - `watchers  []*Watcher` — registered watchers
  - `callbacks []callbackEntry` — registered callbacks
  - `mu        sync.RWMutex` — protects watchers and callbacks (separate from SQLite serialisation)
  - `nextID    uint64` — monotonic ID for callbacks
- [ ] **`type Watcher struct`** — `Ch <-chan Event` (public read-only channel), `ch chan Event` (internal write), `group string`, `key string`, `id uint64`
- [ ] **`func (s *Store) Watch(group, key string) *Watcher`** — Create a watcher with buffered channel (cap 16). `"*"` as key matches all keys in the group. `"*"` for both group and key matches everything. Returns the watcher.
- [ ] **`func (s *Store) Unwatch(w *Watcher)`** — Remove watcher from slice, close its channel. Safe to call multiple times.

### 3.3 Callback Hook

- [ ] **`func (s *Store) OnChange(fn func(Event)) func()`** — Register a callback for all mutations. Returns an unregister function. Callbacks are called synchronously in the emitting goroutine (caller controls concurrency). This is the go-ws integration point — consumers do:
  ```go
  unreg := store.OnChange(func(e store.Event) {
      hub.SendToChannel("store-events", e)
  })
  defer unreg()
  ```

### 3.4 Emit Events

- [ ] **Modify `Set()`** — After successful DB write, call `s.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})`
- [ ] **Modify `SetWithTTL()`** — Same as Set but includes TTL event
- [ ] **Modify `Delete()`** — Emit `EventDelete` after successful DB write
- [ ] **Modify `DeleteGroup()`** — Emit `EventDeleteGroup` with Key="" after successful DB write
- [ ] **`func (s *Store) notify(e Event)`** — Internal method:
  1. Lock `s.mu` (read lock), iterate watchers: if watcher matches (group/key or wildcard), non-blocking send to `w.ch` (drop if full — don't block writer)
  2. Call each callback `fn(e)` synchronously
  3. Unlock

### 3.5 ScopedStore Events

- [ ] **ScopedStore mutations emit events with full prefixed group** — No extra work needed since ScopedStore delegates to Store methods which already emit. The Event.Group will contain the full `namespace:group` string, which is correct for consumers.

### 3.6 Tests (`events_test.go`)

- [ ] **Watch specific key** — Set triggers event on matching watcher, non-matching key gets nothing
- [ ] **Watch wildcard `"*"`** — Multiple Sets to different keys in same group all trigger
- [ ] **Watch all `("*", "*")`** — All mutations across all groups trigger
- [ ] **Unwatch stops delivery** — After Unwatch, no more events on channel, channel is closed
- [ ] **Delete triggers event** — EventDelete with correct group/key
- [ ] **DeleteGroup triggers event** — EventDeleteGroup with empty Key
- [ ] **OnChange callback fires** — Register callback, Set/Delete triggers it
- [ ] **OnChange unregister** — After calling returned func, callback stops firing
- [ ] **Buffer-full doesn't block** — Fill channel buffer (16 events), verify next Set doesn't block/deadlock
- [ ] **Multiple watchers on same key** — Both receive events independently
- [ ] **Concurrent Watch/Unwatch** — 10 goroutines adding/removing watchers while Sets happen (race test)
- [ ] **ScopedStore events** — ScopedStore Set triggers event with prefixed group name
- [ ] **Existing tests still pass** — No regressions

---

## Workflow

1. Virgil in core/go writes tasks here after research
2. This repo's dedicated session picks up tasks in phase order
3. Mark `[x]` when done, note commit hash
