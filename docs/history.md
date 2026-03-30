# Project History — go-store

## Origin

Extracted from `forge.lthn.ai/core/go` (`pkg/store/`) on 19 February 2026 by Virgil. The extraction gave the package its own module path (`forge.lthn.ai/core/go-store`), its own repository, and independent versioning.

At extraction the package comprised a single source file and a single test file. It provided basic CRUD with group namespacing and template rendering but had no TTL, no namespace isolation, and no event system. Test coverage was 73.1%.

---

## Phase 0 — Hardening and Test Coverage

**Agent:** Charon
**Completed:** 2026-02-20

### Concurrency fix: SQLITE_BUSY under contention

**Problem.** The `database/sql` connection pool hands out different physical connections for each `Exec` or `Query` call. SQLite pragmas (`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout`) are per-connection. Under concurrent write load (10 goroutines, 100 ops each), connections from the pool that had not received the WAL pragma would block and return `SQLITE_BUSY` immediately rather than waiting.

**Fix.** `db.SetMaxOpenConns(1)` serialises all database access through a single connection. Because SQLite is a single-writer database by design (it serialises writes at the file-lock level regardless of pool size), this does not reduce write throughput. It eliminates the BUSY errors by ensuring the pragma settings always apply.

**Defence in depth.** `PRAGMA busy_timeout=5000` was added to make the single connection wait up to 5 seconds before reporting a timeout error, providing additional resilience.

### Extended test coverage

Added tests for:

- Concurrent read/write with 10 goroutines and the race detector
- `Render()` with invalid template syntax (parse error)
- `Render()` with template execution error (calling a string as a function)
- `Get()` on a non-existent group vs. a non-existent key
- `DeleteGroup()` followed by `GetAll()` returning empty
- `Count()` after 500 bulk inserts
- In-memory vs. file-backed store (persistence across open/close)
- WAL mode verification via `PRAGMA journal_mode` query
- Edge cases: empty key, empty value, empty group, 10K-character key, binary-like values with null bytes, Unicode (accented, CJK, Arabic), SQL injection attempts, special characters

Coverage: 73.1% to 90.9%.

The remaining 9.1% comprised defensive error paths in `New()`, `GetAll()`, and `Render()` that are unreachable through integration tests against a healthy SQLite database (driver initialisation failures, scan errors on NULL columns, rows iteration errors on corrupted pages).

### Benchmarks

Established baseline benchmark results:

```
BenchmarkSet-32               119280    10290 ns/op
BenchmarkGet-32               335707     3589 ns/op
BenchmarkGetAll-32 (10K keys)    258  4741451 ns/op
BenchmarkSet_FileBacked-32      4525   265868 ns/op
```

`go vet ./...` made clean. No warnings.

---

## Phase 1 — TTL Support

**Agent:** Charon
**Completed:** 2026-02-20

Added optional time-to-live for keys.

### Changes

- `expires_at INTEGER` nullable column added to the `kv` schema.
- `SetWithTTL(group, key, value string, ttl time.Duration)` stores the current time plus TTL as a Unix millisecond timestamp in `expires_at`.
- `Get()` performs lazy deletion: if a key is found with an `expires_at` in the past, it is deleted and `NotFoundError` is returned (`ErrNotFound` remains as a compatibility alias).
- `Count()`, `GetAll()`, and `Render()` include `(expires_at IS NULL OR expires_at > ?)` in all queries, excluding expired keys from results.
- `PurgeExpired()` public method deletes all physically stored expired rows and returns the count removed.
- Background goroutine calls `PurgeExpired()` every 60 seconds, controlled by a `context.WithCancel` that is cancelled on `Close()`.
- `Set()` clears any existing TTL when overwriting a key (sets `expires_at = NULL`).
- Schema migration: `ALTER TABLE kv ADD COLUMN expires_at INTEGER` runs on `New()`. The "duplicate column" error on already-upgraded databases is silently ignored.

### Tests added

TTL functionality tests covering: normal expiry on `Get`, exclusion from `Count`/`GetAll`/`Render`, `SetWithTTL` upsert, plain `Set` clearing TTL, future TTL remaining accessible, background purge goroutine, concurrent TTL access with 10 goroutines, schema migration from a pre-TTL database (manually constructed without `expires_at`).

Coverage: 90.9% to 94.7%.

---

## Phase 2 — Namespace Isolation

**Agent:** Charon
**Completed:** 2026-02-20

Added `ScopedStore` for multi-tenant namespace isolation.

### Changes

- `scope.go` introduced with `ScopedStore` wrapping `*Store`.
- Namespace strings validated against `^[a-zA-Z0-9-]+$`.
- `NewScoped(store, namespace)` constructor.
- All `Store` methods delegated with group automatically prefixed as `namespace + ":" + group`.
- `QuotaConfig{MaxKeys, MaxGroups int}` struct; zero means unlimited.
- `NewScopedWithQuota(store, namespace, quota)` constructor.
- `QuotaExceededError` sentinel error (`ErrQuotaExceeded` remains as a compatibility alias).
- `checkQuota(group, key)` internal method: skips upserts (existing key), checks `CountAll(namespace+":")` against `MaxKeys`, checks `Groups(namespace+":")` against `MaxGroups` only when the group is new.
- `CountAll(prefix string)` added to `Store`: counts non-expired keys across all groups matching a prefix. Empty prefix counts across all groups.
- `Groups(prefix string)` added to `Store`: returns distinct non-expired group names matching a prefix. Empty prefix returns all groups.

### Tests added

ScopedStore basic CRUD, cross-namespace isolation, `GetAll` scoping. Quota tests: MaxKeys limit, upsert does not count, delete and re-insert stays within quota, MaxGroups limit, zero quota is unlimited, TTL-expired keys do not count towards quota. `CountAll` and `Groups` tests with mixed namespaces and expired key exclusion.

Coverage: 94.7% to 95.5% (approximate; coverage_test.go added to cover defensive paths).

---

## Phase 3 — Event Hooks

**Agent:** Charon
**Completed:** 2026-02-20

Added a reactive notification system for store mutations.

### Changes

- `events.go` introduced with `EventType` (`EventSet`, `EventDelete`, `EventDeleteGroup`), `Event` struct, `Watcher` struct, `callbackEntry` struct.
- `watcherBufferSize = 16` constant.
- `Watch(group, key string) *Watcher`: creates a buffered channel watcher. Wildcard `"*"` supported for both group and key. Uses `atomic.AddUint64` for monotonic watcher IDs.
- `Unwatch(w *Watcher)`: removes watcher from the registry and closes its channel. Idempotent.
- `OnChange(fn func(Event)) func()`: registers a synchronous callback. Returns an idempotent unregister function using `sync.Once`.
- `notify(e Event)`: internal dispatch. Acquires read-lock on `s.mu`; non-blocking send to each matching watcher channel (drop-on-full); calls each callback synchronously. Separate `watcherMatches` helper handles wildcard logic.
- `Set()`, `SetWithTTL()`, `Delete()`, `DeleteGroup()` each call `notify()` after the successful database write.
- `Store` struct extended with `watchers []*Watcher`, `callbacks []callbackEntry`, `mu sync.RWMutex`, `nextID uint64`.
- ScopedStore mutations automatically emit events with the full prefixed group name — no extra implementation required.

### Tests added

Specific-key watcher receives matching events and ignores non-matching keys. Wildcard-key watcher receives all keys in a group. Global wildcard `("*", "*")` receives all mutations across all groups. `Unwatch` stops delivery and closes the channel. `Unwatch` is idempotent. Delete and DeleteGroup emit correct event types with correct populated fields. `OnChange` callback fires on Set and Delete. `OnChange` unregister stops future invocations (idempotent). Buffer-full (32 writes against cap-16 channel) does not block the writer. Multiple watchers on the same key receive events independently. Concurrent Watch/Unwatch during concurrent writes (race test, 10 goroutines). `ScopedStore` events carry the prefixed group name. `SetWithTTL` emits `EventSet`. `EventType.String()` returns correct labels including `"unknown"` for undefined values.

Coverage: 94.7% to 95.5%.

---

## Coverage Test Suite

`coverage_test.go` exercises defensive error paths that integration tests cannot reach through normal usage:

- Schema conflict: pre-existing SQLite index named `kv` causes `New()` to return `store.New: schema: ...`.
- `GetAll` scan error: NULL key in a row (requires manually altering the schema to remove the NOT NULL constraint).
- `GetAll` rows iteration error: physically corrupting database pages mid-file to trigger `rows.Err()` during multi-page scans.
- `Render` scan error: same NULL-key technique.
- `Render` rows iteration error: same corruption technique.

These tests exercise correct defensive code. They must continue to pass but are not indicative of real failure modes in production.

---

## Known Limitations

**Single writer.** `SetMaxOpenConns(1)` serialises all access through one connection. Write throughput is bounded by SQLite's single-writer architecture. This is appropriate for the intended use cases (configuration storage, session state, per-tenant key-value data) but is not suitable for high-throughput append-only workloads.

**File-backed write throughput.** File-backed `Set` operations (~3,800 ops/sec on Apple M-series) are dominated by fsync. Applications writing at higher rates should use in-memory stores or consider WAL checkpoint tuning.

**`GetAll` memory usage.** Fetching a group with 10,000 keys allocates approximately 2.3 MB per call. There is no pagination API. Applications with very large groups should restructure data into smaller groups or query selectively.

**No cross-group transactions.** There is no API for atomic multi-group operations. Each method is individually atomic at the SQLite level, but there is no `Begin`/`Commit` exposed to callers.

**No wildcard deletes.** There is no `DeletePrefix` or pattern-based delete. To delete all groups under a namespace, callers must retrieve the group list via `Groups()` and delete each individually.

**Callback deadlock risk.** `OnChange` callbacks run synchronously in the writer's goroutine while holding `s.mu` (read). Calling any `Store` method that calls `notify()` from within a callback will attempt to re-acquire `s.mu` (read), which is permitted with a read-lock but calling `Watch`/`Unwatch`/`OnChange` within a callback will deadlock (they require a write-lock). Document this constraint prominently in callback usage.

**No persistence of watcher registrations.** Watchers and callbacks are in-memory only. They are not persisted across `Close`/`New` cycles.

---

## Future Considerations

These are design notes, not committed work:

- **Pagination for `GetAll`.** A `GetPage(group string, offset, limit int)` method would support large groups without full in-memory materialisation.
- **Indexed prefix keys.** An additional index on `(grp, key)` prefix would accelerate prefix scans without a full-table scan.
- **TTL background purge interval as constructor option.** Currently only settable by mutating `s.purgeInterval` directly in tests. A `WithPurgeInterval(d time.Duration)` functional option would make this part of the public API.
- **Cross-group atomic operations.** Exposing a `Transaction(func(tx *StoreTx) error)` API would allow callers to compose atomic multi-group operations.
- **`DeletePrefix(prefix string)` method.** Would enable efficient cleanup of an entire namespace without first listing groups.
