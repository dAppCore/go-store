# FINDINGS.md -- go-store

## 2026-02-19: Split from core/go (Virgil)

### Origin

Extracted from `forge.lthn.ai/core/go` `pkg/store/` on 19 Feb 2026.

### Architecture

- SQLite database with WAL mode enabled for concurrent read performance
- Compound primary key: `(group, key)` -- groups act as logical namespaces
- UPSERT semantics on write (INSERT OR REPLACE)
- Template rendering support via Go `text/template` for dynamic values
- Pure Go SQLite driver via `modernc.org/sqlite` (no CGO required)

### Dependencies

- `modernc.org/sqlite` -- pure Go SQLite implementation

### Tests

- 1 test file covering CRUD operations, group isolation, and template rendering

## 2026-02-20: Phase 0 & Phase 1 (Charon)

### Concurrency Fix: SQLITE_BUSY under contention

**Problem:** With `database/sql`'s default connection pool, concurrent goroutines would get different connections from the pool. PRAGMA statements (journal_mode, busy_timeout) are per-connection, so some connections would lack the WAL and busy_timeout settings. Under heavy concurrent writes (10 goroutines x 100 ops), this caused persistent `SQLITE_BUSY` errors even with busy_timeout set.

**Root cause:** `database/sql` pools connections. Each `Exec`/`Query` call might use a different connection. PRAGMAs applied to one connection do not apply to others.

**Fix:** `db.SetMaxOpenConns(1)` serialises all access through a single connection. This ensures PRAGMAs stick and eliminates BUSY errors. SQLite is single-writer by design, so there is no concurrency penalty -- the pool was already being serialised at the SQLite lock level, just with errors instead of queueing.

**Added:** `PRAGMA busy_timeout=5000` as a defence-in-depth measure.

### Coverage: 73.1% -> 90.9%

Remaining uncovered code (9.1%) is entirely defensive error handling in:
- `New()`: sql.Open error, busy_timeout pragma error, schema creation error -- all unreachable with the modernc.org/sqlite driver under normal operation
- `GetAll()`/`Render()`: rows.Scan and rows.Err error paths -- require internal SQLite driver corruption to trigger

These are correct defensive checks that protect against hypothetical driver/OS failures but cannot be triggered through integration tests against a real SQLite database.

### Phase 1: TTL Support

Added optional time-to-live for keys:
- `expires_at INTEGER` nullable column in the `kv` table
- `SetWithTTL(group, key, value, duration)` stores keys that auto-expire
- `Get()` performs lazy deletion of expired keys on read
- `Count()`, `GetAll()`, `Render()` all exclude expired entries from results
- `PurgeExpired()` public method for manual cleanup
- Background goroutine purges expired entries every 60 seconds (configurable via `purgeInterval` field)
- Schema migration: `ALTER TABLE kv ADD COLUMN expires_at INTEGER` handles databases created before TTL support
- `Set()` clears TTL when overwriting a key (sets `expires_at = NULL`)

### Benchmarks

```
BenchmarkSet-32               119280    10290 ns/op     328 B/op    12 allocs/op
BenchmarkGet-32               335707     3589 ns/op     576 B/op    21 allocs/op
BenchmarkGetAll-32 (10K keys)    258  4741451 ns/op 2268787 B/op 80095 allocs/op
BenchmarkSet_FileBacked-32      4525   265868 ns/op     327 B/op    12 allocs/op
```

- In-memory Set: ~97K ops/sec
- In-memory Get: ~279K ops/sec
- File-backed Set: ~3.8K ops/sec (dominated by fsync)
- GetAll with 10K keys: ~2.3MB allocated per call
