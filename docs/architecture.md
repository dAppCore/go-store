---
title: Architecture
description: Internal design of go-store -- storage layer, group/key model, TTL expiry, event system, namespace isolation, and concurrency model.
---

# Architecture

This document describes how go-store works internally. It covers the storage layer, the data model, TTL expiry, the event system, namespace isolation with quota enforcement, and the concurrency model.

## Storage Layer

### SQLite with WAL Mode

Every `Store` instance opens a single SQLite database and immediately applies two pragmas:

```sql
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
```

WAL (Write-Ahead Logging) mode allows concurrent readers to proceed without blocking writers. The `busy_timeout` of 5000 milliseconds causes the driver to wait and retry rather than immediately returning `SQLITE_BUSY` under write contention.

### Single Connection Constraint

The `database/sql` package maintains a connection pool by default. SQLite pragmas are per-connection: if the pool hands out a second connection, that connection inherits none of the WAL or busy-timeout settings, causing `SQLITE_BUSY` errors under concurrent load.

go-store calls `db.SetMaxOpenConns(1)` to pin all access to a single connection. Since SQLite serialises writes at the file level regardless, this introduces no additional throughput penalty. It eliminates the BUSY errors by ensuring the pragma settings always apply.

### Schema

```sql
CREATE TABLE IF NOT EXISTS kv (
    grp        TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      TEXT NOT NULL,
    expires_at INTEGER,
    PRIMARY KEY (grp, key)
)
```

The compound primary key `(grp, key)` enforces uniqueness per group-key pair and provides efficient indexed lookups. The `expires_at` column stores a Unix millisecond timestamp (nullable); a `NULL` value means the key never expires.

**Schema migration.** Databases created before TTL support lacked the `expires_at` column. On `New()`, go-store runs `ALTER TABLE kv ADD COLUMN expires_at INTEGER`. If the column already exists, SQLite returns a "duplicate column" error which is silently ignored. This allows seamless upgrades of existing databases.

## Group/Key Model

Keys are addressed by a two-level path: `(group, key)`. Groups act as logical namespaces within a single database. Groups are implicit -- they exist as a consequence of the keys they contain and are destroyed automatically when all their keys are deleted.

This model maps naturally to domain concepts:

```
group: "user:42:config"     key: "theme"
group: "user:42:config"     key: "language"
group: "session:abc"        key: "token"
```

All read operations (`Get`, `GetAll`, `Count`, `Render`) are scoped to a single group. `DeleteGroup` atomically removes all keys in a group. `CountAll` and `Groups` operate across groups by prefix match.

## UPSERT Semantics

All writes use `INSERT ... ON CONFLICT(grp, key) DO UPDATE`. This means:

- Inserting a new key creates it.
- Inserting an existing key overwrites its value and (for `Set`) clears any TTL.
- UPSERT never duplicates a key.
- The operation is idempotent with respect to row count.

`Set` clears `expires_at` on upsert by setting it to `NULL`. `SetWithTTL` refreshes the expiry timestamp on upsert.

## TTL Expiry

Keys may be created with a time-to-live via `SetWithTTL`. Expiry is stored as a Unix millisecond timestamp in `expires_at`.

Expiry is enforced in three ways:

### 1. Lazy Deletion on Get

If a key is found but its `expires_at` is in the past, it is deleted synchronously before returning `NotFoundError`. This prevents stale values from being returned even if the background purge has not run yet.

### 2. Query-Time Filtering

All bulk operations (`GetAll`, `All`, `Count`, `Render`, `CountAll`, `Groups`, `GroupsSeq`) include `(expires_at IS NULL OR expires_at > ?)` in their `WHERE` clause. Expired keys are excluded from results without being deleted.

### 3. Background Purge Goroutine

`New()` launches a goroutine that calls `PurgeExpired()` every 60 seconds. This recovers disk space by physically removing expired rows. The goroutine is stopped cleanly by `Close()` via `context.WithCancel` and `sync.WaitGroup`.

`PurgeExpired()` is also available as a public method for applications that want manual control over purge timing.

## String Splitting Helpers

Two convenience methods build on `Get` to return iterators over parts of a stored value:

- **`GetSplit(group, key, sep)`** splits the value by a custom separator, returning an `iter.Seq[string]` via `strings.SplitSeq`.
- **`GetFields(group, key)`** splits the value by whitespace, returning an `iter.Seq[string]` via `strings.FieldsSeq`.

Both return `NotFoundError` if the key does not exist or has expired.

## Template Rendering

`Render(templateSource, group)` is a convenience method that fetches all non-expired key-value pairs from a group and renders a Go `text/template` against them. The template data is a `map[string]string` keyed by the field name.

```go
st.Set("miner", "pool", "pool.lthn.io:3333")
st.Set("miner", "wallet", "iz...")
out, _ := st.Render(`{"pool":"{{ .pool }}","wallet":"{{ .wallet }}"}`, "miner")
// out: {"pool":"pool.lthn.io:3333","wallet":"iz..."}
```

Template parse errors and execution errors are both returned as wrapped errors with context (e.g., `store.Render: parse: ...` and `store.Render: exec: ...`).

Missing template variables do not return an error by default -- Go's `text/template` renders them as `<no value>`. Applications requiring strict variable presence should validate data beforehand.

## Event System

go-store provides two mechanisms for observing mutations: channel-based watchers and synchronous callbacks. Both are defined in `events.go`.

### Event Model

```go
type Event struct {
    Type      EventType
    Group     string
    Key       string
    Value     string
    Timestamp time.Time
}
```

| EventType | String() | Key populated | Value populated |
|---|---|---|---|
| `EventSet` | `"set"` | Yes | Yes |
| `EventDelete` | `"delete"` | Yes | No |
| `EventDeleteGroup` | `"delete_group"` | No (empty) | No |

Events are emitted synchronously after each successful database write inside the internal `notify()` method.

### Watch/Unwatch

`Watch(group, key)` creates a `Watcher` with a buffered channel (`Events <-chan Event`, capacity 16).

| group argument | key argument | Receives |
|---|---|---|
| `"mygroup"` | `"mykey"` | Only mutations to that exact key |
| `"mygroup"` | `"*"` | All mutations within the group, including `DeleteGroup` |
| `"*"` | `"*"` | Every mutation across the entire store |

`Unwatch(watcher)` removes the watcher from the registry and closes its channel. It is safe to call multiple times; subsequent calls are no-ops.

**Backpressure.** Event dispatch to a watcher channel is non-blocking: if the channel buffer is full, the event is dropped silently. This prevents a slow consumer from blocking a writer. Applications that cannot afford dropped events should drain the channel promptly or use `OnChange` callbacks instead.

```go
watcher := st.Watch("config", "*")
defer st.Unwatch(watcher)

for event := range watcher.Events {
    fmt.Println(event.Type, event.Group, event.Key, event.Value)
}
```

### OnChange Callbacks

`OnChange(fn func(Event))` registers a synchronous callback that fires on every mutation. The callback runs in the goroutine that performed the write. Returns an idempotent unregister function.

This is the designed integration point for consumers such as go-ws:

```go
unreg := st.OnChange(func(e store.Event) {
    hub.SendToChannel("store-events", e)
})
defer unreg()
```

go-store does not import go-ws. The dependency flows in one direction only: go-ws (or any consumer) imports go-store.

**Important constraint.** `OnChange` callbacks execute while holding the watcher/callback read-lock (`s.mu`). Calling `Watch`, `Unwatch`, or `OnChange` from within a callback will deadlock, because those methods require a write-lock. Offload any significant work to a separate goroutine if needed.

### Internal Dispatch

The `notify(e Event)` method acquires a read-lock on `s.mu`, iterates all watchers with non-blocking channel sends, then calls each registered callback. The read-lock allows multiple concurrent `notify()` calls to proceed simultaneously. `Watch`/`Unwatch`/`OnChange` take a write-lock when modifying the registry.

Watcher matching is handled by the `watcherMatches` helper, which checks the group and key filters against the event. Wildcard `"*"` matches any value in its position.

## Namespace Isolation (ScopedStore)

`ScopedStore` wraps a `*Store` and automatically prefixes all group names with `namespace + ":"`. This prevents key collisions when multiple tenants share a single underlying database.

```go
scopedStore, _ := store.NewScoped(st, "tenant-42")
scopedStore.Set("config", "theme", "dark")
// Stored in underlying store as group="tenant-42:config", key="theme"
```

Namespace strings must match `^[a-zA-Z0-9-]+$`. Invalid namespaces are rejected at construction time.

`ScopedStore` delegates all operations to the underlying `Store` after prefixing. Events emitted by scoped operations carry the full prefixed group name in `Event.Group`, enabling watchers on the underlying store to observe scoped mutations.

`ScopedStore` exposes the same API surface as `Store` for: `Get`, `Set`, `SetWithTTL`, `Delete`, `DeleteGroup`, `GetAll`, `All`, `Count`, and `Render`. The `Namespace()` method returns the namespace string.

### Quota Enforcement

`NewScopedWithQuota(store, namespace, QuotaConfig)` adds per-namespace limits:

```go
type QuotaConfig struct {
    MaxKeys   int // maximum total keys across all groups in the namespace
    MaxGroups int // maximum distinct groups in the namespace
}
```

Zero values mean unlimited. Before each `Set` or `SetWithTTL`, the scoped store:

1. Checks whether the key already exists (upserts never consume quota).
2. If the key is new, queries `CountAll(namespace + ":")` and compares against `MaxKeys`.
3. If the group is new (current count for that group is zero), queries `GroupsSeq(namespace + ":")` and compares against `MaxGroups`.

Exceeding a limit returns `QuotaExceededError`.

## Concurrency Model

All SQLite access is serialised through a single connection (`SetMaxOpenConns(1)`). The store's watcher/callback registry is protected by a separate `sync.RWMutex` (`s.mu`). These two locks do not interact:

- DB writes acquire no application-level lock.
- `notify()` acquires `s.mu` (read) after the DB write completes.
- `Watch`/`Unwatch`/`OnChange` acquire `s.mu` (write) to modify the registry.

All operations are safe to call from multiple goroutines concurrently. The race detector is clean under the project's standard test suite (`go test -race ./...`).

## File Layout

```
store.go          Core Store type, CRUD, TTL, background purge, iterators, rendering
events.go         EventType, Event, Watcher, OnChange, notify
scope.go          ScopedStore, QuotaConfig, quota enforcement
store_test.go     Tests: CRUD, TTL, concurrency, edge cases, persistence
events_test.go    Tests: Watch, Unwatch, OnChange, event dispatch
scope_test.go     Tests: namespace isolation, quota enforcement
coverage_test.go  Tests: defensive error paths (scan errors, corruption)
bench_test.go     Performance benchmarks
```
