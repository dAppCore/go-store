# go-store RFC — SQLite Key-Value Store

> An agent should be able to use this store from this document alone.

**Module:** `dappco.re/go/store`
**Repository:** `core/go-store`
**Files:** 9

---

## 1. Overview

SQLite-backed key-value store with TTL, namespace isolation, reactive events, and quota enforcement. Pure Go (no CGO). Used by core/ide for memory caching and by agents for workspace state.

---

## 2. Architecture

| File | Purpose |
|------|---------|
| `store.go` | Core `Store`: CRUD on `(grp, key)` compound PK, TTL via `expires_at` (Unix ms), background purge (60s), `text/template` rendering, `iter.Seq2` iterators |
| `transaction.go` | `Store.Transaction`, transaction-scoped read/write helpers, staged event dispatch |
| `events.go` | `Watch`/`Unwatch` (buffered chan, cap 16, non-blocking sends) + `OnChange` callbacks (synchronous) |
| `scope.go` | `ScopedStore` wraps `*Store`, prefixes groups with `namespace:`. Quota enforcement (`MaxKeys`/`MaxGroups`) |
| `workspace.go` | `Workspace` buffer: SQLite-backed mutable accumulation in `.duckdb` files, atomic commit to journal |
| `journal.go` | SQLite journal table: write completed units, query time-series-shaped data, retention |
| `compact.go` | Cold archive: compress journal entries to JSONL.gz |
| `store_test.go` | Store unit tests |
| `workspace_test.go` | Workspace buffer tests |

---

## 3. Key Design Decisions

- **Single-connection SQLite.** `MaxOpenConns(1)` because SQLite pragmas (WAL, busy_timeout) are per-connection — a pool would hand out unpragma'd connections causing `SQLITE_BUSY`
- **TTL is triple-layered:** lazy delete on `Get`, query-time `WHERE` filtering, background purge goroutine
- **LIKE queries use `escapeLike()`** with `^` as escape char to prevent SQL wildcard injection

---

## 4. Store Struct

```go
// Store is the SQLite KV store with optional SQLite journal backing.
type Store struct {
    db      *sql.DB            // SQLite connection (single, WAL mode)
    journal JournalConfiguration // SQLite journal metadata (nil-equivalent when zero-valued)
    bucket  string             // Journal bucket name
    org     string             // Journal organisation
    mu      sync.RWMutex
    watchers map[string][]chan Event
}

// Event is emitted on Watch channels when a key changes.
type Event struct {
    Group string
    Key   string
    Value string
}
```

```go
// New creates a store. Journal is optional — pass WithJournal() to enable.
//
//   storeInstance, _ := store.New(":memory:")                           // SQLite only
//   storeInstance, _ := store.New("/path/to/db", store.WithJournal(
//       "http://localhost:8086", "core-org", "core-bucket",
//   ))
func New(path string, opts ...StoreOption) (*Store, error) { }

type StoreOption func(*Store)

func WithJournal(url, org, bucket string) StoreOption { }
```

---

## 5. API

```go
storeInstance, _ := store.New(":memory:")      // or store.New("/path/to/db")
defer storeInstance.Close()

storeInstance.Set("group", "key", "value")
storeInstance.SetWithTTL("group", "key", "value", 5*time.Minute)
value, _ := storeInstance.Get("group", "key")    // lazy-deletes expired

// Atomic multi-key/multi-group update
storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
    if err := transaction.Set("group", "first", "1"); err != nil {
        return err
    }
    return transaction.Set("group", "second", "2")
})

// Iteration
for key, value := range storeInstance.AllSeq("group") { ... }
for group := range storeInstance.GroupsSeq() { ... }

// Events
events := storeInstance.Watch("group")
storeInstance.OnChange(func(event store.Event) { ... })
```

---

## 6. ScopedStore

```go
// ScopedStore wraps a Store with a namespace prefix and optional quotas.
//
//   scopedStore, _ := store.NewScopedConfigured(storeInstance, store.ScopedStoreConfig{
//       Namespace: "mynamespace",
//       Quota:     store.QuotaConfig{MaxKeys: 100, MaxGroups: 10},
//   })
//   scopedStore.Set("key", "value")           // stored as group "mynamespace:default", key "key"
//   scopedStore.SetIn("mygroup", "key", "v")  // stored as group "mynamespace:mygroup", key "key"
type ScopedStore struct {
    store     *Store
    namespace string  // validated: ^[a-zA-Z0-9-]+$
    MaxKeys   int     // 0 = unlimited
    MaxGroups int     // 0 = unlimited
}

func NewScoped(storeInstance *Store, namespace string) (*ScopedStore, error) { }

func NewScopedConfigured(storeInstance *Store, scopedConfig ScopedStoreConfig) (*ScopedStore, error) { }

// Set stores a value in the default group ("namespace:default")
func (scopedStore *ScopedStore) Set(key, value string) error { }

// SetIn stores a value in an explicit group ("namespace:group")
func (scopedStore *ScopedStore) SetIn(group, key, value string) error { }

// Get retrieves a value from the default group
func (scopedStore *ScopedStore) Get(key string) (string, error) { }

// GetFrom retrieves a value from an explicit group
func (scopedStore *ScopedStore) GetFrom(group, key string) (string, error) { }
```

- Namespace regex: `^[a-zA-Z0-9-]+$`
- Default group: `Set(key, value)` uses literal `"default"` as group, prefixed: `"mynamespace:default"`
- `SetIn(group, key, value)` allows explicit group within the namespace
- Quota: `MaxKeys`, `MaxGroups` — checked before writes, upserts bypass

---

## 7. Transaction API

`Store.Transaction(fn)` is the supported atomic API for multi-key and multi-group work. It opens one SQLite transaction, passes a `StoreTransaction` helper to the callback, then commits only if the callback returns `nil`.

```go
func (storeInstance *Store) Transaction(operation func(*StoreTransaction) error) error { }

type StoreTransaction struct { }

func (transaction *StoreTransaction) Exists(group, key string) (bool, error) { }
func (transaction *StoreTransaction) GroupExists(group string) (bool, error) { }
func (transaction *StoreTransaction) Get(group, key string) (string, error) { }
func (transaction *StoreTransaction) Set(group, key, value string) error { }
func (transaction *StoreTransaction) SetWithTTL(group, key, value string, ttl time.Duration) error { }
func (transaction *StoreTransaction) Delete(group, key string) error { }
func (transaction *StoreTransaction) DeleteGroup(group string) error { }
func (transaction *StoreTransaction) DeletePrefix(groupPrefix string) error { }
func (transaction *StoreTransaction) GetAll(group string) (map[string]string, error) { }
func (transaction *StoreTransaction) GetPage(group string, offset, limit int) ([]KeyValue, error) { }
func (transaction *StoreTransaction) All(group string) iter.Seq2[KeyValue, error] { }
func (transaction *StoreTransaction) AllSeq(group string) iter.Seq2[KeyValue, error] { }
func (transaction *StoreTransaction) Count(group string) (int, error) { }
func (transaction *StoreTransaction) CountAll(groupPrefix string) (int, error) { }
func (transaction *StoreTransaction) Groups(groupPrefix ...string) ([]string, error) { }
func (transaction *StoreTransaction) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] { }
func (transaction *StoreTransaction) Render(templateSource, group string) (string, error) { }
func (transaction *StoreTransaction) GetSplit(group, key, separator string) (iter.Seq[string], error) { }
func (transaction *StoreTransaction) GetFields(group, key string) (iter.Seq[string], error) { }
func (transaction *StoreTransaction) PurgeExpired() (int64, error) { }
```

Contract:

- `operation == nil` returns an error before opening a transaction.
- If `operation` returns an error, the transaction rolls back and `Store.Transaction` returns that error wrapped with transaction context.
- If `operation` returns `nil`, `Store.Transaction` commits. A commit failure is returned and the deferred rollback path is attempted.
- Panics are not recovered by this API; the deferred rollback path still runs while the panic unwinds.
- Reads through `StoreTransaction` see uncommitted writes made earlier in the same callback.
- Mutations stage events during the callback. Watchers and `OnChange` callbacks are notified only after a successful commit, so rolled-back work does not propagate events.
- Callers should return helper errors from the callback. Ignoring a helper error and returning `nil` can still commit any successful earlier operations.
- Callers should use the supplied transaction helper inside the callback. Calling parent `Store` methods from inside the callback is outside the contract and may block behind the single SQLite connection.

Example:

```go
err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
    if err := transaction.Set("accounts", "alice", "10"); err != nil {
        return err
    }
    if err := transaction.Set("accounts", "bob", "12"); err != nil {
        return err
    }
    total, err := transaction.Count("accounts") // sees alice and bob
    if err != nil {
        return err
    }
    if total > 100 {
        return core.E("accounts", "too many accounts", nil) // rollback
    }
    return nil // commit
})
```

### 7.1 ScopedStoreTransaction

`ScopedStore.Transaction(fn)` delegates to `Store.Transaction` and passes a `ScopedStoreTransaction`. The scoped helper preserves the same commit, rollback, read-your-writes, and post-commit event semantics, while keeping every operation inside the scoped namespace.

```go
func (scopedStore *ScopedStore) Transaction(operation func(*ScopedStoreTransaction) error) error { }

type ScopedStoreTransaction struct { }

func (transaction *ScopedStoreTransaction) Exists(key string) (bool, error) { }
func (transaction *ScopedStoreTransaction) ExistsIn(group, key string) (bool, error) { }
func (transaction *ScopedStoreTransaction) GroupExists(group string) (bool, error) { }
func (transaction *ScopedStoreTransaction) Get(key string) (string, error) { }
func (transaction *ScopedStoreTransaction) GetFrom(group, key string) (string, error) { }
func (transaction *ScopedStoreTransaction) Set(key, value string) error { }
func (transaction *ScopedStoreTransaction) SetIn(group, key, value string) error { }
func (transaction *ScopedStoreTransaction) SetWithTTL(group, key, value string, ttl time.Duration) error { }
func (transaction *ScopedStoreTransaction) Delete(group, key string) error { }
func (transaction *ScopedStoreTransaction) DeleteGroup(group string) error { }
func (transaction *ScopedStoreTransaction) DeletePrefix(groupPrefix string) error { }
func (transaction *ScopedStoreTransaction) GetAll(group string) (map[string]string, error) { }
func (transaction *ScopedStoreTransaction) GetPage(group string, offset, limit int) ([]KeyValue, error) { }
func (transaction *ScopedStoreTransaction) All(group string) iter.Seq2[KeyValue, error] { }
func (transaction *ScopedStoreTransaction) AllSeq(group string) iter.Seq2[KeyValue, error] { }
func (transaction *ScopedStoreTransaction) Count(group string) (int, error) { }
func (transaction *ScopedStoreTransaction) CountAll(groupPrefix ...string) (int, error) { }
func (transaction *ScopedStoreTransaction) Groups(groupPrefix ...string) ([]string, error) { }
func (transaction *ScopedStoreTransaction) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] { }
func (transaction *ScopedStoreTransaction) Render(templateSource, group string) (string, error) { }
func (transaction *ScopedStoreTransaction) GetSplit(group, key, separator string) (iter.Seq[string], error) { }
func (transaction *ScopedStoreTransaction) GetFields(group, key string) (iter.Seq[string], error) { }
func (transaction *ScopedStoreTransaction) PurgeExpired() (int64, error) { }
```

Scope isolation rules:

- `Set(key, value)`, `Get(key)`, and `Exists(key)` operate in the scoped default group, stored as `"namespace:default"`.
- Methods that accept `group` prefix the group before touching storage, so `SetIn("config", "theme", "dark")` writes `"namespace:config"`.
- `Groups` and `GroupsSeq` query only groups under `"namespace:"` and return namespace-local names such as `"config"`, not `"namespace:config"`.
- `CountAll`, `DeletePrefix`, and `PurgeExpired` are namespace-local. `DeletePrefix("")` deletes only groups in the scoped namespace, not the whole store.
- Quotas are evaluated through the same SQLite transaction, so pending writes count toward `MaxKeys` and `MaxGroups`. A returned `QuotaExceededError` rolls back the transaction when the callback returns it.
- Staged events use the full prefixed group internally. Scoped watchers and scoped `OnChange` callbacks localise committed events back to namespace-local group names.

Example:

```go
scopedStore, _ := store.NewScopedConfigured(storeInstance, store.ScopedStoreConfig{
    Namespace: "tenant-a",
    Quota:     store.QuotaConfig{MaxKeys: 100, MaxGroups: 10},
})

err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
    if err := transaction.Set("theme", "dark"); err != nil {
        return err
    }
    if err := transaction.SetIn("preferences", "locale", "en-GB"); err != nil {
        return err
    }
    groups, err := transaction.Groups()
    if err != nil {
        return err
    }
    // groups == []string{"default", "preferences"}
    return nil
})
```

---

## 8. Event System

- `Watch(group string) <-chan Event` — returns buffered channel (cap 16), non-blocking sends drop events
- `Unwatch(group string, ch <-chan Event)` — remove a watcher
- `OnChange(callback)` — synchronous callback in writer goroutine
- `notify()` snapshots callbacks after watcher delivery, so callbacks may register or unregister subscriptions re-entrantly without deadlocking

---

## 9. Workspace Buffer

Stateful work accumulation over time. A workspace is a named SQLite buffer for mutable work-in-progress stored in a `.duckdb` file for path compatibility. When a unit of work completes, the full state commits atomically to the journal table. A summary updates the identity store.

### 9.1 The Problem

Writing every micro-event directly to a time-series makes deltas meaningless — 4000 writes of "+1" produces noise. A mutable buffer accumulates the work, then commits once as a complete unit. The time-series only sees finished work, so deltas between entries represent real change.

### 9.2 Three Layers

```
Store (SQLite): "this thing exists"     — identity, current summary
Buffer (SQLite workspace file): "this thing is working" — mutable temp state, atomic
Journal (SQLite journal table): "this thing completed" — immutable, delta-ready
```

| Layer | Store | Mutability | Lifetime |
|-------|-------|-----------|----------|
| Identity | SQLite (go-store) | Mutable | Permanent |
| Hot | SQLite `.duckdb` file | Mutable | Session/cycle |
| Journal | SQLite journal table | Append-only | Retention policy |
| Cold | Compressed JSONL | Immutable | Archive |

### 9.3 Workspace API

```go
// Workspace is a named SQLite buffer for mutable work-in-progress.
// It holds a reference to the parent Store for identity updates and journal writes.
//
//   workspace, _ := storeInstance.NewWorkspace("scroll-session-2026-03-30")
//   workspace.Put("like", map[string]any{"user": "@handle", "post": "video_123"})
//   workspace.Commit()  // atomic → journal + identity summary
type Workspace struct {
    name  string
    store *Store   // parent store for identity updates + journal config
    db    *sql.DB  // SQLite via database/sql driver (temp file, deleted on commit/discard)
}

// NewWorkspace creates a workspace buffer. The SQLite file is created at .core/state/{name}.duckdb.
//
//   workspace, _ := storeInstance.NewWorkspace("scroll-session-2026-03-30")
func (s *Store) NewWorkspace(name string) (*Workspace, error) { }
```

```go
// Put accumulates an entry in the workspace buffer. Returns error on write failure.
//
//   err := workspace.Put("like", map[string]any{"user": "@handle"})
func (workspace *Workspace) Put(kind string, data map[string]any) error { }

// Aggregate returns a summary of the current workspace state
//
//   summary := workspace.Aggregate()  // {"like": 4000, "profile_match": 12}
func (workspace *Workspace) Aggregate() map[string]any { }

// Commit writes the aggregated state to the journal and updates the identity store
//
//   result := workspace.Commit()
func (workspace *Workspace) Commit() core.Result { }

// Discard drops the workspace without committing
//
//   workspace.Discard()
func (workspace *Workspace) Discard() { }

// Query runs SQL against the buffer for ad-hoc analysis.
// Returns core.Result where Value is []map[string]any (rows as maps).
//
//   result := workspace.Query("SELECT kind, COUNT(*) as n FROM entries GROUP BY kind")
//   rows := result.Value.([]map[string]any)  // [{"kind": "like", "n": 4000}]
func (workspace *Workspace) Query(sql string) core.Result { }
```

### 9.4 Journal

Commit writes a single point per completed workspace. One point = one unit of work.

```go
// CommitToJournal writes aggregated state as a single journal entry.
// Called by Workspace.Commit() internally, but exported for testing.
//
//   storeInstance.CommitToJournal("scroll-session", fields, tags)
func (s *Store) CommitToJournal(measurement string, fields map[string]any, tags map[string]string) core.Result { }

// QueryJournal runs a Flux-shaped filter or raw SQL query against the journal table.
// Returns core.Result where Value is []map[string]any (rows as maps).
//
//   result := s.QueryJournal(`from(bucket: "core") |> range(start: -7d)`)
//   rows := result.Value.([]map[string]any)
func (s *Store) QueryJournal(flux string) core.Result { }
```

Because each point is a complete unit, queries naturally produce meaningful results without complex aggregation.

### 9.5 Cold Archive

When journal entries age past retention, they compact to cold storage:

```go
// CompactOptions controls cold archive generation.
type CompactOptions struct {
    Before time.Time // archive entries before this time
    Output string    // output directory (default: .core/archive/)
    Format string    // gzip or zstd (default: gzip)
}

// Compact archives journal entries to compressed JSONL
//
//   storeInstance.Compact(store.CompactOptions{Before: time.Now().Add(-90*24*time.Hour), Output: "/archive/"})
func (s *Store) Compact(opts CompactOptions) core.Result { }
```

Output: gzip JSONL files. Each line is a complete unit of work — ready for training data ingestion, CDN publishing, or long-term analytics.

### 9.6 File Lifecycle

Workspace files are ephemeral:

```
Created:   workspace opens → .core/state/{name}.duckdb
Active:    Put() accumulates entries
Committed: Commit() → journal write → identity update → file deleted
Discarded: Discard() → file deleted
Crashed:   Orphaned .duckdb files detected on next New() call
```

Orphan recovery on `New()`:

```go
// New() scans .core/state/ for leftover .duckdb files.
// Each orphan is opened and cached for RecoverOrphans().
// The caller decides whether to commit or discard orphan data.
//
//   orphanWorkspaces := storeInstance.RecoverOrphans(".core/state/")
//   for _, workspace := range orphanWorkspaces {
//       // inspect workspace.Aggregate(), decide whether to commit or discard
//       workspace.Discard()
//   }
func (s *Store) RecoverOrphans(stateDir string) []*Workspace { }
```

---

## 10. Reference Material

| Resource | Location |
|----------|----------|
| Architecture docs | `docs/architecture.md` |
| Development guide | `docs/development.md` |
