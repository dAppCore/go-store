# go-store RFC — SQLite Key-Value Store

> An agent should be able to use this store from this document alone.

**Module:** `dappco.re/go/store`
**Repository:** `core/go-store`
**Files:** 8

---

## 1. Overview

SQLite-backed key-value store with TTL, namespace isolation, reactive events, and quota enforcement. Pure Go (no CGO). Used by core/ide for memory caching and by agents for workspace state.

---

## 2. Architecture

| File | Purpose |
|------|---------|
| `store.go` | Core `Store`: CRUD on `(grp, key)` compound PK, TTL via `expires_at` (Unix ms), background purge (60s), `text/template` rendering, `iter.Seq2` iterators |
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
//   st, _ := store.New(":memory:")                           // SQLite only
//   st, _ := store.New("/path/to/db", store.WithJournal(
//       "http://localhost:8086", "core-org", "core-bucket",
//   ))
func New(path string, opts ...StoreOption) (*Store, error) { }

type StoreOption func(*Store)

func WithJournal(url, org, bucket string) StoreOption { }
```

---

## 5. API

```go
st, _ := store.New(":memory:")      // or store.New("/path/to/db")
defer st.Close()

st.Set("group", "key", "value")
st.SetWithTTL("group", "key", "value", 5*time.Minute)
val, _ := st.Get("group", "key")    // lazy-deletes expired

// Iteration
for key, val := range st.AllSeq("group") { ... }
for group := range st.GroupsSeq() { ... }

// Events
ch := st.Watch("group")
st.OnChange(func(event store.Event) { ... })
```

---

## 6. ScopedStore

```go
// ScopedStore wraps a Store with a namespace prefix and optional quotas.
//
//   scoped := store.NewScoped(st, "mynamespace")
//   scoped.Set("key", "value")           // stored as group "mynamespace:default", key "key"
//   scoped.SetIn("mygroup", "key", "v")  // stored as group "mynamespace:mygroup", key "key"
type ScopedStore struct {
    store     *Store
    namespace string  // validated: ^[a-zA-Z0-9-]+$
    MaxKeys   int     // 0 = unlimited
    MaxGroups int     // 0 = unlimited
}

func NewScoped(st *Store, namespace string) *ScopedStore { }

// Set stores a value in the default group ("namespace:default")
func (ss *ScopedStore) Set(key, value string) error { }

// SetIn stores a value in an explicit group ("namespace:group")
func (ss *ScopedStore) SetIn(group, key, value string) error { }

// Get retrieves a value from the default group
func (ss *ScopedStore) Get(key string) (string, error) { }

// GetFrom retrieves a value from an explicit group
func (ss *ScopedStore) GetFrom(group, key string) (string, error) { }
```

- Namespace regex: `^[a-zA-Z0-9-]+$`
- Default group: `Set(key, value)` uses literal `"default"` as group, prefixed: `"mynamespace:default"`
- `SetIn(group, key, value)` allows explicit group within the namespace
- Quota: `MaxKeys`, `MaxGroups` — checked before writes, upserts bypass

---

## 7. Event System

- `Watch(group string) <-chan Event` — returns buffered channel (cap 16), non-blocking sends drop events
- `Unwatch(group string, ch <-chan Event)` — remove a watcher
- `OnChange(callback)` — synchronous callback in writer goroutine
- `notify()` snapshots callbacks after watcher delivery, so callbacks may register or unregister subscriptions re-entrantly without deadlocking

---

## 8. Workspace Buffer

Stateful work accumulation over time. A workspace is a named SQLite buffer for mutable work-in-progress stored in a `.duckdb` file for path compatibility. When a unit of work completes, the full state commits atomically to the journal table. A summary updates the identity store.

### 7.1 The Problem

Writing every micro-event directly to a time-series makes deltas meaningless — 4000 writes of "+1" produces noise. A mutable buffer accumulates the work, then commits once as a complete unit. The time-series only sees finished work, so deltas between entries represent real change.

### 7.2 Three Layers

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

### 7.3 Workspace API

```go
// Workspace is a named SQLite buffer for mutable work-in-progress.
// It holds a reference to the parent Store for identity updates and journal writes.
//
//   ws, _ := st.NewWorkspace("scroll-session-2026-03-30")
//   ws.Put("like", map[string]any{"user": "@handle", "post": "video_123"})
//   ws.Commit()  // atomic → journal + identity summary
type Workspace struct {
    name  string
    store *Store   // parent store for identity updates + journal config
    db    *sql.DB  // SQLite via database/sql driver (temp file, deleted on commit/discard)
}

// NewWorkspace creates a workspace buffer. The SQLite file is created at .core/state/{name}.duckdb.
//
//   ws, _ := st.NewWorkspace("scroll-session-2026-03-30")
func (s *Store) NewWorkspace(name string) (*Workspace, error) { }
```

```go
// Put accumulates an entry in the workspace buffer. Returns error on write failure.
//
//   err := ws.Put("like", map[string]any{"user": "@handle"})
func (ws *Workspace) Put(kind string, data map[string]any) error { }

// Aggregate returns a summary of the current workspace state
//
//   summary := ws.Aggregate()  // {"like": 4000, "profile_match": 12}
func (ws *Workspace) Aggregate() map[string]any { }

// Commit writes the aggregated state to the journal and updates the identity store
//
//   result := ws.Commit()
func (ws *Workspace) Commit() core.Result { }

// Discard drops the workspace without committing
//
//   ws.Discard()
func (ws *Workspace) Discard() { }

// Query runs SQL against the buffer for ad-hoc analysis.
// Returns core.Result where Value is []map[string]any (rows as maps).
//
//   result := ws.Query("SELECT kind, COUNT(*) as n FROM entries GROUP BY kind")
//   rows := result.Value.([]map[string]any)  // [{"kind": "like", "n": 4000}]
func (ws *Workspace) Query(sql string) core.Result { }
```

### 7.4 Journal

Commit writes a single point per completed workspace. One point = one unit of work.

```go
// CommitToJournal writes aggregated state as a single journal entry.
// Called by Workspace.Commit() internally, but exported for testing.
//
//   s.CommitToJournal("scroll-session", fields, tags)
func (s *Store) CommitToJournal(measurement string, fields map[string]any, tags map[string]string) core.Result { }

// QueryJournal runs a Flux-shaped filter or raw SQL query against the journal table.
// Returns core.Result where Value is []map[string]any (rows as maps).
//
//   result := s.QueryJournal(`from(bucket: "core") |> range(start: -7d)`)
//   rows := result.Value.([]map[string]any)
func (s *Store) QueryJournal(flux string) core.Result { }
```

Because each point is a complete unit, queries naturally produce meaningful results without complex aggregation.

### 7.5 Cold Archive

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
//   st.Compact(store.CompactOptions{Before: time.Now().Add(-90*24*time.Hour), Output: "/archive/"})
func (s *Store) Compact(opts CompactOptions) core.Result { }
```

Output: gzip JSONL files. Each line is a complete unit of work — ready for training data ingestion, CDN publishing, or long-term analytics.

### 8.1 File Lifecycle

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
//   orphans := st.RecoverOrphans(".core/state/")
//   for _, ws := range orphans {
//       // inspect ws.Aggregate(), decide whether to commit or discard
//       ws.Discard()
//   }
func (s *Store) RecoverOrphans(stateDir string) []*Workspace { }
```

---

## 9. Reference Material

| Resource | Location |
|----------|----------|
| Architecture docs | `docs/architecture.md` |
| Development guide | `docs/development.md` |
