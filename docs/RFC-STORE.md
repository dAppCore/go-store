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
| `workspace.go` | `Workspace` buffer: DuckDB-backed mutable accumulation, atomic commit to journal |
| `journal.go` | InfluxDB journal: write completed units, query time-series, retention |
| `compact.go` | Cold archive: compress journal entries to JSONL.gz |
| `store_test.go` | Store unit tests |
| `workspace_test.go` | Workspace buffer tests |

---

## 3. Key Design Decisions

- **Single-connection SQLite.** `MaxOpenConns(1)` because SQLite pragmas (WAL, busy_timeout) are per-connection — a pool would hand out unpragma'd connections causing `SQLITE_BUSY`
- **TTL is triple-layered:** lazy delete on `Get`, query-time `WHERE` filtering, background purge goroutine
- **LIKE queries use `escapeLike()`** with `^` as escape char to prevent SQL wildcard injection

---

## 4. API

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
st.OnChange("group", func(key, val string) { ... })
```

---

## 5. ScopedStore

```go
scoped := store.NewScoped(st, "mynamespace")
scoped.Set("key", "value")           // stored as group "mynamespace:default", key "key"
scoped.SetIn("mygroup", "key", "v")  // stored as group "mynamespace:mygroup", key "key"
```

- Namespace regex: `^[a-zA-Z0-9-]+$`
- Default group: when `Set(key, value)` is called without a group, the literal string `"default"` is used as the group name, prefixed with the namespace: `"mynamespace:default"`
- `SetIn(group, key, value)` allows explicit group within the namespace
- Quota: `MaxKeys`, `MaxGroups` — checked before writes, upserts bypass

---

## 6. Event System

- `Watch(group)` — returns buffered channel (cap 16), non-blocking sends drop events
- `Unwatch(group, ch)` — remove a watcher
- `OnChange(group, callback)` — synchronous callback in writer goroutine; callbacks can manage subscriptions re-entrantly

---

## 7. Workspace Buffer

Stateful work accumulation over time. A workspace is a named DuckDB buffer for mutable work-in-progress. When a unit of work completes, the full state commits atomically to a time-series journal (InfluxDB). A summary updates the identity store (the existing SQLite store or an external database).

### 7.1 The Problem

Writing every micro-event directly to a time-series makes deltas meaningless — 4000 writes of "+1" produces noise. A mutable buffer accumulates the work, then commits once as a complete unit. The time-series only sees finished work, so deltas between entries represent real change.

### 7.2 Three Layers

```
Store (SQLite): "this thing exists"     — identity, current summary
Buffer (DuckDB): "this thing is working" — mutable temp state, atomic
Journal (InfluxDB): "this thing completed" — immutable, delta-ready
```

| Layer | Store | Mutability | Lifetime |
|-------|-------|-----------|----------|
| Identity | SQLite (go-store) | Mutable | Permanent |
| Hot | DuckDB (temp file) | Mutable | Session/cycle |
| Journal | InfluxDB | Append-only | Retention policy |
| Cold | Compressed JSONL | Immutable | Archive |

### 7.3 Workspace API

```go
// Workspace is a named DuckDB buffer for mutable work-in-progress.
// It holds a reference to the parent Store for identity updates and journal writes.
//
//   ws, _ := st.NewWorkspace("scroll-session-2026-03-30")
//   ws.Put("like", map[string]any{"user": "@handle", "post": "video_123"})
//   ws.Commit()  // atomic → journal + identity summary
type Workspace struct {
    name  string
    store *Store      // parent store for identity updates + journal config
    db    *duckdb.DB  // mutable buffer (temp file, deleted on commit/discard)
}

// NewWorkspace creates a workspace buffer. The DuckDB file is created at .core/state/{name}.duckdb.
//
//   ws, _ := st.NewWorkspace("scroll-session-2026-03-30")
func (s *Store) NewWorkspace(name string) (*Workspace, error) { }
```

```go
// Put accumulates an entry in the workspace buffer
//
//   ws.Put("like", map[string]any{"user": "@handle"})
func (ws *Workspace) Put(kind string, data map[string]any) { }

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
// commitToJournal writes aggregated state as a single InfluxDB point
//
//   s.commitToJournal("scroll-session", fields, tags)
func (s *Store) commitToJournal(measurement string, fields map[string]any, tags map[string]string) core.Result { }

// QueryJournal runs a Flux query against the time-series.
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

### 7.6 File Lifecycle

DuckDB files are ephemeral:

```
Created:   workspace opens → .core/state/{name}.duckdb
Active:    Put() accumulates entries
Committed: Commit() → journal write → identity update → file deleted
Discarded: Discard() → file deleted
Crashed:   Orphaned .duckdb files recovered on next startup
```

---

## 8. Reference Material

| Resource | Location |
|----------|----------|
| Core Go RFC | `code/core/go/RFC.md` |
| IO RFC | `code/core/go/io/RFC.md` |
