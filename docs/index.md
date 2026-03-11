---
title: go-store
description: Group-namespaced SQLite key-value store with TTL expiry, namespace isolation, quota enforcement, and reactive event hooks.
---

# go-store

`go-store` is a group-namespaced key-value store backed by SQLite. It provides persistent or in-memory storage with optional TTL expiry, namespace isolation for multi-tenant use, quota enforcement, and a reactive event system for observing mutations.

The package has a single runtime dependency -- a pure-Go SQLite driver (`modernc.org/sqlite`). No CGO is required. It compiles and runs on all platforms that Go supports.

**Module path:** `forge.lthn.ai/core/go-store`
**Go version:** 1.26+
**Licence:** EUPL-1.2

## Quick Start

```go
package main

import (
    "fmt"
    "time"

    "forge.lthn.ai/core/go-store"
)

func main() {
    // Open a store. Use ":memory:" for ephemeral data or a file path for persistence.
    st, err := store.New("/tmp/app.db")
    if err != nil {
        panic(err)
    }
    defer st.Close()

    // Basic CRUD
    st.Set("config", "theme", "dark")
    val, _ := st.Get("config", "theme")
    fmt.Println(val) // "dark"

    // TTL expiry -- key disappears after the duration elapses
    st.SetWithTTL("session", "token", "abc123", 24*time.Hour)

    // Fetch all keys in a group
    all, _ := st.GetAll("config")
    fmt.Println(all) // map[theme:dark]

    // Template rendering from stored values
    st.Set("mail", "host", "smtp.example.com")
    st.Set("mail", "port", "587")
    out, _ := st.Render(`{{ .host }}:{{ .port }}`, "mail")
    fmt.Println(out) // "smtp.example.com:587"

    // Namespace isolation for multi-tenant use
    sc, _ := store.NewScoped(st, "tenant-42")
    sc.Set("prefs", "locale", "en-GB")
    // Stored internally as group "tenant-42:prefs", key "locale"

    // Quota enforcement
    quota := store.QuotaConfig{MaxKeys: 100, MaxGroups: 5}
    sq, _ := store.NewScopedWithQuota(st, "tenant-99", quota)
    err = sq.Set("g", "k", "v") // returns store.ErrQuotaExceeded if limits are hit

    // Watch for mutations via a buffered channel
    w := st.Watch("config", "*")
    defer st.Unwatch(w)
    go func() {
        for e := range w.Ch {
            fmt.Printf("event: %s %s/%s\n", e.Type, e.Group, e.Key)
        }
    }()

    // Or register a synchronous callback
    unreg := st.OnChange(func(e store.Event) {
        fmt.Printf("changed: %s\n", e.Key)
    })
    defer unreg()
}
```

## Package Layout

The entire package lives in a single Go package (`package store`) with three source files:

| File | Purpose |
|------|---------|
| `store.go` | Core `Store` type, CRUD operations (`Get`, `Set`, `SetWithTTL`, `Delete`, `DeleteGroup`), bulk queries (`GetAll`, `All`, `Count`, `CountAll`, `Groups`, `GroupsSeq`), string splitting helpers (`GetSplit`, `GetFields`), template rendering (`Render`), TTL expiry, background purge goroutine |
| `events.go` | `EventType` constants, `Event` struct, `Watcher` type, `Watch`/`Unwatch` subscription management, `OnChange` callback registration, internal `notify` dispatch |
| `scope.go` | `ScopedStore` wrapper for namespace isolation, `QuotaConfig` struct, `NewScoped`/`NewScopedWithQuota` constructors, quota enforcement logic |

Tests are organised in corresponding files:

| File | Covers |
|------|--------|
| `store_test.go` | CRUD, TTL, concurrency, edge cases, persistence, WAL verification |
| `events_test.go` | Watch/Unwatch, OnChange, event dispatch, wildcard matching, buffer overflow |
| `scope_test.go` | Namespace isolation, quota enforcement, cross-namespace behaviour |
| `coverage_test.go` | Defensive error paths (scan errors, schema conflicts, database corruption) |
| `bench_test.go` | Performance benchmarks for all major operations |

## Dependencies

**Runtime:**

| Module | Purpose |
|--------|---------|
| `modernc.org/sqlite` | Pure-Go SQLite driver (no CGO). Registered as a `database/sql` driver. |

**Test only:**

| Module | Purpose |
|--------|---------|
| `github.com/stretchr/testify` | Assertion helpers (`assert`, `require`) for tests. |

There are no other direct dependencies. The package uses only the Go standard library (`database/sql`, `context`, `sync`, `time`, `text/template`, `iter`, `errors`, `fmt`, `strings`, `regexp`, `slices`, `sync/atomic`) beyond the SQLite driver.

## Key Types

- **`Store`** -- the central type. Holds a `*sql.DB`, manages the background purge goroutine, and maintains the watcher/callback registry.
- **`ScopedStore`** -- wraps a `*Store` with an auto-prefixed namespace. Provides the same API surface with group names transparently prefixed.
- **`QuotaConfig`** -- configures per-namespace limits on total keys and distinct groups.
- **`Event`** -- describes a single store mutation (type, group, key, value, timestamp).
- **`Watcher`** -- a channel-based subscription to store events, created by `Watch`.
- **`KV`** -- a simple key-value pair struct, used by the `All` iterator.

## Sentinel Errors

- **`ErrNotFound`** -- returned by `Get` when the requested key does not exist or has expired.
- **`ErrQuotaExceeded`** -- returned by `ScopedStore.Set`/`SetWithTTL` when a namespace quota limit is reached.

## Further Reading

- [Architecture](architecture.md) -- storage layer internals, TTL model, event system, concurrency design
- [Development Guide](development.md) -- building, testing, benchmarks, contribution workflow
