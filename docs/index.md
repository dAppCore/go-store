---
title: go-store
description: Group-namespaced SQLite key-value store with TTL expiry, namespace isolation, quota enforcement, and reactive event hooks.
---

# go-store

`go-store` is a group-namespaced key-value store backed by SQLite. It provides persistent or in-memory storage with optional TTL expiry, namespace isolation for multi-tenant use, quota enforcement, and a reactive event system for observing mutations.

The package has a single runtime dependency -- a pure-Go SQLite driver (`modernc.org/sqlite`). No CGO is required. It compiles and runs on all platforms that Go supports.

**Module path:** `dappco.re/go/core/store`
**Go version:** 1.26+
**Licence:** EUPL-1.2

## Quick Start

```go
package main

import (
    "time"

    "dappco.re/go/core"
    "dappco.re/go/core/store"
)

func main() {
    // Open a store. Use ":memory:" for ephemeral data or a file path for persistence.
    storeInstance, err := store.New("/tmp/app.db")
    if err != nil {
        panic(err)
    }
    defer storeInstance.Close()

    // Basic CRUD
    storeInstance.Set("config", "theme", "dark")
    themeValue, _ := storeInstance.Get("config", "theme")
    core.Println(themeValue) // "dark"

    // TTL expiry -- key disappears after the duration elapses
    storeInstance.SetWithTTL("session", "token", "abc123", 24*time.Hour)

    // Fetch all keys in a group
    configEntries, _ := storeInstance.GetAll("config")
    core.Println(configEntries) // map[theme:dark]

    // Template rendering from stored values
    storeInstance.Set("mail", "host", "smtp.example.com")
    storeInstance.Set("mail", "port", "587")
    renderedTemplate, _ := storeInstance.Render(`{{ .host }}:{{ .port }}`, "mail")
    core.Println(renderedTemplate) // "smtp.example.com:587"

    // Namespace isolation for multi-tenant use
    scopedStore, _ := store.NewScoped(storeInstance, "tenant-42")
    scopedStore.Set("prefs", "locale", "en-GB")
    // Stored internally as group "tenant-42:prefs", key "locale"

    // Quota enforcement
    quota := store.QuotaConfig{MaxKeys: 100, MaxGroups: 5}
    quotaScopedStore, _ := store.NewScopedWithQuota(storeInstance, "tenant-99", quota)
    err = quotaScopedStore.Set("g", "k", "v") // returns store.QuotaExceededError if limits are hit

    // Watch for mutations via a buffered channel
    watcher := storeInstance.Watch("config", "*")
    defer storeInstance.Unwatch(watcher)
    go func() {
        for event := range watcher.Events {
            core.Println("event", event.Type, event.Group, event.Key, event.Value)
        }
    }()

    // Or register a synchronous callback
    unregister := storeInstance.OnChange(func(event store.Event) {
        core.Println("changed", event.Group, event.Key, event.Value)
    })
    defer unregister()
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

There are no other direct dependencies. The package uses the Go standard library plus `dappco.re/go/core` helper primitives for error wrapping, string handling, and filesystem-safe path composition.

## Key Types

- **`Store`** -- the central type. Holds a `*sql.DB`, manages the background purge goroutine, and maintains the watcher/callback registry.
- **`ScopedStore`** -- wraps a `*Store` with an auto-prefixed namespace. Provides the same API surface with group names transparently prefixed.
- **`QuotaConfig`** -- configures per-namespace limits on total keys and distinct groups.
- **`Event`** -- describes a single store mutation (type, group, key, value, timestamp).
- **`Watcher`** -- a channel-based subscription to store events, created by `Watch`. `Events` is the read-only channel to select on.
- **`KeyValue`** -- a simple key-value pair struct, used by the `All` iterator.

## Sentinel Errors

- **`NotFoundError`** -- returned by `Get` when the requested key does not exist or has expired.
- **`QuotaExceededError`** -- returned by `ScopedStore.Set`/`SetWithTTL` when a namespace quota limit is reached.

## Further Reading

- [Agent Conventions](../CODEX.md) -- Codex-facing repo rules and AX notes
- [Architecture](architecture.md) -- storage layer internals, TTL model, event system, concurrency design
- [Development Guide](development.md) -- building, testing, benchmarks, contribution workflow
