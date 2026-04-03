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
    "fmt"
    "time"

    "dappco.re/go/core/store"
)

func main() {
    // Open /tmp/app.db for persistence, or use ":memory:" for ephemeral data.
    storeInstance, err := store.New("/tmp/app.db")
    if err != nil {
        return
    }
    defer storeInstance.Close()

    // Store "blue" under config/colour and read it back.
    if err := storeInstance.Set("config", "colour", "blue"); err != nil {
        return
    }
    colourValue, err := storeInstance.Get("config", "colour")
    if err != nil {
        return
    }
    fmt.Println(colourValue) // "blue"

    // Store a session token that expires after 24 hours.
    if err := storeInstance.SetWithTTL("session", "token", "abc123", 24*time.Hour); err != nil {
        return
    }

    // Read config/colour back into a map.
    configEntries, err := storeInstance.GetAll("config")
    if err != nil {
        return
    }
    fmt.Println(configEntries) // map[colour:blue]

    // Render the mail host and port into smtp.example.com:587.
    if err := storeInstance.Set("mail", "host", "smtp.example.com"); err != nil {
        return
    }
    if err := storeInstance.Set("mail", "port", "587"); err != nil {
        return
    }
    renderedTemplate, err := storeInstance.Render(`{{ .host }}:{{ .port }}`, "mail")
    if err != nil {
        return
    }
    fmt.Println(renderedTemplate) // "smtp.example.com:587"

    // Store tenant-42 preferences under the tenant-42: namespace prefix.
    scopedStore, err := store.NewScoped(storeInstance, "tenant-42")
    if err != nil {
        return
    }
    if err := scopedStore.Set("preferences", "locale", "en-GB"); err != nil {
        return
    }
    // Stored internally as group "tenant-42:preferences", key "locale"

    // Cap tenant-99 at 100 keys and 5 groups.
    quotaScopedStore, err := store.NewScopedWithQuota(storeInstance, "tenant-99", store.QuotaConfig{MaxKeys: 100, MaxGroups: 5})
    if err != nil {
        return
    }
    // A write past the limit returns store.QuotaExceededError.
    if err := quotaScopedStore.Set("g", "k", "v"); err != nil {
        return
    }

    // Watch "config" changes and print each event as it arrives.
    events := storeInstance.Watch("config")
    defer storeInstance.Unwatch("config", events)
    go func() {
        for event := range events {
            fmt.Println("event", event.Type, event.Group, event.Key, event.Value)
        }
    }()

    // Or register a synchronous callback for the same mutations.
    unregister := storeInstance.OnChange(func(event store.Event) {
        fmt.Println("changed", event.Group, event.Key, event.Value)
    })
    defer unregister()
}
```

## Package Layout

The entire package lives in a single Go package (`package store`) with three implementation files plus `doc.go` for the package comment:

| File | Purpose |
|------|---------|
| `doc.go` | Package comment with concrete usage examples |
| `store.go` | Core `Store` type, CRUD operations (`Get`, `Set`, `SetWithTTL`, `Delete`, `DeleteGroup`), bulk queries (`GetAll`, `All`, `Count`, `CountAll`, `Groups`, `GroupsSeq`), string splitting helpers (`GetSplit`, `GetFields`), template rendering (`Render`), TTL expiry, background purge goroutine |
| `events.go` | `EventType` constants, `Event` struct, `Watcher` type, `Watch`/`Unwatch` subscription management, `OnChange` callback registration, internal `notify` dispatch |
| `scope.go` | `ScopedStore` wrapper for namespace isolation, `QuotaConfig` struct, `NewScoped`/`NewScopedWithQuota` constructors, namespace-local helper delegation, quota enforcement logic |

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
- [AX RFC](RFC-CORE-008-AGENT-EXPERIENCE.md) -- naming, comment, and path conventions for agent consumers
- [Architecture](architecture.md) -- storage layer internals, TTL model, event system, concurrency design
- [Development Guide](development.md) -- building, testing, benchmarks, contribution workflow
