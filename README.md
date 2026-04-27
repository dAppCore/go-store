[![Go Reference](https://pkg.go.dev/badge/dappco.re/go/store.svg)](https://pkg.go.dev/dappco.re/go/store)
[![Licence: EUPL-1.2](https://img.shields.io/badge/Licence-EUPL--1.2-blue.svg)](LICENCE.md)
[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?style=flat&logo=go)](go.mod)

# go-store

Group-namespaced SQLite key-value store with TTL expiry, namespace isolation, quota enforcement, and a reactive event system. Backed by a pure-Go SQLite driver (no CGO), uses WAL mode for concurrent reads, and enforces a single connection to keep pragma settings consistent. Supports scoped stores for multi-tenant use, Watch/Unwatch subscriptions, and OnChange callbacks for downstream event consumers.

**Module**: `dappco.re/go/store`
**Licence**: EUPL-1.2
**Language**: Go 1.26

## Quick Start

```go
package main

import (
	"fmt"
	"time"

	"dappco.re/go/store"
)

func main() {
	// Configure a persistent store with "/tmp/go-store.db", or use ":memory:" for ephemeral data.
	storeInstance, err := store.NewConfigured(store.StoreConfig{
		DatabasePath: "/tmp/go-store.db",
		Journal: store.JournalConfiguration{
			EndpointURL:  "http://127.0.0.1:8086",
			Organisation: "core",
			BucketName:   "events",
		},
		PurgeInterval: 30 * time.Second,
		WorkspaceStateDirectory: "/tmp/core-state",
	})
	if err != nil {
		return
	}
	defer storeInstance.Close()

	if err := storeInstance.Set("config", "colour", "blue"); err != nil {
		return
	}
	if err := storeInstance.SetWithTTL("session", "token", "abc123", 24*time.Hour); err != nil {
		return
	}
	colourValue, err := storeInstance.Get("config", "colour")
	if err != nil {
		return
	}
	fmt.Println(colourValue)

	// Watch "config" mutations and print each event as it arrives.
	events := storeInstance.Watch("config")
	defer storeInstance.Unwatch("config", events)
	go func() {
		for event := range events {
			fmt.Println(event.Type, event.Group, event.Key, event.Value)
		}
	}()

	// Store tenant-42 preferences under the "tenant-42:" prefix.
	scopedStore, err := store.NewScopedConfigured(storeInstance, store.ScopedStoreConfig{
		Namespace: "tenant-42",
		Quota:     store.QuotaConfig{MaxKeys: 100, MaxGroups: 10},
	})
	if err != nil {
		return
	}
	if err := scopedStore.SetIn("preferences", "locale", "en-GB"); err != nil {
		return
	}
}
```

## Documentation

- [Agent Conventions](CODEX.md) - Codex-facing repo rules and AX notes
- [AX RFC](docs/RFC-CORE-008-AGENT-EXPERIENCE.md) - naming, comment, and path conventions for agent consumers
- [Architecture](docs/architecture.md) — storage layer, group/key model, TTL expiry, event system, namespace isolation
- [Development Guide](docs/development.md) — prerequisites, test patterns, benchmarks, adding methods
- [Project History](docs/history.md) — completed phases, known limitations, future considerations
- [Dependency Exceptions](DEPENDENCIES.md) — documented runtime dependency exceptions

## Build & Test

```bash
go test ./...
go test -race ./...
go test -bench=. ./...
go build ./...
```

## Licence

European Union Public Licence 1.2 — see [LICENCE.md](LICENCE.md) for details.
