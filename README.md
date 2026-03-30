[![Go Reference](https://pkg.go.dev/badge/dappco.re/go/core/store.svg)](https://pkg.go.dev/dappco.re/go/core/store)
[![License: EUPL-1.2](https://img.shields.io/badge/License-EUPL--1.2-blue.svg)](LICENSE.md)
[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?style=flat&logo=go)](go.mod)

# go-store

Group-namespaced SQLite key-value store with TTL expiry, namespace isolation, quota enforcement, and a reactive event system. Backed by a pure-Go SQLite driver (no CGO), uses WAL mode for concurrent reads, and enforces a single connection to keep pragma settings consistent. Supports scoped stores for multi-tenant use, Watch/Unwatch subscriptions, and OnChange callbacks for go-ws event streaming.

**Module**: `dappco.re/go/core/store`
**Licence**: EUPL-1.2
**Language**: Go 1.25

## Quick Start

```go
package main

import (
	"fmt"
	"time"

	"dappco.re/go/core/store"
)

func main() {
	storeInstance, err := store.New("/path/to/store.db") // or store.New(":memory:")
	if err != nil {
		panic(err)
	}
	defer storeInstance.Close()

	storeInstance.Set("config", "theme", "dark")
	storeInstance.SetWithTTL("session", "token", "abc123", 24*time.Hour)
	value, err := storeInstance.Get("config", "theme")
	fmt.Println(value, err)

	// Watch for mutations
	watcher := storeInstance.Watch("config", "*")
	defer storeInstance.Unwatch(watcher)
	go func() {
		for event := range watcher.Events {
			fmt.Println(event.Type, event.Key)
		}
	}()

	// Scoped store for tenant isolation
	scopedStore, _ := store.NewScoped(storeInstance, "tenant-42")
	scopedStore.Set("prefs", "locale", "en-GB")
}
```

## Documentation

- [Agent Conventions](CODEX.md) - Codex-facing repo rules and AX notes
- [Architecture](docs/architecture.md) — storage layer, group/key model, TTL expiry, event system, namespace isolation
- [Development Guide](docs/development.md) — prerequisites, test patterns, benchmarks, adding methods
- [Project History](docs/history.md) — completed phases, known limitations, future considerations

## Build & Test

```bash
go test ./...
go test -race ./...
go test -bench=. ./...
go build ./...
```

## Licence

European Union Public Licence 1.2 — see [LICENCE](LICENCE) for details.
