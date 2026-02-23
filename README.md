[![Go Reference](https://pkg.go.dev/badge/forge.lthn.ai/core/go-store.svg)](https://pkg.go.dev/forge.lthn.ai/core/go-store)
[![License: EUPL-1.2](https://img.shields.io/badge/License-EUPL--1.2-blue.svg)](LICENSE.md)
[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?style=flat&logo=go)](go.mod)

# go-store

Group-namespaced SQLite key-value store with TTL expiry, namespace isolation, quota enforcement, and a reactive event system. Backed by a pure-Go SQLite driver (no CGO), uses WAL mode for concurrent reads, and enforces a single connection to ensure pragma consistency. Supports scoped stores for multi-tenant use, Watch/Unwatch subscriptions, and OnChange callbacks — the designed integration point for go-ws real-time streaming.

**Module**: `forge.lthn.ai/core/go-store`
**Licence**: EUPL-1.2
**Language**: Go 1.25

## Quick Start

```go
import "forge.lthn.ai/core/go-store"

st, err := store.New("/path/to/store.db")  // or store.New(":memory:")
defer st.Close()

st.Set("config", "theme", "dark")
st.SetWithTTL("session", "token", "abc123", 24*time.Hour)
val, err := st.Get("config", "theme")

// Watch for mutations
w := st.Watch("config", "*")
defer st.Unwatch(w)
for e := range w.Ch { fmt.Println(e.Type, e.Key) }

// Scoped store for tenant isolation
sc, _ := store.NewScoped(st, "tenant-42")
sc.Set("prefs", "locale", "en-GB")
```

## Documentation

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
