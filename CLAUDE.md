# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

SQLite key-value store with TTL, namespace isolation, and reactive events. Pure Go (no CGO). Module: `dappco.re/go/store`

## AX Notes

- Prefer descriptive names over abbreviations.
- Public comments should show real usage with concrete values.
- Keep examples in UK English.
- Prefer `StoreConfig` and `ScopedStoreConfig` literals over option chains when the configuration is already known.
- Do not add compatibility aliases; the primary API names are the contract.
- Preserve the single-connection SQLite design.
- Verify with `go test ./...`, `go test -race ./...`, and `go vet ./...` before committing.
- Use conventional commits and include the `Co-Authored-By: Virgil <virgil@lethean.io>` trailer.

## Getting Started

Part of the Go workspace at `~/Code/go.work`—run `go work sync` after cloning. Single Go package with `store.go` (core store API), `events.go` (watchers/callbacks), `scope.go` (scoping/quota), `journal.go` (journal persistence/query), `workspace.go` (workspace buffering), and `compact.go` (archive generation).

```bash
go test ./... -count=1
```

## Commands

```bash
go test ./...                        # Run all tests
go test -v -run TestEvents_Watch_Good_SpecificKey ./... # Run single test
go test -race ./...                  # Race detector (must pass before commit)
go test -cover ./...                 # Coverage (target: 95%+)
go test -bench=. -benchmem ./...     # Benchmarks
golangci-lint run ./...              # Lint (config in .golangci.yml)
go vet ./...                         # Vet
```

## Architecture

**Single-connection SQLite.** `store.go` pins `MaxOpenConns(1)` because SQLite pragmas (WAL, busy_timeout) are per-connection — a pool would hand out unpragma'd connections causing SQLITE_BUSY. This is the most important architectural decision; don't change it.

**Three-layer design:**
- `store.go` — Core `Store` type: CRUD on an `entries` table keyed by `(group_name, entry_key)`, TTL via `expires_at` (Unix ms), background purge goroutine (60s interval), `text/template` rendering, `iter.Seq2` iterators
- `events.go` — Event system: `Watch`/`Unwatch` (buffered chan, cap 16, non-blocking sends drop events) and `OnChange` callbacks (synchronous in writer goroutine). Watcher and callback registries use separate locks, so callbacks can register or unregister subscriptions without deadlocking.
- `scope.go` — `ScopedStore` wraps `*Store`, prefixes groups with `namespace:`. Quota enforcement (`MaxKeys`/`MaxGroups`) checked before writes; upserts bypass quota. Namespace regex: `^[a-zA-Z0-9-]+$`
- `journal.go` — Journal persistence and query helpers layered on SQLite.
- `workspace.go` — Workspace buffers, commit flow, and orphan recovery.
- `compact.go` — Cold archive generation for completed journal entries.

**TTL enforcement is triple-layered:** lazy delete on `Get`, query-time `WHERE` filtering on bulk reads, and background purge goroutine.

**LIKE queries use `escapeLike()`** with `^` as escape char to prevent SQL wildcard injection in `CountAll` and `Groups`/`GroupsSeq`.

## Key API

```go
package main

import (
	"fmt"
	"time"

	"dappco.re/go/store"
)

func main() {
	storeInstance, err := store.New(":memory:")
	if err != nil {
		return
	}
	defer storeInstance.Close()

	configuredStore, err := store.NewConfigured(store.StoreConfig{
		DatabasePath: ":memory:",
		Journal: store.JournalConfiguration{
			EndpointURL:  "http://127.0.0.1:8086",
			Organisation: "core",
			BucketName:   "events",
		},
		PurgeInterval: 30 * time.Second,
	})
	if err != nil {
		return
	}
	defer configuredStore.Close()

	if err := configuredStore.Set("group", "key", "value"); err != nil {
		return
	}
	value, err := configuredStore.Get("group", "key")
	if err != nil {
		return
	}
	fmt.Println(value)

	if err := configuredStore.SetWithTTL("session", "token", "abc123", 5*time.Minute); err != nil {
		return
	}

	scopedStore, err := store.NewScopedConfigured(configuredStore, store.ScopedStoreConfig{
		Namespace: "tenant",
		Quota:     store.QuotaConfig{MaxKeys: 100, MaxGroups: 10},
	})
	if err != nil {
		return
	}
	if err := scopedStore.SetIn("config", "theme", "dark"); err != nil {
		return
	}

	events := configuredStore.Watch("group")
	defer configuredStore.Unwatch("group", events)
	go func() {
		for event := range events {
			fmt.Println(event.Type, event.Group, event.Key, event.Value)
		}
	}()

	unregister := configuredStore.OnChange(func(event store.Event) {
		fmt.Println("changed", event.Group, event.Key, event.Value)
	})
	defer unregister()
}
```

## Coding Standards

- **UK English** in all code, comments, docs (colour, behaviour, serialise, organisation)
- Error strings: `"store.Method: what failed"` — self-identifying without stack traces
- `go test -race ./...` must pass before commit
- Conventional commits: `type(scope): description`
- Co-Author: `Co-Authored-By: Virgil <virgil@lethean.io>`
- Only runtime dependency allowed: `modernc.org/sqlite`. No CGO. New deps must be pure Go with EUPL-1.2-compatible licence.

## Test Conventions

- Test names follow `Test<File>_<Function>_<Good|Bad|Ugly>`, for example `TestEvents_Watch_Good_SpecificKey`
- Use `New(":memory:")` unless testing persistence; use `t.TempDir()` for file-backed
- TTL tests: 1ms TTL + 5ms sleep; use `sync.WaitGroup` not sleeps for goroutine sync
- `require` for preconditions, `assert` for verifications (`testify`)

## Adding a New Method

1. Implement on `*Store` in `store.go`
2. If mutating, call `storeInstance.notify(Event{...})` after successful database write
3. Add delegation method on `ScopedStore` in `scope.go` (prefix the group)
4. Update `checkQuota` in `scope.go` if it affects key/group counts
5. Write `Test<File>_<Function>_<Good|Bad|Ugly>` tests
6. Run `go test -race ./...` and `go vet ./...`

## Docs

- `docs/architecture.md` — full internal design details
- `docs/development.md` — test patterns, benchmarks, coding standards
- `docs/history.md` — completed phases, known limitations
