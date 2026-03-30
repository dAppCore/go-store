# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

SQLite key-value store with TTL, namespace isolation, and reactive events. Pure Go (no CGO). Module: `dappco.re/go/core/store`

## Getting Started

Part of the Go workspace at `~/Code/go.work`—run `go work sync` after cloning. Single Go package with `store.go` (core) and `scope.go` (scoping/quota).

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
- `store.go` — Core `Store` type: CRUD on a `(grp, key)` compound-PK table, TTL via `expires_at` (Unix ms), background purge goroutine (60s interval), `text/template` rendering, `iter.Seq2` iterators
- `events.go` — Event system: `Watch`/`Unwatch` (buffered chan, cap 16, non-blocking sends drop events) and `OnChange` callbacks (synchronous in writer goroutine). Watcher and callback registries use separate locks, so callbacks can register or unregister subscriptions without deadlocking.
- `scope.go` — `ScopedStore` wraps `*Store`, prefixes groups with `namespace:`. Quota enforcement (`MaxKeys`/`MaxGroups`) checked before writes; upserts bypass quota. Namespace regex: `^[a-zA-Z0-9-]+$`

**TTL enforcement is triple-layered:** lazy delete on `Get`, query-time `WHERE` filtering on bulk reads, and background purge goroutine.

**LIKE queries use `escapeLike()`** with `^` as escape char to prevent SQL wildcard injection in `CountAll` and `Groups`/`GroupsSeq`.

## Key API

```go
storeInstance, _ := store.New(":memory:")      // or store.New("/path/to/db")
defer storeInstance.Close()

storeInstance.Set("group", "key", "value")                         // no expiry
storeInstance.SetWithTTL("group", "key", "value", 5*time.Minute)   // expires after TTL
value, _ := storeInstance.Get("group", "key")                       // lazy-deletes expired
storeInstance.Delete("group", "key")
storeInstance.DeleteGroup("group")
entries, _ := storeInstance.GetAll("group")    // excludes expired
count, _ := storeInstance.Count("group")       // excludes expired
output, _ := storeInstance.Render("Hello {{ .name }}", "group") // excludes expired
removed, _ := storeInstance.PurgeExpired()     // manual purge
total, _ := storeInstance.CountAll("prefix:")  // count keys matching prefix (excludes expired)
groupNames, _ := storeInstance.Groups("prefix:") // distinct group names matching prefix

// Namespace isolation (auto-prefixes groups with "tenant:")
scopedStore, _ := store.NewScoped(storeInstance, "tenant")
scopedStore.Set("config", "key", "value") // stored as "tenant:config" in underlying store

// With quota enforcement
quotaScopedStore, _ := store.NewScopedWithQuota(storeInstance, "tenant", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10})
quotaScopedStore.Set("g", "k", "v") // returns QuotaExceededError if limits hit

// Event hooks
watcher := storeInstance.Watch("group", "*") // wildcard: all keys in group ("*","*" for all)
defer storeInstance.Unwatch(watcher)
event := <-watcher.Events         // buffered chan, cap 16

unregister := storeInstance.OnChange(func(e store.Event) { /* synchronous in writer goroutine */ })
defer unregister()
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
2. If mutating, call `s.notify(Event{...})` after successful DB write
3. Add delegation method on `ScopedStore` in `scope.go` (prefix the group)
4. Update `checkQuota` in `scope.go` if it affects key/group counts
5. Write `Test<File>_<Function>_<Good|Bad|Ugly>` tests
6. Run `go test -race ./...` and `go vet ./...`

## Docs

- `docs/architecture.md` — full internal design details
- `docs/development.md` — test patterns, benchmarks, coding standards
- `docs/history.md` — completed phases, known limitations
