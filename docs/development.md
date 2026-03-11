---
title: Development Guide
description: How to build, test, benchmark, and contribute to go-store.
---

# Development Guide

## Prerequisites

- Go 1.26 or later
- No CGO required (`modernc.org/sqlite` is a pure-Go SQLite implementation)
- No external tools beyond the Go toolchain

## Build and Test

The package is a standard Go module. All standard `go` commands apply.

```bash
# Run all tests
go test ./...

# Run with the race detector (required before any commit touching concurrency)
go test -race ./...

# Run a single test by name
go test -v -run TestWatch_Good_SpecificKey ./...

# Run tests with coverage
go test -cover ./...

# Generate a coverage profile and view it in the browser
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run benchmarks
go test -bench=. -benchmem ./...

# Run a specific benchmark
go test -bench=BenchmarkSet -benchmem ./...
```

Alternatively, if the `core` CLI is available:

```bash
core go test
core go cov --open      # generates and opens coverage HTML
core go qa              # fmt + vet + lint + test
```

**Coverage target: 95%.** The remaining uncovered lines are defensive error paths (scan errors, rows iteration errors on corrupted databases) covered by `coverage_test.go`. Do not remove these checks to chase coverage -- they protect against driver and OS-level failures that are unreachable through integration tests against a healthy SQLite database.

## Test Patterns

Tests follow the `_Good`, `_Bad`, `_Ugly` suffix convention used across the Core Go ecosystem:

- `_Good` -- happy-path behaviour, including edge cases that should succeed
- `_Bad` -- expected error conditions (closed store, invalid input, quota exceeded)
- `_Ugly` -- not currently used in this package (reserved for panic/edge cases)

Tests are grouped into sections by the method under test, marked with comment banners:

```go
// ---------------------------------------------------------------------------
// Watch -- specific key
// ---------------------------------------------------------------------------

func TestWatch_Good_SpecificKey(t *testing.T) { ... }
func TestWatch_Good_WildcardKey(t *testing.T) { ... }
```

### In-Memory vs File-Backed Stores

Use `New(":memory:")` for all tests that do not require persistence. In-memory stores are faster and leave no filesystem artefacts.

Use `filepath.Join(t.TempDir(), "name.db")` for tests that verify WAL mode, persistence across open/close cycles, or concurrent writes. `t.TempDir()` is cleaned up automatically at the end of the test.

### TTL Tests

TTL expiry tests require short sleeps. Use the minimum duration that reliably demonstrates expiry (typically 1ms TTL + 5ms sleep). Do not use sleeps as synchronisation barriers for goroutines -- use `sync.WaitGroup` instead.

### Assertion Library

The test suite uses `github.com/stretchr/testify/assert` and `github.com/stretchr/testify/require`. Use `require` for preconditions (test should abort on failure) and `assert` for verifications (test should continue and report all failures).

### Race Detector

Run `go test -race ./...` before marking any concurrency work as complete. The test suite must be clean under the race detector.

## Benchmarks

The benchmark suite covers the major operations. Reference results (Apple M-series, in-memory store):

```
BenchmarkSet-32               119280    10290 ns/op     328 B/op    12 allocs/op
BenchmarkGet-32               335707     3589 ns/op     576 B/op    21 allocs/op
BenchmarkGetAll-32 (10K keys)    258  4741451 ns/op 2268787 B/op 80095 allocs/op
BenchmarkSet_FileBacked-32      4525   265868 ns/op     327 B/op    12 allocs/op
```

Derived throughput:

| Operation | Approximate ops/sec | Notes |
|-----------|---------------------|-------|
| In-memory `Set` | 97,000 | |
| In-memory `Get` | 279,000 | |
| File-backed `Set` | 3,800 | Dominated by fsync |
| `GetAll` (10K keys) | 211 | ~2.3 MB allocated per call |

Additional benchmarks in `bench_test.go` cover:

- `GetAll` with varying group sizes (10, 100, 1,000, 10,000 keys)
- Parallel `Set`/`Get` throughput (`b.RunParallel`)
- `Count` on a 10,000-key group
- `Delete` throughput
- `SetWithTTL` throughput
- `Render` with 50 keys and a 3-variable template

`GetAll` allocations scale linearly with the number of keys (one map entry per row). Applications fetching very large groups should consider pagination at a higher layer or restructuring data into multiple smaller groups.

## Coding Standards

### Language

Use UK English throughout all documentation, comments, and error messages. This applies to spelling (colour, organisation, behaviour, serialise, initialise) and terminology. American spellings are not acceptable.

### Code Style

- `gofmt` formatting is mandatory. Run `go fmt ./...` before committing.
- `go vet ./...` must report no warnings.
- All error strings begin with the package function context: `"store.Method: what failed"`. This convention makes errors self-identifying in log output without requiring a stack trace.
- Exported identifiers must have Go doc comments.
- Internal helpers (unexported) should have comments explaining non-obvious behaviour.

### Dependencies

go-store is intentionally minimal. Before adding any new dependency:

1. Verify it cannot be replaced with a standard library alternative.
2. Verify it is pure Go (no CGO) to preserve cross-compilation.
3. Verify it has a compatible open-source licence (EUPL-1.2 compatible).

The only permitted runtime dependency is `modernc.org/sqlite`. Test-only dependencies (`github.com/stretchr/testify`) are acceptable.

## Adding a New Method

1. Implement the method on `*Store` in `store.go` (or `scope.go` if it is namespace-scoped).
2. If it is a mutating operation, call `s.notify(Event{...})` after the successful database write.
3. Add a corresponding delegation method to `ScopedStore` in `scope.go` that prefixes the group.
4. Write tests covering the happy path, error conditions, and closed-store behaviour.
5. Update quota checks in `checkQuota` if the operation affects key or group counts.
6. Run `go test -race ./...` and `go vet ./...`.
7. Update `docs/architecture.md` if the method introduces a new concept or changes an existing one.

## Commit Guidelines

Use conventional commit format:

```
type(scope): description
```

Common types: `feat`, `fix`, `test`, `docs`, `refactor`, `perf`, `chore`.

Examples:

```
feat(store): add PurgeExpired public method
fix(events): prevent deadlock when callback calls store methods
test(scope): add quota enforcement for new groups
docs(architecture): document WAL single-connection constraint
perf(store): replace linear watcher scan with index lookup
```

Every commit must include the co-author trailer:

```
Co-Authored-By: Virgil <virgil@lethean.io>
```

All tests must pass before committing:

```bash
go test -race ./...
go vet ./...
```

## Licence

go-store is licensed under the European Union Public Licence 1.2 (EUPL-1.2). All contributions are made under the same licence.
