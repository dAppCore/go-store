# TODO.md — go-store

Dispatched from core/go orchestration. Pick up tasks in order.

---

## Phase 0: Hardening & Test Coverage

- [ ] **Expand test coverage** — `store_test.go` exists. Add tests for: concurrent `Set`/`Get` with 10 goroutines (race test), `Render()` with invalid template syntax, `Render()` with missing template vars, `Get()` on non-existent group (vs non-existent key), `DeleteGroup()` then verify `GetAll()` returns empty, `Count()` after bulk inserts, `:memory:` vs file-backed store, WAL mode verification.
- [ ] **Edge cases** — Test: empty key, empty value, empty group, very long key (10K chars), binary-ish value (null bytes), Unicode keys and values.
- [ ] **Benchmark** — `BenchmarkSet`, `BenchmarkGet`, `BenchmarkGetAll` with 10K keys in a group. Measure SQLite WAL write throughput.
- [ ] **`go vet ./...` clean** — Fix any warnings.

## Phase 1: TTL Support

- [ ] Add optional expiry timestamp for keys
- [ ] Background goroutine to purge expired entries
- [ ] `SetWithTTL(group, key, value, duration)` API
- [ ] Lazy expiry check on `Get` as fallback

## Phase 2: Namespace Isolation

- [ ] Group-based access control for multi-tenant use
- [ ] Namespace prefixing to prevent key collisions across tenants
- [ ] Per-namespace quota limits (max keys, max total size)

## Phase 3: Event Hooks

- [ ] Notify on `Set` / `Delete` for reactive patterns
- [ ] Channel-based subscription: `Watch(group, key) <-chan Event`
- [ ] Support wildcard watches (`Watch(group, "*")`)
- [ ] Integration hook for go-ws to broadcast store changes via WebSocket

---

## Workflow

1. Virgil in core/go writes tasks here after research
2. This repo's dedicated session picks up tasks in phase order
3. Mark `[x]` when done, note commit hash
