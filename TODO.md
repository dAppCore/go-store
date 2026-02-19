# TODO.md -- go-store

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
