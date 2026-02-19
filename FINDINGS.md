# FINDINGS.md -- go-store

## 2026-02-19: Split from core/go (Virgil)

### Origin

Extracted from `forge.lthn.ai/core/go` `pkg/store/` on 19 Feb 2026.

### Architecture

- SQLite database with WAL mode enabled for concurrent read performance
- Compound primary key: `(group, key)` -- groups act as logical namespaces
- UPSERT semantics on write (INSERT OR REPLACE)
- Template rendering support via Go `text/template` for dynamic values
- Pure Go SQLite driver via `modernc.org/sqlite` (no CGO required)

### Dependencies

- `modernc.org/sqlite` -- pure Go SQLite implementation

### Tests

- 1 test file covering CRUD operations, group isolation, and template rendering
