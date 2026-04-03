# CODEX.md

This repository uses the same working conventions described in [`CLAUDE.md`](CLAUDE.md).
Keep the two files aligned.

## AX Notes

- Prefer descriptive names over abbreviations.
- Public comments should show real usage with concrete values.
- Keep examples in UK English.
- Do not add compatibility aliases; the primary API names are the contract.
- Preserve the single-connection SQLite design.
- Verify with `go test ./...`, `go test -race ./...`, and `go vet ./...` before committing.
- Use conventional commits and include the `Co-Authored-By: Virgil <virgil@lethean.io>` trailer.

## Repository Shape

- `store.go` contains the core store API and SQLite lifecycle.
- `events.go` contains mutation events, watchers, and callbacks.
- `scope.go` contains namespace isolation and quota enforcement.
- `journal.go` contains journal persistence and query helpers.
- `workspace.go` contains workspace buffering and orphan recovery.
- `compact.go` contains cold archive generation.
- `docs/` contains the package docs, architecture notes, and history.
