# CLAUDE.md

## What This Is

SQLite key-value store wrapper. Module: `forge.lthn.ai/core/go-store`

## Commands

```bash
go test ./...          # Run all tests
go test -v -run Name   # Run single test
```

## Key API

```go
st, _ := store.New(":memory:")
defer st.Close()
```

## Coding Standards

- UK English
- `go test ./...` must pass before commit
- Conventional commits: `type(scope): description`
- Co-Author: `Co-Authored-By: Virgil <virgil@lethean.io>`
