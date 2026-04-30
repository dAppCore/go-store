<!-- SPDX-License-Identifier: EUPL-1.2 -->

# Agent Notes

This repository is the Core Go store module. Keep changes narrow, preserve the
public store contracts, and verify from the `go/` module before handing work
back.

## Code Map

- `go/store.go` owns the core SQLite-backed key-value store API and lifecycle.
- `go/events.go` contains mutation events, watchers, and callbacks.
- `go/scope.go` contains namespace isolation and quota enforcement.
- `go/journal.go` contains journal persistence and query helpers.
- `go/workspace.go` contains workspace buffering, commit flow, and orphan recovery.
- `go/compact.go` contains cold archive generation.
- `docs/` contains package docs, architecture notes, and development guidance.

## Compliance Rules

Follow the v0.9.0 Core compliance shape. Use `dappco.re/go` primitives for JSON,
errors, formatting, strings, bytes, filesystem, process, and environment helpers
whenever a wrapper exists. Do not add files named `ax7*.go`, versioned test
files, compatibility shims, or monolithic compliance dumps.

For every production source file with public symbols, keep tests and examples
beside that file. Test names use `Test<File>_<Symbol>_Good`,
`Test<File>_<Symbol>_Bad`, and `Test<File>_<Symbol>_Ugly`. Examples use
`Example<Symbol>` or a valid lowercase suffix variant.

## Before Stopping

Use the exact repository gate:

```bash
cd go
GOWORK=off GOPROXY=direct GOSUMDB=off go build ./...
GOWORK=off GOPROXY=direct GOSUMDB=off go vet ./...
GOWORK=off GOPROXY=direct GOSUMDB=off go test -count=1 -short ./...
cd ..
bash /Users/snider/Code/core/go/tests/cli/v090-upgrade/audit.sh .
```
