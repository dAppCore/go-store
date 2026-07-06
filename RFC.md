# RFC — Windows support for go-store

**Status:** draft · **Author:** Cladius · **Date:** 2026-05-12
**Triggered by:** Lethean Desktop (`lthn/desktop`) Windows cross-compile blocked at `dappco.re/go/store` duckdb driver registration. Sibling to [go-process RFC](../go-process/RFC.md) and [go-io RFC](../go-io/RFC.md) — same demo deadline, same "compile clean on `GOOS=windows GOARCH=amd64`" target.

## Problem

`wails3 build GOOS=windows GOARCH=amd64` against the desktop fails with:

```
package dappco.re/lthn/desktop/cmd/lthn
  imports dappco.re/go/store
  imports github.com/marcboeker/go-duckdb: build constraints exclude all Go files in
    ~/go/pkg/mod/github.com/marcboeker/go-duckdb@v1.8.5
```

or — if the github mirror's v2 bump (`37ed852`) is picked up:

```
imports github.com/marcboeker/go-duckdb/v2
  imports github.com/marcboeker/go-duckdb/mapping@v0.0.21
    /mapping_windows_amd64.go:11:22: undefined: bindings.Type
    /mapping_windows_amd64.go:56:23: undefined: bindings.State
    /mapping_windows_amd64.go:63:30: undefined: bindings.PendingState
    [... and 8 more ...]
```

Both errors trace to the **same root cause**: `CGO_ENABLED=0` is the wails3 default for `GOOS=windows`, and **every published duckdb-go library requires CGO to compile on every platform — including Windows**.

The `bindings.Type` symbols ARE defined in `github.com/duckdb/duckdb-go-bindings/windows-amd64@v0.1.21/bindings.go` line 22 onward — but every type definition there is a `C.duckdb_*` alias inside a file with a leading cgo preamble. With CGO off, the whole file is excluded by the toolchain and the symbols vanish, breaking the dependent `mapping_windows_amd64.go`.

The cross-platform docs (`wails-master/docs/src/content/docs/guides/build/cross-platform.mdx` lines 50-60) explicitly call this out: `CGO_ENABLED=1` forces Docker auto-routing for non-Windows hosts, and the `wails-cross` image bundles a Zig cross-compiler that handles Windows-amd64 CGO. That path is **not currently exercised** by `lthn/desktop` because the per-window-Taskfile default is CGO off.

## Scope

In: `go/duckdb.go`, `go/go.mod` (one version pin choice), and a `docs/windows.md` checklist documenting the CGO-required build flow.

Out of scope:
- Migrating away from duckdb (e.g. SQLite-only workspace buffer) — would change the public surface
- Static-linking duckdb (`-tags duckdb_use_lib` / `duckdb_use_static_lib`) — Phase 2, not needed for the demo
- Bumping the workspace-mode `go.work` references — those track this repo's `dev` HEAD, so this fix propagates automatically once committed

## Constraints

- Keep the public `store` API stable — current callers (`lthn/desktop/pkg/desktop`, `core/ide/pkg/store`) must continue to compile against `OpenDuckDB` / `OpenDuckDBReadWrite` without any signature change.
- Don't break Linux/macOS — the existing CGO_ENABLED=1 path on those targets already works (build proven on the desktop via `wails3 task darwin:package` 2026-05-12).
- Keep `database/sql` driver registration intact — the blank import line in `duckdb.go` is what allows `sql.Open("duckdb", …)` to find the driver. The driver name `duckdb` is registered by every variant (v1 marcboeker, v2 marcboeker, dappcore-mirrored) — drop-in compatible.

## Design

Three coordinated changes — version pick, driver-registration site, and build
documentation.

### 1. Module choice

Three published paths exist today; the table covers them all so future-Snider has the full menu rather than picking blind:

| Module | Latest | Status | Windows-amd64 | Notes |
|---|---|---|---|---|
| `github.com/marcboeker/go-duckdb` (v1) | `v1.8.5` | maintained but frozen tagging | yes (CGO) | Current local `~/Code/core/go-store/dev` pin |
| `github.com/marcboeker/go-duckdb/v2` | `v2.4.3` | **deprecated**, module moved | yes (CGO) | Github mirror's `37ed852` HEAD; depends on `mapping@v0.0.21` + `bindings@v0.1.21` (these versions are also frozen — no escape from version-skew failures on this path) |
| `github.com/duckdb/duckdb-go` | `v1.8.5` | **active canonical home** | yes (CGO) | Same code as marcboeker/v1, rehosted under the official duckdb org. `go.mod` still declares the module path as `github.com/marcboeker/go-duckdb` for backwards compat, so the import statement keeps the marcboeker path |

**Recommendation: stay on `github.com/marcboeker/go-duckdb v1.8.5` (current local state).**

Reasoning:
- Local `dev` HEAD already on this — no migration overhead
- Self-contained module (no separate `mapping`/`bindings` sub-modules to keep in sync — the version-skew bug only exists in the v2 chain)
- Cross-compiles cleanly on Windows amd64 with `CGO_ENABLED=1` + Zig (proven by Wails docs + the source-of-truth `windows_amd64.go` file having no missing symbols)
- The deprecation only affects the *v2* sub-module — `marcboeker/go-duckdb` (v1 path) is unchanged and tracks the same upstream as `duckdb/duckdb-go`

**Action:** the existing `go/go.mod` line `github.com/marcboeker/go-duckdb v1.8.5 // Note: DuckDB workspace buffer driver; no core equivalent` stays as-is. The two homelab→github divergent commits (`a84fc8c`, `060c623`) cherry-forward cleanly on top of `37ed852` if you choose to keep the v2 bump on github/dev instead; this RFC's recommendation is to **revert `37ed852` on github/dev** so all three remotes (homelab, github, forge.lthn.ai/origin) converge on v1.

### 2. Driver-registration site

`go/duckdb.go` keeps the public DuckDB API and SQL helper implementation. The
driver registration lives in a separate CGO-only source file:

```go
//go:build cgo

package store

import _ "github.com/marcboeker/go-duckdb"
```

This lets `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...` compile the
package for consumers that do not exercise DuckDB-backed paths. When CGO is
disabled, calls such as `OpenDuckDB`, `OpenDuckDBReadWrite`, or
`NewWorkspace` fail at runtime with the normal `database/sql` unknown-driver
error rather than failing the whole application compile.

Full DuckDB functionality still requires `CGO_ENABLED=1` and a Windows-capable
C compiler. If you choose to migrate to `github.com/duckdb/duckdb-go`, the
blank import path remains `github.com/marcboeker/go-duckdb` because the new
module declares that module path internally — so the only module file change is:

```diff
-require github.com/marcboeker/go-duckdb v1.8.5
+require github.com/duckdb/duckdb-go v1.8.5
```

…and `go mod tidy` does the rest.

### 3. Windows build checklist

The repo-level checklist lives in `docs/windows.md`. It records two supported
lanes:

- compile-only Windows builds with `CGO_ENABLED=0`
- DuckDB-enabled Windows builds with `CGO_ENABLED=1` and a cross C compiler

### 4. Consumer-side Taskfile defaults (`lthn/desktop`)

The Windows-build Taskfile at `lthn/desktop/build/windows/Taskfile.yml` lines 22, 29, 59 defaults `CGO_ENABLED` to `"0"`. If the desktop build needs workspace buffering or explicit DuckDB reads, **change required at the consumer**, not in this repo:

```diff
-    vars:
-      CGO_ENABLED: '{{.CGO_ENABLED | default "0"}}'
+    vars:
+      CGO_ENABLED: '{{.CGO_ENABLED | default "1"}}'
```

…at both `build` and `build:native` call sites. That flips the auto-routing trigger and the Docker/Zig path takes over (per cross-platform.mdx line 60: "The Taskfile detects `CGO_ENABLED=1` on non-Windows hosts and automatically uses the Docker image").

This RFC notes the consumer change so the load-bearing context isn't lost; the actual edit lives in `lthn/desktop` and not in `go-store`.

## Phase 1 — verification commands

Once the desktop submodule pulls a `go-store dev` that's on
`marcboeker/go-duckdb v1.8.5` with CGO-gated driver registration:

```bash
# go-store local self-check (workspace + GOWORK=off paths)
cd ~/Code/core/go-store/go
GOWORK=off CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./...

# DuckDB-enabled Windows build; requires Wails Docker/Zig or another Windows C compiler.
GOWORK=off CGO_ENABLED=1 GOOS=windows GOARCH=amd64 go build ./...

GOWORK=off GOPROXY=direct GOSUMDB=off go build ./...
GOWORK=off GOPROXY=direct GOSUMDB=off go vet ./...
GOWORK=off GOPROXY=direct GOSUMDB=off go test -count=1 -short ./...
GOWORK=off go test -count=1 ./...
gofmt -l .

# Audit
~/Code/core/go/scripts/audit-v0.9.sh   # expect: COMPLIANT

# End-to-end via desktop (Docker + Zig)
cd ~/Code/lthn/desktop
wails3 task setup:docker        # if not already done
wails3 build GOOS=windows GOARCH=amd64
ls -lh bin/lthn.exe              # should produce ~100MB binary
```

Native Windows verification (homelab Windows VM) — same `go build` / `go vet` / `go test` with `CGO_ENABLED=1` and no `GOOS`/`GOARCH` override.

## Phase 2 — deferred

If the duckdb CGO toolchain itself proves unreliable across the wails-cross Docker image (Zig's gcc emulation has had `-m64` regressions on the linux/amd64 lane — see desktop's failed `wails3 package GOOS=linux` run on 2026-05-12), the static-link tags are the escape hatch:

```bash
go build -tags duckdb_use_static_lib ./...
```

This requires a prebuilt `libduckdb.a` for the target triple, which `go-duckdb`'s build constraints expect at `LD_LIBRARY_PATH`. Out of scope for the demo; revisit if Phase 1 hits Zig/glibc ABI snags.

## Open questions

1. **Remote divergence policy.** Local `~/Code/core/go-store/dev` is 2 commits ahead of `github/dev` and the v2 bump (`37ed852`) is github-only. Should this RFC's "revert 37ed852 on github/dev + force-push from local" be the canonical resolution, or do we cherry-forward `a84fc8c` + `060c623` on top? Either reconciles the divergence; the first matches this RFC's recommendation.
2. **Submodule pin in `lthn/desktop/external/store`.** Currently `37ed852`. After this fix lands, bump to the new local `dev` HEAD via `git submodule update --remote external/store` from the desktop repo.

## Closing

The actual store-side change is minimal: keep the v1 module pin, split DuckDB
driver registration into a CGO-only file, and document the Windows lanes. The
consumer-side Taskfile still needs `CGO_ENABLED=1` for DuckDB-enabled Windows
executables. This RFC exists to document the **mental model** so the next agent
/ human doesn't re-discover the marcboeker/v2 vs duckdb-go/v1 module-path maze.

Mantis ticket suggestion: **#1395 — go-store Windows compile path (RFC)** — sibling to #1390 (go-webview) / #1391 (go-forge) / #1394 (ide). Mark as **documentation-only on the go-store side** with a child issue against `lthn/desktop` for the Taskfile CGO_ENABLED flip.
