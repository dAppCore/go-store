<!-- SPDX-License-Identifier: EUPL-1.2 -->

# Windows Builds

`go-store` compiles for Windows without CGO so consumers can build pure-Go
application paths:

```bash
cd go
GOWORK=off CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./...
```

DuckDB-backed APIs still require CGO at runtime. The DuckDB Go driver is a CGO
driver on every platform, including Windows, so `OpenDuckDB`,
`OpenDuckDBReadWrite`, and workspace buffering need a Windows-capable C
toolchain when those paths are used.

For Wails cross-compiles from macOS or Linux, set `CGO_ENABLED=1` so Wails can
route the build through its Docker/Zig cross toolchain:

```bash
cd go
GOWORK=off CGO_ENABLED=1 GOOS=windows GOARCH=amd64 go build ./...
```

If this command is run outside Wails, `CC` must point at a Windows cross
compiler. Without that compiler, the host C compiler will fail before the Go
package is tested.

The module intentionally stays on `github.com/marcboeker/go-duckdb v1.8.5`.
The v2 mirror depends on separate mapping and bindings modules and has produced
Windows symbol-skew failures when CGO is disabled.
