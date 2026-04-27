# Dependency Exceptions

This repository is pure Go by default and permits `modernc.org/sqlite` as the
normal runtime database dependency. The following exception is documented
because the current PR contains load-bearing analytical workspace code that
cannot be replaced by a pure-Go DuckDB-compatible driver.

## `github.com/marcboeker/go-duckdb`

`github.com/marcboeker/go-duckdb` is retained only for DuckDB-backed workspace
buffers and LEM analytical import helpers. DuckDB files are produced and
consumed by existing data pipelines, and no pure-Go DuckDB implementation with
compatible SQL semantics is currently available. Replacing it with
`modernc.org/sqlite` would remove DuckDB JSON import, analytical table, and
workspace recovery behaviour rather than preserving the feature.

This is a CGO and MIT-licensed dependency exception. It must not be used for the
primary SQLite store path, and new runtime storage features should continue to
use pure-Go dependencies compatible with EUPL-1.2.
