//go:build cgo && !noduckdb

// SPDX-License-Identifier: EUPL-1.2

package store

// DuckDB comes in with cgo, unless the build says -tags noduckdb: an app
// that only uses the key-value store (SQLite, pure Go) stays small that way.
// Without it, the workspace and OpenDuckDB report the driver missing, and
// DuckDBAvailable says false.
import _ "github.com/duckdb/duckdb-go/v2" // Note: the official DuckDB-hosted driver; registers the database/sql "duckdb" driver when CGO is available.
