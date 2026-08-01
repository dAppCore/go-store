//go:build cgo

// SPDX-License-Identifier: EUPL-1.2

package store

import _ "github.com/duckdb/duckdb-go/v2" // Note: the official DuckDB-hosted driver; registers the database/sql "duckdb" driver when CGO is available.
