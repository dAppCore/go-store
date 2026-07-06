//go:build cgo

// SPDX-License-Identifier: EUPL-1.2

package store

import _ "github.com/marcboeker/go-duckdb" // Note: registers the database/sql "duckdb" driver when CGO is available.
