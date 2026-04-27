// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"database/sql"

	core "dappco.re/go/core"
	_ "github.com/marcboeker/go-duckdb"
)

// DuckDB table names for checkpoint scoring and probe results.
//
// Usage example:
//
//	_ = db.EnsureScoringTables()
//	db.Exec(core.Sprintf("SELECT * FROM %s", store.TableCheckpointScores))
const (
	// TableCheckpointScores is the table name for checkpoint scoring data.
	//
	// Usage example:
	//
	//	store.TableCheckpointScores // "checkpoint_scores"
	TableCheckpointScores = "checkpoint_scores"

	// TableProbeResults is the table name for probe result data.
	//
	// Usage example:
	//
	//	store.TableProbeResults // "probe_results"
	TableProbeResults = "probe_results"
)

// DuckDB wraps a DuckDB connection for analytical queries against training
// data, benchmark results, and scoring tables.
//
// Usage example:
//
//	db, err := store.OpenDuckDB("/Volumes/Data/lem/lem.duckdb")
//	if err != nil { return }
//	defer func() { _ = db.Close() }()
//	rows, _ := db.QueryGoldenSet(500)
type DuckDB struct {
	conn *sql.DB
	path string
}

// OpenDuckDB opens a DuckDB database file in read-only mode to avoid locking
// issues with the Python pipeline.
//
// Usage example:
//
//	db, err := store.OpenDuckDB("/Volumes/Data/lem/lem.duckdb")
func OpenDuckDB(path string) (*DuckDB, error) {
	conn, err := sql.Open("duckdb", path+"?access_mode=READ_ONLY")
	if err != nil {
		return nil, core.E("store.OpenDuckDB", core.Sprintf("open duckdb %s", path), err)
	}
	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return nil, core.E("store.OpenDuckDB", core.Sprintf("ping duckdb %s", path), err)
	}
	return &DuckDB{conn: conn, path: path}, nil
}

// OpenDuckDBReadWrite opens a DuckDB database in read-write mode.
//
// Usage example:
//
//	db, err := store.OpenDuckDBReadWrite("/Volumes/Data/lem/lem.duckdb")
func OpenDuckDBReadWrite(path string) (*DuckDB, error) {
	conn, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, core.E("store.OpenDuckDBReadWrite", core.Sprintf("open duckdb %s", path), err)
	}
	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return nil, core.E("store.OpenDuckDBReadWrite", core.Sprintf("ping duckdb %s", path), err)
	}
	return &DuckDB{conn: conn, path: path}, nil
}

// Close closes the database connection.
//
// Usage example:
//
//	defer func() { _ = db.Close() }()
func (db *DuckDB) Close() error {
	return db.conn.Close()
}

// Path returns the database file path.
//
// Usage example:
//
//	p := db.Path() // "/Volumes/Data/lem/lem.duckdb"
func (db *DuckDB) Path() string {
	return db.path
}

// Conn returns the underlying *sql.DB connection. Prefer the typed helpers
// (Exec, QueryRowScan, QueryRows) when possible; this accessor exists for
// callers that need streaming row iteration or transaction control.
//
// Usage example:
//
//	rows, err := db.Conn().Query("SELECT id, name FROM models WHERE kind = ?", "lem")
func (db *DuckDB) Conn() *sql.DB {
	return db.conn
}

// Exec executes a query without returning rows.
//
// Usage example:
//
//	err := db.Exec("INSERT INTO golden_set VALUES (?, ?)", idx, prompt)
func (db *DuckDB) Exec(query string, args ...any) error {
	_, err := db.conn.Exec(query, args...)
	if err != nil {
		return core.E("store.DuckDB.Exec", "execute query", err)
	}
	return nil
}

// QueryRowScan executes a query expected to return at most one row and scans
// the result into dest. It is a convenience wrapper around sql.DB.QueryRow.
//
// Usage example:
//
//	var count int
//	err := db.QueryRowScan("SELECT COUNT(*) FROM golden_set", &count)
func (db *DuckDB) QueryRowScan(query string, dest any, args ...any) error {
	return db.conn.QueryRow(query, args...).Scan(dest)
}

// GoldenSetRow represents one row from the golden_set table.
//
// Usage example:
//
//	rows, err := db.QueryGoldenSet(500)
//	for _, row := range rows { core.Println(row.Prompt) }
type GoldenSetRow struct {
	// Idx is the row index.
	//
	// Usage example:
	//
	//	row.Idx // 42
	Idx int

	// SeedID is the seed identifier that produced this row.
	//
	// Usage example:
	//
	//	row.SeedID // "seed-001"
	SeedID string

	// Domain is the content domain (e.g. "philosophy", "science").
	//
	// Usage example:
	//
	//	row.Domain // "philosophy"
	Domain string

	// Voice is the writing voice/style used for generation.
	//
	// Usage example:
	//
	//	row.Voice // "watts"
	Voice string

	// Prompt is the input prompt text.
	//
	// Usage example:
	//
	//	row.Prompt // "What is sovereignty?"
	Prompt string

	// Response is the generated response text.
	//
	// Usage example:
	//
	//	row.Response // "Sovereignty is..."
	Response string

	// GenTime is the generation time in seconds.
	//
	// Usage example:
	//
	//	row.GenTime // 2.5
	GenTime float64

	// CharCount is the character count of the response.
	//
	// Usage example:
	//
	//	row.CharCount // 1500
	CharCount int
}

// ExpansionPromptRow represents one row from the expansion_prompts table.
//
// Usage example:
//
//	prompts, err := db.QueryExpansionPrompts("pending", 100)
//	for _, p := range prompts { core.Println(p.Prompt) }
type ExpansionPromptRow struct {
	// Idx is the row index.
	//
	// Usage example:
	//
	//	p.Idx // 42
	Idx int64

	// SeedID is the seed identifier that produced this prompt.
	//
	// Usage example:
	//
	//	p.SeedID // "seed-001"
	SeedID string

	// Region is the geographic/cultural region for the prompt.
	//
	// Usage example:
	//
	//	p.Region // "western"
	Region string

	// Domain is the content domain (e.g. "philosophy", "science").
	//
	// Usage example:
	//
	//	p.Domain // "philosophy"
	Domain string

	// Language is the ISO language code for the prompt.
	//
	// Usage example:
	//
	//	p.Language // "en"
	Language string

	// Prompt is the prompt text in the original language.
	//
	// Usage example:
	//
	//	p.Prompt // "What is sovereignty?"
	Prompt string

	// PromptEn is the English translation of the prompt.
	//
	// Usage example:
	//
	//	p.PromptEn // "What is sovereignty?"
	PromptEn string

	// Priority is the generation priority (lower is higher priority).
	//
	// Usage example:
	//
	//	p.Priority // 1
	Priority int

	// Status is the processing status (e.g. "pending", "done").
	//
	// Usage example:
	//
	//	p.Status // "pending"
	Status string
}

// QueryGoldenSet returns all golden set rows with responses >= minChars.
//
// Usage example:
//
//	rows, err := db.QueryGoldenSet(500)
func (db *DuckDB) QueryGoldenSet(minChars int) ([]GoldenSetRow, error) {
	rows, err := db.conn.Query(
		"SELECT idx, seed_id, domain, voice, prompt, response, gen_time, char_count "+
			"FROM golden_set WHERE char_count >= ? ORDER BY idx",
		minChars,
	)
	if err != nil {
		return nil, core.E("store.DuckDB.QueryGoldenSet", "query golden_set", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var result []GoldenSetRow
	for rows.Next() {
		var r GoldenSetRow
		if err := rows.Scan(&r.Idx, &r.SeedID, &r.Domain, &r.Voice,
			&r.Prompt, &r.Response, &r.GenTime, &r.CharCount); err != nil {
			return nil, core.E("store.DuckDB.QueryGoldenSet", "scan golden_set row", err)
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// CountGoldenSet returns the total count of golden set rows.
//
// Usage example:
//
//	count, err := db.CountGoldenSet()
func (db *DuckDB) CountGoldenSet() (int, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM golden_set").Scan(&count)
	if err != nil {
		return 0, core.E("store.DuckDB.CountGoldenSet", "count golden_set", err)
	}
	return count, nil
}

// QueryExpansionPrompts returns expansion prompts filtered by status.
//
// Usage example:
//
//	prompts, err := db.QueryExpansionPrompts("pending", 100)
func (db *DuckDB) QueryExpansionPrompts(status string, limit int) ([]ExpansionPromptRow, error) {
	query := "SELECT idx, seed_id, region, domain, language, prompt, prompt_en, priority, status " +
		"FROM expansion_prompts"
	var args []any

	if status != "" {
		query += " WHERE status = ?"
		args = append(args, status)
	}
	query += " ORDER BY priority, idx"

	if limit > 0 {
		query += core.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, core.E("store.DuckDB.QueryExpansionPrompts", "query expansion_prompts", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var result []ExpansionPromptRow
	for rows.Next() {
		var r ExpansionPromptRow
		if err := rows.Scan(&r.Idx, &r.SeedID, &r.Region, &r.Domain,
			&r.Language, &r.Prompt, &r.PromptEn, &r.Priority, &r.Status); err != nil {
			return nil, core.E("store.DuckDB.QueryExpansionPrompts", "scan expansion_prompt row", err)
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// CountExpansionPrompts returns counts by status.
//
// Usage example:
//
//	total, pending, err := db.CountExpansionPrompts()
func (db *DuckDB) CountExpansionPrompts() (total int, pending int, err error) {
	err = db.conn.QueryRow("SELECT COUNT(*) FROM expansion_prompts").Scan(&total)
	if err != nil {
		return 0, 0, core.E("store.DuckDB.CountExpansionPrompts", "count expansion_prompts", err)
	}
	err = db.conn.QueryRow("SELECT COUNT(*) FROM expansion_prompts WHERE status = 'pending'").Scan(&pending)
	if err != nil {
		return total, 0, core.E("store.DuckDB.CountExpansionPrompts", "count pending expansion_prompts", err)
	}
	return total, pending, nil
}

// UpdateExpansionStatus updates the status of an expansion prompt by idx.
//
// Usage example:
//
//	err := db.UpdateExpansionStatus(42, "done")
func (db *DuckDB) UpdateExpansionStatus(idx int64, status string) error {
	_, err := db.conn.Exec("UPDATE expansion_prompts SET status = ? WHERE idx = ?", status, idx)
	if err != nil {
		return core.E("store.DuckDB.UpdateExpansionStatus", core.Sprintf("update expansion_prompt %d", idx), err)
	}
	return nil
}

// QueryRows executes an arbitrary SQL query and returns results as maps.
//
// Usage example:
//
//	rows, err := db.QueryRows("SELECT COUNT(*) AS n FROM golden_set")
func (db *DuckDB) QueryRows(query string, args ...any) ([]map[string]any, error) {
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, core.E("store.DuckDB.QueryRows", "query", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	cols, err := rows.Columns()
	if err != nil {
		return nil, core.E("store.DuckDB.QueryRows", "columns", err)
	}

	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, core.E("store.DuckDB.QueryRows", "scan", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// EnsureScoringTables creates the scoring tables if they do not exist.
//
// Usage example:
//
//	if err := db.EnsureScoringTables(); err != nil { return }
func (db *DuckDB) EnsureScoringTables() error {
	if _, err := db.conn.Exec(core.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		model TEXT, run_id TEXT, label TEXT, iteration INTEGER,
		correct INTEGER, total INTEGER, accuracy DOUBLE,
		scored_at TIMESTAMP DEFAULT current_timestamp,
		PRIMARY KEY (run_id, label)
	)`, TableCheckpointScores)); err != nil {
		return core.E("store.DuckDB.EnsureScoringTables", "create checkpoint_scores", err)
	}
	if _, err := db.conn.Exec(core.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		model TEXT, run_id TEXT, label TEXT, probe_id TEXT,
		passed BOOLEAN, response TEXT, iteration INTEGER,
		scored_at TIMESTAMP DEFAULT current_timestamp,
		PRIMARY KEY (run_id, label, probe_id)
	)`, TableProbeResults)); err != nil {
		return core.E("store.DuckDB.EnsureScoringTables", "create probe_results", err)
	}
	if _, err := db.conn.Exec(`CREATE TABLE IF NOT EXISTS scoring_results (
		model TEXT, prompt_id TEXT, suite TEXT,
		dimension TEXT, score DOUBLE,
		scored_at TIMESTAMP DEFAULT current_timestamp
	)`); err != nil {
		return core.E("store.DuckDB.EnsureScoringTables", "create scoring_results", err)
	}
	return nil
}

// WriteScoringResult writes a single scoring dimension result to DuckDB.
//
// Usage example:
//
//	err := db.WriteScoringResult("lem-8b", "p-001", "ethics", "honesty", 0.95)
func (db *DuckDB) WriteScoringResult(model, promptID, suite, dimension string, score float64) error {
	_, err := db.conn.Exec(
		`INSERT INTO scoring_results (model, prompt_id, suite, dimension, score) VALUES (?, ?, ?, ?, ?)`,
		model, promptID, suite, dimension, score,
	)
	if err != nil {
		return core.E("store.DuckDB.WriteScoringResult", "insert scoring result", err)
	}
	return nil
}

// TableCounts returns row counts for all known tables.
//
// Usage example:
//
//	counts, err := db.TableCounts()
//	n := counts["golden_set"]
func (db *DuckDB) TableCounts() (map[string]int, error) {
	tables := []string{"golden_set", "expansion_prompts", "seeds", "prompts",
		"training_examples", "gemini_responses", "benchmark_questions", "benchmark_results", "validations",
		TableCheckpointScores, TableProbeResults, "scoring_results"}

	counts := make(map[string]int)
	for _, t := range tables {
		var count int
		err := db.conn.QueryRow(core.Sprintf("SELECT COUNT(*) FROM %s", t)).Scan(&count)
		if err != nil {
			continue
		}
		counts[t] = count
	}
	return counts, nil
}
