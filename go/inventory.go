// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"io"

	core "dappco.re/go"
)

// TargetTotal is the golden set target size used for progress reporting.
//
// Usage example:
//
//	pct := float64(count) / float64(store.TargetTotal) * 100
const TargetTotal = 15000

// duckDBTableOrder defines the canonical display order for DuckDB inventory
// tables.
var duckDBTableOrder = []string{
	"golden_set", "expansion_prompts", "seeds", "prompts",
	"training_examples", "gemini_responses", "benchmark_questions",
	"benchmark_results", "validations", TableCheckpointScores,
	TableProbeResults, "scoring_results",
}

// duckDBTableDetail holds extra context for a single table beyond its row count.
type duckDBTableDetail struct {
	notes []string
}

// PrintDuckDBInventory queries all known DuckDB tables and prints a formatted
// inventory with row counts, detail breakdowns, and a grand total.
//
// Usage example:
//
//	err := store.PrintDuckDBInventory(db, os.Stdout)
func PrintDuckDBInventory(db *DuckDB, w io.Writer) core.Result {
	counts, result := db.TableCounts()
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E("store.PrintDuckDBInventory", "table counts", err))
	}

	details := gatherDuckDBDetails(db, counts)

	core.Print(w, "DuckDB Inventory")
	core.Print(w, "%s", repeat("-", 52))

	grand := 0
	for _, table := range duckDBTableOrder {
		count, ok := counts[table]
		if !ok {
			continue
		}
		grand += count
		line := core.Sprintf("  %-24s %8d rows", table, count)

		if d, has := details[table]; has && len(d.notes) > 0 {
			line += core.Sprintf("  (%s)", core.Join(", ", d.notes...))
		}
		core.Print(w, "%s", line)
	}

	core.Print(w, "%s", repeat("-", 52))
	core.Print(w, "  %-24s %8d rows", "TOTAL", grand)

	return core.Ok(nil)
}

// gatherDuckDBDetails runs per-table detail queries and returns annotations
// keyed by table name. Errors on individual queries are silently ignored so
// the inventory always prints.
func gatherDuckDBDetails(db *DuckDB, counts map[string]int) map[string]*duckDBTableDetail {
	details := make(map[string]*duckDBTableDetail)

	if detail := goldenSetDetail(counts); detail != nil {
		details["golden_set"] = detail
	}
	if detail := trainingExamplesDetail(db, counts); detail != nil {
		details["training_examples"] = detail
	}
	if detail := promptsDetail(db, counts); detail != nil {
		details["prompts"] = detail
	}
	if detail := geminiResponsesDetail(db, counts); detail != nil {
		details["gemini_responses"] = detail
	}
	if detail := benchmarkResultsDetail(db, counts); detail != nil {
		details["benchmark_results"] = detail
	}

	return details
}

func goldenSetDetail(counts map[string]int) *duckDBTableDetail {
	count, ok := counts["golden_set"]
	if !ok {
		return nil
	}
	pct := float64(count) / float64(TargetTotal) * 100
	return &duckDBTableDetail{notes: []string{core.Sprintf("%.1f%% of %d target", pct, TargetTotal)}}
}

func trainingExamplesDetail(db *DuckDB, counts map[string]int) *duckDBTableDetail {
	if _, ok := counts["training_examples"]; !ok {
		return nil
	}
	rows, result := db.QueryRows("SELECT COUNT(DISTINCT source) AS n FROM training_examples")
	if !result.OK || len(rows) == 0 {
		return nil
	}
	return &duckDBTableDetail{notes: []string{core.Sprintf("%d sources", duckDBToInt(rows[0]["n"]))}}
}

func promptsDetail(db *DuckDB, counts map[string]int) *duckDBTableDetail {
	if _, ok := counts["prompts"]; !ok {
		return nil
	}
	detail := &duckDBTableDetail{}
	appendDistinctPromptCount(db, detail, "domain", "domains")
	appendDistinctPromptCount(db, detail, "voice", "voices")
	if len(detail.notes) == 0 {
		return nil
	}
	return detail
}

func appendDistinctPromptCount(db *DuckDB, detail *duckDBTableDetail, column, label string) {
	rows, result := db.QueryRows(core.Sprintf("SELECT COUNT(DISTINCT %s) AS n FROM prompts", column))
	if result.OK && len(rows) > 0 {
		detail.notes = append(detail.notes, core.Sprintf("%d %s", duckDBToInt(rows[0]["n"]), label))
	}
}

func geminiResponsesDetail(db *DuckDB, counts map[string]int) *duckDBTableDetail {
	if _, ok := counts["gemini_responses"]; !ok {
		return nil
	}
	rows, result := db.QueryRows(
		"SELECT source_model, COUNT(*) AS n FROM gemini_responses GROUP BY source_model ORDER BY n DESC",
	)
	if !result.OK || len(rows) == 0 {
		return nil
	}
	var parts []string
	for _, row := range rows {
		model := duckDBStrVal(row, "source_model")
		if model != "" {
			parts = append(parts, core.Sprintf("%s:%d", model, duckDBToInt(row["n"])))
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return &duckDBTableDetail{notes: parts}
}

func benchmarkResultsDetail(db *DuckDB, counts map[string]int) *duckDBTableDetail {
	if _, ok := counts["benchmark_results"]; !ok {
		return nil
	}
	rows, result := db.QueryRows("SELECT COUNT(DISTINCT source) AS n FROM benchmark_results")
	if !result.OK || len(rows) == 0 {
		return nil
	}
	return &duckDBTableDetail{notes: []string{core.Sprintf("%d categories", duckDBToInt(rows[0]["n"]))}}
}

// duckDBToInt converts a DuckDB value to int. DuckDB returns integers as int64
// (not float64 like InfluxDB), so we handle both types.
func duckDBToInt(v any) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case int32:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

// duckDBStrVal extracts a string value from a row map.
func duckDBStrVal(row map[string]any, key string) string {
	if v, ok := row[key]; ok {
		return core.Sprint(v)
	}
	return ""
}
