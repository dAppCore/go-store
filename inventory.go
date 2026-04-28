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
func PrintDuckDBInventory(db *DuckDB, w io.Writer) error {
	counts, err := db.TableCounts()
	if err != nil {
		return core.E("store.PrintDuckDBInventory", "table counts", err)
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

	return nil
}

// gatherDuckDBDetails runs per-table detail queries and returns annotations
// keyed by table name. Errors on individual queries are silently ignored so
// the inventory always prints.
func gatherDuckDBDetails(db *DuckDB, counts map[string]int) map[string]*duckDBTableDetail {
	details := make(map[string]*duckDBTableDetail)

	// golden_set: progress towards target
	if count, ok := counts["golden_set"]; ok {
		pct := float64(count) / float64(TargetTotal) * 100
		details["golden_set"] = &duckDBTableDetail{
			notes: []string{core.Sprintf("%.1f%% of %d target", pct, TargetTotal)},
		}
	}

	// training_examples: distinct sources
	if _, ok := counts["training_examples"]; ok {
		rows, err := db.QueryRows("SELECT COUNT(DISTINCT source) AS n FROM training_examples")
		if err == nil && len(rows) > 0 {
			n := duckDBToInt(rows[0]["n"])
			details["training_examples"] = &duckDBTableDetail{
				notes: []string{core.Sprintf("%d sources", n)},
			}
		}
	}

	// prompts: distinct domains and voices
	if _, ok := counts["prompts"]; ok {
		d := &duckDBTableDetail{}
		rows, err := db.QueryRows("SELECT COUNT(DISTINCT domain) AS n FROM prompts")
		if err == nil && len(rows) > 0 {
			d.notes = append(d.notes, core.Sprintf("%d domains", duckDBToInt(rows[0]["n"])))
		}
		rows, err = db.QueryRows("SELECT COUNT(DISTINCT voice) AS n FROM prompts")
		if err == nil && len(rows) > 0 {
			d.notes = append(d.notes, core.Sprintf("%d voices", duckDBToInt(rows[0]["n"])))
		}
		if len(d.notes) > 0 {
			details["prompts"] = d
		}
	}

	// gemini_responses: group by source_model
	if _, ok := counts["gemini_responses"]; ok {
		rows, err := db.QueryRows(
			"SELECT source_model, COUNT(*) AS n FROM gemini_responses GROUP BY source_model ORDER BY n DESC",
		)
		if err == nil && len(rows) > 0 {
			var parts []string
			for _, row := range rows {
				model := duckDBStrVal(row, "source_model")
				n := duckDBToInt(row["n"])
				if model != "" {
					parts = append(parts, core.Sprintf("%s:%d", model, n))
				}
			}
			if len(parts) > 0 {
				details["gemini_responses"] = &duckDBTableDetail{notes: parts}
			}
		}
	}

	// benchmark_results: distinct source categories
	if _, ok := counts["benchmark_results"]; ok {
		rows, err := db.QueryRows("SELECT COUNT(DISTINCT source) AS n FROM benchmark_results")
		if err == nil && len(rows) > 0 {
			n := duckDBToInt(rows[0]["n"])
			details["benchmark_results"] = &duckDBTableDetail{
				notes: []string{core.Sprintf("%d categories", n)},
			}
		}
	}

	return details
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
