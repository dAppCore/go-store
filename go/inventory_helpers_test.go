package store

import (
	"testing"
)

// Real-behaviour coverage for the DuckDB inventory helpers. The pure value
// converters (duckDBToInt, duckDBStrVal) and the per-table detail builders
// (promptsDetail/appendDistinctPromptCount, geminiResponsesDetail,
// benchmarkResultsDetail, trainingExamplesDetail) were 0% or barely covered.
// Detail builders run against a live DuckDB fixture with the relevant tables.

// ---------------------------------------------------------------------------
// duckDBToInt — type-switch over DuckDB scalar shapes
// ---------------------------------------------------------------------------

func TestInventoryHelpers_DuckDBToInt_Good_SupportedTypes(t *testing.T) {
	assertEqual(t, 5, duckDBToInt(int64(5)))
	assertEqual(t, 5, duckDBToInt(int32(5)))
	assertEqual(t, 5, duckDBToInt(float64(5.9))) // truncates toward zero
}

func TestInventoryHelpers_DuckDBToInt_Bad_UnsupportedTypeIsZero(t *testing.T) {
	assertEqual(t, 0, duckDBToInt("not a number"))
	assertEqual(t, 0, duckDBToInt(nil))
}

func TestInventoryHelpers_DuckDBToInt_Ugly_NegativeAndZero(t *testing.T) {
	assertEqual(t, -7, duckDBToInt(int64(-7)))
	assertEqual(t, 0, duckDBToInt(int32(0)))
}

// ---------------------------------------------------------------------------
// duckDBStrVal — string extraction with missing-key fallback
// ---------------------------------------------------------------------------

func TestInventoryHelpers_DuckDBStrVal_Good_PresentKey(t *testing.T) {
	row := map[string]any{"source_model": "gemini-3.5"}
	assertEqual(t, "gemini-3.5", duckDBStrVal(row, "source_model"))
}

func TestInventoryHelpers_DuckDBStrVal_Bad_MissingKeyIsEmpty(t *testing.T) {
	row := map[string]any{"other": 1}
	assertEqual(t, "", duckDBStrVal(row, "source_model"))
}

func TestInventoryHelpers_DuckDBStrVal_Ugly_NonStringValueStringified(t *testing.T) {
	row := map[string]any{"n": int64(42)}
	assertEqual(t, "42", duckDBStrVal(row, "n"))
}

// ---------------------------------------------------------------------------
// promptsDetail / appendDistinctPromptCount — distinct domain/voice counts
// ---------------------------------------------------------------------------

func TestInventoryHelpers_PromptsDetail_Good_CountsDistinctDomainsAndVoices(t *testing.T) {
	db := fixtureDuckDB(t)
	assertNoError(t, db.Exec(`CREATE TABLE prompts (domain VARCHAR, voice VARCHAR)`))
	assertNoError(t, db.Exec(`INSERT INTO prompts VALUES ('ethics','plain'),('ethics','formal'),('logic','plain')`))

	detail := promptsDetail(db, map[string]int{"prompts": 3})
	assertNotNil(t, detail)
	assertEqual(t, 2, len(detail.notes))
	assertContainsString(t, detail.notes[0], "2 domains")
	assertContainsString(t, detail.notes[1], "2 voices")
}

func TestInventoryHelpers_PromptsDetail_Bad_AbsentTableKeyReturnsNil(t *testing.T) {
	db := fixtureDuckDB(t)
	assertNil(t, promptsDetail(db, map[string]int{}))
}

func TestInventoryHelpers_AppendDistinctPromptCount_Ugly_MissingTableLeavesNotesEmpty(t *testing.T) {
	db := fixtureDuckDB(t)
	detail := &duckDBTableDetail{}
	// No prompts table exists: the query fails, so no note is appended.
	appendDistinctPromptCount(db, detail, "domain", "domains")
	assertEqual(t, 0, len(detail.notes))
}

// ---------------------------------------------------------------------------
// geminiResponsesDetail — per-model breakdown
// ---------------------------------------------------------------------------

func TestInventoryHelpers_GeminiResponsesDetail_Good_PerModelCounts(t *testing.T) {
	db := fixtureDuckDB(t)
	assertNoError(t, db.Exec(`CREATE TABLE gemini_responses (source_model VARCHAR)`))
	assertNoError(t, db.Exec(`INSERT INTO gemini_responses VALUES ('a'),('a'),('b')`))

	detail := geminiResponsesDetail(db, map[string]int{"gemini_responses": 3})
	assertNotNil(t, detail)
	assertContainsElement(t, detail.notes, "a:2")
	assertContainsElement(t, detail.notes, "b:1")
}

func TestInventoryHelpers_GeminiResponsesDetail_Bad_AbsentKeyReturnsNil(t *testing.T) {
	db := fixtureDuckDB(t)
	assertNil(t, geminiResponsesDetail(db, map[string]int{}))
}

// ---------------------------------------------------------------------------
// benchmarkResultsDetail / trainingExamplesDetail — distinct-category counts
// ---------------------------------------------------------------------------

func TestInventoryHelpers_BenchmarkResultsDetail_Good_DistinctSources(t *testing.T) {
	db := fixtureDuckDB(t)
	assertNoError(t, db.Exec(`CREATE TABLE benchmark_results (source VARCHAR)`))
	assertNoError(t, db.Exec(`INSERT INTO benchmark_results VALUES ('mmlu'),('mmlu'),('gsm8k')`))

	detail := benchmarkResultsDetail(db, map[string]int{"benchmark_results": 3})
	assertNotNil(t, detail)
	assertContainsString(t, detail.notes[0], "2 categories")
}

func TestInventoryHelpers_TrainingExamplesDetail_Good_DistinctSources(t *testing.T) {
	db := fixtureDuckDB(t)
	assertNoError(t, db.Exec(`CREATE TABLE training_examples (source VARCHAR)`))
	assertNoError(t, db.Exec(`INSERT INTO training_examples VALUES ('corpus-a'),('corpus-b'),('corpus-b')`))

	detail := trainingExamplesDetail(db, map[string]int{"training_examples": 3})
	assertNotNil(t, detail)
	assertContainsString(t, detail.notes[0], "2 sources")
}
