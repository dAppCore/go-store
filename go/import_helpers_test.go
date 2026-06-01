package store

import (
	"testing"

	core "dappco.re/go"
)

// Real-behaviour coverage for the import-path helpers: the pure shape helpers
// (seedListFromRaw, seedPrompt, trainingSplit, escapeSQLPath) and the
// DuckDB-backed seed ingestion (insertSeed, importSeedFile, readSeedList,
// queryRowScan via duckDBImportTransaction). These were 0% covered.

// newImportTx wraps a live DuckDB transaction in the duckDBImportTransaction
// used throughout the import path, plus the seeds table the inserts target.
func newImportTx(t *testing.T) (duckDBImportTransaction, func()) {
	t.Helper()
	db := fixtureDuckDB(t)
	assertNoError(t, db.Exec(`CREATE TABLE seeds (rel VARCHAR, region VARCHAR, seed_id VARCHAR, domain VARCHAR, prompt VARCHAR)`))
	tx, err := db.Conn().Begin()
	assertNoError(t, err)
	session := duckDBImportTransaction{transaction: tx}
	return session, func() { _ = tx.Rollback() }
}

func seedRowCount(t *testing.T, session duckDBImportTransaction) int {
	t.Helper()
	var n int
	assertNoError(t, session.queryRowScan("SELECT COUNT(*) FROM seeds", &n))
	return n
}

// ---------------------------------------------------------------------------
// seedListFromRaw — unwraps the three accepted JSON shapes
// ---------------------------------------------------------------------------

func TestImportHelpers_SeedListFromRaw_Good_TopLevelArray(t *testing.T) {
	list := seedListFromRaw([]any{"a", "b"})
	assertEqual(t, 2, len(list))
}

func TestImportHelpers_SeedListFromRaw_Good_PromptsAndSeedsKeys(t *testing.T) {
	prompts := seedListFromRaw(map[string]any{"prompts": []any{"x"}})
	assertEqual(t, 1, len(prompts))
	seeds := seedListFromRaw(map[string]any{"seeds": []any{"y", "z"}})
	assertEqual(t, 2, len(seeds))
}

func TestImportHelpers_SeedListFromRaw_Bad_UnrecognisedShapeIsNil(t *testing.T) {
	assertNil(t, seedListFromRaw(map[string]any{"other": 1}))
	assertNil(t, seedListFromRaw(42))
}

// ---------------------------------------------------------------------------
// seedPrompt — first non-empty of prompt/text/question
// ---------------------------------------------------------------------------

func TestImportHelpers_SeedPrompt_Good_PrefersPromptThenTextThenQuestion(t *testing.T) {
	assertEqual(t, "p", seedPrompt(map[string]any{"prompt": "p", "text": "t"}))
	assertEqual(t, "t", seedPrompt(map[string]any{"text": "t", "question": "q"}))
	assertEqual(t, "q", seedPrompt(map[string]any{"question": "q"}))
}

func TestImportHelpers_SeedPrompt_Bad_NoRecognisedKeyIsEmpty(t *testing.T) {
	assertEqual(t, "", seedPrompt(map[string]any{"body": "b"}))
}

// ---------------------------------------------------------------------------
// trainingSplit — path-keyword classification
// ---------------------------------------------------------------------------

func TestImportHelpers_TrainingSplit_Good_KeywordRouting(t *testing.T) {
	assertEqual(t, "valid", trainingSplit("data/valid/shard.jsonl"))
	assertEqual(t, "test", trainingSplit("data/test/shard.jsonl"))
	assertEqual(t, "train", trainingSplit("data/anything/shard.jsonl"))
}

// ---------------------------------------------------------------------------
// escapeSQLPath — single-quote doubling for DuckDB string literals
// ---------------------------------------------------------------------------

func TestImportHelpers_EscapeSQLPath_Ugly_DoublesSingleQuotes(t *testing.T) {
	assertEqual(t, "/tmp/plain.csv", escapeSQLPath("/tmp/plain.csv"))
	assertEqual(t, "/tmp/o''brien.csv", escapeSQLPath("/tmp/o'brien.csv"))
	assertEqual(t, "''''", escapeSQLPath("''"))
}

// ---------------------------------------------------------------------------
// insertSeed — map / string / unsupported seed shapes
// ---------------------------------------------------------------------------

func TestImportHelpers_InsertSeed_Good_MapSeed(t *testing.T) {
	session, done := newImportTx(t)
	defer done()
	inserted, r := insertSeed(session, "western.json", "western", map[string]any{
		"seed_id": "s1", "domain": "ethics", "prompt": "be kind",
	})
	assertNoError(t, r)
	assertTrue(t, inserted)
	assertEqual(t, 1, seedRowCount(t, session))
}

func TestImportHelpers_InsertSeed_Good_StringSeed(t *testing.T) {
	session, done := newImportTx(t)
	defer done()
	inserted, r := insertSeed(session, "rel.json", "region", "a bare prompt")
	assertNoError(t, r)
	assertTrue(t, inserted)
	assertEqual(t, 1, seedRowCount(t, session))
}

func TestImportHelpers_InsertSeed_Ugly_UnsupportedSeedSkipped(t *testing.T) {
	session, done := newImportTx(t)
	defer done()
	inserted, r := insertSeed(session, "rel.json", "region", 12345)
	assertNoError(t, r)
	assertFalse(t, inserted)
	assertEqual(t, 0, seedRowCount(t, session))
}

// ---------------------------------------------------------------------------
// importSeedFile + readSeedList — read JSON from disk and ingest
// ---------------------------------------------------------------------------

func TestImportHelpers_ImportSeedFile_Good_IngestsPromptsArray(t *testing.T) {
	session, done := newImportTx(t)
	defer done()
	dir := t.TempDir()
	path := core.Path(dir, "western.json")
	assertNoError(t, localFs.Write(path, `{"prompts":[{"seed_id":"s1","domain":"ethics","prompt":"be kind"},"plain string"]}`))

	count, r := importSeedFile(session, dir, path)
	assertNoError(t, r)
	assertEqual(t, 2, count)
	assertEqual(t, 2, seedRowCount(t, session))
}

func TestImportHelpers_ImportSeedFile_Bad_NonJSONExtensionSkipped(t *testing.T) {
	session, done := newImportTx(t)
	defer done()
	dir := t.TempDir()
	path := core.Path(dir, "notes.txt")
	assertNoError(t, localFs.Write(path, "irrelevant"))

	count, r := importSeedFile(session, dir, path)
	assertNoError(t, r)
	assertEqual(t, 0, count)
}

func TestImportHelpers_ReadSeedList_Bad_MissingFile(t *testing.T) {
	_, r := readSeedList(core.Path(t.TempDir(), "absent.json"), "absent.json")
	assertError(t, r)
	assertContainsString(t, r.Error(), "read seed file")
}

func TestImportHelpers_ReadSeedList_Bad_MalformedJSON(t *testing.T) {
	dir := t.TempDir()
	path := core.Path(dir, "bad.json")
	assertNoError(t, localFs.Write(path, "{not json"))
	_, r := readSeedList(path, "bad.json")
	assertError(t, r)
	assertContainsString(t, r.Error(), "parse seed file")
}

// ---------------------------------------------------------------------------
// queryRowScan — error path on a bad query
// ---------------------------------------------------------------------------

func TestImportHelpers_QueryRowScan_Bad_InvalidQuery(t *testing.T) {
	session, done := newImportTx(t)
	defer done()
	var dest int
	r := session.queryRowScan("SELECT * FROM nonexistent_table", &dest)
	assertError(t, r)
}

// ---------------------------------------------------------------------------
// walkDir — recursive file visiting
// ---------------------------------------------------------------------------

func TestImportHelpers_WalkDir_Good_VisitsAllRegularFiles(t *testing.T) {
	dir := t.TempDir()
	assertNoError(t, localFs.EnsureDir(core.Path(dir, "sub")))
	assertNoError(t, localFs.Write(core.Path(dir, "a.txt"), "a"))
	assertNoError(t, localFs.Write(core.Path(dir, "sub", "b.txt"), "b"))

	var visited []string
	r := walkDir(dir, func(path string) core.Result {
		visited = append(visited, path)
		return core.Ok(nil)
	})
	assertNoError(t, r)
	assertEqual(t, 2, len(visited))
}

func TestImportHelpers_WalkDir_Bad_MissingRoot(t *testing.T) {
	r := walkDir(core.Path(t.TempDir(), "no-such-root"), func(string) core.Result { return core.Ok(nil) })
	assertError(t, r)
}

func TestImportHelpers_WalkDir_Ugly_PropagatesCallbackError(t *testing.T) {
	dir := t.TempDir()
	assertNoError(t, localFs.Write(core.Path(dir, "a.txt"), "a"))
	r := walkDir(dir, func(string) core.Result {
		return core.Fail(core.E("test.callback", "boom", nil))
	})
	assertError(t, r)
	assertContainsString(t, r.Error(), "boom")
}
