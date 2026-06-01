package store

import (
	"testing"
)

// Real-behaviour coverage for the training-example assembly path:
// trainingExampleFromMessages (first user/assistant extraction + assistant
// counting), trainingExampleFromJSON (chat-message decode), insertTrainingExample
// against a live DuckDB transaction, and the pure floatOrZero map helper.

func newTrainingTx(t *testing.T) (duckDBImportTransaction, func()) {
	t.Helper()
	db := fixtureDuckDB(t)
	assertNoError(t, db.Exec(`CREATE TABLE training_examples (
		source VARCHAR, split VARCHAR, prompt VARCHAR, response VARCHAR,
		assistant_count INTEGER, messages VARCHAR, response_len INTEGER
	)`))
	tx, err := db.Conn().Begin()
	assertNoError(t, err)
	return duckDBImportTransaction{transaction: tx}, func() { _ = tx.Rollback() }
}

// ---------------------------------------------------------------------------
// trainingExampleFromMessages
// ---------------------------------------------------------------------------

func TestTrainingExample_FromMessages_Good_FirstUserAndAssistant(t *testing.T) {
	example := trainingExampleFromMessages([]ChatMessage{
		{Role: "system", Content: "be helpful"},
		{Role: "user", Content: "first question"},
		{Role: "assistant", Content: "first answer"},
		{Role: "user", Content: "second question"},
		{Role: "assistant", Content: "second answer"},
	})
	assertEqual(t, "first question", example.prompt)
	assertEqual(t, "first answer", example.response)
	assertEqual(t, 2, example.assistantCount)
}

func TestTrainingExample_FromMessages_Bad_NoAssistantTurns(t *testing.T) {
	example := trainingExampleFromMessages([]ChatMessage{
		{Role: "user", Content: "only a question"},
	})
	assertEqual(t, "only a question", example.prompt)
	assertEqual(t, "", example.response)
	assertEqual(t, 0, example.assistantCount)
}

func TestTrainingExample_FromMessages_Ugly_EmptyConversation(t *testing.T) {
	example := trainingExampleFromMessages(nil)
	assertEqual(t, "", example.prompt)
	assertEqual(t, "", example.response)
	assertEqual(t, 0, example.assistantCount)
}

// ---------------------------------------------------------------------------
// trainingExampleFromJSON
// ---------------------------------------------------------------------------

func TestTrainingExample_FromJSON_Good_DecodesMessages(t *testing.T) {
	example, r := trainingExampleFromJSON([]byte(`{"messages":[{"role":"user","content":"q"},{"role":"assistant","content":"a"}]}`))
	assertNoError(t, r)
	assertEqual(t, "q", example.prompt)
	assertEqual(t, "a", example.response)
	assertEqual(t, 1, example.assistantCount)
}

func TestTrainingExample_FromJSON_Bad_MalformedJSON(t *testing.T) {
	_, r := trainingExampleFromJSON([]byte(`{not json`))
	assertError(t, r)
}

// ---------------------------------------------------------------------------
// insertTrainingExample
// ---------------------------------------------------------------------------

func TestTrainingExample_InsertTrainingExample_Good_PersistsRow(t *testing.T) {
	session, done := newTrainingTx(t)
	defer done()
	example := trainingExampleFromMessages([]ChatMessage{
		{Role: "user", Content: "q"},
		{Role: "assistant", Content: "answer"},
	})
	assertNoError(t, session.exec(`SELECT 1`)) // sanity that the tx is live
	assertNoError(t, insertTrainingExample(session, "corpus", "train", example))

	var count, responseLen int
	assertNoError(t, session.queryRowScan("SELECT COUNT(*) FROM training_examples", &count))
	assertEqual(t, 1, count)
	assertNoError(t, session.queryRowScan("SELECT response_len FROM training_examples LIMIT 1", &responseLen))
	assertEqual(t, len("answer"), responseLen)
}

// ---------------------------------------------------------------------------
// floatOrZero — pure map helper
// ---------------------------------------------------------------------------

func TestTrainingExample_FloatOrZero_Good_PresentFloat(t *testing.T) {
	assertEqual(t, 1.5, floatOrZero(map[string]any{"gen_time": 1.5}, "gen_time"))
}

func TestTrainingExample_FloatOrZero_Bad_MissingKeyIsZero(t *testing.T) {
	assertEqual(t, float64(0), floatOrZero(map[string]any{}, "gen_time"))
}

func TestTrainingExample_FloatOrZero_Ugly_NonFloatValueIsZero(t *testing.T) {
	assertEqual(t, float64(0), floatOrZero(map[string]any{"gen_time": "1.5"}, "gen_time"))
	assertEqual(t, float64(0), floatOrZero(map[string]any{"gen_time": int64(2)}, "gen_time"))
}

// strOrEmpty round-trip on a non-string value for completeness.
func TestTrainingExample_StrOrEmpty_Ugly_NonStringStringified(t *testing.T) {
	assertEqual(t, "7", strOrEmpty(map[string]any{"k": int64(7)}, "k"))
	assertEqual(t, "", strOrEmpty(map[string]any{}, "k"))
}
