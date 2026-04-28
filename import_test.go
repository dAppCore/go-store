package store

import (
	"testing"

	core "dappco.re/go"
)

type importSessionStub struct {
	inserts int
}

func (session *importSessionStub) exec(string, ...any) error {
	session.inserts++
	return nil
}

func (session *importSessionStub) queryRowScan(string, any, ...any) error {
	return nil
}

func TestImport_ImportTrainingFile_Bad_MalformedJSONL(t *testing.T) {
	path := testPath(t, "training.jsonl")
	requireCoreWriteBytes(t, path, []byte("{\"messages\":[]}\n{broken\n"))
	session := &importSessionStub{}

	count, err := importTrainingFile(session, path, "training", "train")

	assertError(t, err)
	assertContainsString(t, err.Error(), "line 2")
	assertEqual(t, 1, count)
	assertEqual(t, 1, session.inserts)
}

func TestImport_ImportBenchmarkFile_Bad_MalformedJSONL(t *testing.T) {
	path := testPath(t, "benchmark.jsonl")
	requireCoreWriteBytes(t, path, []byte("{\"id\":\"row-1\"}\n{broken\n"))
	session := &importSessionStub{}

	count, err := importBenchmarkFile(session, path, "benchmark")

	assertError(t, err)
	assertContainsString(t, err.Error(), "line 2")
	assertEqual(t, 1, count)
	assertEqual(t, 1, session.inserts)
}

func TestImport_ImportBenchmarkQuestions_Bad_MalformedJSONL(t *testing.T) {
	path := testPath(t, "questions.jsonl")
	requireCoreWriteBytes(t, path, []byte("{\"id\":\"q-1\"}\n{broken\n"))
	session := &importSessionStub{}

	count, err := importBenchmarkQuestions(session, path, "truthfulqa")

	assertError(t, err)
	assertContainsString(t, err.Error(), "line 2")
	assertEqual(t, 1, count)
	assertEqual(t, 1, session.inserts)
}

func TestImport_ImportSeeds_Bad_WalkFailure(t *testing.T) {
	session := &importSessionStub{}

	count, err := importSeeds(session, core.JoinPath(t.TempDir(), "missing-seeds"))

	assertError(t, err)
	assertContainsString(t, err.Error(), "store.walkDir")
	assertEqual(t, 0, count)
	assertEqual(t, 0, session.inserts)
}
