package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestImportV090_ImportAll_Good(t *T) {
	database := ax7DuckDB(t)
	output := NewBuffer()
	err := store.ImportAll(database, store.ImportConfig{DataDir: t.TempDir(), SkipM3: true}, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "LEM Database Import Complete")
}

func TestImportV090_ImportAll_Bad(t *T) {
	output := NewBuffer()
	err := store.ImportAll(nil, store.ImportConfig{DataDir: t.TempDir(), SkipM3: true}, output)
	AssertError(t, err)
	AssertEqual(t, "", output.String())
}

func TestImportV090_ImportAll_Ugly(t *T) {
	database := ax7DuckDB(t)
	output := NewBuffer()
	err := store.ImportAll(database, store.ImportConfig{DataDir: t.TempDir(), SkipM3: false, Scp: func(string, string) error { return NewError("offline") }}, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "seeds")
}
