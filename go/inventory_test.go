package store_test

import (
	store "github.com/dappcore/go-store/go"
)

func TestInventory_PrintDuckDBInventory_Good(t *T) {
	if !store.DuckDBAvailable() {
		t.Skip("needs DuckDB: built without cgo, or with -tags noduckdb")
	}
	database := fixtureDuckDB(t)
	fixtureSeedDuckDB(t, database)
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "DuckDB Inventory")
}

func TestInventory_PrintDuckDBInventory_Bad(t *T) {
	if !store.DuckDBAvailable() {
		t.Skip("needs DuckDB: built without cgo, or with -tags noduckdb")
	}
	database := fixtureDuckDB(t)
	RequireNoError(t, database.Close())
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "TOTAL")
}

func TestInventory_PrintDuckDBInventory_Ugly(t *T) {
	if !store.DuckDBAvailable() {
		t.Skip("needs DuckDB: built without cgo, or with -tags noduckdb")
	}
	database := fixtureDuckDB(t)
	RequireNoError(t, database.EnsureScoringTables())
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "scoring_results")
}
