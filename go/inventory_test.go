package store_test

import (
	store "dappco.re/go/store"
)

func TestInventory_PrintDuckDBInventory_Good(t *T) {
	database := fixtureDuckDB(t)
	fixtureSeedDuckDB(t, database)
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "DuckDB Inventory")
}

func TestInventory_PrintDuckDBInventory_Bad(t *T) {
	database := fixtureDuckDB(t)
	RequireNoError(t, database.Close())
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "TOTAL")
}

func TestInventory_PrintDuckDBInventory_Ugly(t *T) {
	database := fixtureDuckDB(t)
	RequireNoError(t, database.EnsureScoringTables())
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "scoring_results")
}
