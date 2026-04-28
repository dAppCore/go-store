package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestInventoryV090_PrintDuckDBInventory_Good(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "DuckDB Inventory")
}

func TestInventoryV090_PrintDuckDBInventory_Bad(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Close())
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "TOTAL")
}

func TestInventoryV090_PrintDuckDBInventory_Ugly(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.EnsureScoringTables())
	output := NewBuffer()
	err := store.PrintDuckDBInventory(database, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "scoring_results")
}
