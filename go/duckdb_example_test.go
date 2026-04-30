package store

import core "dappco.re/go"

func exampleDuckDB() *DuckDB {
	database, result := OpenDuckDBReadWrite("example.duckdb")
	exampleRequireOK(result)
	return database
}

func ExampleOpenDuckDB() {
	database, result := OpenDuckDB("example.duckdb")
	if result.OK {
		defer exampleRequireOK(database.Close())
	}
	core.Println(result.OK)
}

func ExampleOpenDuckDBReadWrite() {
	database, result := OpenDuckDBReadWrite("example.duckdb")
	exampleRequireOK(result)
	defer exampleRequireOK(database.Close())
	core.Println(database.Path())
}

func ExampleDuckDB_Close() {
	database := exampleDuckDB()
	result := database.Close()
	exampleRequireOK(result)
}

func ExampleDuckDB_Path() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	core.Println(database.Path())
}

func ExampleDuckDB_Conn() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	core.Println(database.Conn() != nil)
}

func ExampleDuckDB_Exec() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	result := database.Exec("CREATE TABLE example_items (id INTEGER)")
	exampleRequireOK(result)
}

func ExampleDuckDB_QueryRowScan() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	var value int
	result := database.QueryRowScan("SELECT 1", &value)
	exampleRequireOK(result)
	core.Println(value)
}

func ExampleDuckDB_QueryGoldenSet() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	rows, result := database.QueryGoldenSet(10)
	exampleRequireOK(result)
	core.Println(len(rows))
}

func ExampleDuckDB_CountGoldenSet() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	count, result := database.CountGoldenSet()
	exampleRequireOK(result)
	core.Println(count)
}

func ExampleDuckDB_QueryExpansionPrompts() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	rows, result := database.QueryExpansionPrompts("pending", 10)
	exampleRequireOK(result)
	core.Println(len(rows))
}

func ExampleDuckDB_CountExpansionPrompts() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	total, pending, result := database.CountExpansionPrompts()
	exampleRequireOK(result)
	core.Println(total, pending)
}

func ExampleDuckDB_UpdateExpansionStatus() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	result := database.UpdateExpansionStatus(7, "done")
	exampleRequireOK(result)
}

func ExampleDuckDB_QueryRows() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	rows, result := database.QueryRows("SELECT 1 AS value")
	exampleRequireOK(result)
	core.Println(rows)
}

func ExampleDuckDB_EnsureScoringTables() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	result := database.EnsureScoringTables()
	exampleRequireOK(result)
}

func ExampleDuckDB_WriteScoringResult() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	exampleRequireOK(database.EnsureScoringTables())
	result := database.WriteScoringResult("model", "prompt-1", "suite", "helpfulness", 0.9)
	exampleRequireOK(result)
}

func ExampleDuckDB_TableCounts() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	counts, result := database.TableCounts()
	exampleRequireOK(result)
	core.Println(counts)
}
