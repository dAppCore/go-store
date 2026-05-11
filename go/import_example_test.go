package store

import core "dappco.re/go"

func ExampleImportAll() {
	r := OpenDuckDBReadWrite("import.duckdb")
	exampleRequireOK(r)
	database := r.Value.(*DuckDB)
	defer exampleRequireOK(database.Close())
	buffer := core.NewBuffer()
	result := ImportAll(database, ImportConfig{DataDir: "data", SkipM3: true}, buffer)
	core.Println(result.OK)
}
