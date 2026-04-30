package store

import core "dappco.re/go"

func ExampleImportAll() {
	database, result := OpenDuckDBReadWrite("import.duckdb")
	exampleRequireOK(result)
	defer exampleRequireOK(database.Close())
	buffer := core.NewBuffer()
	result = ImportAll(database, ImportConfig{DataDir: "data", SkipM3: true}, buffer)
	core.Println(result.OK)
}
