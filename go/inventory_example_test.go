package store

import core "github.com/dappcore/go"

func ExamplePrintDuckDBInventory() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	buffer := core.NewBuffer()
	result := PrintDuckDBInventory(database, buffer)
	exampleRequireOK(result)
	core.Println(buffer.Len())
}
