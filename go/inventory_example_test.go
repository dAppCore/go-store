package store

import core "dappco.re/go"

func ExamplePrintDuckDBInventory() {
	database := exampleDuckDB()
	defer exampleRequireOK(database.Close())
	buffer := core.NewBuffer()
	result := PrintDuckDBInventory(database, buffer)
	exampleRequireOK(result)
	core.Println(buffer.Len())
}
