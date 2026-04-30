package store

import core "dappco.re/go"

func ExampleStore_CommitToJournal() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, map[string]string{"kind": "example"})
	exampleRequireOK(result)
}

func ExampleStore_QueryJournal() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	exampleRequireOK(storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, map[string]string{"kind": "example"}))
	result := storeInstance.QueryJournal("SELECT measurement FROM journal_entries")
	exampleRequireOK(result)
	core.Println(result.Value)
}
