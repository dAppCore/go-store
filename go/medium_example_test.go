package store

import core "dappco.re/go"

func ExampleWithMedium() {
	medium := newFixtureMedium()
	storeInstance, result := New(":memory:", WithMedium(medium))
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.Medium() != nil)
}

func ExampleStore_Medium() {
	storeInstance, result := New(":memory:", WithMedium(newFixtureMedium()))
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	medium := storeInstance.Medium()
	core.Println(medium != nil)
}

func ExampleImport() {
	_, workspace := exampleWorkspace()
	defer workspace.Discard()
	medium := newFixtureMedium()
	exampleRequireOK(medium.Write("entries.jsonl", `{"kind":"note","value":"blue"}`))
	result := Import(workspace, medium, "entries.jsonl")
	exampleRequireOK(result)
}

func ExampleExport() {
	_, workspace := exampleWorkspace()
	defer workspace.Discard()
	exampleRequireOK(workspace.Put("note", map[string]any{"value": "blue"}))
	medium := newFixtureMedium()
	result := Export(workspace, medium, "summary.json")
	exampleRequireOK(result)
	content, readResult := medium.Read("summary.json")
	exampleRequireOK(readResult)
	core.Println(content)
}
