package store

import core "dappco.re/go"

func ExampleWorkspace_Name() {
	_, workspace := exampleWorkspace()
	defer workspace.Discard()
	core.Println(workspace.Name())
}

func ExampleWorkspace_DatabasePath() {
	_, workspace := exampleWorkspace()
	defer workspace.Discard()
	core.Println(workspace.DatabasePath())
}

func ExampleWorkspace_Close() {
	_, workspace := exampleWorkspace()
	result := workspace.Close()
	exampleRequireOK(result)
}

func ExampleStore_NewWorkspace() {
	storeInstance := exampleOpenConfiguredStore()
	defer exampleCloseStore(storeInstance)
	workspace, result := storeInstance.NewWorkspace("example-workspace")
	exampleRequireOK(result)
	defer workspace.Discard()
	core.Println(workspace.Name())
}

func ExampleStore_RecoverOrphans() {
	storeInstance := exampleOpenConfiguredStore()
	defer exampleCloseStore(storeInstance)
	workspaces := storeInstance.RecoverOrphans(".core/example-state")
	core.Println(len(workspaces))
}

func ExampleWorkspace_Put() {
	_, workspace := exampleWorkspace()
	defer workspace.Discard()
	result := workspace.Put("note", map[string]any{"value": "blue"})
	exampleRequireOK(result)
}

func ExampleWorkspace_Count() {
	_, workspace := exampleWorkspace()
	defer workspace.Discard()
	exampleRequireOK(workspace.Put("note", map[string]any{"value": "blue"}))
	count, result := workspace.Count()
	exampleRequireOK(result)
	core.Println(count)
}

func ExampleWorkspace_Aggregate() {
	_, workspace := exampleWorkspace()
	defer workspace.Discard()
	exampleRequireOK(workspace.Put("note", map[string]any{"value": "blue"}))
	summary := workspace.Aggregate()
	core.Println(summary)
}

func ExampleWorkspace_Commit() {
	_, workspace := exampleWorkspace()
	exampleRequireOK(workspace.Put("note", map[string]any{"value": "blue"}))
	result := workspace.Commit()
	exampleRequireOK(result)
	core.Println(result.Value)
}

func ExampleWorkspace_Discard() {
	_, workspace := exampleWorkspace()
	workspace.Discard()
}

func ExampleWorkspace_Query() {
	_, workspace := exampleWorkspace()
	defer workspace.Discard()
	exampleRequireOK(workspace.Put("note", map[string]any{"value": "blue"}))
	result := workspace.Query("SELECT entry_kind FROM workspace_entries")
	exampleRequireOK(result)
	core.Println(result.Value)
}
