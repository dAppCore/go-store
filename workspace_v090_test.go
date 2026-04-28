package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestWorkspaceV090_Store_NewWorkspace_Good(t *T) {
	storeInstance, _ := ax7ConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("ax7")
	RequireNoError(t, err)
	defer workspace.Discard()
	AssertEqual(t, "ax7", workspace.Name())
}

func TestWorkspaceV090_Store_NewWorkspace_Bad(t *T) {
	storeInstance, _ := ax7ConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("")
	AssertError(t, err)
	AssertNil(t, workspace)
}

func TestWorkspaceV090_Store_NewWorkspace_Ugly(t *T) {
	storeInstance, _ := ax7ConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("ax7-42")
	RequireNoError(t, err)
	defer workspace.Discard()
	AssertContains(t, workspace.DatabasePath(), "ax7-42")
}

func TestWorkspaceV090_Store_RecoverOrphans_Good(t *T) {
	storeInstance, stateDirectory := ax7ConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("orphan")
	RequireNoError(t, err)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	RequireNoError(t, workspace.Close())
	orphans := storeInstance.RecoverOrphans(stateDirectory)
	AssertLen(t, orphans, 1)
}

func TestWorkspaceV090_Store_RecoverOrphans_Bad(t *T) {
	var storeInstance *store.Store
	orphans := storeInstance.RecoverOrphans(t.TempDir())
	AssertNil(t, orphans)
}

func TestWorkspaceV090_Store_RecoverOrphans_Ugly(t *T) {
	storeInstance, stateDirectory := ax7ConfiguredStore(t)
	orphans := storeInstance.RecoverOrphans(stateDirectory)
	AssertEmpty(t, orphans)
}

func TestWorkspaceV090_Workspace_Name_Good(t *T) {
	_, workspace := ax7Workspace(t)
	name := workspace.Name()
	AssertEqual(t, "ax7-workspace", name)
}

func TestWorkspaceV090_Workspace_Name_Bad(t *T) {
	var workspace *store.Workspace
	name := workspace.Name()
	AssertEqual(t, "", name)
}

func TestWorkspaceV090_Workspace_Name_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Close())
	AssertEqual(t, "ax7-workspace", workspace.Name())
}

func TestWorkspaceV090_Workspace_DatabasePath_Good(t *T) {
	_, workspace := ax7Workspace(t)
	path := workspace.DatabasePath()
	AssertContains(t, path, "ax7-workspace")
}

func TestWorkspaceV090_Workspace_DatabasePath_Bad(t *T) {
	var workspace *store.Workspace
	path := workspace.DatabasePath()
	AssertEqual(t, "", path)
}

func TestWorkspaceV090_Workspace_DatabasePath_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Close())
	AssertContains(t, workspace.DatabasePath(), ".duckdb")
}

func TestWorkspaceV090_Workspace_Close_Good(t *T) {
	_, workspace := ax7Workspace(t)
	err := workspace.Close()
	AssertNoError(t, err)
	AssertNotEmpty(t, workspace.DatabasePath())
}

func TestWorkspaceV090_Workspace_Close_Bad(t *T) {
	var workspace *store.Workspace
	err := workspace.Close()
	AssertNoError(t, err)
}

func TestWorkspaceV090_Workspace_Close_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Close())
	err := workspace.Close()
	AssertNoError(t, err)
}

func TestWorkspaceV090_Workspace_Put_Good(t *T) {
	_, workspace := ax7Workspace(t)
	err := workspace.Put("entry", map[string]any{"name": "alice"})
	AssertNoError(t, err)
	AssertEqual(t, 1, len(workspace.Aggregate()))
}

func TestWorkspaceV090_Workspace_Put_Bad(t *T) {
	_, workspace := ax7Workspace(t)
	err := workspace.Put("", map[string]any{"name": "alice"})
	AssertError(t, err)
}

func TestWorkspaceV090_Workspace_Put_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	err := workspace.Put("entry", nil)
	AssertNoError(t, err)
	AssertEqual(t, 1, len(workspace.Aggregate()))
}

func TestWorkspaceV090_Workspace_Count_Good(t *T) {
	_, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	count, err := workspace.Count()
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestWorkspaceV090_Workspace_Count_Bad(t *T) {
	_, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Close())
	count, err := workspace.Count()
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestWorkspaceV090_Workspace_Count_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	count, err := workspace.Count()
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestWorkspaceV090_Workspace_Aggregate_Good(t *T) {
	_, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	summary := workspace.Aggregate()
	AssertEqual(t, "1", Sprint(summary["entry"]))
}

func TestWorkspaceV090_Workspace_Aggregate_Bad(t *T) {
	var workspace *store.Workspace
	summary := workspace.Aggregate()
	AssertEmpty(t, summary)
}

func TestWorkspaceV090_Workspace_Aggregate_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Close())
	summary := workspace.Aggregate()
	AssertEmpty(t, summary)
}

func TestWorkspaceV090_Workspace_Commit_Good(t *T) {
	storeInstance, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	result := workspace.Commit()
	AssertTrue(t, result.OK)
	AssertTrue(t, ax7MustGroupExists(t, storeInstance, "workspace:ax7-workspace"))
}

func TestWorkspaceV090_Workspace_Commit_Bad(t *T) {
	var workspace *store.Workspace
	result := workspace.Commit()
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "workspace")
}

func TestWorkspaceV090_Workspace_Commit_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	result := workspace.Commit()
	AssertTrue(t, result.OK)
	AssertEmpty(t, result.Value.(map[string]any))
}

func TestWorkspaceV090_Workspace_Discard_Good(t *T) {
	_, workspace := ax7Workspace(t)
	workspace.Discard()
	result := workspace.Query("SELECT 1")
	AssertFalse(t, result.OK)
}

func TestWorkspaceV090_Workspace_Discard_Bad(t *T) {
	var workspace *store.Workspace
	AssertNotPanics(t, func() { workspace.Discard() })
	AssertEqual(t, "", workspace.Name())
}

func TestWorkspaceV090_Workspace_Discard_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	workspace.Discard()
	AssertNotPanics(t, func() { workspace.Discard() })
}

func TestWorkspaceV090_Workspace_Query_Good(t *T) {
	_, workspace := ax7Workspace(t)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	result := workspace.Query("SELECT entry_kind FROM workspace_entries")
	AssertTrue(t, result.OK)
	AssertNotEmpty(t, result.Value)
}

func TestWorkspaceV090_Workspace_Query_Bad(t *T) {
	_, workspace := ax7Workspace(t)
	result := workspace.Query("SELECT * FROM missing_table")
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "query")
}

func TestWorkspaceV090_Workspace_Query_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	result := workspace.Query("SELECT COUNT(*) AS n FROM workspace_entries")
	AssertTrue(t, result.OK)
	AssertNotEmpty(t, result.Value)
}
