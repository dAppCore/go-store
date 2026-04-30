package store

import (
	"testing"
	"time"

	core "dappco.re/go"
)

func TestWorkspace_NewWorkspace_Good_CreatePutAggregateQuery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace(testScrollSession)
	assertNoError(t, err)
	defer workspace.Discard()

	assertEqual(t, workspaceFilePath(stateDirectory, testScrollSession), workspace.databasePath)
	assertTrue(t, testFilesystem().Exists(workspace.databasePath))

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": testActorCharlie}))

	assertEqual(t, map[string]any{"like": 2, "profile_match": 1}, workspace.Aggregate())

	rows := requireResultRows(
		t,
		workspace.Query("SELECT entry_kind, COUNT(*) AS entry_count FROM workspace_entries GROUP BY entry_kind ORDER BY entry_kind"),
	)
	assertLen(t, rows, 2)
	assertEqual(t, "like", rows[0]["entry_kind"])
	assertEqual(t, int64(2), rows[0]["entry_count"])
	assertEqual(t, "profile_match", rows[1]["entry_kind"])
	assertEqual(t, int64(1), rows[1]["entry_count"])
}

func TestWorkspace_DatabasePath_Good(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace(testScrollSession)
	assertNoError(t, err)
	defer workspace.Discard()

	assertEqual(t, workspaceFilePath(stateDirectory, testScrollSession), workspace.DatabasePath())
}

func TestWorkspace_Count_Good_Empty(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("count-empty")
	assertNoError(t, err)
	defer workspace.Discard()

	count, err := workspace.Count()
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestWorkspace_Count_Good_AfterPuts(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("count-puts")
	assertNoError(t, err)
	defer workspace.Discard()

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": testActorCharlie}))

	count, err := workspace.Count()
	assertNoError(t, err)
	assertEqual(t, 3, count)
}

func TestWorkspace_Count_Bad_ClosedWorkspace(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("count-closed")
	assertNoError(t, err)
	workspace.Discard()

	_, err = workspace.Count()
	assertError(t, err)
}

func TestWorkspace_Query_Good_RFCEntriesView(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace(testScrollSession)
	assertNoError(t, err)
	defer workspace.Discard()

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": testActorCharlie}))

	rows := requireResultRows(
		t,
		workspace.Query("SELECT kind, COUNT(*) AS entry_count FROM entries GROUP BY kind ORDER BY kind"),
	)
	assertLen(t, rows, 2)
	assertEqual(t, "like", rows[0]["kind"])
	assertEqual(t, int64(2), rows[0]["entry_count"])
	assertEqual(t, "profile_match", rows[1]["kind"])
	assertEqual(t, int64(1), rows[1]["entry_count"])
}

func TestWorkspace_Commit_Good_JournalAndSummary(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace(testScrollSession)
	assertNoError(t, err)

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": testActorCharlie}))

	result := workspace.Commit()
	assertTruef(t, result.OK, testWorkspaceCommitFailedFormat, result.Value)
	assertEqual(t, map[string]any{"like": 2, "profile_match": 1}, result.Value)
	assertFalse(t, testFilesystem().Exists(workspace.databasePath))

	summaryJSON, err := storeInstance.Get(workspaceSummaryGroup(testScrollSession), "summary")
	assertNoError(t, err)

	summary := make(map[string]any)
	summaryResult := core.JSONUnmarshalString(summaryJSON, &summary)
	assertTruef(t, summaryResult.OK, "summary unmarshal failed: %v", summaryResult.Value)
	assertEqual(t, float64(2), summary["like"])
	assertEqual(t, float64(1), summary["profile_match"])

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r._measurement == "scroll-session")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testScrollSession, rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, testUnexpectedFieldsTypeFormat, rows[0]["fields"])
	assertEqual(t, float64(2), fields["like"])
	assertEqual(t, float64(1), fields["profile_match"])

	tags, ok := rows[0]["tags"].(map[string]string)
	assertTruef(t, ok, testUnexpectedTagsTypeFormat, rows[0]["tags"])
	assertEqual(t, testScrollSession, tags["workspace"])
}

func TestWorkspace_Commit_Good_ResultCopiesAggregatedMap(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace(testScrollSession)
	assertNoError(t, err)

	aggregateSource := map[string]any{"like": 1}
	assertNoError(t, workspace.Put("like", aggregateSource))

	result := workspace.Commit()
	assertTruef(t, result.OK, testWorkspaceCommitFailedFormat, result.Value)

	aggregateSource["like"] = 99

	value, ok := result.Value.(map[string]any)
	assertTruef(t, ok, "unexpected result type: %T", result.Value)
	assertEqual(t, 1, value["like"])
}

func TestWorkspace_Commit_Good_EmitsSummaryEvent(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	events := storeInstance.Watch(workspaceSummaryGroup(testScrollSession))
	defer storeInstance.Unwatch(workspaceSummaryGroup(testScrollSession), events)

	workspace, err := storeInstance.NewWorkspace(testScrollSession)
	assertNoError(t, err)

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": testActorCharlie}))

	result := workspace.Commit()
	assertTruef(t, result.OK, testWorkspaceCommitFailedFormat, result.Value)

	select {
	case event := <-events:
		assertEqual(t, EventSet, event.Type)
		assertEqual(t, workspaceSummaryGroup(testScrollSession), event.Group)
		assertEqual(t, "summary", event.Key)
		assertFalse(t, event.Timestamp.IsZero())

		summary := make(map[string]any)
		summaryResult := core.JSONUnmarshalString(event.Value, &summary)
		assertTruef(t, summaryResult.OK, "summary event unmarshal failed: %v", summaryResult.Value)
		assertEqual(t, float64(1), summary["like"])
		assertEqual(t, float64(1), summary["profile_match"])
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for workspace summary event")
	}
}

func TestWorkspace_RecoverOrphans_Good_SkipsAlreadyCommittedWorkspaceFile(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("committed-leftover")
	assertNoError(t, err)

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	fields, err := workspace.aggregateFields()
	assertNoError(t, err)
	assertNoError(t, storeInstance.commitWorkspaceAggregate(workspace.Name(), fields))
	assertNoError(t, workspace.closeWithoutRemovingFiles())
	assertTrue(t, testFilesystem().Exists(workspace.databasePath))

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 0)
	assertFalse(t, testFilesystem().Exists(workspace.databasePath))
}

func TestWorkspace_Discard_Good_Idempotent(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("discard-session")
	assertNoError(t, err)

	workspace.Discard()
	workspace.Discard()

	assertFalse(t, testFilesystem().Exists(workspace.databasePath))
}

func TestWorkspace_Close_Good_PreservesFileForRecovery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("close-session")
	assertNoError(t, err)

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Close())

	assertTrue(t, testFilesystem().Exists(workspace.databasePath))

	err = workspace.Put("like", map[string]any{"user": "@bob"})
	assertError(t, err)

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, "close-session", orphans[0].Name())
	assertEqual(t, map[string]any{"like": 1}, orphans[0].Aggregate())
	orphans[0].Discard()
	assertFalse(t, testFilesystem().Exists(workspace.databasePath))
}

func TestWorkspace_Close_Good_ClosesDatabaseWithoutFilesystem(t *testing.T) {
	databasePath := testPath(t, "workspace-no-filesystem.duckdb")

	database, err := openWorkspaceDatabase(databasePath)
	assertNoError(t, err)

	workspace := &Workspace{
		name:         "partial-workspace",
		db:           database,
		databasePath: databasePath,
	}

	assertNoError(t, workspace.Close())

	_, execErr := database.Exec("SELECT 1")
	assertError(t, execErr)
	assertContainsString(t, execErr.Error(), "closed")

	assertTrue(t, testFilesystem().Exists(databasePath))
	for _, path := range workspaceDatabaseFilePaths(databasePath) {
		_ = testFilesystem().Delete(path)
	}
}

func TestWorkspace_RecoverOrphans_Good(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace(testOrphanSession)
	assertNoError(t, err)
	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.db.Close())

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, testOrphanSession, orphans[0].Name())
	assertEqual(t, map[string]any{"like": 1}, orphans[0].Aggregate())

	orphans[0].Discard()
	assertFalse(t, testFilesystem().Exists(workspaceFilePath(stateDirectory, testOrphanSession)))
}

func TestWorkspace_New_Good_LeavesOrphanedWorkspacesForRecovery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, testOrphanSession)
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	_, sqlErr := orphanDatabase.Exec(
		testSQLInsertIntoPrefix+workspaceEntriesTableName+testWorkspaceEntryInsertSuffix,
		"like",
		`{"user":"@alice"}`,
		time.Now().UnixMilli(),
	)
	assertNoError(t, sqlErr)
	assertNoError(t, orphanDatabase.Close())
	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, testOrphanSession, orphans[0].Name())
	orphans[0].Discard()
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath+"-wal"))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath+"-shm"))
}

func TestWorkspace_New_Good_CachesOrphansDuringConstruction(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, testOrphanSession)
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	_, sqlErr := orphanDatabase.Exec(
		testSQLInsertIntoPrefix+workspaceEntriesTableName+testWorkspaceEntryInsertSuffix,
		"like",
		`{"user":"@alice"}`,
		time.Now().UnixMilli(),
	)
	assertNoError(t, sqlErr)
	assertNoError(t, orphanDatabase.Close())
	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	requireCoreOK(t, testFilesystem().DeleteAll(stateDirectory))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, testOrphanSession, orphans[0].Name())
	assertEqual(t, map[string]any{"like": 1}, orphans[0].Aggregate())
	orphans[0].Discard()
}

func TestWorkspace_NewConfigured_Good_CachesOrphansFromConfiguredStateDirectory(t *testing.T) {
	stateDirectory := testPath(t, "configured-state")
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, testOrphanSession)
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	_, sqlErr := orphanDatabase.Exec(
		testSQLInsertIntoPrefix+workspaceEntriesTableName+testWorkspaceEntryInsertSuffix,
		"like",
		`{"user":"@alice"}`,
		time.Now().UnixMilli(),
	)
	assertNoError(t, sqlErr)
	assertNoError(t, orphanDatabase.Close())

	storeInstance, err := NewConfigured(StoreConfig{
		DatabasePath:            testMemoryDatabasePath,
		WorkspaceStateDirectory: stateDirectory,
	})
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	requireCoreOK(t, testFilesystem().DeleteAll(stateDirectory))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans("")
	assertLen(t, orphans, 1)
	assertEqual(t, testOrphanSession, orphans[0].Name())
	assertEqual(t, map[string]any{"like": 1}, orphans[0].Aggregate())
	orphans[0].Discard()
}

func TestWorkspace_RecoverOrphans_Good_TrailingSlashUsesCache(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, testOrphanSession)
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	assertNoError(t, orphanDatabase.Close())
	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	requireCoreOK(t, testFilesystem().DeleteAll(stateDirectory))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans(stateDirectory + "/")
	assertLen(t, orphans, 1)
	assertEqual(t, testOrphanSession, orphans[0].Name())
	orphans[0].Discard()
}

func TestWorkspace_Close_Good_PreservesOrphansForRecovery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, testOrphanSession)
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	assertNoError(t, orphanDatabase.Close())
	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)

	assertNoError(t, storeInstance.Close())

	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	recoveryStore, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = recoveryStore.Close() }()

	orphans := recoveryStore.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, testOrphanSession, orphans[0].Name())
	orphans[0].Discard()
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))
}

func TestWorkspace_Store_NewWorkspace_Good(t *T) {
	storeInstance, _ := fixtureConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("ax7")
	RequireNoError(t, err)
	defer workspace.Discard()
	AssertEqual(t, "ax7", workspace.Name())
}

func TestWorkspace_Store_NewWorkspace_Bad(t *T) {
	storeInstance, _ := fixtureConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("")
	AssertError(t, err)
	AssertNil(t, workspace)
}

func TestWorkspace_Store_NewWorkspace_Ugly(t *T) {
	storeInstance, _ := fixtureConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("ax7-42")
	RequireNoError(t, err)
	defer workspace.Discard()
	AssertContains(t, workspace.DatabasePath(), "ax7-42")
}

func TestWorkspace_Store_RecoverOrphans_Good(t *T) {
	storeInstance, stateDirectory := fixtureConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("orphan")
	RequireNoError(t, err)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	RequireNoError(t, workspace.Close())
	orphans := storeInstance.RecoverOrphans(stateDirectory)
	AssertLen(t, orphans, 1)
}

func TestWorkspace_Store_RecoverOrphans_Bad(t *T) {
	var storeInstance *Store
	orphans := storeInstance.RecoverOrphans(t.TempDir())
	AssertNil(t, orphans)
}

func TestWorkspace_Store_RecoverOrphans_Ugly(t *T) {
	storeInstance, stateDirectory := fixtureConfiguredStore(t)
	orphans := storeInstance.RecoverOrphans(stateDirectory)
	AssertEmpty(t, orphans)
}

func TestWorkspace_Workspace_Name_Good(t *T) {
	_, workspace := fixtureWorkspace(t)
	name := workspace.Name()
	AssertEqual(t, testFixtureWorkspaceName, name)
}

func TestWorkspace_Workspace_Name_Bad(t *T) {
	var workspace *Workspace
	name := workspace.Name()
	AssertEqual(t, "", name)
}

func TestWorkspace_Workspace_Name_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Close())
	AssertEqual(t, testFixtureWorkspaceName, workspace.Name())
}

func TestWorkspace_Workspace_DatabasePath_Good(t *T) {
	_, workspace := fixtureWorkspace(t)
	path := workspace.DatabasePath()
	AssertContains(t, path, testFixtureWorkspaceName)
}

func TestWorkspace_Workspace_DatabasePath_Bad(t *T) {
	var workspace *Workspace
	path := workspace.DatabasePath()
	AssertEqual(t, "", path)
}

func TestWorkspace_Workspace_DatabasePath_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Close())
	AssertContains(t, workspace.DatabasePath(), duckDBExtension)
}

func TestWorkspace_Workspace_Close_Good(t *T) {
	_, workspace := fixtureWorkspace(t)
	err := workspace.Close()
	AssertNoError(t, err)
	AssertNotEmpty(t, workspace.DatabasePath())
}

func TestWorkspace_Workspace_Close_Bad(t *T) {
	var workspace *Workspace
	err := workspace.Close()
	AssertNoError(t, err)
}

func TestWorkspace_Workspace_Close_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Close())
	err := workspace.Close()
	AssertNoError(t, err)
}

func TestWorkspace_Workspace_Put_Good(t *T) {
	_, workspace := fixtureWorkspace(t)
	err := workspace.Put("entry", map[string]any{"name": "alice"})
	AssertNoError(t, err)
	AssertEqual(t, 1, len(workspace.Aggregate()))
}

func TestWorkspace_Workspace_Put_Bad(t *T) {
	_, workspace := fixtureWorkspace(t)
	err := workspace.Put("", map[string]any{"name": "alice"})
	AssertError(t, err)
}

func TestWorkspace_Workspace_Put_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	err := workspace.Put("entry", nil)
	AssertNoError(t, err)
	AssertEqual(t, 1, len(workspace.Aggregate()))
}

func TestWorkspace_Workspace_Count_Good(t *T) {
	_, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	count, err := workspace.Count()
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestWorkspace_Workspace_Count_Bad(t *T) {
	_, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Close())
	count, err := workspace.Count()
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestWorkspace_Workspace_Count_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	count, err := workspace.Count()
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestWorkspace_Workspace_Aggregate_Good(t *T) {
	_, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	summary := workspace.Aggregate()
	AssertEqual(t, "1", Sprint(summary["entry"]))
}

func TestWorkspace_Workspace_Aggregate_Bad(t *T) {
	var workspace *Workspace
	summary := workspace.Aggregate()
	AssertEmpty(t, summary)
}

func TestWorkspace_Workspace_Aggregate_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Close())
	summary := workspace.Aggregate()
	AssertEmpty(t, summary)
}

func TestWorkspace_Workspace_Commit_Good(t *T) {
	storeInstance, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	result := workspace.Commit()
	AssertTrue(t, result.OK)
	AssertTrue(t, fixtureMustGroupExists(t, storeInstance, "workspace:ax7-workspace"))
}

func TestWorkspace_Workspace_Commit_Bad(t *T) {
	var workspace *Workspace
	result := workspace.Commit()
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "workspace")
}

func TestWorkspace_Workspace_Commit_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	result := workspace.Commit()
	AssertTrue(t, result.OK)
	AssertEmpty(t, result.Value.(map[string]any))
}

func TestWorkspace_Workspace_Discard_Good(t *T) {
	_, workspace := fixtureWorkspace(t)
	workspace.Discard()
	result := workspace.Query("SELECT 1")
	AssertFalse(t, result.OK)
}

func TestWorkspace_Workspace_Discard_Bad(t *T) {
	var workspace *Workspace
	AssertNotPanics(t, func() { workspace.Discard() })
	AssertEqual(t, "", workspace.Name())
}

func TestWorkspace_Workspace_Discard_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	workspace.Discard()
	AssertNotPanics(t, func() { workspace.Discard() })
}

func TestWorkspace_Workspace_Query_Good(t *T) {
	_, workspace := fixtureWorkspace(t)
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	result := workspace.Query("SELECT entry_kind FROM workspace_entries")
	AssertTrue(t, result.OK)
	AssertNotEmpty(t, result.Value)
}

func TestWorkspace_Workspace_Query_Bad(t *T) {
	_, workspace := fixtureWorkspace(t)
	result := workspace.Query("SELECT * FROM missing_table")
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "query")
}

func TestWorkspace_Workspace_Query_Ugly(t *T) {
	_, workspace := fixtureWorkspace(t)
	result := workspace.Query("SELECT COUNT(*) AS n FROM workspace_entries")
	AssertTrue(t, result.OK)
	AssertNotEmpty(t, result.Value)
}
