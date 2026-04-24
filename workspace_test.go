package store

import (
	"testing"
	"time"

	core "dappco.re/go/core"
)

func TestWorkspace_NewWorkspace_Good_CreatePutAggregateQuery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	assertNoError(t, err)
	defer workspace.Discard()

	assertEqual(t, workspaceFilePath(stateDirectory, "scroll-session"), workspace.databasePath)
	assertTrue(t, testFilesystem().Exists(workspace.databasePath))

	assertNoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

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

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	assertNoError(t, err)
	defer workspace.Discard()

	assertEqual(t, workspaceFilePath(stateDirectory, "scroll-session"), workspace.DatabasePath())
}

func TestWorkspace_Count_Good_Empty(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("count-empty")
	assertNoError(t, err)
	defer workspace.Discard()

	count, err := workspace.Count()
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestWorkspace_Count_Good_AfterPuts(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("count-puts")
	assertNoError(t, err)
	defer workspace.Discard()

	assertNoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

	count, err := workspace.Count()
	assertNoError(t, err)
	assertEqual(t, 3, count)
}

func TestWorkspace_Count_Bad_ClosedWorkspace(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("count-closed")
	assertNoError(t, err)
	workspace.Discard()

	_, err = workspace.Count()
	assertError(t, err)
}

func TestWorkspace_Query_Good_RFCEntriesView(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	assertNoError(t, err)
	defer workspace.Discard()

	assertNoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

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

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	assertNoError(t, err)

	assertNoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

	result := workspace.Commit()
	assertTruef(t, result.OK, "workspace commit failed: %v", result.Value)
	assertEqual(t, map[string]any{"like": 2, "profile_match": 1}, result.Value)
	assertFalse(t, testFilesystem().Exists(workspace.databasePath))

	summaryJSON, err := storeInstance.Get(workspaceSummaryGroup("scroll-session"), "summary")
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
	assertEqual(t, "scroll-session", rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, "unexpected fields type: %T", rows[0]["fields"])
	assertEqual(t, float64(2), fields["like"])
	assertEqual(t, float64(1), fields["profile_match"])

	tags, ok := rows[0]["tags"].(map[string]string)
	assertTruef(t, ok, "unexpected tags type: %T", rows[0]["tags"])
	assertEqual(t, "scroll-session", tags["workspace"])
}

func TestWorkspace_Commit_Good_ResultCopiesAggregatedMap(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	assertNoError(t, err)

	aggregateSource := map[string]any{"like": 1}
	assertNoError(t, workspace.Put("like", aggregateSource))

	result := workspace.Commit()
	assertTruef(t, result.OK, "workspace commit failed: %v", result.Value)

	aggregateSource["like"] = 99

	value, ok := result.Value.(map[string]any)
	assertTruef(t, ok, "unexpected result type: %T", result.Value)
	assertEqual(t, 1, value["like"])
}

func TestWorkspace_Commit_Good_EmitsSummaryEvent(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer storeInstance.Close()

	events := storeInstance.Watch(workspaceSummaryGroup("scroll-session"))
	defer storeInstance.Unwatch(workspaceSummaryGroup("scroll-session"), events)

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	assertNoError(t, err)

	assertNoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

	result := workspace.Commit()
	assertTruef(t, result.OK, "workspace commit failed: %v", result.Value)

	select {
	case event := <-events:
		assertEqual(t, EventSet, event.Type)
		assertEqual(t, workspaceSummaryGroup("scroll-session"), event.Group)
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

func TestWorkspace_Discard_Good_Idempotent(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("discard-session")
	assertNoError(t, err)

	workspace.Discard()
	workspace.Discard()

	assertFalse(t, testFilesystem().Exists(workspace.databasePath))
}

func TestWorkspace_Close_Good_PreservesFileForRecovery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("close-session")
	assertNoError(t, err)

	assertNoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
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

	sqliteDatabase, err := openWorkspaceDatabase(databasePath)
	assertNoError(t, err)

	workspace := &Workspace{
		name:           "partial-workspace",
		sqliteDatabase: sqliteDatabase,
		databasePath:   databasePath,
	}

	assertNoError(t, workspace.Close())

	_, execErr := sqliteDatabase.Exec("SELECT 1")
	assertError(t, execErr)
	assertContainsString(t, execErr.Error(), "closed")

	assertTrue(t, testFilesystem().Exists(databasePath))
	requireCoreOK(t, testFilesystem().Delete(databasePath))
	_ = testFilesystem().Delete(databasePath + "-wal")
	_ = testFilesystem().Delete(databasePath + "-shm")
}

func TestWorkspace_RecoverOrphans_Good(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("orphan-session")
	assertNoError(t, err)
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	assertNoError(t, workspace.sqliteDatabase.Close())

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, "orphan-session", orphans[0].Name())
	assertEqual(t, map[string]any{"like": 1}, orphans[0].Aggregate())

	orphans[0].Discard()
	assertFalse(t, testFilesystem().Exists(workspaceFilePath(stateDirectory, "orphan-session")))
}

func TestWorkspace_New_Good_LeavesOrphanedWorkspacesForRecovery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, "orphan-session")
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	_, err = orphanDatabase.Exec(
		"INSERT INTO "+workspaceEntriesTableName+" (entry_kind, entry_data, created_at) VALUES (?, ?, ?)",
		"like",
		`{"user":"@alice"}`,
		time.Now().UnixMilli(),
	)
	assertNoError(t, err)
	assertNoError(t, orphanDatabase.Close())
	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, "orphan-session", orphans[0].Name())
	orphans[0].Discard()
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath+"-wal"))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath+"-shm"))
}

func TestWorkspace_New_Good_CachesOrphansDuringConstruction(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, "orphan-session")
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	_, err = orphanDatabase.Exec(
		"INSERT INTO "+workspaceEntriesTableName+" (entry_kind, entry_data, created_at) VALUES (?, ?, ?)",
		"like",
		`{"user":"@alice"}`,
		time.Now().UnixMilli(),
	)
	assertNoError(t, err)
	assertNoError(t, orphanDatabase.Close())
	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	requireCoreOK(t, testFilesystem().DeleteAll(stateDirectory))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, "orphan-session", orphans[0].Name())
	assertEqual(t, map[string]any{"like": 1}, orphans[0].Aggregate())
	orphans[0].Discard()
}

func TestWorkspace_NewConfigured_Good_CachesOrphansFromConfiguredStateDirectory(t *testing.T) {
	stateDirectory := testPath(t, "configured-state")
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, "orphan-session")
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	_, err = orphanDatabase.Exec(
		"INSERT INTO "+workspaceEntriesTableName+" (entry_kind, entry_data, created_at) VALUES (?, ?, ?)",
		"like",
		`{"user":"@alice"}`,
		time.Now().UnixMilli(),
	)
	assertNoError(t, err)
	assertNoError(t, orphanDatabase.Close())

	storeInstance, err := NewConfigured(StoreConfig{
		DatabasePath:            ":memory:",
		WorkspaceStateDirectory: stateDirectory,
	})
	assertNoError(t, err)
	defer storeInstance.Close()

	requireCoreOK(t, testFilesystem().DeleteAll(stateDirectory))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans("")
	assertLen(t, orphans, 1)
	assertEqual(t, "orphan-session", orphans[0].Name())
	assertEqual(t, map[string]any{"like": 1}, orphans[0].Aggregate())
	orphans[0].Discard()
}

func TestWorkspace_RecoverOrphans_Good_TrailingSlashUsesCache(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, "orphan-session")
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	assertNoError(t, orphanDatabase.Close())
	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	requireCoreOK(t, testFilesystem().DeleteAll(stateDirectory))
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans(stateDirectory + "/")
	assertLen(t, orphans, 1)
	assertEqual(t, "orphan-session", orphans[0].Name())
	orphans[0].Discard()
}

func TestWorkspace_Close_Good_PreservesOrphansForRecovery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, "orphan-session")
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	assertNoError(t, err)
	assertNoError(t, orphanDatabase.Close())
	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(":memory:")
	assertNoError(t, err)

	assertNoError(t, storeInstance.Close())

	assertTrue(t, testFilesystem().Exists(orphanDatabasePath))

	recoveryStore, err := New(":memory:")
	assertNoError(t, err)
	defer recoveryStore.Close()

	orphans := recoveryStore.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, "orphan-session", orphans[0].Name())
	orphans[0].Discard()
	assertFalse(t, testFilesystem().Exists(orphanDatabasePath))
}
