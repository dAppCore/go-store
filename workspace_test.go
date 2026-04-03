package store

import (
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkspace_NewWorkspace_Good_CreatePutAggregateQuery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	require.NoError(t, err)
	defer workspace.Discard()

	assert.Equal(t, workspaceFilePath(stateDirectory, "scroll-session"), workspace.databasePath)
	assert.True(t, testFilesystem().Exists(workspace.databasePath))

	require.NoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	require.NoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	require.NoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

	assert.Equal(t, map[string]any{"like": 2, "profile_match": 1}, workspace.Aggregate())

	rows := requireResultRows(
		t,
		workspace.Query("SELECT entry_kind, COUNT(*) AS entry_count FROM workspace_entries GROUP BY entry_kind ORDER BY entry_kind"),
	)
	require.Len(t, rows, 2)
	assert.Equal(t, "like", rows[0]["entry_kind"])
	assert.Equal(t, int64(2), rows[0]["entry_count"])
	assert.Equal(t, "profile_match", rows[1]["entry_kind"])
	assert.Equal(t, int64(1), rows[1]["entry_count"])
}

func TestWorkspace_Query_Good_RFCEntriesView(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	require.NoError(t, err)
	defer workspace.Discard()

	require.NoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	require.NoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	require.NoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

	rows := requireResultRows(
		t,
		workspace.Query("SELECT kind, COUNT(*) AS entry_count FROM entries GROUP BY kind ORDER BY kind"),
	)
	require.Len(t, rows, 2)
	assert.Equal(t, "like", rows[0]["kind"])
	assert.Equal(t, int64(2), rows[0]["entry_count"])
	assert.Equal(t, "profile_match", rows[1]["kind"])
	assert.Equal(t, int64(1), rows[1]["entry_count"])
}

func TestWorkspace_Commit_Good_JournalAndSummary(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	require.NoError(t, err)

	require.NoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	require.NoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	require.NoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

	result := workspace.Commit()
	require.True(t, result.OK, "workspace commit failed: %v", result.Value)
	assert.Equal(t, map[string]any{"like": 2, "profile_match": 1}, result.Value)
	assert.False(t, testFilesystem().Exists(workspace.databasePath))

	summaryJSON, err := storeInstance.Get(workspaceSummaryGroup("scroll-session"), "summary")
	require.NoError(t, err)

	summary := make(map[string]any)
	summaryResult := core.JSONUnmarshalString(summaryJSON, &summary)
	require.True(t, summaryResult.OK, "summary unmarshal failed: %v", summaryResult.Value)
	assert.Equal(t, float64(2), summary["like"])
	assert.Equal(t, float64(1), summary["profile_match"])

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r._measurement == "scroll-session")`),
	)
	require.Len(t, rows, 1)
	assert.Equal(t, "scroll-session", rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	require.True(t, ok, "unexpected fields type: %T", rows[0]["fields"])
	assert.Equal(t, float64(2), fields["like"])
	assert.Equal(t, float64(1), fields["profile_match"])

	tags, ok := rows[0]["tags"].(map[string]string)
	require.True(t, ok, "unexpected tags type: %T", rows[0]["tags"])
	assert.Equal(t, "scroll-session", tags["workspace"])
}

func TestWorkspace_Commit_Good_EmitsSummaryEvent(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	events := storeInstance.Watch(workspaceSummaryGroup("scroll-session"))
	defer storeInstance.Unwatch(workspaceSummaryGroup("scroll-session"), events)

	workspace, err := storeInstance.NewWorkspace("scroll-session")
	require.NoError(t, err)

	require.NoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	require.NoError(t, workspace.Put("profile_match", map[string]any{"user": "@charlie"}))

	result := workspace.Commit()
	require.True(t, result.OK, "workspace commit failed: %v", result.Value)

	select {
	case event := <-events:
		assert.Equal(t, EventSet, event.Type)
		assert.Equal(t, workspaceSummaryGroup("scroll-session"), event.Group)
		assert.Equal(t, "summary", event.Key)
		assert.False(t, event.Timestamp.IsZero())

		summary := make(map[string]any)
		summaryResult := core.JSONUnmarshalString(event.Value, &summary)
		require.True(t, summaryResult.OK, "summary event unmarshal failed: %v", summaryResult.Value)
		assert.Equal(t, float64(1), summary["like"])
		assert.Equal(t, float64(1), summary["profile_match"])
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for workspace summary event")
	}
}

func TestWorkspace_Discard_Good_Idempotent(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("discard-session")
	require.NoError(t, err)

	workspace.Discard()
	workspace.Discard()

	assert.False(t, testFilesystem().Exists(workspace.databasePath))
}

func TestWorkspace_RecoverOrphans_Good(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("orphan-session")
	require.NoError(t, err)
	require.NoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	require.NoError(t, workspace.database.Close())

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	require.Len(t, orphans, 1)
	assert.Equal(t, "orphan-session", orphans[0].Name())
	assert.Equal(t, map[string]any{"like": 1}, orphans[0].Aggregate())

	orphans[0].Discard()
	assert.False(t, testFilesystem().Exists(workspaceFilePath(stateDirectory, "orphan-session")))
}

func TestWorkspace_New_Good_LeavesOrphanedWorkspacesForRecovery(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)
	requireCoreOK(t, testFilesystem().EnsureDir(stateDirectory))

	orphanDatabasePath := workspaceFilePath(stateDirectory, "orphan-session")
	orphanDatabase, err := openWorkspaceDatabase(orphanDatabasePath)
	require.NoError(t, err)
	require.NoError(t, orphanDatabase.Close())
	assert.True(t, testFilesystem().Exists(orphanDatabasePath))

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	assert.True(t, testFilesystem().Exists(orphanDatabasePath))

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	require.Len(t, orphans, 1)
	assert.Equal(t, "orphan-session", orphans[0].Name())
	orphans[0].Discard()
	assert.False(t, testFilesystem().Exists(orphanDatabasePath))
	assert.False(t, testFilesystem().Exists(orphanDatabasePath+"-wal"))
	assert.False(t, testFilesystem().Exists(orphanDatabasePath+"-shm"))
}
