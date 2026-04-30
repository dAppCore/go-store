package store

import "testing"

func TestRecover_Orphans_Good_RecoversOrphan(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace(testRecoverGood)
	assertNoError(t, err)
	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Close())

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 1)
	assertEqual(t, testRecoverGood, orphans[0].Name())
	assertEqual(t, map[string]any{"like": 1}, orphans[0].Aggregate())

	orphans[0].Discard()
	assertFalse(t, testFilesystem().Exists(workspaceFilePath(stateDirectory, testRecoverGood)))
}

func TestRecover_Orphans_Bad_CorruptMetadataQuarantined(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	corruptDatabasePath := workspaceFilePath(stateDirectory, "recover-bad")
	requireCoreWriteBytes(t, corruptDatabasePath, []byte("not a duckdb database"))
	requireCoreWriteBytes(t, corruptDatabasePath+".wal", []byte("wal"))

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 0)
	assertFalse(t, testFilesystem().Exists(corruptDatabasePath))
	assertFalse(t, testFilesystem().Exists(corruptDatabasePath+".wal"))

	quarantinePath := workspaceQuarantineFilePath(stateDirectory, corruptDatabasePath)
	assertTrue(t, testFilesystem().Exists(quarantinePath))
	assertTrue(t, testFilesystem().Exists(quarantinePath+".wal"))
	assertEqual(t, "not a duckdb database", string(requireCoreReadBytes(t, quarantinePath)))
}

func TestRecover_Orphans_Ugly_NoOrphansNoop(t *testing.T) {
	stateDirectory := useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	orphans := storeInstance.RecoverOrphans(stateDirectory)
	assertLen(t, orphans, 0)
	assertFalse(t, testFilesystem().Exists(joinPath(stateDirectory, workspaceQuarantineDirName)))
}
