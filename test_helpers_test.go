package store

import (
	"testing"

	core "dappco.re/go"
)

const (
	testMemoryDatabasePath               = memoryDatabasePath
	testJournalEndpoint                  = "http://127.0.0.1:8086"
	testKeyFormat                        = "key-%d"
	testSessionA                         = "session-a"
	testSessionB                         = "session-b"
	testTenantA                          = exampleTenantA
	testTenantB                          = "tenant-b"
	testTenant42                         = exampleTenant42
	testTenantAConfigGroup               = "tenant-a:config"
	testTenantAPrefix                    = "tenant-a:"
	testScopedStoreNilMessage            = scopedStoreNilMessage
	testNamespacedGroupOne               = "ns-a:g1"
	testNamespacedGroupTwo               = "ns-a:g2"
	testCacheA                           = "cache-a"
	testScrollSession                    = "scroll-session"
	testOrphanSession                    = "orphan-session"
	testRecoverGood                      = "recover-good"
	testGroupFormat                      = "grp-%d"
	testTTLKey                           = "ttl-key"
	testTTLValue                         = "ttl-val"
	testWALPragma                        = sqliteWALPragma
	testCloseDriverName                  = "test.CloseDriver"
	testRowsAffectedDriverName           = "test.RowsAffectedDriver"
	testUnexpectedFieldsTypeFormat       = "unexpected fields type: %T"
	testUnexpectedTagsTypeFormat         = "unexpected tags type: %T"
	testSetCommittedAtByMeasurementSQL   = " SET committed_at = ? WHERE measurement = ?"
	testSQLUpdatePrefix                  = "UPDATE "
	testAppDatabaseFile                  = "app.db"
	testActorAlice                       = "@alice"
	testActorCharlie                     = "@charlie"
	testUsersJSONFile                    = "users.json"
	testFindingsCSVFile                  = "findings.csv"
	testReportJSONFile                   = "report.json"
	testReportJSONLFile                  = "report.jsonl"
	testRecordsJSONLFile                 = "records.jsonl"
	testFileNotFoundPrefix               = "file not found: "
	testFileNotFoundMessage              = "file not found"
	testLineTwo                          = "line 2"
	testInsertFailedMessage              = "insert failed"
	testTableInfoQueryFailedMessage      = "table_info query failed"
	testRenameEntriesBackupSQL           = "ALTER TABLE entries RENAME TO entries_backup"
	testInsertEntriesFromBackupSQL       = "INSERT INTO entries SELECT * FROM entries_backup"
	testDropEntriesBackupSQL             = "DROP TABLE entries_backup"
	testUsageExampleMarker               = "Usage example:"
	testGoTestFileSuffix                 = "_test.go"
	testWorkspaceCommitFailedFormat      = "workspace commit failed: %v"
	testWorkspaceEntryInsertSuffix       = " (entry_kind, entry_data, created_at) VALUES (?, ?, ?)"
	testSQLInsertIntoPrefix              = "INSERT INTO "
	testAX7WorkspaceName                 = "ax7-workspace"
	testHFDatasetID                      = "user/dataset"
	testCompactFailedFormat              = "compact failed: %v"
	testUnexpectedArchivePathTypeFormat  = "unexpected archive path type: %T"
	testArchiveLineUnmarshalFailedFormat = "archive line unmarshal failed: %v"
)

func testFilesystem() *core.Fs {
	return (&core.Fs{}).NewUnrestricted()
}

func noopCancelPurge() {
	// Intentionally empty: tests only need a non-nil purge cancel hook.
}

func testPath(tb testing.TB, name string) string {
	tb.Helper()
	return core.Path(tb.TempDir(), name)
}

func requireCoreOK(tb testing.TB, result core.Result) {
	tb.Helper()
	assertTruef(tb, result.OK, "core result failed: %v", result.Value)
}

func requireCoreReadBytes(tb testing.TB, path string) []byte {
	tb.Helper()
	result := testFilesystem().Read(path)
	requireCoreOK(tb, result)
	return []byte(result.Value.(string))
}

func requireCoreWriteBytes(tb testing.TB, path string, data []byte) {
	tb.Helper()
	requireCoreOK(tb, testFilesystem().Write(path, string(data)))
}

func repeatString(value string, count int) string {
	if count <= 0 {
		return ""
	}
	builder := core.NewBuilder()
	for range count {
		builder.WriteString(value)
	}
	return builder.String()
}

func useWorkspaceStateDirectory(tb testing.TB) string {
	tb.Helper()

	previous := defaultWorkspaceStateDirectory
	stateDirectory := testPath(tb, "state")
	defaultWorkspaceStateDirectory = stateDirectory
	tb.Cleanup(func() {
		defaultWorkspaceStateDirectory = previous
		_ = testFilesystem().DeleteAll(stateDirectory)
	})
	return stateDirectory
}

func useArchiveOutputDirectory(tb testing.TB) string {
	tb.Helper()

	previous := defaultArchiveOutputDirectory
	outputDirectory := testPath(tb, "archive")
	defaultArchiveOutputDirectory = outputDirectory
	tb.Cleanup(func() {
		defaultArchiveOutputDirectory = previous
		_ = testFilesystem().DeleteAll(outputDirectory)
	})
	return outputDirectory
}

func requireResultRows(tb testing.TB, result core.Result) []map[string]any {
	tb.Helper()

	assertTruef(tb, result.OK, "core result failed: %v", result.Value)
	rows, ok := result.Value.([]map[string]any)
	assertTruef(tb, ok, "unexpected row type: %T", result.Value)
	return rows
}
