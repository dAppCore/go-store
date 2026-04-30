package store

import (
	"testing"
	"time"
)

func TestJournal_CommitToJournal_Good_WithQueryJournalSQL(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	first := storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 4}, map[string]string{"workspace": testSessionA})
	second := storeInstance.CommitToJournal(testSessionB, map[string]any{"profile_match": 2}, map[string]string{"workspace": testSessionB})
	assertTruef(t, first.OK, "first journal commit failed: %v", first.Value)
	assertTruef(t, second.OK, "second journal commit failed: %v", second.Value)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal("SELECT bucket_name, measurement, fields_json, tags_json FROM journal_entries ORDER BY entry_id"),
	)
	assertLen(t, rows, 2)
	assertEqual(t, "events", rows[0]["bucket_name"])
	assertEqual(t, testSessionA, rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, testUnexpectedFieldsTypeFormat, rows[0]["fields"])
	assertEqual(t, float64(4), fields["like"])

	tags, ok := rows[1]["tags"].(map[string]string)
	assertTruef(t, ok, testUnexpectedTagsTypeFormat, rows[1]["tags"])
	assertEqual(t, testSessionB, tags["workspace"])
}

func TestJournal_CommitToJournal_Good_ResultCopiesInputMaps(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	fields := map[string]any{"like": 4}
	tags := map[string]string{"workspace": testSessionA}

	result := storeInstance.CommitToJournal(testSessionA, fields, tags)
	assertTruef(t, result.OK, "journal commit failed: %v", result.Value)

	fields["like"] = 99
	tags["workspace"] = testSessionB

	value, ok := result.Value.(map[string]any)
	assertTruef(t, ok, "unexpected result type: %T", result.Value)

	resultFields, ok := value["fields"].(map[string]any)
	assertTruef(t, ok, testUnexpectedFieldsTypeFormat, value["fields"])
	assertEqual(t, 4, resultFields["like"])

	resultTags, ok := value["tags"].(map[string]string)
	assertTruef(t, ok, testUnexpectedTagsTypeFormat, value["tags"])
	assertEqual(t, testSessionA, resultTags["workspace"])
}

func TestJournal_QueryJournal_Good_RawSQLWithCTE(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 4}, map[string]string{"workspace": testSessionA}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`
			WITH journal_rows AS (
				SELECT bucket_name, measurement, fields_json, tags_json, committed_at, archived_at
				FROM journal_entries
			)
			SELECT bucket_name, measurement, fields_json, tags_json, committed_at, archived_at
			FROM journal_rows
			ORDER BY committed_at
		`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testSessionA, rows[0]["measurement"])
}

func TestJournal_QueryJournal_Good_PragmaSQL(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal("PRAGMA table_info(journal_entries)"),
	)
	assertNotEmpty(t, rows)
	var columnNames []string
	for _, row := range rows {
		name, ok := row["name"].(string)
		assertTruef(t, ok, "unexpected column name type: %T", row["name"])
		columnNames = append(columnNames, name)
	}
	assertContainsElement(t, columnNames, "bucket_name")
}

func TestJournal_QueryJournal_Good_FluxFilters(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 1}, map[string]string{"workspace": testSessionA}).OK)
	assertTrue(t, storeInstance.CommitToJournal(testSessionB, map[string]any{"like": 2}, map[string]string{"workspace": testSessionB}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r._measurement == "session-b")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testSessionB, rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, testUnexpectedFieldsTypeFormat, rows[0]["fields"])
	assertEqual(t, float64(2), fields["like"])
}

func TestJournal_QueryJournal_Good_TagFilter(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 1}, map[string]string{"workspace": testSessionA}).OK)
	assertTrue(t, storeInstance.CommitToJournal(testSessionB, map[string]any{"like": 2}, map[string]string{"workspace": testSessionB}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r.workspace == "session-b")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testSessionB, rows[0]["measurement"])

	tags, ok := rows[0]["tags"].(map[string]string)
	assertTruef(t, ok, testUnexpectedTagsTypeFormat, rows[0]["tags"])
	assertEqual(t, testSessionB, tags["workspace"])
}

func TestJournal_QueryJournal_Good_NumericFieldFilter(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 1}, map[string]string{"workspace": testSessionA}).OK)
	assertTrue(t, storeInstance.CommitToJournal(testSessionB, map[string]any{"like": 2}, map[string]string{"workspace": testSessionB}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r.like == 2)`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testSessionB, rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, testUnexpectedFieldsTypeFormat, rows[0]["fields"])
	assertEqual(t, float64(2), fields["like"])
}

func TestJournal_QueryJournal_Good_BooleanFieldFilter(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"complete": false}, map[string]string{"workspace": testSessionA}).OK)
	assertTrue(t, storeInstance.CommitToJournal(testSessionB, map[string]any{"complete": true}, map[string]string{"workspace": testSessionB}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r["complete"] == true)`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testSessionB, rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, testUnexpectedFieldsTypeFormat, rows[0]["fields"])
	assertEqual(t, true, fields["complete"])
}

func TestJournal_QueryJournal_Good_BucketFilter(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 1}, map[string]string{"workspace": testSessionA}).OK)
	assertNoError(t, commitJournalEntry(storeInstance.sqliteDatabase, "events", testSessionB, `{"like":2}`, `{"workspace":"session-b"}`, time.Now().UnixMilli()))

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r._bucket == "events")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testSessionB, rows[0]["measurement"])
	assertEqual(t, "events", rows[0]["bucket_name"])
}

func TestJournal_QueryJournal_Good_DeterministicOrderingForSameTimestamp(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()
	assertNoError(t, ensureJournalSchema(storeInstance.sqliteDatabase))

	committedAt := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	assertNoError(t, commitJournalEntry(storeInstance.sqliteDatabase, "events", testSessionB, `{"like":2}`, `{"workspace":"session-b"}`, committedAt))
	assertNoError(t, commitJournalEntry(storeInstance.sqliteDatabase, "events", testSessionA, `{"like":1}`, `{"workspace":"session-a"}`, committedAt))

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(""),
	)
	assertLen(t, rows, 2)
	assertEqual(t, testSessionB, rows[0]["measurement"])
	assertEqual(t, testSessionA, rows[1]["measurement"])
}

func TestJournal_QueryJournal_Good_AbsoluteRangeWithStop(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 1}, map[string]string{"workspace": testSessionA}).OK)
	assertTrue(t, storeInstance.CommitToJournal(testSessionB, map[string]any{"like": 2}, map[string]string{"workspace": testSessionB}).OK)

	_, err = storeInstance.sqliteDatabase.Exec(
		testSQLUpdatePrefix+journalEntriesTableName+testSetCommittedAtByMeasurementSQL,
		time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC).UnixMilli(),
		testSessionA,
	)
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(
		testSQLUpdatePrefix+journalEntriesTableName+testSetCommittedAtByMeasurementSQL,
		time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli(),
		testSessionB,
	)
	assertNoError(t, err)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: "2026-03-30T00:00:00Z", stop: now())`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testSessionB, rows[0]["measurement"])
}

func TestJournal_QueryJournal_Good_AbsoluteRangeHonoursStop(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 1}, map[string]string{"workspace": testSessionA}).OK)
	assertTrue(t, storeInstance.CommitToJournal(testSessionB, map[string]any{"like": 2}, map[string]string{"workspace": testSessionB}).OK)

	_, err = storeInstance.sqliteDatabase.Exec(
		testSQLUpdatePrefix+journalEntriesTableName+testSetCommittedAtByMeasurementSQL,
		time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC).UnixMilli(),
		testSessionA,
	)
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(
		testSQLUpdatePrefix+journalEntriesTableName+testSetCommittedAtByMeasurementSQL,
		time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli(),
		testSessionB,
	)
	assertNoError(t, err)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: "2026-03-29T00:00:00Z", stop: "2026-03-30T00:00:00Z")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, testSessionA, rows[0]["measurement"])
}

func TestJournal_CommitToJournal_Bad_EmptyMeasurement(t *testing.T) {
	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	result := storeInstance.CommitToJournal("", map[string]any{"like": 1}, map[string]string{"workspace": "missing"})
	assertFalse(t, result.OK)
	assertContainsString(t, result.Value.(error).Error(), "measurement is empty")
}

func TestJournal_Store_CommitToJournal_Good(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, map[string]string{"kind": "ax7"})
	AssertTrue(t, result.OK)
	AssertEqual(t, "measurement", result.Value.(map[string]any)["measurement"])
}

func TestJournal_Store_CommitToJournal_Bad(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.CommitToJournal("", nil, nil)
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "measurement")
}

func TestJournal_Store_CommitToJournal_Ugly(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.CommitToJournal("measurement", nil, nil)
	AssertTrue(t, result.OK)
	AssertNotNil(t, result.Value)
}

func TestJournal_Store_QueryJournal_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireTrue(t, storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, nil).OK)
	result := storeInstance.QueryJournal("SELECT measurement FROM journal_entries")
	AssertTrue(t, result.OK)
	AssertNotEmpty(t, result.Value)
}

func TestJournal_Store_QueryJournal_Bad(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.QueryJournal("from(bucket: \"events\") |> range(start: nope)")
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "parse")
}

func TestJournal_Store_QueryJournal_Ugly(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.QueryJournal("")
	AssertTrue(t, result.OK)
	AssertEmpty(t, result.Value)
}
