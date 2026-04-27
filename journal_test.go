package store

import (
	"testing"
	"time"
)

func TestJournal_CommitToJournal_Good_WithQueryJournalSQL(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	first := storeInstance.CommitToJournal("session-a", map[string]any{"like": 4}, map[string]string{"workspace": "session-a"})
	second := storeInstance.CommitToJournal("session-b", map[string]any{"profile_match": 2}, map[string]string{"workspace": "session-b"})
	assertTruef(t, first.OK, "first journal commit failed: %v", first.Value)
	assertTruef(t, second.OK, "second journal commit failed: %v", second.Value)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal("SELECT bucket_name, measurement, fields_json, tags_json FROM journal_entries ORDER BY entry_id"),
	)
	assertLen(t, rows, 2)
	assertEqual(t, "events", rows[0]["bucket_name"])
	assertEqual(t, "session-a", rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, "unexpected fields type: %T", rows[0]["fields"])
	assertEqual(t, float64(4), fields["like"])

	tags, ok := rows[1]["tags"].(map[string]string)
	assertTruef(t, ok, "unexpected tags type: %T", rows[1]["tags"])
	assertEqual(t, "session-b", tags["workspace"])
}

func TestJournal_CommitToJournal_Good_ResultCopiesInputMaps(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	fields := map[string]any{"like": 4}
	tags := map[string]string{"workspace": "session-a"}

	result := storeInstance.CommitToJournal("session-a", fields, tags)
	assertTruef(t, result.OK, "journal commit failed: %v", result.Value)

	fields["like"] = 99
	tags["workspace"] = "session-b"

	value, ok := result.Value.(map[string]any)
	assertTruef(t, ok, "unexpected result type: %T", result.Value)

	resultFields, ok := value["fields"].(map[string]any)
	assertTruef(t, ok, "unexpected fields type: %T", value["fields"])
	assertEqual(t, 4, resultFields["like"])

	resultTags, ok := value["tags"].(map[string]string)
	assertTruef(t, ok, "unexpected tags type: %T", value["tags"])
	assertEqual(t, "session-a", resultTags["workspace"])
}

func TestJournal_QueryJournal_Good_RawSQLWithCTE(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 4}, map[string]string{"workspace": "session-a"}).OK)

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
	assertEqual(t, "session-a", rows[0]["measurement"])
}

func TestJournal_QueryJournal_Good_PragmaSQL(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
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
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK)
	assertTrue(t, storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r._measurement == "session-b")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, "session-b", rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, "unexpected fields type: %T", rows[0]["fields"])
	assertEqual(t, float64(2), fields["like"])
}

func TestJournal_QueryJournal_Good_TagFilter(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK)
	assertTrue(t, storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r.workspace == "session-b")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, "session-b", rows[0]["measurement"])

	tags, ok := rows[0]["tags"].(map[string]string)
	assertTruef(t, ok, "unexpected tags type: %T", rows[0]["tags"])
	assertEqual(t, "session-b", tags["workspace"])
}

func TestJournal_QueryJournal_Good_NumericFieldFilter(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK)
	assertTrue(t, storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r.like == 2)`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, "session-b", rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, "unexpected fields type: %T", rows[0]["fields"])
	assertEqual(t, float64(2), fields["like"])
}

func TestJournal_QueryJournal_Good_BooleanFieldFilter(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"complete": false}, map[string]string{"workspace": "session-a"}).OK)
	assertTrue(t, storeInstance.CommitToJournal("session-b", map[string]any{"complete": true}, map[string]string{"workspace": "session-b"}).OK)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r["complete"] == true)`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, "session-b", rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	assertTruef(t, ok, "unexpected fields type: %T", rows[0]["fields"])
	assertEqual(t, true, fields["complete"])
}

func TestJournal_QueryJournal_Good_BucketFilter(t *testing.T) {
	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK)
	assertNoError(t, commitJournalEntry(storeInstance.sqliteDatabase, "events", "session-b", `{"like":2}`, `{"workspace":"session-b"}`, time.Now().UnixMilli()))

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r._bucket == "events")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, "session-b", rows[0]["measurement"])
	assertEqual(t, "events", rows[0]["bucket_name"])
}

func TestJournal_QueryJournal_Good_DeterministicOrderingForSameTimestamp(t *testing.T) {
	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()
	assertNoError(t, ensureJournalSchema(storeInstance.sqliteDatabase))

	committedAt := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	assertNoError(t, commitJournalEntry(storeInstance.sqliteDatabase, "events", "session-b", `{"like":2}`, `{"workspace":"session-b"}`, committedAt))
	assertNoError(t, commitJournalEntry(storeInstance.sqliteDatabase, "events", "session-a", `{"like":1}`, `{"workspace":"session-a"}`, committedAt))

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(""),
	)
	assertLen(t, rows, 2)
	assertEqual(t, "session-b", rows[0]["measurement"])
	assertEqual(t, "session-a", rows[1]["measurement"])
}

func TestJournal_QueryJournal_Good_AbsoluteRangeWithStop(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK)
	assertTrue(t, storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK)

	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC).UnixMilli(),
		"session-a",
	)
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli(),
		"session-b",
	)
	assertNoError(t, err)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: "2026-03-30T00:00:00Z", stop: now())`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, "session-b", rows[0]["measurement"])
}

func TestJournal_QueryJournal_Good_AbsoluteRangeHonoursStop(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK)
	assertTrue(t, storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK)

	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC).UnixMilli(),
		"session-a",
	)
	assertNoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli(),
		"session-b",
	)
	assertNoError(t, err)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: "2026-03-29T00:00:00Z", stop: "2026-03-30T00:00:00Z")`),
	)
	assertLen(t, rows, 1)
	assertEqual(t, "session-a", rows[0]["measurement"])
}

func TestJournal_CommitToJournal_Bad_EmptyMeasurement(t *testing.T) {
	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	result := storeInstance.CommitToJournal("", map[string]any{"like": 1}, map[string]string{"workspace": "missing"})
	assertFalse(t, result.OK)
	assertContainsString(t, result.Value.(error).Error(), "measurement is empty")
}
