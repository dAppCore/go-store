package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJournal_CommitToJournal_Good_WithQueryJournalSQL(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	first := storeInstance.CommitToJournal("session-a", map[string]any{"like": 4}, map[string]string{"workspace": "session-a"})
	second := storeInstance.CommitToJournal("session-b", map[string]any{"profile_match": 2}, map[string]string{"workspace": "session-b"})
	require.True(t, first.OK, "first journal commit failed: %v", first.Value)
	require.True(t, second.OK, "second journal commit failed: %v", second.Value)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal("SELECT bucket_name, measurement, fields_json, tags_json FROM journal_entries ORDER BY entry_id"),
	)
	require.Len(t, rows, 2)
	assert.Equal(t, "events", rows[0]["bucket_name"])
	assert.Equal(t, "session-a", rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	require.True(t, ok, "unexpected fields type: %T", rows[0]["fields"])
	assert.Equal(t, float64(4), fields["like"])

	tags, ok := rows[1]["tags"].(map[string]string)
	require.True(t, ok, "unexpected tags type: %T", rows[1]["tags"])
	assert.Equal(t, "session-b", tags["workspace"])
}

func TestJournal_CommitToJournal_Good_ResultCopiesInputMaps(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	fields := map[string]any{"like": 4}
	tags := map[string]string{"workspace": "session-a"}

	result := storeInstance.CommitToJournal("session-a", fields, tags)
	require.True(t, result.OK, "journal commit failed: %v", result.Value)

	fields["like"] = 99
	tags["workspace"] = "session-b"

	value, ok := result.Value.(map[string]any)
	require.True(t, ok, "unexpected result type: %T", result.Value)

	resultFields, ok := value["fields"].(map[string]any)
	require.True(t, ok, "unexpected fields type: %T", value["fields"])
	assert.Equal(t, 4, resultFields["like"])

	resultTags, ok := value["tags"].(map[string]string)
	require.True(t, ok, "unexpected tags type: %T", value["tags"])
	assert.Equal(t, "session-a", resultTags["workspace"])
}

func TestJournal_QueryJournal_Good_RawSQLWithCTE(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	require.True(t,
		storeInstance.CommitToJournal("session-a", map[string]any{"like": 4}, map[string]string{"workspace": "session-a"}).OK,
	)

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
	require.Len(t, rows, 1)
	assert.Equal(t, "session-a", rows[0]["measurement"])
}

func TestJournal_QueryJournal_Good_PragmaSQL(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal("PRAGMA table_info(journal_entries)"),
	)
	require.NotEmpty(t, rows)
	var columnNames []string
	for _, row := range rows {
		name, ok := row["name"].(string)
		require.True(t, ok, "unexpected column name type: %T", row["name"])
		columnNames = append(columnNames, name)
	}
	assert.Contains(t, columnNames, "bucket_name")
}

func TestJournal_QueryJournal_Good_FluxFilters(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	require.True(t,
		storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK,
	)
	require.True(t,
		storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK,
	)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r._measurement == "session-b")`),
	)
	require.Len(t, rows, 1)
	assert.Equal(t, "session-b", rows[0]["measurement"])

	fields, ok := rows[0]["fields"].(map[string]any)
	require.True(t, ok, "unexpected fields type: %T", rows[0]["fields"])
	assert.Equal(t, float64(2), fields["like"])
}

func TestJournal_QueryJournal_Good_TagFilter(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	require.True(t,
		storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK,
	)
	require.True(t,
		storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK,
	)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r.workspace == "session-b")`),
	)
	require.Len(t, rows, 1)
	assert.Equal(t, "session-b", rows[0]["measurement"])

	tags, ok := rows[0]["tags"].(map[string]string)
	require.True(t, ok, "unexpected tags type: %T", rows[0]["tags"])
	assert.Equal(t, "session-b", tags["workspace"])
}

func TestJournal_QueryJournal_Good_BucketFilter(t *testing.T) {
	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	require.True(t,
		storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK,
	)
	require.NoError(t, commitJournalEntry(
		storeInstance.sqliteDatabase,
		"events",
		"session-b",
		`{"like":2}`,
		`{"workspace":"session-b"}`,
		time.Now().UnixMilli(),
	))

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r._bucket == "events")`),
	)
	require.Len(t, rows, 1)
	assert.Equal(t, "session-b", rows[0]["measurement"])
	assert.Equal(t, "events", rows[0]["bucket_name"])
}

func TestJournal_QueryJournal_Good_DeterministicOrderingForSameTimestamp(t *testing.T) {
	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()
	require.NoError(t, ensureJournalSchema(storeInstance.sqliteDatabase))

	committedAt := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli()
	require.NoError(t, commitJournalEntry(
		storeInstance.sqliteDatabase,
		"events",
		"session-b",
		`{"like":2}`,
		`{"workspace":"session-b"}`,
		committedAt,
	))
	require.NoError(t, commitJournalEntry(
		storeInstance.sqliteDatabase,
		"events",
		"session-a",
		`{"like":1}`,
		`{"workspace":"session-a"}`,
		committedAt,
	))

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(""),
	)
	require.Len(t, rows, 2)
	assert.Equal(t, "session-b", rows[0]["measurement"])
	assert.Equal(t, "session-a", rows[1]["measurement"])
}

func TestJournal_QueryJournal_Good_AbsoluteRangeWithStop(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	require.True(t,
		storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK,
	)
	require.True(t,
		storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK,
	)

	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC).UnixMilli(),
		"session-a",
	)
	require.NoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli(),
		"session-b",
	)
	require.NoError(t, err)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: "2026-03-30T00:00:00Z", stop: now())`),
	)
	require.Len(t, rows, 1)
	assert.Equal(t, "session-b", rows[0]["measurement"])
}

func TestJournal_QueryJournal_Good_AbsoluteRangeHonoursStop(t *testing.T) {
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	require.True(t,
		storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK,
	)
	require.True(t,
		storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK,
	)

	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC).UnixMilli(),
		"session-a",
	)
	require.NoError(t, err)
	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC).UnixMilli(),
		"session-b",
	)
	require.NoError(t, err)

	rows := requireResultRows(
		t,
		storeInstance.QueryJournal(`from(bucket: "events") |> range(start: "2026-03-29T00:00:00Z", stop: "2026-03-30T00:00:00Z")`),
	)
	require.Len(t, rows, 1)
	assert.Equal(t, "session-a", rows[0]["measurement"])
}

func TestJournal_CommitToJournal_Bad_EmptyMeasurement(t *testing.T) {
	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	result := storeInstance.CommitToJournal("", map[string]any{"like": 1}, map[string]string{"workspace": "missing"})
	require.False(t, result.OK)
	assert.Contains(t, result.Value.(error).Error(), "measurement is empty")
}
