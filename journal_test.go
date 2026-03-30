package store

import (
	"testing"

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

func TestJournal_CommitToJournal_Bad_EmptyMeasurement(t *testing.T) {
	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	result := storeInstance.CommitToJournal("", map[string]any{"like": 1}, map[string]string{"workspace": "missing"})
	require.False(t, result.OK)
	assert.Contains(t, result.Value.(error).Error(), "measurement is empty")
}
