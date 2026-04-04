package store

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompact_Compact_Good_GzipArchive(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

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
		time.Now().Add(-48*time.Hour).UnixMilli(),
		"session-a",
	)
	require.NoError(t, err)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "gzip",
	})
	require.True(t, result.OK, "compact failed: %v", result.Value)

	archivePath, ok := result.Value.(string)
	require.True(t, ok, "unexpected archive path type: %T", result.Value)
	assert.True(t, testFilesystem().Exists(archivePath))

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, err := gzip.NewReader(bytes.NewReader(archiveData))
	require.NoError(t, err)
	defer reader.Close()

	decompressedData, err := io.ReadAll(reader)
	require.NoError(t, err)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	require.Len(t, lines, 1)

	archivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &archivedRow)
	require.True(t, unmarshalResult.OK, "archive line unmarshal failed: %v", unmarshalResult.Value)
	assert.Equal(t, "session-a", archivedRow["measurement"])

	remainingRows := requireResultRows(t, storeInstance.QueryJournal(""))
	require.Len(t, remainingRows, 1)
	assert.Equal(t, "session-b", remainingRows[0]["measurement"])
}

func TestCompact_Compact_Good_ZstdArchive(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()

	require.True(t,
		storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK,
	)

	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Now().Add(-48*time.Hour).UnixMilli(),
		"session-a",
	)
	require.NoError(t, err)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "zstd",
	})
	require.True(t, result.OK, "compact failed: %v", result.Value)

	archivePath, ok := result.Value.(string)
	require.True(t, ok, "unexpected archive path type: %T", result.Value)
	assert.True(t, testFilesystem().Exists(archivePath))
	assert.Contains(t, archivePath, ".jsonl.zst")

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, err := zstd.NewReader(bytes.NewReader(archiveData))
	require.NoError(t, err)
	defer reader.Close()

	decompressedData, err := io.ReadAll(reader)
	require.NoError(t, err)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	require.Len(t, lines, 1)

	archivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &archivedRow)
	require.True(t, unmarshalResult.OK, "archive line unmarshal failed: %v", unmarshalResult.Value)
	assert.Equal(t, "session-a", archivedRow["measurement"])
}

func TestCompact_Compact_Good_NoRows(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now(),
		Output: outputDirectory,
		Format: "gzip",
	})
	require.True(t, result.OK, "compact failed: %v", result.Value)
	assert.Equal(t, "", result.Value)
}

func TestCompact_Compact_Good_DeterministicOrderingForSameTimestamp(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	require.NoError(t, err)
	defer storeInstance.Close()
	require.NoError(t, ensureJournalSchema(storeInstance.sqliteDatabase))

	committedAt := time.Now().Add(-48 * time.Hour).UnixMilli()
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

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "gzip",
	})
	require.True(t, result.OK, "compact failed: %v", result.Value)

	archivePath, ok := result.Value.(string)
	require.True(t, ok, "unexpected archive path type: %T", result.Value)

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, err := gzip.NewReader(bytes.NewReader(archiveData))
	require.NoError(t, err)
	defer reader.Close()

	decompressedData, err := io.ReadAll(reader)
	require.NoError(t, err)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	require.Len(t, lines, 2)

	firstArchivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &firstArchivedRow)
	require.True(t, unmarshalResult.OK, "archive line unmarshal failed: %v", unmarshalResult.Value)
	assert.Equal(t, "session-b", firstArchivedRow["measurement"])

	secondArchivedRow := make(map[string]any)
	unmarshalResult = core.JSONUnmarshalString(lines[1], &secondArchivedRow)
	require.True(t, unmarshalResult.OK, "archive line unmarshal failed: %v", unmarshalResult.Value)
	assert.Equal(t, "session-a", secondArchivedRow["measurement"])
}

func TestCompact_CompactOptions_Good_Normalised(t *testing.T) {
	options := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
	}).Normalised()

	assert.Equal(t, defaultArchiveOutputDirectory, options.Output)
	assert.Equal(t, "gzip", options.Format)
}

func TestCompact_CompactOptions_Good_Validate(t *testing.T) {
	err := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Format: "zstd",
	}).Validate()
	require.NoError(t, err)
}

func TestCompact_CompactOptions_Bad_ValidateUnsupportedFormat(t *testing.T) {
	err := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Format: "zip",
	}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `format must be "gzip" or "zstd"`)
}
