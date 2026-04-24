package store

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/klauspost/compress/zstd"
)

func TestCompact_Compact_Good_GzipArchive(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer storeInstance.Close()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK)
	assertTrue(t, storeInstance.CommitToJournal("session-b", map[string]any{"like": 2}, map[string]string{"workspace": "session-b"}).OK)

	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Now().Add(-48*time.Hour).UnixMilli(),
		"session-a",
	)
	assertNoError(t, err)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "gzip",
	})
	assertTruef(t, result.OK, "compact failed: %v", result.Value)

	archivePath, ok := result.Value.(string)
	assertTruef(t, ok, "unexpected archive path type: %T", result.Value)
	assertTrue(t, testFilesystem().Exists(archivePath))

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, err := gzip.NewReader(bytes.NewReader(archiveData))
	assertNoError(t, err)
	defer reader.Close()

	decompressedData, err := io.ReadAll(reader)
	assertNoError(t, err)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	assertLen(t, lines, 1)

	archivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &archivedRow)
	assertTruef(t, unmarshalResult.OK, "archive line unmarshal failed: %v", unmarshalResult.Value)
	assertEqual(t, "session-a", archivedRow["measurement"])

	remainingRows := requireResultRows(t, storeInstance.QueryJournal(""))
	assertLen(t, remainingRows, 1)
	assertEqual(t, "session-b", remainingRows[0]["measurement"])
}

func TestCompact_Compact_Good_ZstdArchive(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer storeInstance.Close()

	assertTrue(t, storeInstance.CommitToJournal("session-a", map[string]any{"like": 1}, map[string]string{"workspace": "session-a"}).OK)

	_, err = storeInstance.sqliteDatabase.Exec(
		"UPDATE "+journalEntriesTableName+" SET committed_at = ? WHERE measurement = ?",
		time.Now().Add(-48*time.Hour).UnixMilli(),
		"session-a",
	)
	assertNoError(t, err)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "zstd",
	})
	assertTruef(t, result.OK, "compact failed: %v", result.Value)

	archivePath, ok := result.Value.(string)
	assertTruef(t, ok, "unexpected archive path type: %T", result.Value)
	assertTrue(t, testFilesystem().Exists(archivePath))
	assertContainsString(t, archivePath, ".jsonl.zst")

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, err := zstd.NewReader(bytes.NewReader(archiveData))
	assertNoError(t, err)
	defer reader.Close()

	decompressedData, err := io.ReadAll(reader)
	assertNoError(t, err)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	assertLen(t, lines, 1)

	archivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &archivedRow)
	assertTruef(t, unmarshalResult.OK, "archive line unmarshal failed: %v", unmarshalResult.Value)
	assertEqual(t, "session-a", archivedRow["measurement"])
}

func TestCompact_Compact_Good_NoRows(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer storeInstance.Close()

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now(),
		Output: outputDirectory,
		Format: "gzip",
	})
	assertTruef(t, result.OK, "compact failed: %v", result.Value)
	assertEqual(t, "", result.Value)
}

func TestCompact_Compact_Good_DeterministicOrderingForSameTimestamp(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	assertNoError(t, err)
	defer storeInstance.Close()
	assertNoError(t, ensureJournalSchema(storeInstance.sqliteDatabase))

	committedAt := time.Now().Add(-48 * time.Hour).UnixMilli()
	assertNoError(t, commitJournalEntry( storeInstance.sqliteDatabase, "events", "session-b", `{"like":2}`, `{"workspace":"session-b"}`, committedAt, ))
	assertNoError(t, commitJournalEntry( storeInstance.sqliteDatabase, "events", "session-a", `{"like":1}`, `{"workspace":"session-a"}`, committedAt, ))

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "gzip",
	})
	assertTruef(t, result.OK, "compact failed: %v", result.Value)

	archivePath, ok := result.Value.(string)
	assertTruef(t, ok, "unexpected archive path type: %T", result.Value)

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, err := gzip.NewReader(bytes.NewReader(archiveData))
	assertNoError(t, err)
	defer reader.Close()

	decompressedData, err := io.ReadAll(reader)
	assertNoError(t, err)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	assertLen(t, lines, 2)

	firstArchivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &firstArchivedRow)
	assertTruef(t, unmarshalResult.OK, "archive line unmarshal failed: %v", unmarshalResult.Value)
	assertEqual(t, "session-b", firstArchivedRow["measurement"])

	secondArchivedRow := make(map[string]any)
	unmarshalResult = core.JSONUnmarshalString(lines[1], &secondArchivedRow)
	assertTruef(t, unmarshalResult.OK, "archive line unmarshal failed: %v", unmarshalResult.Value)
	assertEqual(t, "session-a", secondArchivedRow["measurement"])
}

func TestCompact_CompactOptions_Good_Normalised(t *testing.T) {
	options := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
	}).Normalised()

	assertEqual(t, defaultArchiveOutputDirectory, options.Output)
	assertEqual(t, "gzip", options.Format)
}

func TestCompact_CompactOptions_Good_Validate(t *testing.T) {
	err := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Format: "zstd",
	}).Validate()
	assertNoError(t, err)
}

func TestCompact_CompactOptions_Bad_ValidateMissingCutoff(t *testing.T) {
	err := (CompactOptions{
		Format: "gzip",
	}).Validate()
	assertError(t, err)
	assertContainsString(t, err.Error(), "before cutoff time is empty")
}

func TestCompact_CompactOptions_Good_ValidateNormalisesFormatCase(t *testing.T) {
	err := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Format: " GZIP ",
	}).Validate()
	assertNoError(t, err)

	options := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Format: " ZsTd ",
	}).Normalised()
	assertEqual(t, "zstd", options.Format)
}

func TestCompact_CompactOptions_Good_ValidateWhitespaceFormatDefaultsToGzip(t *testing.T) {
	options := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Format: "   ",
	}).Normalised()

	assertEqual(t, "gzip", options.Format)
	assertNoError(t, options.Validate())
}

func TestCompact_CompactOptions_Bad_ValidateUnsupportedFormat(t *testing.T) {
	err := (CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Format: "zip",
	}).Validate()
	assertError(t, err)
	assertContainsString(t, err.Error(), `format must be "gzip" or "zstd"`)
}
