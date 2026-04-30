package store

import (
	"compress/gzip"
	"io"
	"testing"
	"time"

	core "dappco.re/go"
	"github.com/klauspost/compress/zstd"
)

func TestCompact_Compact_Good_GzipArchive(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 1}, map[string]string{"workspace": testSessionA}).OK)
	assertTrue(t, storeInstance.CommitToJournal(testSessionB, map[string]any{"like": 2}, map[string]string{"workspace": testSessionB}).OK)

	_, sqlErr := storeInstance.sqliteDatabase.Exec(
		testSQLUpdatePrefix+journalEntriesTableName+testSetCommittedAtByMeasurementSQL,
		time.Now().Add(-48*time.Hour).UnixMilli(),
		testSessionA,
	)
	assertNoError(t, sqlErr)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "gzip",
	})
	assertTruef(t, result.OK, testCompactFailedFormat, result.Value)

	archivePath, ok := result.Value.(string)
	assertTruef(t, ok, testUnexpectedArchivePathTypeFormat, result.Value)
	assertTrue(t, testFilesystem().Exists(archivePath))

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, gzipErr := gzip.NewReader(core.NewBuffer(archiveData))
	assertNoError(t, gzipErr)
	defer func() {
		_ = reader.Close()
	}()

	decompressedData, readErr := io.ReadAll(reader)
	assertNoError(t, readErr)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	assertLen(t, lines, 1)

	archivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &archivedRow)
	assertTruef(t, unmarshalResult.OK, testArchiveLineUnmarshalFailedFormat, unmarshalResult.Value)
	assertEqual(t, testSessionA, archivedRow["measurement"])

	remainingRows := requireResultRows(t, storeInstance.QueryJournal(""))
	assertLen(t, remainingRows, 1)
	assertEqual(t, testSessionB, remainingRows[0]["measurement"])
}

func TestCompact_Compact_Good_ZstdArchive(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal(testSessionA, map[string]any{"like": 1}, map[string]string{"workspace": testSessionA}).OK)

	_, sqlErr := storeInstance.sqliteDatabase.Exec(
		testSQLUpdatePrefix+journalEntriesTableName+testSetCommittedAtByMeasurementSQL,
		time.Now().Add(-48*time.Hour).UnixMilli(),
		testSessionA,
	)
	assertNoError(t, sqlErr)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "zstd",
	})
	assertTruef(t, result.OK, testCompactFailedFormat, result.Value)

	archivePath, ok := result.Value.(string)
	assertTruef(t, ok, testUnexpectedArchivePathTypeFormat, result.Value)
	assertTrue(t, testFilesystem().Exists(archivePath))
	assertContainsString(t, archivePath, ".jsonl.zst")

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, zstdErr := zstd.NewReader(core.NewBuffer(archiveData))
	assertNoError(t, zstdErr)
	defer reader.Close()

	decompressedData, readErr := io.ReadAll(reader)
	assertNoError(t, readErr)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	assertLen(t, lines, 1)

	archivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &archivedRow)
	assertTruef(t, unmarshalResult.OK, testArchiveLineUnmarshalFailedFormat, unmarshalResult.Value)
	assertEqual(t, testSessionA, archivedRow["measurement"])
}

func TestCompact_Compact_Good_NoRows(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now(),
		Output: outputDirectory,
		Format: "gzip",
	})
	assertTruef(t, result.OK, testCompactFailedFormat, result.Value)
	assertEqual(t, "", result.Value)
}

func TestCompact_Compact_Good_DeterministicOrderingForSameTimestamp(t *testing.T) {
	outputDirectory := useArchiveOutputDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()
	assertNoError(t, ensureJournalSchema(storeInstance.sqliteDatabase))

	committedAt := time.Now().Add(-48 * time.Hour).UnixMilli()
	assertNoError(t, commitJournalEntry(storeInstance.sqliteDatabase, "events", testSessionB, `{"like":2}`, `{"workspace":"session-b"}`, committedAt))
	assertNoError(t, commitJournalEntry(storeInstance.sqliteDatabase, "events", testSessionA, `{"like":1}`, `{"workspace":"session-a"}`, committedAt))

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(-24 * time.Hour),
		Output: outputDirectory,
		Format: "gzip",
	})
	assertTruef(t, result.OK, testCompactFailedFormat, result.Value)

	archivePath, ok := result.Value.(string)
	assertTruef(t, ok, testUnexpectedArchivePathTypeFormat, result.Value)

	archiveData := requireCoreReadBytes(t, archivePath)
	reader, gzipErr := gzip.NewReader(core.NewBuffer(archiveData))
	assertNoError(t, gzipErr)
	defer func() {
		_ = reader.Close()
	}()

	decompressedData, readErr := io.ReadAll(reader)
	assertNoError(t, readErr)
	lines := core.Split(core.Trim(string(decompressedData)), "\n")
	assertLen(t, lines, 2)

	firstArchivedRow := make(map[string]any)
	unmarshalResult := core.JSONUnmarshalString(lines[0], &firstArchivedRow)
	assertTruef(t, unmarshalResult.OK, testArchiveLineUnmarshalFailedFormat, unmarshalResult.Value)
	assertEqual(t, testSessionB, firstArchivedRow["measurement"])

	secondArchivedRow := make(map[string]any)
	unmarshalResult = core.JSONUnmarshalString(lines[1], &secondArchivedRow)
	assertTruef(t, unmarshalResult.OK, testArchiveLineUnmarshalFailedFormat, unmarshalResult.Value)
	assertEqual(t, testSessionA, secondArchivedRow["measurement"])
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

func TestCompact_Store_Compact_Good(t *T) {
	storeInstance := fixtureStore(t)
	RequireTrue(t, storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, nil).OK)
	result := storeInstance.Compact(CompactOptions{Before: Now().Add(Second), Output: t.TempDir(), Format: "gzip"})
	AssertTrue(t, result.OK)
	AssertContains(t, result.Value.(string), ".jsonl.gz")
}

func TestCompact_Store_Compact_Bad(t *T) {
	storeInstance := fixtureStore(t)
	result := storeInstance.Compact(CompactOptions{})
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "before")
}

func TestCompact_Store_Compact_Ugly(t *T) {
	storeInstance := fixtureStore(t)
	result := storeInstance.Compact(CompactOptions{Before: Now().Add(-Hour), Output: t.TempDir(), Format: "zstd"})
	AssertTrue(t, result.OK)
	AssertEqual(t, "", result.Value)
}

func TestCompact_CompactOptions_Normalised_Good(t *T) {
	options := CompactOptions{Before: Now()}
	normalised := options.Normalised()
	AssertEqual(t, "gzip", normalised.Format)
	AssertNotEmpty(t, normalised.Output)
}

func TestCompact_CompactOptions_Normalised_Bad(t *T) {
	options := CompactOptions{Format: "zstd", Output: "out"}
	normalised := options.Normalised()
	AssertEqual(t, "zstd", normalised.Format)
	AssertEqual(t, "out", normalised.Output)
}

func TestCompact_CompactOptions_Normalised_Ugly(t *T) {
	options := CompactOptions{Format: "  ", Output: ""}
	normalised := options.Normalised()
	AssertEqual(t, "gzip", normalised.Format)
	AssertNotEmpty(t, normalised.Output)
}

func TestCompact_CompactOptions_Validate_Good(t *T) {
	options := CompactOptions{Before: Now(), Format: "gzip", Output: t.TempDir()}
	err := options.Validate()
	AssertNoError(t, err)
}

func TestCompact_CompactOptions_Validate_Bad(t *T) {
	options := CompactOptions{Format: "gzip", Output: t.TempDir()}
	err := options.Validate()
	AssertError(t, err)
}

func TestCompact_CompactOptions_Validate_Ugly(t *T) {
	options := CompactOptions{Before: Now(), Format: "brotli", Output: t.TempDir()}
	err := options.Validate()
	AssertError(t, err)
}
