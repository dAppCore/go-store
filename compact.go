package store

import (
	"bytes"
	"compress/gzip"
	"time"
	"unicode"

	core "dappco.re/go"
	"github.com/klauspost/compress/zstd"
)

var defaultArchiveOutputDirectory = ".core/archive/"

// Usage example: `options := store.CompactOptions{Before: time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC), Output: "/tmp/archive", Format: "gzip"}`
// Usage example: `result := storeInstance.Compact(store.CompactOptions{Before: time.Now().Add(-90 * 24 * time.Hour)})`
// Leave `Output` empty to write gzip JSONL archives under `.core/archive/`, or
// set `Format` to `zstd` when downstream tooling expects `.jsonl.zst`.
type CompactOptions struct {
	// Usage example: `options := store.CompactOptions{Before: time.Now().Add(-90 * 24 * time.Hour)}`
	Before time.Time
	// Usage example: `options := store.CompactOptions{Output: "/tmp/archive"}`
	Output string
	// Usage example: `options := store.CompactOptions{Format: "zstd"}`
	Format string
	// Usage example: `medium, _ := s3.New(s3.Options{Bucket: "archive"}); options := store.CompactOptions{Before: time.Now().Add(-90 * 24 * time.Hour), Medium: medium}`
	// Medium routes the archive write through a store.Medium instead of the raw
	// filesystem. When set, Output is the path inside the medium; leave empty
	// to use `.core/archive/`. When nil, Compact falls back to the store-level
	// medium (if configured via WithMedium), then to the local filesystem.
	Medium Medium
}

// Usage example: `normalisedOptions := (store.CompactOptions{Before: time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC)}).Normalised()`
func (compactOptions CompactOptions) Normalised() CompactOptions {
	if compactOptions.Output == "" {
		compactOptions.Output = defaultArchiveOutputDirectory
	}
	compactOptions.Format = lowercaseText(core.Trim(compactOptions.Format))
	if compactOptions.Format == "" {
		compactOptions.Format = "gzip"
	}
	return compactOptions
}

// Usage example: `if err := (store.CompactOptions{Before: time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC), Format: "gzip"}).Validate(); err != nil { return }`
func (compactOptions CompactOptions) Validate() error {
	if compactOptions.Before.IsZero() {
		return core.E(
			"store.CompactOptions.Validate",
			"before cutoff time is empty; use a value like time.Now().Add(-24 * time.Hour)",
			nil,
		)
	}
	switch lowercaseText(core.Trim(compactOptions.Format)) {
	case "", "gzip", "zstd":
		return nil
	default:
		return core.E(
			"store.CompactOptions.Validate",
			core.Concat(`format must be "gzip" or "zstd"; got `, compactOptions.Format),
			nil,
		)
	}
}

func lowercaseText(text string) string {
	builder := core.NewBuilder()
	for _, r := range text {
		builder.WriteRune(unicode.ToLower(r))
	}
	return builder.String()
}

type compactArchiveEntry struct {
	journalEntryID              int64
	journalBucketName           string
	journalMeasurementName      string
	journalFieldsJSON           string
	journalTagsJSON             string
	journalCommittedAtUnixMilli int64
}

// Usage example: `result := storeInstance.Compact(store.CompactOptions{Before: time.Now().Add(-30 * 24 * time.Hour), Output: "/tmp/archive", Format: "gzip"})`
func (storeInstance *Store) Compact(options CompactOptions) core.Result {
	if err := storeInstance.ensureReady(opCompact); err != nil {
		return core.Fail(err)
	}
	if err := ensureJournalSchema(storeInstance.sqliteDatabase); err != nil {
		return core.Fail(core.E(opCompact, "ensure journal schema", err))
	}

	options = options.Normalised()
	if err := options.Validate(); err != nil {
		return core.Fail(core.E(opCompact, "validate options", err))
	}

	medium := storeInstance.compactMedium(options)
	if medium == nil {
		return core.Fail(core.E(opCompact, "local medium is unavailable", nil))
	}
	if err := ensureMediumDir(medium, options.Output); err != nil {
		return core.Fail(core.E(opCompact, "ensure medium archive directory", err))
	}

	archiveEntries, err := storeInstance.compactArchiveEntries(options.Before)
	if err != nil {
		return core.Fail(err)
	}
	if len(archiveEntries) == 0 {
		return core.Ok("")
	}

	archiveContent, err := compactArchiveContent(archiveEntries, options.Format)
	if err != nil {
		return core.Fail(err)
	}
	outputPath := compactOutputPath(options.Output, options.Format)
	stagedOutputPath := core.Concat(outputPath, ".tmp")
	stagedOutputPublished := false
	if err := medium.Write(stagedOutputPath, archiveContent); err != nil {
		return core.Fail(core.E(opCompact, "write staged archive via medium", err))
	}
	defer cleanupStagedCompactArchive(medium, stagedOutputPath, &stagedOutputPublished)

	if err := storeInstance.markCompactEntriesArchived(archiveEntries); err != nil {
		return core.Fail(err)
	}
	stagedOutputPublished = true

	if err := medium.Rename(stagedOutputPath, outputPath); err != nil {
		return core.Fail(core.E(opCompact, "publish staged archive", err))
	}

	return core.Ok(outputPath)
}

func (storeInstance *Store) compactMedium(options CompactOptions) Medium {
	if options.Medium != nil {
		return options.Medium
	}
	if storeInstance.medium != nil {
		return storeInstance.medium
	}
	return localMedium()
}

func (storeInstance *Store) compactArchiveEntries(before time.Time) ([]compactArchiveEntry, error) {
	rows, queryErr := storeInstance.sqliteDatabase.Query(
		"SELECT entry_id, bucket_name, measurement, fields_json, tags_json, committed_at FROM "+journalEntriesTableName+" WHERE archived_at IS NULL AND committed_at < ? ORDER BY committed_at, entry_id",
		before.UnixMilli(),
	)
	if queryErr != nil {
		return nil, core.E(opCompact, "query journal rows", queryErr)
	}
	defer func() { _ = rows.Close() }()

	var archiveEntries []compactArchiveEntry
	for rows.Next() {
		var entry compactArchiveEntry
		if err := rows.Scan(
			&entry.journalEntryID,
			&entry.journalBucketName,
			&entry.journalMeasurementName,
			&entry.journalFieldsJSON,
			&entry.journalTagsJSON,
			&entry.journalCommittedAtUnixMilli,
		); err != nil {
			return nil, core.E(opCompact, "scan journal row", err)
		}
		archiveEntries = append(archiveEntries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, core.E(opCompact, "iterate journal rows", err)
	}
	return archiveEntries, nil
}

func compactArchiveContent(archiveEntries []compactArchiveEntry, format string) (string, error) {
	archiveContent, err := newCompactArchiveBuffer()
	if err != nil {
		return "", core.E(opCompact, "create archive buffer", err)
	}
	writer, err := archiveWriter(archiveContent, format)
	if err != nil {
		return "", err
	}
	archiveWriteFinished := false
	defer func() {
		if !archiveWriteFinished {
			_ = writer.Close()
		}
	}()

	for _, entry := range archiveEntries {
		lineMap, err := archiveEntryLine(entry)
		if err != nil {
			return "", err
		}
		lineJSON, err := marshalJSONText(lineMap, opCompact, "marshal archive line")
		if err != nil {
			return "", err
		}
		if _, err := writer.Write([]byte(lineJSON + "\n")); err != nil {
			return "", core.E(opCompact, "write archive line", err)
		}
	}
	if err := writer.Close(); err != nil {
		return "", core.E(opCompact, "close archive writer", err)
	}
	archiveWriteFinished = true
	compressedArchive, err := archiveContent.content()
	if err != nil {
		return "", core.E(opCompact, "read archive buffer", err)
	}
	return compressedArchive, nil
}

func cleanupStagedCompactArchive(medium Medium, stagedOutputPath string, published *bool) {
	if !*published && medium.Exists(stagedOutputPath) {
		_ = medium.Delete(stagedOutputPath)
	}
}

func (storeInstance *Store) markCompactEntriesArchived(archiveEntries []compactArchiveEntry) error {
	transaction, err := storeInstance.sqliteDatabase.Begin()
	if err != nil {
		return core.E(opCompact, "begin archive transaction", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()

	archivedAt := time.Now().UnixMilli()
	for _, entry := range archiveEntries {
		if _, err := transaction.Exec(
			sqlUpdatePrefix+journalEntriesTableName+" SET archived_at = ? WHERE entry_id = ?",
			archivedAt,
			entry.journalEntryID,
		); err != nil {
			return core.E(opCompact, "mark journal row archived", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return core.E(opCompact, "commit archive transaction", err)
	}
	committed = true
	return nil
}

func archiveEntryLine(entry compactArchiveEntry) (map[string]any, error) {
	fields := make(map[string]any)
	fieldsResult := core.JSONUnmarshalString(entry.journalFieldsJSON, &fields)
	if !fieldsResult.OK {
		return nil, core.E(opCompact, "unmarshal fields", fieldsResult.Value.(error))
	}

	tags := make(map[string]string)
	tagsResult := core.JSONUnmarshalString(entry.journalTagsJSON, &tags)
	if !tagsResult.OK {
		return nil, core.E(opCompact, "unmarshal tags", tagsResult.Value.(error))
	}

	return map[string]any{
		"bucket":       entry.journalBucketName,
		"measurement":  entry.journalMeasurementName,
		"fields":       fields,
		"tags":         tags,
		"committed_at": entry.journalCommittedAtUnixMilli,
	}, nil
}

type compactArchiveWriter interface {
	Write([]byte) (int, error)
	Close() error
}

type compactArchiveWriteTarget interface {
	Write([]byte) (int, error)
}

type compactarchivebuffer struct {
	buffer bytes.Buffer
}

func newCompactArchiveBuffer() (*compactarchivebuffer, error) {
	return &compactarchivebuffer{}, nil
}

// Usage example: `buffer, _ := newCompactArchiveBuffer(); _, _ = buffer.Write([]byte("archive"))`
func (buffer *compactarchivebuffer) Write(data []byte) (int, error) {
	return buffer.buffer.Write(data)
}

func (buffer *compactarchivebuffer) content() (string, error) {
	return buffer.buffer.String(), nil
}

func archiveWriter(writer compactArchiveWriteTarget, format string) (compactArchiveWriter, error) {
	switch format {
	case "gzip":
		return gzip.NewWriter(writer), nil
	case "zstd":
		zstdWriter, err := zstd.NewWriter(writer)
		if err != nil {
			return nil, core.E(opCompact, "create zstd writer", err)
		}
		return zstdWriter, nil
	default:
		return nil, core.E(opCompact, core.Concat("unsupported archive format: ", format), nil)
	}
}

func compactOutputPath(outputDirectory, format string) string {
	extension := jsonlExtension
	if format == "gzip" {
		extension = ".jsonl.gz"
	}
	if format == "zstd" {
		extension = ".jsonl.zst"
	}
	// Include nanoseconds so two compactions in the same second never collide.
	filename := core.Concat("journal-", time.Now().UTC().Format("20060102-150405.000000000"), extension)
	return joinPath(outputDirectory, filename)
}
