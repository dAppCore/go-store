package store

import (
	"compress/gzip"
	"io"
	"time"

	core "dappco.re/go/core"
	"github.com/klauspost/compress/zstd"
)

var defaultArchiveOutputDirectory = ".core/archive/"

// CompactOptions selects which completed journal rows move into cold archive
// output and where the compressed file is written.
//
// Usage example: `options := store.CompactOptions{Before: time.Now().Add(-90 * 24 * time.Hour), Output: "/tmp/archive", Format: "gzip"}`
type CompactOptions struct {
	// Usage example: `options := store.CompactOptions{Before: time.Now().Add(-90 * 24 * time.Hour)}`
	Before time.Time
	// Usage example: `options := store.CompactOptions{Output: "/tmp/archive"}`
	Output string
	// Usage example: `options := store.CompactOptions{Format: "zstd"}`
	Format string
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
	if err := storeInstance.ensureReady("store.Compact"); err != nil {
		return core.Result{Value: err, OK: false}
	}
	if err := ensureJournalSchema(storeInstance.sqliteDatabase); err != nil {
		return core.Result{Value: core.E("store.Compact", "ensure journal schema", err), OK: false}
	}

	outputDirectory := options.Output
	if outputDirectory == "" {
		outputDirectory = defaultArchiveOutputDirectory
	}
	format := options.Format
	if format == "" {
		format = "gzip"
	}
	if format != "gzip" && format != "zstd" {
		return core.Result{Value: core.E("store.Compact", core.Concat("unsupported archive format: ", format), nil), OK: false}
	}

	filesystem := (&core.Fs{}).NewUnrestricted()
	if result := filesystem.EnsureDir(outputDirectory); !result.OK {
		return core.Result{Value: core.E("store.Compact", "ensure archive directory", result.Value.(error)), OK: false}
	}

	rows, err := storeInstance.sqliteDatabase.Query(
		"SELECT entry_id, bucket_name, measurement, fields_json, tags_json, committed_at FROM "+journalEntriesTableName+" WHERE archived_at IS NULL AND committed_at < ? ORDER BY committed_at, entry_id",
		options.Before.UnixMilli(),
	)
	if err != nil {
		return core.Result{Value: core.E("store.Compact", "query journal rows", err), OK: false}
	}
	defer rows.Close()

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
			return core.Result{Value: core.E("store.Compact", "scan journal row", err), OK: false}
		}
		archiveEntries = append(archiveEntries, entry)
	}
	if err := rows.Err(); err != nil {
		return core.Result{Value: core.E("store.Compact", "iterate journal rows", err), OK: false}
	}
	if len(archiveEntries) == 0 {
		return core.Result{Value: "", OK: true}
	}

	outputPath := compactOutputPath(outputDirectory, format)
	archiveFileResult := filesystem.Create(outputPath)
	if !archiveFileResult.OK {
		return core.Result{Value: core.E("store.Compact", "create archive file", archiveFileResult.Value.(error)), OK: false}
	}

	file, ok := archiveFileResult.Value.(io.WriteCloser)
	if !ok {
		return core.Result{Value: core.E("store.Compact", "archive file is not writable", nil), OK: false}
	}
	fileClosed := false
	defer func() {
		if !fileClosed {
			_ = file.Close()
		}
	}()

	writer, err := archiveWriter(file, format)
	if err != nil {
		return core.Result{Value: err, OK: false}
	}
	writeOK := false
	defer func() {
		if !writeOK {
			_ = writer.Close()
		}
	}()

	for _, entry := range archiveEntries {
		lineMap, err := archiveEntryLine(entry)
		if err != nil {
			return core.Result{Value: err, OK: false}
		}
		lineJSON, err := marshalJSONText(lineMap, "store.Compact", "marshal archive line")
		if err != nil {
			return core.Result{Value: err, OK: false}
		}
		if _, err := io.WriteString(writer, lineJSON+"\n"); err != nil {
			return core.Result{Value: core.E("store.Compact", "write archive line", err), OK: false}
		}
	}
	if err := writer.Close(); err != nil {
		return core.Result{Value: core.E("store.Compact", "close archive writer", err), OK: false}
	}
	writeOK = true
	if err := file.Close(); err != nil {
		return core.Result{Value: core.E("store.Compact", "close archive file", err), OK: false}
	}
	fileClosed = true

	transaction, err := storeInstance.sqliteDatabase.Begin()
	if err != nil {
		return core.Result{Value: core.E("store.Compact", "begin archive transaction", err), OK: false}
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
			"UPDATE "+journalEntriesTableName+" SET archived_at = ? WHERE entry_id = ?",
			archivedAt,
			entry.journalEntryID,
		); err != nil {
			return core.Result{Value: core.E("store.Compact", "mark journal row archived", err), OK: false}
		}
	}
	if err := transaction.Commit(); err != nil {
		return core.Result{Value: core.E("store.Compact", "commit archive transaction", err), OK: false}
	}
	committed = true

	return core.Result{Value: outputPath, OK: true}
}

func archiveEntryLine(entry compactArchiveEntry) (map[string]any, error) {
	fields := make(map[string]any)
	fieldsResult := core.JSONUnmarshalString(entry.journalFieldsJSON, &fields)
	if !fieldsResult.OK {
		return nil, core.E("store.Compact", "unmarshal fields", fieldsResult.Value.(error))
	}

	tags := make(map[string]string)
	tagsResult := core.JSONUnmarshalString(entry.journalTagsJSON, &tags)
	if !tagsResult.OK {
		return nil, core.E("store.Compact", "unmarshal tags", tagsResult.Value.(error))
	}

	return map[string]any{
		"bucket":       entry.journalBucketName,
		"measurement":  entry.journalMeasurementName,
		"fields":       fields,
		"tags":         tags,
		"committed_at": entry.journalCommittedAtUnixMilli,
	}, nil
}

func archiveWriter(writer io.Writer, format string) (io.WriteCloser, error) {
	switch format {
	case "gzip":
		return gzip.NewWriter(writer), nil
	case "zstd":
		zstdWriter, err := zstd.NewWriter(writer)
		if err != nil {
			return nil, core.E("store.Compact", "create zstd writer", err)
		}
		return zstdWriter, nil
	default:
		return nil, core.E("store.Compact", core.Concat("unsupported archive format: ", format), nil)
	}
}

func compactOutputPath(outputDirectory, format string) string {
	extension := ".jsonl"
	if format == "gzip" {
		extension = ".jsonl.gz"
	}
	if format == "zstd" {
		extension = ".jsonl.zst"
	}
	filename := core.Concat("journal-", time.Now().UTC().Format("20060102-150405"), extension)
	return joinPath(outputDirectory, filename)
}
