package store

import (
	"compress/gzip"
	"io"
	"time"
	"unicode"

	core "dappco.re/go/core"
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
	// Medium routes the archive write through an io.Medium instead of the raw
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
	if err := storeInstance.ensureReady("store.Compact"); err != nil {
		return core.Result{Value: err, OK: false}
	}
	if err := ensureJournalSchema(storeInstance.sqliteDatabase); err != nil {
		return core.Result{Value: core.E("store.Compact", "ensure journal schema", err), OK: false}
	}

	options = options.Normalised()
	if err := options.Validate(); err != nil {
		return core.Result{Value: core.E("store.Compact", "validate options", err), OK: false}
	}

	medium := options.Medium
	if medium == nil {
		medium = storeInstance.medium
	}

	filesystem := (&core.Fs{}).NewUnrestricted()
	if medium == nil {
		if result := filesystem.EnsureDir(options.Output); !result.OK {
			return core.Result{Value: core.E("store.Compact", "ensure archive directory", result.Value.(error)), OK: false}
		}
	} else if err := ensureMediumDir(medium, options.Output); err != nil {
		return core.Result{Value: core.E("store.Compact", "ensure medium archive directory", err), OK: false}
	}

	rows, queryErr := storeInstance.sqliteDatabase.Query(
		"SELECT entry_id, bucket_name, measurement, fields_json, tags_json, committed_at FROM "+journalEntriesTableName+" WHERE archived_at IS NULL AND committed_at < ? ORDER BY committed_at, entry_id",
		options.Before.UnixMilli(),
	)
	if queryErr != nil {
		return core.Result{Value: core.E("store.Compact", "query journal rows", queryErr), OK: false}
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

	outputPath := compactOutputPath(options.Output, options.Format)
	var (
		file       io.WriteCloser
		createErr  error
	)
	if medium != nil {
		file, createErr = medium.Create(outputPath)
		if createErr != nil {
			return core.Result{Value: core.E("store.Compact", "create archive via medium", createErr), OK: false}
		}
	} else {
		archiveFileResult := filesystem.Create(outputPath)
		if !archiveFileResult.OK {
			return core.Result{Value: core.E("store.Compact", "create archive file", archiveFileResult.Value.(error)), OK: false}
		}
		existingFile, ok := archiveFileResult.Value.(io.WriteCloser)
		if !ok {
			return core.Result{Value: core.E("store.Compact", "archive file is not writable", nil), OK: false}
		}
		file = existingFile
	}
	archiveFileClosed := false
	defer func() {
		if !archiveFileClosed {
			_ = file.Close()
		}
	}()

	writer, err := archiveWriter(file, options.Format)
	if err != nil {
		return core.Result{Value: err, OK: false}
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
	archiveWriteFinished = true
	if err := file.Close(); err != nil {
		return core.Result{Value: core.E("store.Compact", "close archive file", err), OK: false}
	}
	archiveFileClosed = true

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
	// Include nanoseconds so two compactions in the same second never collide.
	filename := core.Concat("journal-", time.Now().UTC().Format("20060102-150405.000000000"), extension)
	return joinPath(outputDirectory, filename)
}
