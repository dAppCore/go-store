// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bytes"
	"encoding/csv"
	goio "io"
	"io/fs"

	core "dappco.re/go"
)

// Medium is the minimal storage transport used by the go-store workspace
// import and export helpers and by Compact when writing cold archives.
//
// This structural interface matches the legacy core/io medium contract, so
// callers can pass any upstream medium implementation directly without an
// adapter while go-store no longer imports the legacy module path.
//
// Usage example: `medium, _ := local.New("/tmp/exports"); storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", Medium: medium})`
type Medium interface {
	Read(path string) (string, error)
	Write(path, content string) error
	WriteMode(path, content string, mode fs.FileMode) error
	EnsureDir(path string) error
	IsFile(path string) bool
	Delete(path string) error
	DeleteAll(path string) error
	Rename(oldPath, newPath string) error
	List(path string) ([]fs.DirEntry, error)
	Stat(path string) (fs.FileInfo, error)
	Open(path string) (fs.File, error)
	Create(path string) (goio.WriteCloser, error)
	Append(path string) (goio.WriteCloser, error)
	ReadStream(path string) (goio.ReadCloser, error)
	WriteStream(path string) (goio.WriteCloser, error)
	Exists(path string) bool
	IsDir(path string) bool
}

type filesystemmedium struct {
	filesystem *core.Fs
}

func localMedium() Medium {
	return &filesystemmedium{filesystem: (&core.Fs{}).NewUnrestricted()}
}

// Usage example: `content, err := localMedium().Read("archive.jsonl")`
func (medium *filesystemmedium) Read(path string) (string, error) {
	result := medium.filesystem.Read(path)
	if !result.OK {
		return "", resultError(result)
	}
	return result.Value.(string), nil
}

// Usage example: `err := localMedium().Write("archive.jsonl", payload)`
func (medium *filesystemmedium) Write(path, content string) error {
	result := medium.filesystem.Write(path, content)
	if !result.OK {
		return resultError(result)
	}
	return nil
}

// Usage example: `err := localMedium().WriteMode("archive.jsonl", payload, 0o600)`
func (medium *filesystemmedium) WriteMode(path, content string, mode fs.FileMode) error {
	result := medium.filesystem.WriteMode(path, content, mode)
	if !result.OK {
		return resultError(result)
	}
	return nil
}

// Usage example: `err := localMedium().EnsureDir("archives")`
func (medium *filesystemmedium) EnsureDir(path string) error {
	result := medium.filesystem.EnsureDir(path)
	if !result.OK {
		return resultError(result)
	}
	return nil
}

// Usage example: `exists := localMedium().IsFile("archive.jsonl")`
func (medium *filesystemmedium) IsFile(path string) bool {
	return medium.filesystem.IsFile(path)
}

// Usage example: `err := localMedium().Delete("archive.jsonl")`
func (medium *filesystemmedium) Delete(path string) error {
	result := medium.filesystem.Delete(path)
	if !result.OK {
		return resultError(result)
	}
	return nil
}

// Usage example: `err := localMedium().DeleteAll("archives")`
func (medium *filesystemmedium) DeleteAll(path string) error {
	result := medium.filesystem.DeleteAll(path)
	if !result.OK {
		return resultError(result)
	}
	return nil
}

// Usage example: `err := localMedium().Rename("old.jsonl", "new.jsonl")`
func (medium *filesystemmedium) Rename(oldPath, newPath string) error {
	result := medium.filesystem.Rename(oldPath, newPath)
	if !result.OK {
		return resultError(result)
	}
	return nil
}

// Usage example: `entries, err := localMedium().List("archives")`
func (medium *filesystemmedium) List(path string) ([]fs.DirEntry, error) {
	result := medium.filesystem.List(path)
	if !result.OK {
		return nil, resultError(result)
	}
	return result.Value.([]fs.DirEntry), nil
}

// Usage example: `info, err := localMedium().Stat("archive.jsonl")`
func (medium *filesystemmedium) Stat(path string) (fs.FileInfo, error) {
	result := medium.filesystem.Stat(path)
	if !result.OK {
		return nil, resultError(result)
	}
	return result.Value.(fs.FileInfo), nil
}

// Usage example: `file, err := localMedium().Open("archive.jsonl")`
func (medium *filesystemmedium) Open(path string) (fs.File, error) {
	result := medium.filesystem.Open(path)
	if !result.OK {
		return nil, resultError(result)
	}
	return result.Value.(fs.File), nil
}

// Usage example: `writer, err := localMedium().Create("archive.jsonl")`
func (medium *filesystemmedium) Create(path string) (goio.WriteCloser, error) {
	result := medium.filesystem.Create(path)
	if !result.OK {
		return nil, resultError(result)
	}
	return result.Value.(goio.WriteCloser), nil
}

// Usage example: `writer, err := localMedium().Append("archive.jsonl")`
func (medium *filesystemmedium) Append(path string) (goio.WriteCloser, error) {
	result := medium.filesystem.Append(path)
	if !result.OK {
		return nil, resultError(result)
	}
	return result.Value.(goio.WriteCloser), nil
}

// Usage example: `reader, err := localMedium().ReadStream("archive.jsonl")`
func (medium *filesystemmedium) ReadStream(path string) (goio.ReadCloser, error) {
	result := medium.filesystem.ReadStream(path)
	if !result.OK {
		return nil, resultError(result)
	}
	return result.Value.(goio.ReadCloser), nil
}

// Usage example: `writer, err := localMedium().WriteStream("archive.jsonl")`
func (medium *filesystemmedium) WriteStream(path string) (goio.WriteCloser, error) {
	result := medium.filesystem.WriteStream(path)
	if !result.OK {
		return nil, resultError(result)
	}
	return result.Value.(goio.WriteCloser), nil
}

// Usage example: `exists := localMedium().Exists("archive.jsonl")`
func (medium *filesystemmedium) Exists(path string) bool {
	return medium.filesystem.Exists(path)
}

// Usage example: `isDirectory := localMedium().IsDir("archives")`
func (medium *filesystemmedium) IsDir(path string) bool {
	return medium.filesystem.IsDir(path)
}

func resultError(result core.Result) error {
	if err, ok := result.Value.(error); ok {
		return err
	}
	return core.E("store.Medium", core.Sprint(result.Value), nil)
}

// Usage example: `medium, _ := local.New("/srv/core"); storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", Medium: medium})`
// WithMedium installs an io.Medium-compatible transport on the Store so that
// Compact archives and Import/Export helpers route through the medium instead
// of the raw filesystem.
func WithMedium(medium Medium) StoreOption {
	return func(storeInstance *Store) {
		if storeInstance == nil {
			return
		}
		storeInstance.medium = medium
	}
}

// Usage example: `medium := storeInstance.Medium(); if medium != nil { _ = medium.EnsureDir("exports") }`
func (storeInstance *Store) Medium() Medium {
	if storeInstance == nil {
		return nil
	}
	return storeInstance.medium
}

// Usage example: `err := store.Import(workspace, medium, "dataset.jsonl")`
// Import reads a JSON, JSONL, or CSV payload from the provided medium and
// appends each record to the workspace buffer as a `Put` entry. Format is
// chosen from the file extension: `.json` expects either a top-level array or
// `{"entries":[...]}` shape, `.jsonl`/`.ndjson` parse line-by-line, and `.csv`
// uses the first row as the header.
func Import(workspace *Workspace, medium Medium, path string) error {
	if workspace == nil {
		return core.E(opImport, "workspace is nil", nil)
	}
	if medium == nil {
		return core.E(opImport, "medium is nil", nil)
	}
	if path == "" {
		return core.E(opImport, "path is empty", nil)
	}

	content, err := medium.Read(path)
	if err != nil {
		return core.E(opImport, "read from medium", err)
	}

	kind := importEntryKind(path)
	switch lowercaseText(importExtension(path)) {
	case jsonlExtension, ".ndjson":
		return importJSONLines(workspace, kind, content)
	case ".csv":
		return importCSV(workspace, kind, content)
	case ".json":
		return importJSON(workspace, kind, content)
	default:
		return importJSONLines(workspace, kind, content)
	}
}

// Usage example: `err := store.Export(workspace, medium, "report.json")`
// Export writes the workspace aggregate summary to the medium at the given
// path. Format is chosen from the extension: `.jsonl` writes one record per
// query row, `.csv` writes header + rows, everything else writes the
// aggregate as JSON.
func Export(workspace *Workspace, medium Medium, path string) error {
	if workspace == nil {
		return core.E(opExport, "workspace is nil", nil)
	}
	if medium == nil {
		return core.E(opExport, "medium is nil", nil)
	}
	if path == "" {
		return core.E(opExport, "path is empty", nil)
	}

	if err := ensureMediumDir(medium, core.PathDir(path)); err != nil {
		return core.E(opExport, "ensure directory", err)
	}

	switch lowercaseText(importExtension(path)) {
	case jsonlExtension, ".ndjson":
		return exportJSONLines(workspace, medium, path)
	case ".csv":
		return exportCSV(workspace, medium, path)
	default:
		return exportJSON(workspace, medium, path)
	}
}

func ensureMediumDir(medium Medium, directory string) error {
	if directory == "" || directory == "." || directory == "/" {
		return nil
	}
	if err := medium.EnsureDir(directory); err != nil {
		return core.E("store.ensureMediumDir", "ensure directory", err)
	}
	return nil
}

func importExtension(path string) string {
	base := core.PathBase(path)
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == '.' {
			return base[i:]
		}
	}
	return ""
}

func importEntryKind(path string) string {
	base := core.PathBase(path)
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == '.' {
			base = base[:i]
			break
		}
	}
	if base == "" {
		return "entry"
	}
	return base
}

func importJSONLines(workspace *Workspace, kind, content string) error {
	scanner := core.Split(content, "\n")
	for _, rawLine := range scanner {
		line := core.Trim(rawLine)
		if line == "" {
			continue
		}
		record := map[string]any{}
		if result := core.JSONUnmarshalString(line, &record); !result.OK {
			err, _ := result.Value.(error)
			return core.E(opImport, "parse jsonl line", err)
		}
		if err := workspace.Put(kind, record); err != nil {
			return core.E(opImport, "put jsonl record", err)
		}
	}
	return nil
}

func importJSON(workspace *Workspace, kind, content string) error {
	trimmed := core.Trim(content)
	if trimmed == "" {
		return nil
	}

	var topLevel any
	if result := core.JSONUnmarshalString(trimmed, &topLevel); !result.OK {
		err, _ := result.Value.(error)
		return core.E(opImport, "parse json", err)
	}

	records, err := collectJSONRecords(topLevel)
	if err != nil {
		return core.E(opImport, "normalise json records", err)
	}
	for _, record := range records {
		if err := workspace.Put(kind, record); err != nil {
			return core.E(opImport, "put json record", err)
		}
	}
	return nil
}

func collectJSONRecords(value any) ([]map[string]any, error) {
	switch shape := value.(type) {
	case []any:
		records := make([]map[string]any, 0, len(shape))
		for index, entry := range shape {
			record, ok := entry.(map[string]any)
			if !ok {
				return nil, core.E(opImport, core.Concat("json array element is not an object at index ", core.Sprint(index)), nil)
			}
			records = append(records, record)
		}
		return records, nil
	case map[string]any:
		if nested, ok := shape["entries"].([]any); ok {
			return collectJSONRecords(nested)
		}
		if nested, ok := shape["records"].([]any); ok {
			return collectJSONRecords(nested)
		}
		if nested, ok := shape["data"].([]any); ok {
			return collectJSONRecords(nested)
		}
		return []map[string]any{shape}, nil
	}
	return nil, core.E(opImport, "unsupported json shape", nil)
}

func importCSV(workspace *Workspace, kind, content string) error {
	reader := csv.NewReader(bytes.NewBufferString(content))
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		return core.E(opImport, "parse csv", err)
	}
	if len(rows) == 0 {
		return nil
	}
	header := rows[0]
	if len(header) == 0 {
		return nil
	}
	for _, fields := range rows[1:] {
		if len(fields) == 0 {
			continue
		}
		record := make(map[string]any, len(header))
		for columnIndex, columnName := range header {
			if columnIndex < len(fields) {
				record[columnName] = fields[columnIndex]
			} else {
				record[columnName] = ""
			}
		}
		if err := workspace.Put(kind, record); err != nil {
			return core.E(opImport, "put csv record", err)
		}
	}
	return nil
}

func exportJSON(workspace *Workspace, medium Medium, path string) error {
	summary, err := workspace.aggregateFields()
	if err != nil {
		return core.E(opExport, "aggregate workspace", err)
	}
	content := core.JSONMarshalString(summary)
	if err := medium.Write(path, content); err != nil {
		return core.E(opExport, "write json", err)
	}
	return nil
}

func exportJSONLines(workspace *Workspace, medium Medium, path string) error {
	result := workspace.Query("SELECT entry_kind, entry_data, created_at FROM workspace_entries ORDER BY entry_id")
	if !result.OK {
		err, _ := result.Value.(error)
		return core.E(opExport, "query workspace", err)
	}
	rows, ok := result.Value.([]map[string]any)
	if !ok {
		rows = nil
	}

	builder := core.NewBuilder()
	for _, row := range rows {
		line := core.JSONMarshalString(row)
		builder.WriteString(line)
		builder.WriteString("\n")
	}
	if err := medium.Write(path, builder.String()); err != nil {
		return core.E(opExport, "write jsonl", err)
	}
	return nil
}

func exportCSV(workspace *Workspace, medium Medium, path string) error {
	result := workspace.Query("SELECT entry_kind, entry_data, created_at FROM workspace_entries ORDER BY entry_id")
	if !result.OK {
		err, _ := result.Value.(error)
		return core.E(opExport, "query workspace", err)
	}
	rows, ok := result.Value.([]map[string]any)
	if !ok {
		rows = nil
	}

	builder := core.NewBuilder()
	builder.WriteString("entry_kind,entry_data,created_at\n")
	for _, row := range rows {
		builder.WriteString(csvField(core.Sprint(row["entry_kind"])))
		builder.WriteString(",")
		builder.WriteString(csvField(core.Sprint(row["entry_data"])))
		builder.WriteString(",")
		builder.WriteString(csvField(core.Sprint(row["created_at"])))
		builder.WriteString("\n")
	}
	if err := medium.Write(path, builder.String()); err != nil {
		return core.E(opExport, "write csv", err)
	}
	return nil
}

func csvField(value string) string {
	needsQuote := false
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case ',', '"', '\n', '\r':
			needsQuote = true
		}
		if needsQuote {
			break
		}
	}
	if !needsQuote {
		return value
	}
	escaped := core.Replace(value, `"`, `""`)
	return core.Concat(`"`, escaped, `"`)
}
