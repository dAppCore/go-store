// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bytes"

	core "dappco.re/go/core"
	coreio "dappco.re/go/core/io"
)

// Medium is the minimal storage transport used by the go-store workspace
// import and export helpers and by Compact when writing cold archives.
//
// This is an alias of `dappco.re/go/core/io.Medium`, so callers can pass any
// upstream medium implementation directly without an adapter.
//
// Usage example: `medium, _ := local.New("/tmp/exports"); storeInstance, err := store.New(":memory:", store.WithMedium(medium))`
type Medium = coreio.Medium

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
		return core.E("store.Import", "workspace is nil", nil)
	}
	if medium == nil {
		return core.E("store.Import", "medium is nil", nil)
	}
	if path == "" {
		return core.E("store.Import", "path is empty", nil)
	}

	content, err := medium.Read(path)
	if err != nil {
		return core.E("store.Import", "read from medium", err)
	}

	kind := importEntryKind(path)
	switch lowercaseText(importExtension(path)) {
	case ".jsonl", ".ndjson":
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
		return core.E("store.Export", "workspace is nil", nil)
	}
	if medium == nil {
		return core.E("store.Export", "medium is nil", nil)
	}
	if path == "" {
		return core.E("store.Export", "path is empty", nil)
	}

	if err := ensureMediumDir(medium, core.PathDir(path)); err != nil {
		return core.E("store.Export", "ensure directory", err)
	}

	switch lowercaseText(importExtension(path)) {
	case ".jsonl", ".ndjson":
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
			return core.E("store.Import", "parse jsonl line", err)
		}
		if err := workspace.Put(kind, record); err != nil {
			return core.E("store.Import", "put jsonl record", err)
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
		return core.E("store.Import", "parse json", err)
	}

	records := collectJSONRecords(topLevel)
	for _, record := range records {
		if err := workspace.Put(kind, record); err != nil {
			return core.E("store.Import", "put json record", err)
		}
	}
	return nil
}

func collectJSONRecords(value any) []map[string]any {
	switch shape := value.(type) {
	case []any:
		records := make([]map[string]any, 0, len(shape))
		for _, entry := range shape {
			if record, ok := entry.(map[string]any); ok {
				records = append(records, record)
			}
		}
		return records
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
		return []map[string]any{shape}
	}
	return nil
}

func importCSV(workspace *Workspace, kind, content string) error {
	lines := core.Split(content, "\n")
	if len(lines) == 0 {
		return nil
	}
	header := splitCSVLine(lines[0])
	if len(header) == 0 {
		return nil
	}
	for _, rawLine := range lines[1:] {
		line := trimTrailingCarriageReturn(rawLine)
		if line == "" {
			continue
		}
		fields := splitCSVLine(line)
		record := make(map[string]any, len(header))
		for columnIndex, columnName := range header {
			if columnIndex < len(fields) {
				record[columnName] = fields[columnIndex]
			} else {
				record[columnName] = ""
			}
		}
		if err := workspace.Put(kind, record); err != nil {
			return core.E("store.Import", "put csv record", err)
		}
	}
	return nil
}

func splitCSVLine(line string) []string {
	line = trimTrailingCarriageReturn(line)
	buffer := &bytes.Buffer{}
	var (
		fields   []string
		inQuotes bool
	)
	for index := 0; index < len(line); index++ {
		character := line[index]
		switch {
		case character == '"' && inQuotes && index+1 < len(line) && line[index+1] == '"':
			buffer.WriteByte('"')
			index++
		case character == '"':
			inQuotes = !inQuotes
		case character == ',' && !inQuotes:
			fields = append(fields, buffer.String())
			buffer.Reset()
		default:
			buffer.WriteByte(character)
		}
	}
	fields = append(fields, buffer.String())
	return fields
}

func exportJSON(workspace *Workspace, medium Medium, path string) error {
	summary := workspace.Aggregate()
	content := core.JSONMarshalString(summary)
	if err := medium.Write(path, content); err != nil {
		return core.E("store.Export", "write json", err)
	}
	return nil
}

func exportJSONLines(workspace *Workspace, medium Medium, path string) error {
	result := workspace.Query("SELECT entry_kind, entry_data, created_at FROM workspace_entries ORDER BY entry_id")
	if !result.OK {
		err, _ := result.Value.(error)
		return core.E("store.Export", "query workspace", err)
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
		return core.E("store.Export", "write jsonl", err)
	}
	return nil
}

func exportCSV(workspace *Workspace, medium Medium, path string) error {
	result := workspace.Query("SELECT entry_kind, entry_data, created_at FROM workspace_entries ORDER BY entry_id")
	if !result.OK {
		err, _ := result.Value.(error)
		return core.E("store.Export", "query workspace", err)
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
		return core.E("store.Export", "write csv", err)
	}
	return nil
}

func trimTrailingCarriageReturn(value string) string {
	for len(value) > 0 && value[len(value)-1] == '\r' {
		value = value[:len(value)-1]
	}
	return value
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
