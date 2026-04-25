// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bufio"

	core "dappco.re/go/core"
	"github.com/parquet-go/parquet-go"
)

type readCloser interface {
	Read([]byte) (int, error)
	Close() error
}

type writeCloser interface {
	Write([]byte) (int, error)
	Close() error
}

// ChatMessage represents a single message in a chat conversation, used for
// reading JSONL training data during Parquet export and data import.
//
// Usage example:
//
//	msg := store.ChatMessage{Role: "user", Content: "What is sovereignty?"}
type ChatMessage struct {
	// Role is the message author role (e.g. "user", "assistant", "system").
	//
	// Usage example:
	//
	//	msg.Role // "user"
	Role string `json:"role"`

	// Content is the message text.
	//
	// Usage example:
	//
	//	msg.Content // "What is sovereignty?"
	Content string `json:"content"`
}

// ParquetRow is the schema for exported Parquet files.
//
// Usage example:
//
//	row := store.ParquetRow{Prompt: "What is sovereignty?", Response: "...", System: "You are LEM."}
type ParquetRow struct {
	// Prompt is the user prompt text.
	//
	// Usage example:
	//
	//	row.Prompt // "What is sovereignty?"
	Prompt string `parquet:"prompt"`

	// Response is the assistant response text.
	//
	// Usage example:
	//
	//	row.Response // "Sovereignty is..."
	Response string `parquet:"response"`

	// System is the system prompt text.
	//
	// Usage example:
	//
	//	row.System // "You are LEM."
	System string `parquet:"system"`

	// Messages is the JSON-encoded full conversation messages.
	//
	// Usage example:
	//
	//	row.Messages // `[{"role":"user","content":"..."}]`
	Messages string `parquet:"messages"`
}

// ExportParquet reads JSONL training splits (train.jsonl, valid.jsonl, test.jsonl)
// from trainingDir and writes Parquet files with snappy compression to outputDir.
// Returns total rows exported.
//
// Usage example:
//
//	total, err := store.ExportParquet("/Volumes/Data/lem/training", "/Volumes/Data/lem/parquet")
func ExportParquet(trainingDir, outputDir string) (int, error) {
	if outputDir == "" {
		outputDir = core.JoinPath(trainingDir, "parquet")
	}
	if r := localFs.EnsureDir(outputDir); !r.OK {
		return 0, core.E("store.ExportParquet", "create output directory", r.Value.(error))
	}

	total := 0
	for _, split := range []string{"train", "valid", "test"} {
		jsonlPath := core.JoinPath(trainingDir, split+".jsonl")
		if !localFs.IsFile(jsonlPath) {
			continue
		}

		n, err := ExportSplitParquet(jsonlPath, outputDir, split)
		if err != nil {
			return total, core.E("store.ExportParquet", core.Sprintf("export %s", split), err)
		}
		total += n
	}

	return total, nil
}

// ExportSplitParquet reads a chat JSONL file and writes a Parquet file for the
// given split name. Returns the number of rows written.
//
// Usage example:
//
//	n, err := store.ExportSplitParquet("/data/train.jsonl", "/data/parquet", "train")
func ExportSplitParquet(jsonlPath, outputDir, split string) (int, error) {
	openResult := localFs.Open(jsonlPath)
	if !openResult.OK {
		return 0, core.E("store.ExportSplitParquet", core.Sprintf("open %s", jsonlPath), openResult.Value.(error))
	}
	f := openResult.Value.(readCloser)
	defer f.Close()

	var rows []ParquetRow
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		text := core.Trim(scanner.Text())
		if text == "" {
			continue
		}

		var data struct {
			Messages []ChatMessage `json:"messages"`
		}
		if r := core.JSONUnmarshal([]byte(text), &data); !r.OK {
			continue
		}

		var prompt, response, system string
		for _, m := range data.Messages {
			switch m.Role {
			case "user":
				if prompt == "" {
					prompt = m.Content
				}
			case "assistant":
				if response == "" {
					response = m.Content
				}
			case "system":
				if system == "" {
					system = m.Content
				}
			}
		}

		msgsJSON := core.JSONMarshalString(data.Messages)
		rows = append(rows, ParquetRow{
			Prompt:   prompt,
			Response: response,
			System:   system,
			Messages: msgsJSON,
		})
	}

	if err := scanner.Err(); err != nil {
		return 0, core.E("store.ExportSplitParquet", core.Sprintf("scan %s", jsonlPath), err)
	}

	if len(rows) == 0 {
		return 0, nil
	}

	outPath := core.JoinPath(outputDir, split+".parquet")

	createResult := localFs.Create(outPath)
	if !createResult.OK {
		return 0, core.E("store.ExportSplitParquet", core.Sprintf("create %s", outPath), createResult.Value.(error))
	}
	out := createResult.Value.(writeCloser)

	writer := parquet.NewGenericWriter[ParquetRow](out,
		parquet.Compression(&parquet.Snappy),
	)

	if _, err := writer.Write(rows); err != nil {
		out.Close()
		return 0, core.E("store.ExportSplitParquet", "write parquet rows", err)
	}

	if err := writer.Close(); err != nil {
		out.Close()
		return 0, core.E("store.ExportSplitParquet", "close parquet writer", err)
	}

	if err := out.Close(); err != nil {
		return 0, core.E("store.ExportSplitParquet", "close file", err)
	}

	return len(rows), nil
}
