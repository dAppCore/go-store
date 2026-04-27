// SPDX-License-Identifier: EUPL-1.2

package store

import core "dappco.re/go/core"

// ChatMessage represents a single message in a chat conversation, used for
// reading JSONL training data during data import.
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

// ParquetRow describes the lightweight row shape used by external Parquet
// exporters.
//
// Usage example:
//
//	row := store.ParquetRow{Prompt: "What is sovereignty?", Response: "Sovereignty is...", System: "You are LEM."}
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
	//	row.Messages // `[{"role":"user","content":"What is sovereignty?"}]`
	Messages string `parquet:"messages"`
}

// ExportParquet reports that Parquet export is intentionally kept outside the
// core package dependency graph.
//
// Usage example:
//
//	_, err := store.ExportParquet("/Volumes/Data/lem/training", "/Volumes/Data/lem/parquet")
func ExportParquet(trainingDir, outputDir string) (int, error) {
	return 0, core.E(
		"store.ExportParquet",
		"Parquet export requires an external tool so core does not ship a runtime Parquet dependency",
		nil,
	)
}

// ExportSplitParquet reports that split-level Parquet export is intentionally
// kept outside the core package dependency graph.
//
// Usage example:
//
//	_, err := store.ExportSplitParquet("/data/train.jsonl", "/data/parquet", "train")
func ExportSplitParquet(jsonlPath, outputDir, split string) (int, error) {
	return 0, core.E(
		"store.ExportSplitParquet",
		"Parquet export requires an external tool so core does not ship a runtime Parquet dependency",
		nil,
	)
}
