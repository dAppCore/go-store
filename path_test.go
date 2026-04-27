package store

import (
	"testing"
)

func TestPath_Normalise_Good_TrailingSlashes(t *testing.T) {
	assertEqual(t, ".core/state/scroll-session.duckdb", workspaceFilePath(".core/state/", "scroll-session"))
	assertEqual(t, ".core/archive/journal-20260404-010203.jsonl.gz", joinPath(".core/archive/", "journal-20260404-010203.jsonl.gz"))
	assertEqual(t, ".core/archive", normaliseDirectoryPath(".core/archive///"))
}
