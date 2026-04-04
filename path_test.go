package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPath_Normalise_Good_TrailingSlashes(t *testing.T) {
	assert.Equal(t, ".core/state/scroll-session.duckdb", workspaceFilePath(".core/state/", "scroll-session"))
	assert.Equal(t, ".core/archive/journal-20260404-010203.jsonl.gz", joinPath(".core/archive/", "journal-20260404-010203.jsonl.gz"))
	assert.Equal(t, ".core/archive", normaliseDirectoryPath(".core/archive///"))
}
