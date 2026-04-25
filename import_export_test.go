// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportExport_Import_Good_CSVAndJSONIngestion(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("import-export-good")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	require.NoError(t, medium.Write("findings.csv", "tool,severity\ngosec,high\ngolint,low\n"))
	require.NoError(t, medium.Write("users.json", `{"entries":[{"name":"Alice"},{"name":"Bob"}]}`))

	require.NoError(t, Import(workspace, medium, "findings.csv"))
	require.NoError(t, Import(workspace, medium, "users.json"))

	assert.Equal(t, map[string]any{"findings": 2, "users": 2}, workspace.Aggregate())
}

func TestImportExport_Import_Bad_MalformedPayload(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("import-export-bad")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	require.NoError(t, medium.Write("broken.json", `{"entries":[{"name":"Alice"}`))

	require.Error(t, Import(workspace, medium, "broken.json"))

	count, err := workspace.Count()
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestImportExport_Import_Ugly_EmptyPayload(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("import-export-ugly")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	for _, path := range []string{"empty.csv", "empty.json", "empty.jsonl"} {
		require.NoError(t, medium.Write(path, ""))
		require.NoError(t, Import(workspace, medium, path))
	}

	assert.Equal(t, map[string]any{}, workspace.Aggregate())
}
