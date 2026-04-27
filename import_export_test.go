// SPDX-License-Identifier: EUPL-1.2

package store

import "testing"

func TestImportExport_Import_Good_CSVAndJSONIngestion(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("import-export-good")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertNoError(t, medium.Write("findings.csv", "tool,severity\ngosec,high\ngolint,low\n"))
	assertNoError(t, medium.Write("users.json", `{"entries":[{"name":"Alice"},{"name":"Bob"}]}`))

	assertNoError(t, Import(workspace, medium, "findings.csv"))
	assertNoError(t, Import(workspace, medium, "users.json"))

	assertEqual(t, map[string]any{"findings": 2, "users": 2}, workspace.Aggregate())
}

func TestImportExport_Import_Bad_MalformedPayload(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("import-export-bad")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertNoError(t, medium.Write("broken.json", `{"entries":[{"name":"Alice"}`))

	assertError(t, Import(workspace, medium, "broken.json"))

	count, err := workspace.Count()
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestImportExport_Import_Ugly_EmptyPayload(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("import-export-ugly")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	for _, path := range []string{"empty.csv", "empty.json", "empty.jsonl"} {
		assertNoError(t, medium.Write(path, ""))
		assertNoError(t, Import(workspace, medium, path))
	}

	assertEqual(t, map[string]any{}, workspace.Aggregate())
}
