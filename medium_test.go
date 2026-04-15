// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bytes"
	goio "io"
	"io/fs"
	"sync"
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memoryMedium is an in-memory implementation of `store.Medium` used by the
// medium tests so assertions do not depend on the local filesystem.
type memoryMedium struct {
	lock  sync.Mutex
	files map[string]string
}

func newMemoryMedium() *memoryMedium {
	return &memoryMedium{files: make(map[string]string)}
}

func (medium *memoryMedium) Read(path string) (string, error) {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	content, ok := medium.files[path]
	if !ok {
		return "", core.E("memoryMedium.Read", "file not found: "+path, nil)
	}
	return content, nil
}

func (medium *memoryMedium) Write(path, content string) error {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	medium.files[path] = content
	return nil
}

func (medium *memoryMedium) EnsureDir(string) error { return nil }

func (medium *memoryMedium) Create(path string) (goio.WriteCloser, error) {
	return &memoryWriter{medium: medium, path: path}, nil
}

func (medium *memoryMedium) Exists(path string) bool {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	_, ok := medium.files[path]
	return ok
}

type memoryWriter struct {
	medium *memoryMedium
	path   string
	buffer bytes.Buffer
	closed bool
}

func (writer *memoryWriter) Write(data []byte) (int, error) {
	return writer.buffer.Write(data)
}

func (writer *memoryWriter) Close() error {
	if writer.closed {
		return nil
	}
	writer.closed = true
	return writer.medium.Write(writer.path, writer.buffer.String())
}

// Ensure memoryMedium still satisfies the internal Medium contract.
var _ Medium = (*memoryMedium)(nil)

// Compile-time check for fs.FileInfo usage in the tests.
var _ fs.FileInfo = (*FileInfoStub)(nil)

type FileInfoStub struct{}

func (FileInfoStub) Name() string       { return "" }
func (FileInfoStub) Size() int64        { return 0 }
func (FileInfoStub) Mode() fs.FileMode  { return 0 }
func (FileInfoStub) ModTime() time.Time { return time.Time{} }
func (FileInfoStub) IsDir() bool        { return false }
func (FileInfoStub) Sys() any           { return nil }

func TestMedium_WithMedium_Good(t *testing.T) {
	useWorkspaceStateDirectory(t)

	medium := newMemoryMedium()
	storeInstance, err := New(":memory:", WithMedium(medium))
	require.NoError(t, err)
	defer storeInstance.Close()

	assert.Same(t, medium, storeInstance.Medium(), "medium should round-trip via accessor")
	assert.Same(t, medium, storeInstance.Config().Medium, "medium should appear in Config()")
}

func TestMedium_WithMedium_Bad_NilKeepsFilesystemBackend(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	assert.Nil(t, storeInstance.Medium())
}

func TestMedium_WithMedium_Good_PersistsDatabaseThroughMedium(t *testing.T) {
	useWorkspaceStateDirectory(t)

	medium := newMemoryMedium()

	storeInstance, err := New("app.db", WithMedium(medium))
	require.NoError(t, err)

	require.NoError(t, storeInstance.Set("g", "k", "v"))
	require.NoError(t, storeInstance.Close())

	reopenedStore, err := New("app.db", WithMedium(medium))
	require.NoError(t, err)
	defer reopenedStore.Close()

	value, err := reopenedStore.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)
	assert.True(t, medium.Exists("app.db"))
}

func TestMedium_Import_Good_JSONL(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("medium-import-jsonl")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	require.NoError(t, medium.Write("data.jsonl", `{"user":"@alice"}
{"user":"@bob"}
`))

	require.NoError(t, Import(workspace, medium, "data.jsonl"))

	rows := requireResultRows(t, workspace.Query("SELECT entry_kind, entry_data FROM workspace_entries ORDER BY entry_id"))
	require.Len(t, rows, 2)
	assert.Equal(t, "data", rows[0]["entry_kind"])
	assert.Contains(t, rows[0]["entry_data"], "@alice")
	assert.Contains(t, rows[1]["entry_data"], "@bob")
}

func TestMedium_Import_Good_JSONArray(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("medium-import-json-array")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	require.NoError(t, medium.Write("users.json", `[{"name":"Alice"},{"name":"Bob"},{"name":"Carol"}]`))

	require.NoError(t, Import(workspace, medium, "users.json"))

	assert.Equal(t, map[string]any{"users": 3}, workspace.Aggregate())
}

func TestMedium_Import_Good_CSV(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("medium-import-csv")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	require.NoError(t, medium.Write("findings.csv", "tool,severity\ngosec,high\ngolint,low\n"))

	require.NoError(t, Import(workspace, medium, "findings.csv"))

	assert.Equal(t, map[string]any{"findings": 2}, workspace.Aggregate())
}

func TestMedium_Import_Bad_NilArguments(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("medium-import-bad")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()

	require.Error(t, Import(nil, medium, "data.json"))
	require.Error(t, Import(workspace, nil, "data.json"))
	require.Error(t, Import(workspace, medium, ""))
}

func TestMedium_Import_Ugly_MissingFileReturnsError(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("medium-import-missing")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	require.Error(t, Import(workspace, medium, "ghost.jsonl"))
}

func TestMedium_Export_Good_JSON(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("medium-export-json")
	require.NoError(t, err)
	defer workspace.Discard()

	require.NoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	require.NoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	require.NoError(t, workspace.Put("profile_match", map[string]any{"user": "@carol"}))

	medium := newMemoryMedium()
	require.NoError(t, Export(workspace, medium, "report.json"))

	assert.True(t, medium.Exists("report.json"))
	content, err := medium.Read("report.json")
	require.NoError(t, err)
	assert.Contains(t, content, `"like":2`)
	assert.Contains(t, content, `"profile_match":1`)
}

func TestMedium_Export_Good_JSONLines(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("medium-export-jsonl")
	require.NoError(t, err)
	defer workspace.Discard()

	require.NoError(t, workspace.Put("like", map[string]any{"user": "@alice"}))
	require.NoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))

	medium := newMemoryMedium()
	require.NoError(t, Export(workspace, medium, "report.jsonl"))

	content, err := medium.Read("report.jsonl")
	require.NoError(t, err)
	lines := 0
	for _, line := range splitNewlines(content) {
		if line != "" {
			lines++
		}
	}
	assert.Equal(t, 2, lines)
}

func TestMedium_Export_Bad_NilArguments(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(":memory:")
	require.NoError(t, err)
	defer storeInstance.Close()

	workspace, err := storeInstance.NewWorkspace("medium-export-bad")
	require.NoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()

	require.Error(t, Export(nil, medium, "report.json"))
	require.Error(t, Export(workspace, nil, "report.json"))
	require.Error(t, Export(workspace, medium, ""))
}

func TestMedium_Compact_Good_MediumRoutesArchive(t *testing.T) {
	useWorkspaceStateDirectory(t)
	useArchiveOutputDirectory(t)

	medium := newMemoryMedium()
	storeInstance, err := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"), WithMedium(medium))
	require.NoError(t, err)
	defer storeInstance.Close()

	require.True(t, storeInstance.CommitToJournal("jobs", map[string]any{"count": 3}, map[string]string{"workspace": "jobs-1"}).OK)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(time.Minute),
		Output: "archive/",
		Format: "gzip",
	})
	require.True(t, result.OK, "compact result: %v", result.Value)
	outputPath, ok := result.Value.(string)
	require.True(t, ok)
	require.NotEmpty(t, outputPath)
	assert.True(t, medium.Exists(outputPath), "compact should write through medium at %s", outputPath)
}

func splitNewlines(content string) []string {
	var result []string
	current := core.NewBuilder()
	for index := 0; index < len(content); index++ {
		character := content[index]
		if character == '\n' {
			result = append(result, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(character)
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
}
