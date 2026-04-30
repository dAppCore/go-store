// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bytes"
	goio "io"
	"io/fs"
	"sync"
	"testing"
	"time"

	core "dappco.re/go"
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
		return "", core.E("memoryMedium.Read", testFileNotFoundPrefix+path, nil)
	}
	return content, nil
}

func (medium *memoryMedium) Write(path, content string) error {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	medium.files[path] = content
	return nil
}

func (medium *memoryMedium) WriteMode(path, content string, _ fs.FileMode) error {
	return medium.Write(path, content)
}

func (medium *memoryMedium) EnsureDir(string) error { return nil }

func (medium *memoryMedium) Create(path string) (goio.WriteCloser, error) {
	return &memoryWriter{medium: medium, path: path}, nil
}

func (medium *memoryMedium) Append(path string) (goio.WriteCloser, error) {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	return &memoryWriter{medium: medium, path: path, buffer: *bytes.NewBufferString(medium.files[path])}, nil
}

func (medium *memoryMedium) ReadStream(path string) (goio.ReadCloser, error) {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	return goio.NopCloser(bytes.NewReader([]byte(medium.files[path]))), nil
}

func (medium *memoryMedium) WriteStream(path string) (goio.WriteCloser, error) {
	return medium.Create(path)
}

func (medium *memoryMedium) Exists(path string) bool {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	_, ok := medium.files[path]
	return ok
}

func (medium *memoryMedium) IsFile(path string) bool { return medium.Exists(path) }

func (medium *memoryMedium) Delete(path string) error {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	delete(medium.files, path)
	return nil
}

func (medium *memoryMedium) DeleteAll(path string) error {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	for key := range medium.files {
		if key == path || core.HasPrefix(key, path+"/") {
			delete(medium.files, key)
		}
	}
	return nil
}

func (medium *memoryMedium) Rename(oldPath, newPath string) error {
	medium.lock.Lock()
	defer medium.lock.Unlock()
	content, ok := medium.files[oldPath]
	if !ok {
		return core.E("memoryMedium.Rename", testFileNotFoundPrefix+oldPath, nil)
	}
	medium.files[newPath] = content
	delete(medium.files, oldPath)
	return nil
}

type renameFailMedium struct {
	*memoryMedium
}

func (medium *renameFailMedium) Rename(string, string) error {
	return core.E("renameFailMedium.Rename", "forced rename failure", nil)
}

type writeFailOnceMedium struct {
	*memoryMedium
	failures int
}

func (medium *writeFailOnceMedium) Write(path, content string) error {
	if medium.failures > 0 {
		medium.failures--
		return core.E("writeFailOnceMedium.Write", "forced write failure", nil)
	}
	return medium.memoryMedium.Write(path, content)
}

func (medium *memoryMedium) List(path string) ([]fs.DirEntry, error) { return nil, nil }

func (medium *memoryMedium) Stat(path string) (fs.FileInfo, error) {
	if !medium.Exists(path) {
		return nil, core.E("memoryMedium.Stat", testFileNotFoundPrefix+path, nil)
	}
	return fileInfoStub{name: core.PathBase(path)}, nil
}

func (medium *memoryMedium) Open(path string) (fs.File, error) {
	if !medium.Exists(path) {
		return nil, core.E("memoryMedium.Open", testFileNotFoundPrefix+path, nil)
	}
	return newMemoryFile(path, medium.files[path]), nil
}

func (medium *memoryMedium) IsDir(string) bool { return false }

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

type fileInfoStub struct {
	name string
}

func (fileInfoStub) Size() int64        { return 0 }
func (fileInfoStub) Mode() fs.FileMode  { return 0 }
func (fileInfoStub) ModTime() time.Time { return time.Time{} }
func (fileInfoStub) IsDir() bool        { return false }
func (fileInfoStub) Sys() any           { return nil }
func (info fileInfoStub) Name() string  { return info.name }

type memoryFile struct {
	*bytes.Reader
	name string
}

func newMemoryFile(name, content string) *memoryFile {
	return &memoryFile{Reader: bytes.NewReader([]byte(content)), name: name}
}

func (file *memoryFile) Stat() (fs.FileInfo, error) {
	return fileInfoStub{name: core.PathBase(file.name)}, nil
}
func (file *memoryFile) Close() error { return nil }

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
	storeInstance, err := New(testMemoryDatabasePath, WithMedium(medium))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertSamef(t, medium, storeInstance.Medium(), "medium should round-trip via accessor")
	assertSamef(t, medium, storeInstance.Config().Medium, "medium should appear in Config()")
}

func TestMedium_WithMedium_Bad_NilKeepsFilesystemBackend(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertNil(t, storeInstance.Medium())
}

func TestMedium_WithMedium_Good_PersistsDatabaseThroughMedium(t *testing.T) {
	useWorkspaceStateDirectory(t)

	medium := newMemoryMedium()

	storeInstance, err := New(testAppDatabaseFile, WithMedium(medium))
	assertNoError(t, err)

	assertNoError(t, storeInstance.Set("g", "k", "v"))
	assertNoError(t, storeInstance.Close())

	reopenedStore, err := New(testAppDatabaseFile, WithMedium(medium))
	assertNoError(t, err)
	defer func() { _ = reopenedStore.Close() }()

	value, err := reopenedStore.Get("g", "k")
	assertNoError(t, err)
	assertEqual(t, "v", value)
	assertTrue(t, medium.Exists(testAppDatabaseFile))
}

func TestMedium_Import_Good_JSONL(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-import-jsonl")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertNoError(t, medium.Write("data.jsonl", `{"user":"@alice"}
{"user":"@bob"}
`))

	assertNoError(t, Import(workspace, medium, "data.jsonl"))

	rows := requireResultRows(t, workspace.Query("SELECT entry_kind, entry_data FROM workspace_entries ORDER BY entry_id"))
	assertLen(t, rows, 2)
	assertEqual(t, "data", rows[0]["entry_kind"])
	assertContainsElement(t, rows[0]["entry_data"], testActorAlice)
	assertContainsElement(t, rows[1]["entry_data"], "@bob")
}

func TestMedium_Import_Good_JSONArray(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-import-json-array")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertNoError(t, medium.Write(testUsersJSONFile, `[{"name":"Alice"},{"name":"Bob"},{"name":"Carol"}]`))

	assertNoError(t, Import(workspace, medium, testUsersJSONFile))

	assertEqual(t, map[string]any{"users": 3}, workspace.Aggregate())
}

func TestMedium_Import_Good_CSV(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-import-csv")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertNoError(t, medium.Write(testFindingsCSVFile, "tool,severity\ngosec,high\ngolint,low\n"))

	assertNoError(t, Import(workspace, medium, testFindingsCSVFile))

	assertEqual(t, map[string]any{"findings": 2}, workspace.Aggregate())
}

func TestMedium_Import_Good_CSVQuotedMultiline(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-import-csv-multiline")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertNoError(t, medium.Write("notes.csv", "name,note\nAlice,\"hello\nworld\"\n"))

	assertNoError(t, Import(workspace, medium, "notes.csv"))

	assertEqual(t, map[string]any{"notes": 1}, workspace.Aggregate())
}

func TestMedium_Import_Bad_JSONArrayNonObject(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-import-json-non-object")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertNoError(t, medium.Write(testUsersJSONFile, `[{"name":"Alice"},"Bob"]`))

	assertError(t, Import(workspace, medium, testUsersJSONFile))

	count, err := workspace.Count()
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestMedium_Import_Bad_MalformedCSV(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-import-csv-bad")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertNoError(t, medium.Write(testFindingsCSVFile, "tool,severity\ngosec,\"high\n"))

	assertError(t, Import(workspace, medium, testFindingsCSVFile))

	count, err := workspace.Count()
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestMedium_Import_Bad_NilArguments(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-import-bad")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()

	assertError(t, Import(nil, medium, "data.json"))
	assertError(t, Import(workspace, nil, "data.json"))
	assertError(t, Import(workspace, medium, ""))
}

func TestMedium_Import_Ugly_MissingFileReturnsError(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-import-missing")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()
	assertError(t, Import(workspace, medium, "ghost.jsonl"))
}

func TestMedium_Export_Good_JSON(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-export-json")
	assertNoError(t, err)
	defer workspace.Discard()

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))
	assertNoError(t, workspace.Put("profile_match", map[string]any{"user": "@carol"}))

	medium := newMemoryMedium()
	assertNoError(t, Export(workspace, medium, testReportJSONFile))

	assertTrue(t, medium.Exists(testReportJSONFile))
	content, err := medium.Read(testReportJSONFile)
	assertNoError(t, err)
	assertContainsString(t, content, `"like":2`)
	assertContainsString(t, content, `"profile_match":1`)
}

func TestMedium_Export_Good_JSONLines(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-export-jsonl")
	assertNoError(t, err)
	defer workspace.Discard()

	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Put("like", map[string]any{"user": "@bob"}))

	medium := newMemoryMedium()
	assertNoError(t, Export(workspace, medium, testReportJSONLFile))

	content, err := medium.Read(testReportJSONLFile)
	assertNoError(t, err)
	lines := 0
	for _, line := range splitNewlines(content) {
		if line != "" {
			lines++
		}
	}
	assertEqual(t, 2, lines)
}

func TestMedium_Export_Bad_NilArguments(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-export-bad")
	assertNoError(t, err)
	defer workspace.Discard()

	medium := newMemoryMedium()

	assertError(t, Export(nil, medium, testReportJSONFile))
	assertError(t, Export(workspace, nil, testReportJSONFile))
	assertError(t, Export(workspace, medium, ""))
}

func TestMedium_Export_Bad_JSONPropagatesWorkspaceFailure(t *testing.T) {
	useWorkspaceStateDirectory(t)

	storeInstance, err := New(testMemoryDatabasePath)
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	workspace, err := storeInstance.NewWorkspace("medium-export-json-closed")
	assertNoError(t, err)
	assertNoError(t, workspace.Put("like", map[string]any{"user": testActorAlice}))
	assertNoError(t, workspace.Close())

	medium := newMemoryMedium()
	assertNoError(t, medium.Write(testReportJSONFile, `{"previous":true}`))

	err = Export(workspace, medium, testReportJSONFile)

	assertError(t, err)
	assertContainsString(t, err.Error(), "aggregate workspace")
	content, readErr := medium.Read(testReportJSONFile)
	assertNoError(t, readErr)
	assertEqual(t, `{"previous":true}`, content)
}

func TestMedium_Compact_Good_MediumRoutesArchive(t *testing.T) {
	useWorkspaceStateDirectory(t)
	useArchiveOutputDirectory(t)

	medium := newMemoryMedium()
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"), WithMedium(medium))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("jobs", map[string]any{"count": 3}, map[string]string{"workspace": "jobs-1"}).OK)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(time.Minute),
		Output: "archive/",
		Format: "gzip",
	})
	assertTruef(t, result.OK, "compact result: %v", result.Value)
	outputPath, ok := result.Value.(string)
	assertTrue(t, ok)
	assertNotEmpty(t, outputPath)
	assertTruef(t, medium.Exists(outputPath), "compact should write through medium at %s", outputPath)
}

func TestMedium_Compact_Bad_PreservesStagedArchiveWhenPublishFails(t *testing.T) {
	useWorkspaceStateDirectory(t)
	useArchiveOutputDirectory(t)

	medium := &renameFailMedium{memoryMedium: newMemoryMedium()}
	storeInstance, err := New(testMemoryDatabasePath, WithJournal(testJournalEndpoint, "core", "events"), WithMedium(medium))
	assertNoError(t, err)
	defer func() { _ = storeInstance.Close() }()

	assertTrue(t, storeInstance.CommitToJournal("jobs", map[string]any{"count": 3}, map[string]string{"workspace": "jobs-1"}).OK)

	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(time.Minute),
		Output: "archive/",
		Format: "gzip",
	})
	assertFalse(t, result.OK)

	stagedArchiveFound := false
	medium.lock.Lock()
	for path := range medium.files {
		if core.HasSuffix(path, ".tmp") {
			stagedArchiveFound = true
		}
	}
	medium.lock.Unlock()
	assertTrue(t, stagedArchiveFound)
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

func TestMedium_WithMedium_Bad(t *T) {
	storeInstance, err := New(testMemoryDatabasePath, WithMedium(nil))
	RequireNoError(t, err)
	defer func() { _ = storeInstance.Close() }()
	AssertNil(t, storeInstance.Medium())
}

func TestMedium_WithMedium_Ugly(t *T) {
	option := WithMedium(newAX7Medium())
	AssertNotPanics(t, func() { option(nil) })
	AssertNotNil(t, option)
}

func TestMedium_Store_Medium_Good(t *T) {
	medium := newAX7Medium()
	storeInstance, err := NewConfigured(StoreConfig{DatabasePath: testMemoryDatabasePath, Medium: medium})
	RequireNoError(t, err)
	defer func() { _ = storeInstance.Close() }()
	AssertSame(t, medium, storeInstance.Medium())
}

func TestMedium_Store_Medium_Bad(t *T) {
	var storeInstance *Store
	medium := storeInstance.Medium()
	AssertNil(t, medium)
}

func TestMedium_Store_Medium_Ugly(t *T) {
	storeInstance := ax7Store(t)
	medium := storeInstance.Medium()
	AssertNil(t, medium)
}

func TestMedium_Import_Good(t *T) {
	_, workspace := ax7Workspace(t)
	medium := newAX7Medium()
	RequireNoError(t, medium.Write(testRecordsJSONLFile, `{"name":"alice"}`))
	err := Import(workspace, medium, testRecordsJSONLFile)
	AssertNoError(t, err)
	AssertEqual(t, 1, len(workspace.Aggregate()))
}

func TestMedium_Import_Bad(t *T) {
	medium := newAX7Medium()
	err := Import(nil, medium, testRecordsJSONLFile)
	AssertError(t, err)
}

func TestMedium_Import_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	medium := newAX7Medium()
	RequireNoError(t, medium.Write("records.csv", "name\nalice\n"))
	err := Import(workspace, medium, "records.csv")
	AssertNoError(t, err)
}

func TestMedium_Export_Good(t *T) {
	_, workspace := ax7Workspace(t)
	medium := newAX7Medium()
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	err := Export(workspace, medium, "out/report.json")
	AssertNoError(t, err)
	AssertTrue(t, medium.Exists("out/report.json"))
}

func TestMedium_Export_Bad(t *T) {
	medium := newAX7Medium()
	err := Export(nil, medium, testReportJSONFile)
	AssertError(t, err)
}

func TestMedium_Export_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	medium := newAX7Medium()
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	err := Export(workspace, medium, testReportJSONLFile)
	AssertNoError(t, err)
}
