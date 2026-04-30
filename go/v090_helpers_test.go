//nolint:unused // Compatibility helpers are used by generated v0.9 test lanes.
package store_test

import (
	"testing"

	core "dappco.re/go"
	store "dappco.re/go/store"
)

type (
	T              = core.T
	Seq[V any]     = core.Seq[V]
	Seq2[K, V any] = core.Seq2[K, V]
)

const (
	Hour                     = core.Hour
	Millisecond              = core.Millisecond
	testMemoryDatabasePath   = ":memory:"
	testTenantA              = "tenant-a"
	testFixtureWorkspaceName = "ax7-workspace"
	testFileNotFoundMessage  = "file not found"
)

var (
	AssertContains  = core.AssertContains
	AssertEmpty     = core.AssertEmpty
	AssertEqual     = core.AssertEqual
	AssertFalse     = core.AssertFalse
	AssertLen       = core.AssertLen
	AssertNil       = core.AssertNil
	AssertNotEmpty  = core.AssertNotEmpty
	AssertNotEqual  = core.AssertNotEqual
	AssertNotNil    = core.AssertNotNil
	AssertNotPanics = core.AssertNotPanics
	AssertTrue      = core.AssertTrue
	HasPrefix       = core.HasPrefix
	NewBuffer       = core.NewBuffer
	NewError        = core.NewError
	NewReader       = core.NewReader
	Path            = core.Path
	PathBase        = core.PathBase
	RequireTrue     = core.RequireTrue
	Sprint          = core.Sprint
	UnixTime        = core.UnixTime
)

type (
	FileMode    = core.FileMode
	Fs          = core.Fs
	FsDirEntry  = core.FsDirEntry
	FsFile      = core.FsFile
	FsFileInfo  = core.FsFileInfo
	ReadCloser  = core.ReadCloser
	Reader      = core.Reader
	Time        = core.Time
	WriteCloser = core.WriteCloser
)

func AssertNoError(t testing.TB, value any) {
	t.Helper()
	if err := resultError(value); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func RequireNoError(t testing.TB, value any) {
	t.Helper()
	if err := resultError(value); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func AssertError(t testing.TB, value any) {
	t.Helper()
	if err := resultError(value); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func AssertErrorIs(t testing.TB, value any, target error) {
	t.Helper()
	if err := resultError(value); !core.Is(err, target) {
		t.Fatalf("expected error matching %v, got %v", target, value)
	}
}

func resultError(value any) error {
	switch typed := value.(type) {
	case nil:
		return nil
	case core.Result:
		if typed.OK {
			return nil
		}
		if err, ok := typed.Value.(error); ok {
			return err
		}
		return core.E("store.test", typed.Error(), nil)
	case error:
		return typed
	default:
		return nil
	}
}

func fixtureStore(t *T) *store.Store {
	t.Helper()
	storeInstance, err := store.New(testMemoryDatabasePath, store.WithPurgeInterval(24*Hour))
	RequireNoError(t, err)
	t.Cleanup(func() { _ = storeInstance.Close() })
	return storeInstance
}

func fixtureConfiguredStore(t *T) (*store.Store, string) {
	t.Helper()
	stateDirectory := t.TempDir()
	storeInstance, err := store.NewConfigured(store.StoreConfig{
		DatabasePath:            testMemoryDatabasePath,
		PurgeInterval:           24 * Hour,
		WorkspaceStateDirectory: stateDirectory,
	})
	RequireNoError(t, err)
	t.Cleanup(func() { _ = storeInstance.Close() })
	return storeInstance, stateDirectory
}

func fixtureWorkspace(t *T) (*store.Store, *store.Workspace) {
	t.Helper()
	storeInstance, _ := fixtureConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace(testFixtureWorkspaceName)
	RequireNoError(t, err)
	t.Cleanup(func() { workspace.Discard() })
	return storeInstance, workspace
}

func fixtureScopedStore(t *T) *store.ScopedStore {
	t.Helper()
	scopedStore, err := store.NewScopedConfigured(fixtureStore(t), store.ScopedStoreConfig{Namespace: testTenantA})
	RequireNoError(t, err)
	return scopedStore
}

func fixtureQuotaScopedStore(t *T, maxKeys, maxGroups int) *store.ScopedStore {
	t.Helper()
	scopedStore, err := store.NewScopedConfigured(fixtureStore(t), store.ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     store.QuotaConfig{MaxKeys: maxKeys, MaxGroups: maxGroups},
	})
	RequireNoError(t, err)
	return scopedStore
}

func fixtureDuckDB(t *T) *store.DuckDB {
	t.Helper()
	path := Path(t.TempDir(), "ax7.duckdb")
	database, err := store.OpenDuckDBReadWrite(path)
	RequireNoError(t, err)
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func fixtureSeedDuckDB(t *T, database *store.DuckDB) {
	t.Helper()
	RequireNoError(t, database.Exec(`CREATE TABLE IF NOT EXISTS golden_set (idx INTEGER, seed_id VARCHAR, domain VARCHAR, voice VARCHAR, prompt VARCHAR, response VARCHAR, gen_time DOUBLE, char_count INTEGER)`))
	RequireNoError(t, database.Exec(`DELETE FROM golden_set`))
	RequireNoError(t, database.Exec(`INSERT INTO golden_set VALUES (1, 'seed-1', 'ethics', 'plain', 'prompt', 'response text', 1.5, 13)`))
	RequireNoError(t, database.Exec(`CREATE TABLE IF NOT EXISTS expansion_prompts (idx BIGINT, seed_id VARCHAR, region VARCHAR, domain VARCHAR, language VARCHAR, prompt VARCHAR, prompt_en VARCHAR, priority INTEGER, status VARCHAR)`))
	RequireNoError(t, database.Exec(`DELETE FROM expansion_prompts`))
	RequireNoError(t, database.Exec(`INSERT INTO expansion_prompts VALUES (7, 'seed-7', 'western', 'ethics', 'en', 'prompt', 'prompt en', 1, 'pending')`))
}

func fixtureCollectKeyValues(seq Seq2[store.KeyValue, error]) ([]store.KeyValue, error) {
	var entries []store.KeyValue
	for entry, err := range seq {
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func fixtureCollectGroups(seq Seq2[string, error]) ([]string, error) {
	var groups []string
	for group, err := range seq {
		if err != nil {
			return groups, err
		}
		groups = append(groups, group)
	}
	return groups, nil
}

func fixtureCollectStrings(seq Seq[string]) []string {
	var values []string
	for value := range seq {
		values = append(values, value)
	}
	return values
}

func fixtureWriteFile(t *T, path, content string) {
	t.Helper()
	filesystem := (&Fs{}).NewUnrestricted()
	result := filesystem.Write(path, content)
	RequireTrue(t, result.OK)
}

type fixtureMedium struct {
	files map[string]string
}

func newFixtureMedium() *fixtureMedium {
	return &fixtureMedium{files: make(map[string]string)}
}

func (medium *fixtureMedium) Read(path string) (string, error) {
	content, ok := medium.files[path]
	if !ok {
		return "", NewError(testFileNotFoundMessage)
	}
	return content, nil
}

func (medium *fixtureMedium) Write(path, content string) error {
	medium.files[path] = content
	return nil
}

func (medium *fixtureMedium) WriteMode(path, content string, _ FileMode) error {
	return medium.Write(path, content)
}

func (medium *fixtureMedium) EnsureDir(_ string) error { return nil }
func (medium *fixtureMedium) IsFile(path string) bool  { return medium.Exists(path) }
func (medium *fixtureMedium) Delete(path string) error { delete(medium.files, path); return nil }
func (medium *fixtureMedium) DeleteAll(path string) error {
	for key := range medium.files {
		if key == path || HasPrefix(key, path+"/") {
			delete(medium.files, key)
		}
	}
	return nil
}
func (medium *fixtureMedium) Rename(oldPath, newPath string) error {
	content, ok := medium.files[oldPath]
	if !ok {
		return NewError(testFileNotFoundMessage)
	}
	medium.files[newPath] = content
	delete(medium.files, oldPath)
	return nil
}
func (medium *fixtureMedium) List(_ string) ([]FsDirEntry, error) { return nil, nil }
func (medium *fixtureMedium) Stat(path string) (FsFileInfo, error) {
	content, ok := medium.files[path]
	if !ok {
		return nil, NewError(testFileNotFoundMessage)
	}
	return fixtureFileInfo{name: PathBase(path), size: int64(len(content))}, nil
}
func (medium *fixtureMedium) Open(path string) (FsFile, error) {
	content, ok := medium.files[path]
	if !ok {
		return nil, NewError(testFileNotFoundMessage)
	}
	return &fixtureFile{name: PathBase(path), content: content, reader: NewReader(content)}, nil
}
func (medium *fixtureMedium) Create(path string) (WriteCloser, error) {
	return &fixtureWriteCloser{medium: medium, path: path}, nil
}
func (medium *fixtureMedium) Append(path string) (WriteCloser, error) {
	return &fixtureWriteCloser{medium: medium, path: path, content: medium.files[path]}, nil
}
func (medium *fixtureMedium) ReadStream(path string) (ReadCloser, error) {
	file, err := medium.Open(path)
	if err != nil {
		return nil, err
	}
	return file.(ReadCloser), nil
}
func (medium *fixtureMedium) WriteStream(path string) (WriteCloser, error) {
	return medium.Create(path)
}
func (medium *fixtureMedium) Exists(path string) bool { _, ok := medium.files[path]; return ok }
func (medium *fixtureMedium) IsDir(path string) bool {
	for key := range medium.files {
		if HasPrefix(key, path+"/") {
			return true
		}
	}
	return false
}

type fixtureFile struct {
	name    string
	content string
	reader  Reader
}

func (file *fixtureFile) Read(p []byte) (int, error) { return file.reader.Read(p) }
func (file *fixtureFile) Close() error               { return nil }
func (file *fixtureFile) Stat() (FsFileInfo, error) {
	return fixtureFileInfo{name: file.name, size: int64(len(file.content))}, nil
}

type fixtureWriteCloser struct {
	medium  *fixtureMedium
	path    string
	content string
}

func (writer *fixtureWriteCloser) Write(p []byte) (int, error) {
	writer.content += string(p)
	return len(p), nil
}
func (writer *fixtureWriteCloser) Close() error {
	writer.medium.files[writer.path] = writer.content
	return nil
}

type fixtureFileInfo struct {
	name string
	size int64
}

func (info fixtureFileInfo) Name() string   { return info.name }
func (info fixtureFileInfo) Size() int64    { return info.size }
func (info fixtureFileInfo) Mode() FileMode { return 0644 }
func (info fixtureFileInfo) ModTime() Time  { return UnixTime(0) }
func (info fixtureFileInfo) IsDir() bool    { return false }
func (info fixtureFileInfo) Sys() any       { return nil }
