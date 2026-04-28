package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func ax7Store(t *T) *store.Store {
	t.Helper()
	storeInstance, err := store.New(":memory:", store.WithPurgeInterval(24*Hour))
	RequireNoError(t, err)
	t.Cleanup(func() { storeInstance.Close() })
	return storeInstance
}

func ax7ConfiguredStore(t *T) (*store.Store, string) {
	t.Helper()
	stateDirectory := t.TempDir()
	storeInstance, err := store.NewConfigured(store.StoreConfig{
		DatabasePath:            ":memory:",
		PurgeInterval:           24 * Hour,
		WorkspaceStateDirectory: stateDirectory,
	})
	RequireNoError(t, err)
	t.Cleanup(func() { storeInstance.Close() })
	return storeInstance, stateDirectory
}

func ax7Workspace(t *T) (*store.Store, *store.Workspace) {
	t.Helper()
	storeInstance, _ := ax7ConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace("ax7-workspace")
	RequireNoError(t, err)
	t.Cleanup(func() { workspace.Discard() })
	return storeInstance, workspace
}

func ax7ScopedStore(t *T) *store.ScopedStore {
	t.Helper()
	scopedStore, err := store.NewScopedConfigured(ax7Store(t), store.ScopedStoreConfig{Namespace: "tenant-a"})
	RequireNoError(t, err)
	return scopedStore
}

func ax7QuotaScopedStore(t *T, maxKeys, maxGroups int) *store.ScopedStore {
	t.Helper()
	scopedStore, err := store.NewScopedConfigured(ax7Store(t), store.ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     store.QuotaConfig{MaxKeys: maxKeys, MaxGroups: maxGroups},
	})
	RequireNoError(t, err)
	return scopedStore
}

func ax7DuckDB(t *T) *store.DuckDB {
	t.Helper()
	path := Path(t.TempDir(), "ax7.duckdb")
	database, err := store.OpenDuckDBReadWrite(path)
	RequireNoError(t, err)
	t.Cleanup(func() { database.Close() })
	return database
}

func ax7SeedDuckDB(t *T, database *store.DuckDB) {
	t.Helper()
	RequireNoError(t, database.Exec(`CREATE TABLE IF NOT EXISTS golden_set (idx INTEGER, seed_id VARCHAR, domain VARCHAR, voice VARCHAR, prompt VARCHAR, response VARCHAR, gen_time DOUBLE, char_count INTEGER)`))
	RequireNoError(t, database.Exec(`DELETE FROM golden_set`))
	RequireNoError(t, database.Exec(`INSERT INTO golden_set VALUES (1, 'seed-1', 'ethics', 'plain', 'prompt', 'response text', 1.5, 13)`))
	RequireNoError(t, database.Exec(`CREATE TABLE IF NOT EXISTS expansion_prompts (idx BIGINT, seed_id VARCHAR, region VARCHAR, domain VARCHAR, language VARCHAR, prompt VARCHAR, prompt_en VARCHAR, priority INTEGER, status VARCHAR)`))
	RequireNoError(t, database.Exec(`DELETE FROM expansion_prompts`))
	RequireNoError(t, database.Exec(`INSERT INTO expansion_prompts VALUES (7, 'seed-7', 'western', 'ethics', 'en', 'prompt', 'prompt en', 1, 'pending')`))
}

func ax7CollectKeyValues(seq Seq2[store.KeyValue, error]) ([]store.KeyValue, error) {
	var entries []store.KeyValue
	for entry, err := range seq {
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func ax7CollectGroups(seq Seq2[string, error]) ([]string, error) {
	var groups []string
	for group, err := range seq {
		if err != nil {
			return groups, err
		}
		groups = append(groups, group)
	}
	return groups, nil
}

func ax7CollectStrings(seq Seq[string]) []string {
	var values []string
	for value := range seq {
		values = append(values, value)
	}
	return values
}

func ax7WriteFile(t *T, path, content string) {
	t.Helper()
	filesystem := (&Fs{}).NewUnrestricted()
	result := filesystem.Write(path, content)
	RequireTrue(t, result.OK)
}

type ax7Medium struct {
	files map[string]string
}

func newAX7Medium() *ax7Medium {
	return &ax7Medium{files: make(map[string]string)}
}

func (medium *ax7Medium) Read(path string) (string, error) {
	content, ok := medium.files[path]
	if !ok {
		return "", NewError("file not found")
	}
	return content, nil
}

func (medium *ax7Medium) Write(path, content string) error {
	medium.files[path] = content
	return nil
}

func (medium *ax7Medium) WriteMode(path, content string, _ FileMode) error {
	return medium.Write(path, content)
}

func (medium *ax7Medium) EnsureDir(_ string) error { return nil }
func (medium *ax7Medium) IsFile(path string) bool  { return medium.Exists(path) }
func (medium *ax7Medium) Delete(path string) error { delete(medium.files, path); return nil }
func (medium *ax7Medium) DeleteAll(path string) error {
	for key := range medium.files {
		if key == path || HasPrefix(key, path+"/") {
			delete(medium.files, key)
		}
	}
	return nil
}
func (medium *ax7Medium) Rename(oldPath, newPath string) error {
	content, ok := medium.files[oldPath]
	if !ok {
		return NewError("file not found")
	}
	medium.files[newPath] = content
	delete(medium.files, oldPath)
	return nil
}
func (medium *ax7Medium) List(_ string) ([]FsDirEntry, error) { return nil, nil }
func (medium *ax7Medium) Stat(path string) (FsFileInfo, error) {
	content, ok := medium.files[path]
	if !ok {
		return nil, NewError("file not found")
	}
	return ax7FileInfo{name: PathBase(path), size: int64(len(content))}, nil
}
func (medium *ax7Medium) Open(path string) (FsFile, error) {
	content, ok := medium.files[path]
	if !ok {
		return nil, NewError("file not found")
	}
	return &ax7File{name: PathBase(path), content: content, reader: NewReader(content)}, nil
}
func (medium *ax7Medium) Create(path string) (WriteCloser, error) {
	return &ax7WriteCloser{medium: medium, path: path}, nil
}
func (medium *ax7Medium) Append(path string) (WriteCloser, error) {
	return &ax7WriteCloser{medium: medium, path: path, content: medium.files[path]}, nil
}
func (medium *ax7Medium) ReadStream(path string) (ReadCloser, error) {
	file, err := medium.Open(path)
	if err != nil {
		return nil, err
	}
	return file.(ReadCloser), nil
}
func (medium *ax7Medium) WriteStream(path string) (WriteCloser, error) { return medium.Create(path) }
func (medium *ax7Medium) Exists(path string) bool                      { _, ok := medium.files[path]; return ok }
func (medium *ax7Medium) IsDir(path string) bool {
	for key := range medium.files {
		if HasPrefix(key, path+"/") {
			return true
		}
	}
	return false
}

type ax7File struct {
	name    string
	content string
	reader  Reader
}

func (file *ax7File) Read(p []byte) (int, error) { return file.reader.Read(p) }
func (file *ax7File) Close() error               { return nil }
func (file *ax7File) Stat() (FsFileInfo, error) {
	return ax7FileInfo{name: file.name, size: int64(len(file.content))}, nil
}

type ax7WriteCloser struct {
	medium  *ax7Medium
	path    string
	content string
}

func (writer *ax7WriteCloser) Write(p []byte) (int, error) {
	writer.content += string(p)
	return len(p), nil
}
func (writer *ax7WriteCloser) Close() error {
	writer.medium.files[writer.path] = writer.content
	return nil
}

type ax7FileInfo struct {
	name string
	size int64
}

func (info ax7FileInfo) Name() string   { return info.name }
func (info ax7FileInfo) Size() int64    { return info.size }
func (info ax7FileInfo) Mode() FileMode { return 0644 }
func (info ax7FileInfo) ModTime() Time  { return UnixTime(0) }
func (info ax7FileInfo) IsDir() bool    { return false }
func (info ax7FileInfo) Sys() any       { return nil }
