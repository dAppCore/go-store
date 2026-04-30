package store

import (
	"testing"

	core "dappco.re/go"
)

type (
	T              = core.T
	Seq[V any]     = core.Seq[V]
	Seq2[K, V any] = core.Seq2[K, V]
)

const (
	Millisecond = core.Millisecond
	Second      = core.Second
	Hour        = core.Hour
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
	AssertSame      = core.AssertSame
	AssertTrue      = core.AssertTrue
	NewError        = core.NewError
	NewBuffer       = core.NewBuffer
	Now             = core.Now
	Path            = core.Path
	RequireTrue     = core.RequireTrue
	Sprint          = core.Sprint
)

func AssertNoError(t testing.TB, value any) {
	assertNoError(t, value)
}

func RequireNoError(t testing.TB, value any) {
	assertNoError(t, value)
}

func AssertError(t testing.TB, value any) {
	assertError(t, value)
}

func AssertErrorIs(t testing.TB, value any, target error) {
	assertErrorIs(t, value, target)
}

func fixtureStore(t *T) *Store {
	t.Helper()
	storeInstance, err := New(testMemoryDatabasePath, WithPurgeInterval(24*Hour))
	RequireNoError(t, err)
	t.Cleanup(func() { _ = storeInstance.Close() })
	return storeInstance
}

func fixtureConfiguredStore(t *T) (*Store, string) {
	t.Helper()
	stateDirectory := t.TempDir()
	storeInstance, err := NewConfigured(StoreConfig{
		DatabasePath:            testMemoryDatabasePath,
		PurgeInterval:           24 * Hour,
		WorkspaceStateDirectory: stateDirectory,
	})
	RequireNoError(t, err)
	t.Cleanup(func() { _ = storeInstance.Close() })
	return storeInstance, stateDirectory
}

func fixtureWorkspace(t *T) (*Store, *Workspace) {
	t.Helper()
	storeInstance, _ := fixtureConfiguredStore(t)
	workspace, err := storeInstance.NewWorkspace(testFixtureWorkspaceName)
	RequireNoError(t, err)
	t.Cleanup(func() { workspace.Discard() })
	return storeInstance, workspace
}

func fixtureScopedStore(t *T) *ScopedStore {
	t.Helper()
	scopedStore, err := NewScopedConfigured(fixtureStore(t), ScopedStoreConfig{Namespace: testTenantA})
	RequireNoError(t, err)
	return scopedStore
}

func fixtureQuotaScopedStore(t *T, maxKeys, maxGroups int) *ScopedStore {
	t.Helper()
	scopedStore, err := NewScopedConfigured(fixtureStore(t), ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: maxKeys, MaxGroups: maxGroups},
	})
	RequireNoError(t, err)
	return scopedStore
}

func fixtureDuckDB(t *T) *DuckDB {
	t.Helper()
	path := Path(t.TempDir(), "ax7.duckdb")
	database, err := OpenDuckDBReadWrite(path)
	RequireNoError(t, err)
	t.Cleanup(func() { _ = database.Close() })
	return database
}

//nolint:unused // Compatibility helper kept for generated test lanes.
func fixtureSeedDuckDB(t *T, database *DuckDB) {
	t.Helper()
	RequireNoError(t, database.Exec(`CREATE TABLE IF NOT EXISTS golden_set (idx INTEGER, seed_id VARCHAR, domain VARCHAR, voice VARCHAR, prompt VARCHAR, response VARCHAR, gen_time DOUBLE, char_count INTEGER)`))
	RequireNoError(t, database.Exec(`DELETE FROM golden_set`))
	RequireNoError(t, database.Exec(`INSERT INTO golden_set VALUES (1, 'seed-1', 'ethics', 'plain', 'prompt', 'response text', 1.5, 13)`))
	RequireNoError(t, database.Exec(`CREATE TABLE IF NOT EXISTS expansion_prompts (idx BIGINT, seed_id VARCHAR, region VARCHAR, domain VARCHAR, language VARCHAR, prompt VARCHAR, prompt_en VARCHAR, priority INTEGER, status VARCHAR)`))
	RequireNoError(t, database.Exec(`DELETE FROM expansion_prompts`))
	RequireNoError(t, database.Exec(`INSERT INTO expansion_prompts VALUES (7, 'seed-7', 'western', 'ethics', 'en', 'prompt', 'prompt en', 1, 'pending')`))
}

func fixtureCollectKeyValues(seq Seq2[KeyValue, error]) ([]KeyValue, error) {
	var entries []KeyValue
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
	filesystem := (&core.Fs{}).NewUnrestricted()
	result := filesystem.Write(path, content)
	RequireTrue(t, result.OK)
}

func newFixtureMedium() *memoryMedium {
	return newMemoryMedium()
}

func fixtureMustGet(t *T, storeInstance *Store, group, key string) string {
	t.Helper()
	value, err := storeInstance.Get(group, key)
	RequireNoError(t, err)
	return value
}

func fixtureMustExists(t *T, storeInstance *Store, group, key string) bool {
	t.Helper()
	exists, err := storeInstance.Exists(group, key)
	RequireNoError(t, err)
	return exists
}

func fixtureMustGroupExists(t *T, storeInstance *Store, group string) bool {
	t.Helper()
	exists, err := storeInstance.GroupExists(group)
	RequireNoError(t, err)
	return exists
}

func fixtureScopedExists(t *T, scopedStore *ScopedStore, key string) bool {
	t.Helper()
	exists, err := scopedStore.Exists(key)
	RequireNoError(t, err)
	return exists
}

func fixtureScopedExistsIn(t *T, scopedStore *ScopedStore, group, key string) bool {
	t.Helper()
	exists, err := scopedStore.ExistsIn(group, key)
	RequireNoError(t, err)
	return exists
}

func fixtureScopedGroupExists(t *T, scopedStore *ScopedStore, group string) bool {
	t.Helper()
	exists, err := scopedStore.GroupExists(group)
	RequireNoError(t, err)
	return exists
}
