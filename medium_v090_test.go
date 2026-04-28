package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestMediumV090_WithMedium_Good(t *T) {
	medium := newAX7Medium()
	storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", Medium: medium})
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertSame(t, medium, storeInstance.Medium())
}

func TestMediumV090_WithMedium_Bad(t *T) {
	storeInstance, err := store.New(":memory:", store.WithMedium(nil))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertNil(t, storeInstance.Medium())
}

func TestMediumV090_WithMedium_Ugly(t *T) {
	option := store.WithMedium(newAX7Medium())
	AssertNotPanics(t, func() { option(nil) })
	AssertNotNil(t, option)
}

func TestMediumV090_Store_Medium_Good(t *T) {
	medium := newAX7Medium()
	storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", Medium: medium})
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertSame(t, medium, storeInstance.Medium())
}

func TestMediumV090_Store_Medium_Bad(t *T) {
	var storeInstance *store.Store
	medium := storeInstance.Medium()
	AssertNil(t, medium)
}

func TestMediumV090_Store_Medium_Ugly(t *T) {
	storeInstance := ax7Store(t)
	medium := storeInstance.Medium()
	AssertNil(t, medium)
}

func TestMediumV090_Import_Good(t *T) {
	_, workspace := ax7Workspace(t)
	medium := newAX7Medium()
	RequireNoError(t, medium.Write("records.jsonl", `{"name":"alice"}`))
	err := store.Import(workspace, medium, "records.jsonl")
	AssertNoError(t, err)
	AssertEqual(t, 1, len(workspace.Aggregate()))
}

func TestMediumV090_Import_Bad(t *T) {
	medium := newAX7Medium()
	err := store.Import(nil, medium, "records.jsonl")
	AssertError(t, err)
}

func TestMediumV090_Import_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	medium := newAX7Medium()
	RequireNoError(t, medium.Write("records.csv", "name\nalice\n"))
	err := store.Import(workspace, medium, "records.csv")
	AssertNoError(t, err)
}

func TestMediumV090_Export_Good(t *T) {
	_, workspace := ax7Workspace(t)
	medium := newAX7Medium()
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	err := store.Export(workspace, medium, "out/report.json")
	AssertNoError(t, err)
	AssertTrue(t, medium.Exists("out/report.json"))
}

func TestMediumV090_Export_Bad(t *T) {
	medium := newAX7Medium()
	err := store.Export(nil, medium, "report.json")
	AssertError(t, err)
}

func TestMediumV090_Export_Ugly(t *T) {
	_, workspace := ax7Workspace(t)
	medium := newAX7Medium()
	RequireNoError(t, workspace.Put("entry", map[string]any{"name": "alice"}))
	err := store.Export(workspace, medium, "report.jsonl")
	AssertNoError(t, err)
}

func ax7ScopedExists(t *T, scopedStore *store.ScopedStore, key string) bool {
	t.Helper()
	exists, err := scopedStore.Exists(key)
	RequireNoError(t, err)
	return exists
}

func ax7ScopedExistsIn(t *T, scopedStore *store.ScopedStore, group, key string) bool {
	t.Helper()
	exists, err := scopedStore.ExistsIn(group, key)
	RequireNoError(t, err)
	return exists
}

func ax7ScopedGroupExists(t *T, scopedStore *store.ScopedStore, group string) bool {
	t.Helper()
	exists, err := scopedStore.GroupExists(group)
	RequireNoError(t, err)
	return exists
}
