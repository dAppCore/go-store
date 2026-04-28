package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestStoreV090_StoreConfig_Normalised_Good(t *T) {
	config := store.StoreConfig{DatabasePath: ":memory:"}
	normalised := config.Normalised()
	AssertEqual(t, ":memory:", normalised.DatabasePath)
	AssertTrue(t, normalised.PurgeInterval > 0)
}

func TestStoreV090_StoreConfig_Normalised_Bad(t *T) {
	config := store.StoreConfig{DatabasePath: ""}
	err := config.Validate()
	AssertError(t, err)
}

func TestStoreV090_StoreConfig_Normalised_Ugly(t *T) {
	config := store.StoreConfig{DatabasePath: ":memory:", WorkspaceStateDirectory: "state///"}
	normalised := config.Normalised()
	AssertEqual(t, "state", normalised.WorkspaceStateDirectory)
}

func TestStoreV090_StoreConfig_Validate_Good(t *T) {
	config := store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: Second}
	err := config.Validate()
	AssertNoError(t, err)
}

func TestStoreV090_StoreConfig_Validate_Bad(t *T) {
	config := store.StoreConfig{DatabasePath: ""}
	err := config.Validate()
	AssertError(t, err)
}

func TestStoreV090_StoreConfig_Validate_Ugly(t *T) {
	config := store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: -Second}
	err := config.Validate()
	AssertError(t, err)
}

func TestStoreV090_JournalConfiguration_Validate_Good(t *T) {
	config := store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}
	err := config.Validate()
	AssertNoError(t, err)
}

func TestStoreV090_JournalConfiguration_Validate_Bad(t *T) {
	config := store.JournalConfiguration{Organisation: "core", BucketName: "events"}
	err := config.Validate()
	AssertError(t, err)
}

func TestStoreV090_JournalConfiguration_Validate_Ugly(t *T) {
	config := store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "", BucketName: ""}
	err := config.Validate()
	AssertError(t, err)
}

func TestStoreV090_WithJournal_Good(t *T) {
	storeInstance, err := store.New(":memory:", store.WithJournal("http://127.0.0.1:8086", "core", "events"))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertTrue(t, storeInstance.JournalConfigured())
}

func TestStoreV090_WithJournal_Bad(t *T) {
	option := store.WithJournal("", "", "")
	AssertNotPanics(t, func() { option(nil) })
	AssertNotNil(t, option)
}

func TestStoreV090_WithJournal_Ugly(t *T) {
	storeInstance, err := store.New(":memory:", store.WithJournal("http://127.0.0.1:8086", "", "events"))
	AssertError(t, err)
	AssertNil(t, storeInstance)
}

func TestStoreV090_WithWorkspaceStateDirectory_Good(t *T) {
	directory := t.TempDir()
	storeInstance, err := store.New(":memory:", store.WithWorkspaceStateDirectory(directory))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertEqual(t, directory, storeInstance.WorkspaceStateDirectory())
}

func TestStoreV090_WithWorkspaceStateDirectory_Bad(t *T) {
	option := store.WithWorkspaceStateDirectory("ignored")
	AssertNotPanics(t, func() { option(nil) })
	AssertNotNil(t, option)
}

func TestStoreV090_WithWorkspaceStateDirectory_Ugly(t *T) {
	storeInstance, err := store.New(":memory:", store.WithWorkspaceStateDirectory(""))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertNotEmpty(t, storeInstance.WorkspaceStateDirectory())
}

func TestStoreV090_WithPurgeInterval_Good(t *T) {
	storeInstance, err := store.New(":memory:", store.WithPurgeInterval(5*Second))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertEqual(t, 5*Second, storeInstance.Config().PurgeInterval)
}

func TestStoreV090_WithPurgeInterval_Bad(t *T) {
	option := store.WithPurgeInterval(-Second)
	AssertNotPanics(t, func() { option(nil) })
	AssertNotNil(t, option)
}

func TestStoreV090_WithPurgeInterval_Ugly(t *T) {
	storeInstance, err := store.New(":memory:", store.WithPurgeInterval(0))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertTrue(t, storeInstance.Config().PurgeInterval > 0)
}

func TestStoreV090_NewConfigured_Good(t *T) {
	storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: 24 * Hour})
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertFalse(t, storeInstance.IsClosed())
}

func TestStoreV090_NewConfigured_Bad(t *T) {
	storeInstance, err := store.NewConfigured(store.StoreConfig{})
	AssertError(t, err)
	AssertNil(t, storeInstance)
}

func TestStoreV090_NewConfigured_Ugly(t *T) {
	storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", WorkspaceStateDirectory: t.TempDir()})
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertNotEmpty(t, storeInstance.WorkspaceStateDirectory())
}

func TestStoreV090_New_Good(t *T) {
	storeInstance, err := store.New(":memory:")
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertEqual(t, ":memory:", storeInstance.DatabasePath())
}

func TestStoreV090_New_Bad(t *T) {
	storeInstance, err := store.New("")
	AssertError(t, err)
	AssertNil(t, storeInstance)
}

func TestStoreV090_New_Ugly(t *T) {
	storeInstance, err := store.New(":memory:", nil, store.WithPurgeInterval(24*Hour))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertFalse(t, storeInstance.IsClosed())
}

func TestStoreV090_Store_JournalConfiguration_Good(t *T) {
	storeInstance := ax7Store(t)
	config := storeInstance.JournalConfiguration()
	AssertEqual(t, store.JournalConfiguration{}, config)
}

func TestStoreV090_Store_JournalConfiguration_Bad(t *T) {
	var storeInstance *store.Store
	config := storeInstance.JournalConfiguration()
	AssertEqual(t, store.JournalConfiguration{}, config)
}

func TestStoreV090_Store_JournalConfiguration_Ugly(t *T) {
	storeInstance, err := store.New(":memory:", store.WithJournal("http://127.0.0.1:8086", "core", "events"))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertEqual(t, "events", storeInstance.JournalConfiguration().BucketName)
}

func TestStoreV090_Store_JournalConfigured_Good(t *T) {
	storeInstance, err := store.New(":memory:", store.WithJournal("http://127.0.0.1:8086", "core", "events"))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertTrue(t, storeInstance.JournalConfigured())
}

func TestStoreV090_Store_JournalConfigured_Bad(t *T) {
	var storeInstance *store.Store
	configured := storeInstance.JournalConfigured()
	AssertFalse(t, configured)
}

func TestStoreV090_Store_JournalConfigured_Ugly(t *T) {
	storeInstance := ax7Store(t)
	configured := storeInstance.JournalConfigured()
	AssertFalse(t, configured)
}

func TestStoreV090_Store_Config_Good(t *T) {
	storeInstance := ax7Store(t)
	config := storeInstance.Config()
	AssertEqual(t, ":memory:", config.DatabasePath)
}

func TestStoreV090_Store_Config_Bad(t *T) {
	var storeInstance *store.Store
	config := storeInstance.Config()
	AssertEqual(t, store.StoreConfig{}, config)
}

func TestStoreV090_Store_Config_Ugly(t *T) {
	medium := newAX7Medium()
	storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", Medium: medium})
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertSame(t, medium, storeInstance.Config().Medium)
}

func TestStoreV090_Store_DatabasePath_Good(t *T) {
	storeInstance := ax7Store(t)
	path := storeInstance.DatabasePath()
	AssertEqual(t, ":memory:", path)
}

func TestStoreV090_Store_DatabasePath_Bad(t *T) {
	var storeInstance *store.Store
	path := storeInstance.DatabasePath()
	AssertEqual(t, "", path)
}

func TestStoreV090_Store_DatabasePath_Ugly(t *T) {
	path := Path(t.TempDir(), "store.db")
	storeInstance, err := store.New(path)
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertEqual(t, path, storeInstance.DatabasePath())
}

func TestStoreV090_Store_WorkspaceStateDirectory_Good(t *T) {
	storeInstance, stateDirectory := ax7ConfiguredStore(t)
	path := storeInstance.WorkspaceStateDirectory()
	AssertEqual(t, stateDirectory, path)
}

func TestStoreV090_Store_WorkspaceStateDirectory_Bad(t *T) {
	var storeInstance *store.Store
	path := storeInstance.WorkspaceStateDirectory()
	AssertNotEmpty(t, path)
}

func TestStoreV090_Store_WorkspaceStateDirectory_Ugly(t *T) {
	storeInstance, err := store.New(":memory:", store.WithWorkspaceStateDirectory("state///"))
	RequireNoError(t, err)
	defer storeInstance.Close()
	AssertEqual(t, "state", storeInstance.WorkspaceStateDirectory())
}

func TestStoreV090_Store_IsClosed_Good(t *T) {
	storeInstance := ax7Store(t)
	closed := storeInstance.IsClosed()
	AssertFalse(t, closed)
}

func TestStoreV090_Store_IsClosed_Bad(t *T) {
	var storeInstance *store.Store
	closed := storeInstance.IsClosed()
	AssertTrue(t, closed)
}

func TestStoreV090_Store_IsClosed_Ugly(t *T) {
	storeInstance, err := store.New(":memory:")
	RequireNoError(t, err)
	RequireNoError(t, storeInstance.Close())
	AssertTrue(t, storeInstance.IsClosed())
}

func TestStoreV090_Store_Close_Good(t *T) {
	storeInstance, err := store.New(":memory:")
	RequireNoError(t, err)
	err = storeInstance.Close()
	AssertNoError(t, err)
	AssertTrue(t, storeInstance.IsClosed())
}

func TestStoreV090_Store_Close_Bad(t *T) {
	var storeInstance *store.Store
	err := storeInstance.Close()
	AssertNoError(t, err)
}

func TestStoreV090_Store_Close_Ugly(t *T) {
	storeInstance, err := store.New(":memory:")
	RequireNoError(t, err)
	RequireNoError(t, storeInstance.Close())
	AssertNoError(t, storeInstance.Close())
}

func ax7MustGet(t *T, storeInstance *store.Store, group, key string) string {
	t.Helper()
	value, err := storeInstance.Get(group, key)
	RequireNoError(t, err)
	return value
}

func ax7MustExists(t *T, storeInstance *store.Store, group, key string) bool {
	t.Helper()
	exists, err := storeInstance.Exists(group, key)
	RequireNoError(t, err)
	return exists
}

func ax7MustGroupExists(t *T, storeInstance *store.Store, group string) bool {
	t.Helper()
	exists, err := storeInstance.GroupExists(group)
	RequireNoError(t, err)
	return exists
}

func TestStoreV090_Store_Get_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	got, err := storeInstance.Get("config", "colour")
	AssertNoError(t, err)
	AssertEqual(t, "blue", got)
}

func TestStoreV090_Store_Get_Bad(t *T) {
	storeInstance := ax7Store(t)
	_, err := storeInstance.Get("missing", "key")
	AssertErrorIs(t, err, store.NotFoundError)
	AssertContains(t, err.Error(), "missing/key")
}

func TestStoreV090_Store_Get_Ugly(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("", "", "empty"))
	got, err := storeInstance.Get("", "")
	AssertNoError(t, err)
	AssertEqual(t, "empty", got)
}

func TestStoreV090_Store_Set_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Set("config", "colour", "blue")
	AssertNoError(t, err)
	AssertEqual(t, "blue", ax7MustGet(t, storeInstance, "config", "colour"))
}

func TestStoreV090_Store_Set_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	err := storeInstance.Set("config", "colour", "blue")
	AssertError(t, err)
}

func TestStoreV090_Store_Set_Ugly(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	err := storeInstance.Set("config", "colour", "green")
	AssertNoError(t, err)
	AssertEqual(t, "green", ax7MustGet(t, storeInstance, "config", "colour"))
}

func TestStoreV090_Store_SetWithTTL_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.SetWithTTL("session", "token", "abc", Hour)
	AssertNoError(t, err)
	AssertEqual(t, "abc", ax7MustGet(t, storeInstance, "session", "token"))
}

func TestStoreV090_Store_SetWithTTL_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	err := storeInstance.SetWithTTL("session", "token", "abc", Hour)
	AssertError(t, err)
}

func TestStoreV090_Store_SetWithTTL_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.SetWithTTL("session", "token", "abc", -Millisecond)
	AssertNoError(t, err)
	_, getErr := storeInstance.Get("session", "token")
	AssertErrorIs(t, getErr, store.NotFoundError)
}

func TestStoreV090_Store_Delete_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	err := storeInstance.Delete("config", "colour")
	AssertNoError(t, err)
	_, getErr := storeInstance.Get("config", "colour")
	AssertErrorIs(t, getErr, store.NotFoundError)
}

func TestStoreV090_Store_Delete_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	err := storeInstance.Delete("config", "colour")
	AssertError(t, err)
}

func TestStoreV090_Store_Delete_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Delete("missing", "key")
	AssertNoError(t, err)
	AssertFalse(t, ax7MustExists(t, storeInstance, "missing", "key"))
}

func TestStoreV090_Store_Exists_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	exists, err := storeInstance.Exists("config", "colour")
	AssertNoError(t, err)
	AssertTrue(t, exists)
}

func TestStoreV090_Store_Exists_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	exists, err := storeInstance.Exists("config", "colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestStoreV090_Store_Exists_Ugly(t *T) {
	storeInstance := ax7Store(t)
	exists, err := storeInstance.Exists("missing", "key")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestStoreV090_Store_GroupExists_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	exists, err := storeInstance.GroupExists("config")
	AssertNoError(t, err)
	AssertTrue(t, exists)
}

func TestStoreV090_Store_GroupExists_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	exists, err := storeInstance.GroupExists("config")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestStoreV090_Store_GroupExists_Ugly(t *T) {
	storeInstance := ax7Store(t)
	exists, err := storeInstance.GroupExists("empty")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestStoreV090_Store_Count_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "a", "1"))
	count, err := storeInstance.Count("config")
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestStoreV090_Store_Count_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	count, err := storeInstance.Count("config")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestStoreV090_Store_Count_Ugly(t *T) {
	storeInstance := ax7Store(t)
	count, err := storeInstance.Count("missing")
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestStoreV090_Store_DeleteGroup_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "a", "1"))
	err := storeInstance.DeleteGroup("config")
	AssertNoError(t, err)
	AssertFalse(t, ax7MustGroupExists(t, storeInstance, "config"))
}

func TestStoreV090_Store_DeleteGroup_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	err := storeInstance.DeleteGroup("config")
	AssertError(t, err)
}

func TestStoreV090_Store_DeleteGroup_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.DeleteGroup("missing")
	AssertNoError(t, err)
	AssertFalse(t, ax7MustGroupExists(t, storeInstance, "missing"))
}

func TestStoreV090_Store_DeletePrefix_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("tenant-a:config", "a", "1"))
	err := storeInstance.DeletePrefix("tenant-a:")
	AssertNoError(t, err)
	AssertFalse(t, ax7MustGroupExists(t, storeInstance, "tenant-a:config"))
}

func TestStoreV090_Store_DeletePrefix_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	err := storeInstance.DeletePrefix("tenant-a:")
	AssertError(t, err)
}

func TestStoreV090_Store_DeletePrefix_Ugly(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("one", "a", "1"))
	err := storeInstance.DeletePrefix("")
	AssertNoError(t, err)
	AssertFalse(t, ax7MustGroupExists(t, storeInstance, "one"))
}

func TestStoreV090_Store_GetAll_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "a", "1"))
	entries, err := storeInstance.GetAll("config")
	AssertNoError(t, err)
	AssertEqual(t, "1", entries["a"])
}

func TestStoreV090_Store_GetAll_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	entries, err := storeInstance.GetAll("config")
	AssertError(t, err)
	AssertNil(t, entries)
}

func TestStoreV090_Store_GetAll_Ugly(t *T) {
	storeInstance := ax7Store(t)
	entries, err := storeInstance.GetAll("missing")
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestStoreV090_Store_GetPage_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "a", "1"))
	page, err := storeInstance.GetPage("config", 0, 1)
	AssertNoError(t, err)
	AssertEqual(t, "a", page[0].Key)
}

func TestStoreV090_Store_GetPage_Bad(t *T) {
	storeInstance := ax7Store(t)
	page, err := storeInstance.GetPage("config", -1, 1)
	AssertError(t, err)
	AssertNil(t, page)
}

func TestStoreV090_Store_GetPage_Ugly(t *T) {
	storeInstance := ax7Store(t)
	page, err := storeInstance.GetPage("missing", 0, 10)
	AssertNoError(t, err)
	AssertEmpty(t, page)
}

func TestStoreV090_Store_AllSeq_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "a", "1"))
	entries, err := ax7CollectKeyValues(storeInstance.AllSeq("config"))
	AssertNoError(t, err)
	AssertEqual(t, "a", entries[0].Key)
}

func TestStoreV090_Store_AllSeq_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	entries, err := ax7CollectKeyValues(storeInstance.AllSeq("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestStoreV090_Store_AllSeq_Ugly(t *T) {
	storeInstance := ax7Store(t)
	entries, err := ax7CollectKeyValues(storeInstance.AllSeq("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestStoreV090_Store_All_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "a", "1"))
	entries, err := ax7CollectKeyValues(storeInstance.All("config"))
	AssertNoError(t, err)
	AssertEqual(t, "1", entries[0].Value)
}

func TestStoreV090_Store_All_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	entries, err := ax7CollectKeyValues(storeInstance.All("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestStoreV090_Store_All_Ugly(t *T) {
	storeInstance := ax7Store(t)
	entries, err := ax7CollectKeyValues(storeInstance.All("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestStoreV090_Store_GetSplit_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "hosts", "a,b"))
	seq, err := storeInstance.GetSplit("config", "hosts", ",")
	AssertNoError(t, err)
	AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
}

func TestStoreV090_Store_GetSplit_Bad(t *T) {
	storeInstance := ax7Store(t)
	seq, err := storeInstance.GetSplit("missing", "hosts", ",")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestStoreV090_Store_GetSplit_Ugly(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "hosts", "ab"))
	seq, err := storeInstance.GetSplit("config", "hosts", "")
	AssertNoError(t, err)
	AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
}

func TestStoreV090_Store_GetFields_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "flags", "a b"))
	seq, err := storeInstance.GetFields("config", "flags")
	AssertNoError(t, err)
	AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
}

func TestStoreV090_Store_GetFields_Bad(t *T) {
	storeInstance := ax7Store(t)
	seq, err := storeInstance.GetFields("missing", "flags")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestStoreV090_Store_GetFields_Ugly(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "flags", " a	 b  "))
	seq, err := storeInstance.GetFields("config", "flags")
	AssertNoError(t, err)
	AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
}

func TestStoreV090_Store_Render_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "name", "alice"))
	rendered, err := storeInstance.Render("hello {{ .name }}", "config")
	AssertNoError(t, err)
	AssertEqual(t, "hello alice", rendered)
}

func TestStoreV090_Store_Render_Bad(t *T) {
	storeInstance := ax7Store(t)
	rendered, err := storeInstance.Render("{{", "config")
	AssertError(t, err)
	AssertEqual(t, "", rendered)
}

func TestStoreV090_Store_Render_Ugly(t *T) {
	storeInstance := ax7Store(t)
	rendered, err := storeInstance.Render("empty", "missing")
	AssertNoError(t, err)
	AssertEqual(t, "empty", rendered)
}

func TestStoreV090_Store_CountAll_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("tenant-a:config", "a", "1"))
	count, err := storeInstance.CountAll("tenant-a:")
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestStoreV090_Store_CountAll_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	count, err := storeInstance.CountAll("tenant-a:")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestStoreV090_Store_CountAll_Ugly(t *T) {
	storeInstance := ax7Store(t)
	count, err := storeInstance.CountAll("missing")
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestStoreV090_Store_Groups_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "a", "1"))
	groups, err := storeInstance.Groups()
	AssertNoError(t, err)
	AssertEqual(t, []string{"config"}, groups)
}

func TestStoreV090_Store_Groups_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	groups, err := storeInstance.Groups()
	AssertError(t, err)
	AssertNil(t, groups)
}

func TestStoreV090_Store_Groups_Ugly(t *T) {
	storeInstance := ax7Store(t)
	groups, err := storeInstance.Groups("missing")
	AssertNoError(t, err)
	AssertEmpty(t, groups)
}

func TestStoreV090_Store_GroupsSeq_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Set("config", "a", "1"))
	groups, err := ax7CollectGroups(storeInstance.GroupsSeq())
	AssertNoError(t, err)
	AssertEqual(t, []string{"config"}, groups)
}

func TestStoreV090_Store_GroupsSeq_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	groups, err := ax7CollectGroups(storeInstance.GroupsSeq())
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestStoreV090_Store_GroupsSeq_Ugly(t *T) {
	storeInstance := ax7Store(t)
	groups, err := ax7CollectGroups(storeInstance.GroupsSeq("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, groups)
}

func TestStoreV090_Store_PurgeExpired_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.SetWithTTL("session", "token", "abc", -Millisecond))
	removed, err := storeInstance.PurgeExpired()
	AssertNoError(t, err)
	AssertEqual(t, int64(1), removed)
}

func TestStoreV090_Store_PurgeExpired_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	removed, err := storeInstance.PurgeExpired()
	AssertError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestStoreV090_Store_PurgeExpired_Ugly(t *T) {
	storeInstance := ax7Store(t)
	removed, err := storeInstance.PurgeExpired()
	AssertNoError(t, err)
	AssertEqual(t, int64(0), removed)
}
