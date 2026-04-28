package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestScopeV090_QuotaConfig_Validate_Good(t *T) {
	quota := store.QuotaConfig{MaxKeys: 2, MaxGroups: 1}
	err := quota.Validate()
	AssertNoError(t, err)
}

func TestScopeV090_QuotaConfig_Validate_Bad(t *T) {
	quota := store.QuotaConfig{MaxKeys: -1}
	err := quota.Validate()
	AssertError(t, err)
}

func TestScopeV090_QuotaConfig_Validate_Ugly(t *T) {
	quota := store.QuotaConfig{}
	err := quota.Validate()
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreConfig_Validate_Good(t *T) {
	config := store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxKeys: 2}}
	err := config.Validate()
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreConfig_Validate_Bad(t *T) {
	config := store.ScopedStoreConfig{Namespace: "tenant_a"}
	err := config.Validate()
	AssertError(t, err)
}

func TestScopeV090_ScopedStoreConfig_Validate_Ugly(t *T) {
	config := store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxGroups: -1}}
	err := config.Validate()
	AssertError(t, err)
}

func TestScopeV090_NewScoped_Good(t *T) {
	scopedStore := store.NewScoped(ax7Store(t), "tenant-a")
	AssertNotNil(t, scopedStore)
	AssertEqual(t, "tenant-a", scopedStore.Namespace())
}

func TestScopeV090_NewScoped_Bad(t *T) {
	scopedStore := store.NewScoped(nil, "tenant-a")
	AssertNil(t, scopedStore)
	AssertEqual(t, "", scopedStore.Namespace())
}

func TestScopeV090_NewScoped_Ugly(t *T) {
	scopedStore := store.NewScoped(ax7Store(t), "tenant-42")
	AssertNotNil(t, scopedStore)
	AssertEqual(t, "tenant-42", scopedStore.Namespace())
}

func TestScopeV090_NewScopedConfigured_Good(t *T) {
	scopedStore, err := store.NewScopedConfigured(ax7Store(t), store.ScopedStoreConfig{Namespace: "tenant-a"})
	RequireNoError(t, err)
	AssertEqual(t, "tenant-a", scopedStore.Config().Namespace)
}

func TestScopeV090_NewScopedConfigured_Bad(t *T) {
	scopedStore, err := store.NewScopedConfigured(nil, store.ScopedStoreConfig{Namespace: "tenant-a"})
	AssertError(t, err)
	AssertNil(t, scopedStore)
}

func TestScopeV090_NewScopedConfigured_Ugly(t *T) {
	scopedStore, err := store.NewScopedConfigured(ax7Store(t), store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxKeys: 1, MaxGroups: 1}})
	RequireNoError(t, err)
	AssertEqual(t, 1, scopedStore.Config().Quota.MaxKeys)
}

func TestScopeV090_NewScopedWithQuota_Good(t *T) {
	scopedStore, err := store.NewScopedWithQuota(ax7Store(t), "tenant-a", store.QuotaConfig{MaxKeys: 1})
	RequireNoError(t, err)
	AssertEqual(t, 1, scopedStore.Config().Quota.MaxKeys)
}

func TestScopeV090_NewScopedWithQuota_Bad(t *T) {
	scopedStore, err := store.NewScopedWithQuota(nil, "tenant-a", store.QuotaConfig{})
	AssertError(t, err)
	AssertNil(t, scopedStore)
}

func TestScopeV090_NewScopedWithQuota_Ugly(t *T) {
	scopedStore, err := store.NewScopedWithQuota(ax7Store(t), "tenant-a", store.QuotaConfig{MaxGroups: 1})
	RequireNoError(t, err)
	AssertEqual(t, 1, scopedStore.Config().Quota.MaxGroups)
}

func TestScopeV090_ScopedStore_Namespace_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	namespace := scopedStore.Namespace()
	AssertEqual(t, "tenant-a", namespace)
}

func TestScopeV090_ScopedStore_Namespace_Bad(t *T) {
	var scopedStore *store.ScopedStore
	namespace := scopedStore.Namespace()
	AssertEqual(t, "", namespace)
}

func TestScopeV090_ScopedStore_Namespace_Ugly(t *T) {
	scopedStore := ax7QuotaScopedStore(t, 1, 1)
	namespace := scopedStore.Namespace()
	AssertEqual(t, "tenant-a", namespace)
}

func TestScopeV090_ScopedStore_Config_Good(t *T) {
	scopedStore := ax7QuotaScopedStore(t, 2, 1)
	config := scopedStore.Config()
	AssertEqual(t, 2, config.Quota.MaxKeys)
}

func TestScopeV090_ScopedStore_Config_Bad(t *T) {
	var scopedStore *store.ScopedStore
	config := scopedStore.Config()
	AssertEqual(t, store.ScopedStoreConfig{}, config)
}

func TestScopeV090_ScopedStore_Config_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	config := scopedStore.Config()
	AssertEqual(t, "tenant-a", config.Namespace)
}

func TestScopeV090_ScopedStore_Exists_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.Set("colour", "blue"))
	exists, err := scopedStore.Exists("colour")
	AssertNoError(t, err)
	AssertTrue(t, exists)
}

func TestScopeV090_ScopedStore_Exists_Bad(t *T) {
	var scopedStore *store.ScopedStore
	exists, err := scopedStore.Exists("colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStore_Exists_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	exists, err := scopedStore.Exists("missing")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStore_ExistsIn_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	exists, err := scopedStore.ExistsIn("config", "colour")
	AssertNoError(t, err)
	AssertTrue(t, exists)
}

func TestScopeV090_ScopedStore_ExistsIn_Bad(t *T) {
	var scopedStore *store.ScopedStore
	exists, err := scopedStore.ExistsIn("config", "colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStore_ExistsIn_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	exists, err := scopedStore.ExistsIn("config", "missing")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStore_GroupExists_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	exists, err := scopedStore.GroupExists("config")
	AssertNoError(t, err)
	AssertTrue(t, exists)
}

func TestScopeV090_ScopedStore_GroupExists_Bad(t *T) {
	var scopedStore *store.ScopedStore
	exists, err := scopedStore.GroupExists("config")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStore_GroupExists_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	exists, err := scopedStore.GroupExists("missing")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStore_Get_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.Set("colour", "blue"))
	got, err := scopedStore.Get("colour")
	AssertNoError(t, err)
	AssertEqual(t, "blue", got)
}

func TestScopeV090_ScopedStore_Get_Bad(t *T) {
	var scopedStore *store.ScopedStore
	got, err := scopedStore.Get("colour")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestScopeV090_ScopedStore_Get_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	got, err := scopedStore.Get("missing")
	AssertErrorIs(t, err, store.NotFoundError)
	AssertEqual(t, "", got)
}

func TestScopeV090_ScopedStore_GetFrom_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	got, err := scopedStore.GetFrom("config", "colour")
	AssertNoError(t, err)
	AssertEqual(t, "blue", got)
}

func TestScopeV090_ScopedStore_GetFrom_Bad(t *T) {
	var scopedStore *store.ScopedStore
	got, err := scopedStore.GetFrom("config", "colour")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestScopeV090_ScopedStore_GetFrom_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	got, err := scopedStore.GetFrom("config", "missing")
	AssertErrorIs(t, err, store.NotFoundError)
	AssertEqual(t, "", got)
}

func TestScopeV090_ScopedStore_Set_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Set("colour", "blue")
	AssertNoError(t, err)
	AssertTrue(t, ax7ScopedExists(t, scopedStore, "colour"))
}

func TestScopeV090_ScopedStore_Set_Bad(t *T) {
	var scopedStore *store.ScopedStore
	err := scopedStore.Set("colour", "blue")
	AssertError(t, err)
}

func TestScopeV090_ScopedStore_Set_Ugly(t *T) {
	scopedStore := ax7QuotaScopedStore(t, 1, 0)
	RequireNoError(t, scopedStore.Set("colour", "blue"))
	err := scopedStore.Set("shape", "circle")
	AssertErrorIs(t, err, store.QuotaExceededError)
}

func TestScopeV090_ScopedStore_SetIn_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.SetIn("config", "colour", "blue")
	AssertNoError(t, err)
	AssertTrue(t, ax7ScopedExistsIn(t, scopedStore, "config", "colour"))
}

func TestScopeV090_ScopedStore_SetIn_Bad(t *T) {
	var scopedStore *store.ScopedStore
	err := scopedStore.SetIn("config", "colour", "blue")
	AssertError(t, err)
}

func TestScopeV090_ScopedStore_SetIn_Ugly(t *T) {
	scopedStore := ax7QuotaScopedStore(t, 0, 1)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	err := scopedStore.SetIn("other", "shape", "circle")
	AssertErrorIs(t, err, store.QuotaExceededError)
}

func TestScopeV090_ScopedStore_SetWithTTL_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.SetWithTTL("sessions", "token", "abc", Hour)
	AssertNoError(t, err)
	AssertTrue(t, ax7ScopedExistsIn(t, scopedStore, "sessions", "token"))
}

func TestScopeV090_ScopedStore_SetWithTTL_Bad(t *T) {
	var scopedStore *store.ScopedStore
	err := scopedStore.SetWithTTL("sessions", "token", "abc", Hour)
	AssertError(t, err)
}

func TestScopeV090_ScopedStore_SetWithTTL_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetWithTTL("sessions", "token", "abc", -Millisecond))
	exists, err := scopedStore.ExistsIn("sessions", "token")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStore_Delete_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	err := scopedStore.Delete("config", "colour")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedExistsIn(t, scopedStore, "config", "colour"))
}

func TestScopeV090_ScopedStore_Delete_Bad(t *T) {
	var scopedStore *store.ScopedStore
	err := scopedStore.Delete("config", "colour")
	AssertError(t, err)
}

func TestScopeV090_ScopedStore_Delete_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Delete("missing", "key")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedExistsIn(t, scopedStore, "missing", "key"))
}

func TestScopeV090_ScopedStore_DeleteGroup_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	err := scopedStore.DeleteGroup("config")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedGroupExists(t, scopedStore, "config"))
}

func TestScopeV090_ScopedStore_DeleteGroup_Bad(t *T) {
	var scopedStore *store.ScopedStore
	err := scopedStore.DeleteGroup("config")
	AssertError(t, err)
}

func TestScopeV090_ScopedStore_DeleteGroup_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.DeleteGroup("missing")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedGroupExists(t, scopedStore, "missing"))
}

func TestScopeV090_ScopedStore_DeletePrefix_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("cache-a", "k", "v"))
	err := scopedStore.DeletePrefix("cache")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedGroupExists(t, scopedStore, "cache-a"))
}

func TestScopeV090_ScopedStore_DeletePrefix_Bad(t *T) {
	var scopedStore *store.ScopedStore
	err := scopedStore.DeletePrefix("cache")
	AssertError(t, err)
}

func TestScopeV090_ScopedStore_DeletePrefix_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("cache", "k", "v"))
	err := scopedStore.DeletePrefix("")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedGroupExists(t, scopedStore, "cache"))
}

func TestScopeV090_ScopedStore_GetAll_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	entries, err := scopedStore.GetAll("config")
	AssertNoError(t, err)
	AssertEqual(t, "blue", entries["colour"])
}

func TestScopeV090_ScopedStore_GetAll_Bad(t *T) {
	var scopedStore *store.ScopedStore
	entries, err := scopedStore.GetAll("config")
	AssertError(t, err)
	AssertNil(t, entries)
}

func TestScopeV090_ScopedStore_GetAll_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	entries, err := scopedStore.GetAll("missing")
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestScopeV090_ScopedStore_GetPage_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	page, err := scopedStore.GetPage("config", 0, 1)
	AssertNoError(t, err)
	AssertEqual(t, "colour", page[0].Key)
}

func TestScopeV090_ScopedStore_GetPage_Bad(t *T) {
	var scopedStore *store.ScopedStore
	page, err := scopedStore.GetPage("config", 0, 1)
	AssertError(t, err)
	AssertNil(t, page)
}

func TestScopeV090_ScopedStore_GetPage_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	page, err := scopedStore.GetPage("missing", 0, 1)
	AssertNoError(t, err)
	AssertEmpty(t, page)
}

func TestScopeV090_ScopedStore_All_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	entries, err := ax7CollectKeyValues(scopedStore.All("config"))
	AssertNoError(t, err)
	AssertEqual(t, "colour", entries[0].Key)
}

func TestScopeV090_ScopedStore_All_Bad(t *T) {
	var scopedStore *store.ScopedStore
	entries, err := ax7CollectKeyValues(scopedStore.All("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestScopeV090_ScopedStore_All_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	entries, err := ax7CollectKeyValues(scopedStore.All("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestScopeV090_ScopedStore_AllSeq_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	entries, err := ax7CollectKeyValues(scopedStore.AllSeq("config"))
	AssertNoError(t, err)
	AssertEqual(t, "blue", entries[0].Value)
}

func TestScopeV090_ScopedStore_AllSeq_Bad(t *T) {
	var scopedStore *store.ScopedStore
	entries, err := ax7CollectKeyValues(scopedStore.AllSeq("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestScopeV090_ScopedStore_AllSeq_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	entries, err := ax7CollectKeyValues(scopedStore.AllSeq("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestScopeV090_ScopedStore_Count_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	count, err := scopedStore.Count("config")
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestScopeV090_ScopedStore_Count_Bad(t *T) {
	var scopedStore *store.ScopedStore
	count, err := scopedStore.Count("config")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestScopeV090_ScopedStore_Count_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	count, err := scopedStore.Count("missing")
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestScopeV090_ScopedStore_CountAll_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	count, err := scopedStore.CountAll()
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestScopeV090_ScopedStore_CountAll_Bad(t *T) {
	var scopedStore *store.ScopedStore
	count, err := scopedStore.CountAll()
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestScopeV090_ScopedStore_CountAll_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	count, err := scopedStore.CountAll("missing")
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestScopeV090_ScopedStore_Groups_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	groups, err := scopedStore.Groups()
	AssertNoError(t, err)
	AssertEqual(t, []string{"config"}, groups)
}

func TestScopeV090_ScopedStore_Groups_Bad(t *T) {
	var scopedStore *store.ScopedStore
	groups, err := scopedStore.Groups()
	AssertError(t, err)
	AssertNil(t, groups)
}

func TestScopeV090_ScopedStore_Groups_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	groups, err := scopedStore.Groups("missing")
	AssertNoError(t, err)
	AssertEmpty(t, groups)
}

func TestScopeV090_ScopedStore_GroupsSeq_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	groups, err := ax7CollectGroups(scopedStore.GroupsSeq())
	AssertNoError(t, err)
	AssertEqual(t, []string{"config"}, groups)
}

func TestScopeV090_ScopedStore_GroupsSeq_Bad(t *T) {
	var scopedStore *store.ScopedStore
	groups, err := ax7CollectGroups(scopedStore.GroupsSeq())
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestScopeV090_ScopedStore_GroupsSeq_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	groups, err := ax7CollectGroups(scopedStore.GroupsSeq("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, groups)
}

func TestScopeV090_ScopedStore_Render_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "name", "alice"))
	rendered, err := scopedStore.Render("hello {{ .name }}", "config")
	AssertNoError(t, err)
	AssertEqual(t, "hello alice", rendered)
}

func TestScopeV090_ScopedStore_Render_Bad(t *T) {
	var scopedStore *store.ScopedStore
	rendered, err := scopedStore.Render("hello", "config")
	AssertError(t, err)
	AssertEqual(t, "", rendered)
}

func TestScopeV090_ScopedStore_Render_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	rendered, err := scopedStore.Render("empty", "missing")
	AssertNoError(t, err)
	AssertEqual(t, "empty", rendered)
}

func TestScopeV090_ScopedStore_GetSplit_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "hosts", "a,b"))
	seq, err := scopedStore.GetSplit("config", "hosts", ",")
	AssertNoError(t, err)
	AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
}

func TestScopeV090_ScopedStore_GetSplit_Bad(t *T) {
	var scopedStore *store.ScopedStore
	seq, err := scopedStore.GetSplit("config", "hosts", ",")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestScopeV090_ScopedStore_GetSplit_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	seq, err := scopedStore.GetSplit("config", "missing", ",")
	AssertErrorIs(t, err, store.NotFoundError)
	AssertNil(t, seq)
}

func TestScopeV090_ScopedStore_GetFields_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "flags", "a b"))
	seq, err := scopedStore.GetFields("config", "flags")
	AssertNoError(t, err)
	AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
}

func TestScopeV090_ScopedStore_GetFields_Bad(t *T) {
	var scopedStore *store.ScopedStore
	seq, err := scopedStore.GetFields("config", "flags")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestScopeV090_ScopedStore_GetFields_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	seq, err := scopedStore.GetFields("config", "missing")
	AssertErrorIs(t, err, store.NotFoundError)
	AssertNil(t, seq)
}

func TestScopeV090_ScopedStore_PurgeExpired_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetWithTTL("sessions", "token", "abc", -Millisecond))
	removed, err := scopedStore.PurgeExpired()
	AssertNoError(t, err)
	AssertEqual(t, int64(1), removed)
}

func TestScopeV090_ScopedStore_PurgeExpired_Bad(t *T) {
	var scopedStore *store.ScopedStore
	removed, err := scopedStore.PurgeExpired()
	AssertError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestScopeV090_ScopedStore_PurgeExpired_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	removed, err := scopedStore.PurgeExpired()
	AssertNoError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestScopeV090_ScopedStore_Watch_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	events := scopedStore.Watch("config")
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	event := <-events
	AssertEqual(t, "config", event.Group)
}

func TestScopeV090_ScopedStore_Watch_Bad(t *T) {
	var scopedStore *store.ScopedStore
	events := scopedStore.Watch("config")
	_, ok := <-events
	AssertFalse(t, ok)
}

func TestScopeV090_ScopedStore_Watch_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	events := scopedStore.Watch("*")
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	event := <-events
	AssertEqual(t, "config", event.Group)
}

func TestScopeV090_ScopedStore_Unwatch_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)
	_, ok := <-events
	AssertFalse(t, ok)
}

func TestScopeV090_ScopedStore_Unwatch_Bad(t *T) {
	var scopedStore *store.ScopedStore
	AssertNotPanics(t, func() { scopedStore.Unwatch("config", nil) })
	AssertEqual(t, "", scopedStore.Namespace())
}

func TestScopeV090_ScopedStore_Unwatch_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)
	AssertNotPanics(t, func() { scopedStore.Unwatch("config", events) })
}

func TestScopeV090_ScopedStore_OnChange_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	called := false
	unregister := scopedStore.OnChange(func(event store.Event) { called = event.Group == "config" })
	defer unregister()
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	AssertTrue(t, called)
}

func TestScopeV090_ScopedStore_OnChange_Bad(t *T) {
	var scopedStore *store.ScopedStore
	unregister := scopedStore.OnChange(func(store.Event) {})
	unregister()
	AssertEqual(t, "", scopedStore.Namespace())
}

func TestScopeV090_ScopedStore_OnChange_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	count := 0
	unregister := scopedStore.OnChange(func(store.Event) { count++ })
	unregister()
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	AssertEqual(t, 0, count)
}

func TestScopeV090_ScopedStore_Transaction_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error { return transaction.Set("colour", "blue") })
	AssertNoError(t, err)
	AssertTrue(t, ax7ScopedExists(t, scopedStore, "colour"))
}

func TestScopeV090_ScopedStore_Transaction_Bad(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(nil)
	AssertError(t, err)
	AssertFalse(t, ax7ScopedExists(t, scopedStore, "colour"))
}

func TestScopeV090_ScopedStore_Transaction_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error { return NewError("rollback") })
	AssertError(t, err)
	AssertFalse(t, ax7ScopedExists(t, scopedStore, "colour"))
}

func TestScopeV090_ScopedStoreTransaction_Exists_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.Set("colour", "blue"))
		exists, err := transaction.Exists("colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Exists_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	exists, err := transaction.Exists("colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStoreTransaction_Exists_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		exists, err := transaction.Exists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_ExistsIn_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		exists, err := transaction.ExistsIn("config", "colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_ExistsIn_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	exists, err := transaction.ExistsIn("config", "colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStoreTransaction_ExistsIn_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		exists, err := transaction.ExistsIn("config", "missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GroupExists_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		exists, err := transaction.GroupExists("config")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GroupExists_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	exists, err := transaction.GroupExists("config")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScopeV090_ScopedStoreTransaction_GroupExists_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		exists, err := transaction.GroupExists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Get_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.Set("colour", "blue"))
		got, err := transaction.Get("colour")
		AssertNoError(t, err)
		AssertEqual(t, "blue", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Get_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	got, err := transaction.Get("colour")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestScopeV090_ScopedStoreTransaction_Get_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		got, err := transaction.Get("missing")
		AssertErrorIs(t, err, store.NotFoundError)
		AssertEqual(t, "", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetFrom_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		got, err := transaction.GetFrom("config", "colour")
		AssertNoError(t, err)
		AssertEqual(t, "blue", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetFrom_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	got, err := transaction.GetFrom("config", "colour")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestScopeV090_ScopedStoreTransaction_GetFrom_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		got, err := transaction.GetFrom("config", "missing")
		AssertErrorIs(t, err, store.NotFoundError)
		AssertEqual(t, "", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Set_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		err := transaction.Set("colour", "blue")
		AssertNoError(t, err)
		exists, err := transaction.Exists("colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Set_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	err := transaction.Set("colour", "blue")
	AssertError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Set_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.Set("colour", "blue"))
		err := transaction.Set("shape", "circle")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_SetIn_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		err := transaction.SetIn("config", "colour", "blue")
		AssertNoError(t, err)
		exists, err := transaction.ExistsIn("config", "colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_SetIn_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	err := transaction.SetIn("config", "colour", "blue")
	AssertError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_SetIn_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		err := transaction.SetIn("config", "colour", "green")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_SetWithTTL_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		err := transaction.SetWithTTL("sessions", "token", "abc", Hour)
		AssertNoError(t, err)
		exists, err := transaction.ExistsIn("sessions", "token")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_SetWithTTL_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	err := transaction.SetWithTTL("sessions", "token", "abc", Hour)
	AssertError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_SetWithTTL_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetWithTTL("sessions", "token", "abc", -Millisecond))
		exists, err := transaction.ExistsIn("sessions", "token")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Delete_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		err := transaction.Delete("config", "colour")
		AssertNoError(t, err)
		exists, err := transaction.ExistsIn("config", "colour")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Delete_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	err := transaction.Delete("config", "colour")
	AssertError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Delete_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		err := transaction.Delete("missing", "key")
		AssertNoError(t, err)
		exists, err := transaction.ExistsIn("missing", "key")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_DeleteGroup_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		err := transaction.DeleteGroup("config")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists("config")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_DeleteGroup_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	err := transaction.DeleteGroup("config")
	AssertError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_DeleteGroup_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		err := transaction.DeleteGroup("missing")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_DeletePrefix_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("cache-a", "colour", "blue"))
		err := transaction.DeletePrefix("cache")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists("cache-a")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_DeletePrefix_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	err := transaction.DeletePrefix("cache")
	AssertError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_DeletePrefix_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		err := transaction.DeletePrefix("missing")
		AssertNoError(t, err)
		groups, err := transaction.Groups("missing")
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetAll_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		entries, err := transaction.GetAll("config")
		AssertNoError(t, err)
		AssertEqual(t, "blue", entries["colour"])
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetAll_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	entries, err := transaction.GetAll("config")
	AssertError(t, err)
	AssertNil(t, entries)
}

func TestScopeV090_ScopedStoreTransaction_GetAll_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		entries, err := transaction.GetAll("missing")
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetPage_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		page, err := transaction.GetPage("config", 0, 1)
		AssertNoError(t, err)
		AssertEqual(t, "colour", page[0].Key)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetPage_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	page, err := transaction.GetPage("config", 0, 1)
	AssertError(t, err)
	AssertNil(t, page)
}

func TestScopeV090_ScopedStoreTransaction_GetPage_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		page, err := transaction.GetPage("missing", 0, 1)
		AssertNoError(t, err)
		AssertEmpty(t, page)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_All_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		entries, err := ax7CollectKeyValues(transaction.All("config"))
		AssertNoError(t, err)
		AssertEqual(t, "colour", entries[0].Key)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_All_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	entries, err := ax7CollectKeyValues(transaction.All("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestScopeV090_ScopedStoreTransaction_All_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		entries, err := ax7CollectKeyValues(transaction.All("missing"))
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_AllSeq_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		entries, err := ax7CollectKeyValues(transaction.AllSeq("config"))
		AssertNoError(t, err)
		AssertEqual(t, "blue", entries[0].Value)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_AllSeq_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	entries, err := ax7CollectKeyValues(transaction.AllSeq("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestScopeV090_ScopedStoreTransaction_AllSeq_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		entries, err := ax7CollectKeyValues(transaction.AllSeq("missing"))
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Count_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		count, err := transaction.Count("config")
		AssertNoError(t, err)
		AssertEqual(t, 1, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Count_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	count, err := transaction.Count("config")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestScopeV090_ScopedStoreTransaction_Count_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		count, err := transaction.Count("missing")
		AssertNoError(t, err)
		AssertEqual(t, 0, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_CountAll_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		count, err := transaction.CountAll()
		AssertNoError(t, err)
		AssertEqual(t, 1, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_CountAll_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	count, err := transaction.CountAll()
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestScopeV090_ScopedStoreTransaction_CountAll_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		count, err := transaction.CountAll("missing")
		AssertNoError(t, err)
		AssertEqual(t, 0, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Groups_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		groups, err := transaction.Groups()
		AssertNoError(t, err)
		AssertEqual(t, []string{"config"}, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Groups_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	groups, err := transaction.Groups()
	AssertError(t, err)
	AssertNil(t, groups)
}

func TestScopeV090_ScopedStoreTransaction_Groups_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		groups, err := transaction.Groups("missing")
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GroupsSeq_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		groups, err := ax7CollectGroups(transaction.GroupsSeq())
		AssertNoError(t, err)
		AssertEqual(t, []string{"config"}, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GroupsSeq_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	groups, err := ax7CollectGroups(transaction.GroupsSeq())
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestScopeV090_ScopedStoreTransaction_GroupsSeq_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		groups, err := ax7CollectGroups(transaction.GroupsSeq("missing"))
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Render_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "name", "alice"))
		rendered, err := transaction.Render("hello {{ .name }}", "config")
		AssertNoError(t, err)
		AssertEqual(t, "hello alice", rendered)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_Render_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	rendered, err := transaction.Render("hello", "config")
	AssertError(t, err)
	AssertEqual(t, "", rendered)
}

func TestScopeV090_ScopedStoreTransaction_Render_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		rendered, err := transaction.Render("empty", "missing")
		AssertNoError(t, err)
		AssertEqual(t, "empty", rendered)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetSplit_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "hosts", "a,b"))
		seq, err := transaction.GetSplit("config", "hosts", ",")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetSplit_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	seq, err := transaction.GetSplit("config", "hosts", ",")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestScopeV090_ScopedStoreTransaction_GetSplit_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		seq, err := transaction.GetSplit("missing", "hosts", ",")
		AssertErrorIs(t, err, store.NotFoundError)
		AssertNil(t, seq)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetFields_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "flags", "a b"))
		seq, err := transaction.GetFields("config", "flags")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_GetFields_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	seq, err := transaction.GetFields("config", "flags")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestScopeV090_ScopedStoreTransaction_GetFields_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		seq, err := transaction.GetFields("missing", "flags")
		AssertErrorIs(t, err, store.NotFoundError)
		AssertNil(t, seq)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_PurgeExpired_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetWithTTL("sessions", "token", "abc", -Millisecond))
		removed, err := transaction.PurgeExpired()
		AssertNoError(t, err)
		AssertEqual(t, int64(1), removed)
		return nil
	})
	AssertNoError(t, err)
}

func TestScopeV090_ScopedStoreTransaction_PurgeExpired_Bad(t *T) {
	var transaction *store.ScopedStoreTransaction
	removed, err := transaction.PurgeExpired()
	AssertError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestScopeV090_ScopedStoreTransaction_PurgeExpired_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error {
		removed, err := transaction.PurgeExpired()
		AssertNoError(t, err)
		AssertEqual(t, int64(0), removed)
		return nil
	})
	AssertNoError(t, err)
}
