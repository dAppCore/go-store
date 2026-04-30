package store

import (
	"time"

	core "dappco.re/go"
)

func exampleScopedStore() (*Store, *ScopedStore) {
	storeInstance := exampleOpenStore()
	scopedStore := NewScoped(storeInstance, "tenant-a")
	return storeInstance, scopedStore
}

func exampleScopedStoreWithData() (*Store, *ScopedStore) {
	storeInstance, scopedStore := exampleScopedStore()
	exampleRequireOK(scopedStore.Set("colour", "blue"))
	exampleRequireOK(scopedStore.SetIn("config", "tags", "alpha,beta"))
	return storeInstance, scopedStore
}

func ExampleQuotaConfig_Validate() {
	config := QuotaConfig{MaxKeys: 10, MaxGroups: 2}
	result := config.Validate()
	core.Println(result.OK)
}

func ExampleScopedStoreConfig_Validate() {
	config := ScopedStoreConfig{Namespace: "tenant-a", Quota: QuotaConfig{MaxKeys: 10}}
	result := config.Validate()
	core.Println(result.OK)
}

func ExampleNewScoped() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	scopedStore := NewScoped(storeInstance, "tenant-a")
	core.Println(scopedStore.Namespace())
}

func ExampleNewScopedConfigured() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	scopedStore, result := NewScopedConfigured(storeInstance, ScopedStoreConfig{Namespace: "tenant-a"})
	exampleRequireOK(result)
	core.Println(scopedStore.Namespace())
}

func ExampleNewScopedWithQuota() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	scopedStore, result := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 10})
	exampleRequireOK(result)
	core.Println(scopedStore.Config().Quota.MaxKeys)
}

func ExampleScopedStore_Namespace() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	core.Println(scopedStore.Namespace())
}

func ExampleScopedStore_Config() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	core.Println(scopedStore.Config().Namespace)
}

func ExampleScopedStore_Exists() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	exists, result := scopedStore.Exists("colour")
	exampleRequireOK(result)
	core.Println(exists)
}

func ExampleScopedStore_ExistsIn() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	exists, result := scopedStore.ExistsIn("config", "tags")
	exampleRequireOK(result)
	core.Println(exists)
}

func ExampleScopedStore_GroupExists() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	exists, result := scopedStore.GroupExists("config")
	exampleRequireOK(result)
	core.Println(exists)
}

func ExampleScopedStore_Get() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	value, result := scopedStore.Get("colour")
	exampleRequireOK(result)
	core.Println(value)
}

func ExampleScopedStore_GetFrom() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	value, result := scopedStore.GetFrom("config", "tags")
	exampleRequireOK(result)
	core.Println(value)
}

func ExampleScopedStore_Set() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Set("colour", "blue")
	exampleRequireOK(result)
}

func ExampleScopedStore_SetIn() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.SetIn("config", "colour", "blue")
	exampleRequireOK(result)
}

func ExampleScopedStore_SetWithTTL() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.SetWithTTL(defaultScopedGroupName, "token", "abc", time.Minute)
	exampleRequireOK(result)
}

func ExampleScopedStore_Delete() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Delete(defaultScopedGroupName, "colour")
	exampleRequireOK(result)
}

func ExampleScopedStore_DeleteGroup() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.DeleteGroup("config")
	exampleRequireOK(result)
}

func ExampleScopedStore_DeletePrefix() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.DeletePrefix("conf")
	exampleRequireOK(result)
}

func ExampleScopedStore_GetAll() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	values, result := scopedStore.GetAll("config")
	exampleRequireOK(result)
	core.Println(values["tags"])
}

func ExampleScopedStore_GetPage() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	entries, result := scopedStore.GetPage("config", 0, 10)
	exampleRequireOK(result)
	core.Println(len(entries))
}

func ExampleScopedStore_All() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	entries := exampleCollectKeyValues(scopedStore.All("config"))
	core.Println(len(entries))
}

func ExampleScopedStore_AllSeq() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	entries := exampleCollectKeyValues(scopedStore.AllSeq("config"))
	core.Println(len(entries))
}

func ExampleScopedStore_Count() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	count, result := scopedStore.Count("config")
	exampleRequireOK(result)
	core.Println(count)
}

func ExampleScopedStore_CountAll() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	count, result := scopedStore.CountAll("conf")
	exampleRequireOK(result)
	core.Println(count)
}

func ExampleScopedStore_Groups() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	groups, result := scopedStore.Groups()
	exampleRequireOK(result)
	core.Println(groups)
}

func ExampleScopedStore_GroupsSeq() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	groups := exampleCollectGroups(scopedStore.GroupsSeq())
	core.Println(groups)
}

func ExampleScopedStore_Render() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	rendered, result := scopedStore.Render("tags={{.tags}}", "config")
	exampleRequireOK(result)
	core.Println(rendered)
}

func ExampleScopedStore_GetSplit() {
	storeInstance, scopedStore := exampleScopedStoreWithData()
	defer exampleCloseStore(storeInstance)
	seq, result := scopedStore.GetSplit("config", "tags", ",")
	exampleRequireOK(result)
	core.Println(exampleCollectStrings(seq))
}

func ExampleScopedStore_GetFields() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	exampleRequireOK(scopedStore.SetIn("config", "text", "alpha beta"))
	seq, result := scopedStore.GetFields("config", "text")
	exampleRequireOK(result)
	core.Println(exampleCollectStrings(seq))
}

func ExampleScopedStore_PurgeExpired() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	exampleRequireOK(scopedStore.SetWithTTL(defaultScopedGroupName, "token", "abc", time.Nanosecond))
	removed, result := scopedStore.PurgeExpired()
	exampleRequireOK(result)
	core.Println(removed)
}

func ExampleScopedStore_Watch() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)
}

func ExampleScopedStore_Unwatch() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)
}

func ExampleScopedStore_OnChange() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	unregister := scopedStore.OnChange(func(event Event) {
		core.Println(event.Group)
	})
	unregister()
}

func ExampleScopedStore_Transaction() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		return transaction.Set("colour", "blue")
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_Exists() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("colour", "blue"))
		exists, existsResult := transaction.Exists("colour")
		exampleRequireOK(existsResult)
		core.Println(exists)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_ExistsIn() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		exists, existsResult := transaction.ExistsIn("config", "colour")
		exampleRequireOK(existsResult)
		core.Println(exists)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_GroupExists() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		exists, existsResult := transaction.GroupExists("config")
		exampleRequireOK(existsResult)
		core.Println(exists)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_Get() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("colour", "blue"))
		value, getResult := transaction.Get("colour")
		exampleRequireOK(getResult)
		core.Println(value)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_GetFrom() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		value, getResult := transaction.GetFrom("config", "colour")
		exampleRequireOK(getResult)
		core.Println(value)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_Set() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		return transaction.Set("colour", "blue")
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_SetIn() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		return transaction.SetIn("config", "colour", "blue")
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_SetWithTTL() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		return transaction.SetWithTTL(defaultScopedGroupName, "token", "abc", time.Minute)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_Delete() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		return transaction.Delete("config", "colour")
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_DeleteGroup() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		return transaction.DeleteGroup("config")
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_DeletePrefix() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		return transaction.DeletePrefix("conf")
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_GetAll() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		values, valuesResult := transaction.GetAll("config")
		exampleRequireOK(valuesResult)
		core.Println(values["colour"])
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_GetPage() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		entries, entriesResult := transaction.GetPage("config", 0, 10)
		exampleRequireOK(entriesResult)
		core.Println(len(entries))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_All() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		entries := exampleCollectKeyValues(transaction.All("config"))
		core.Println(len(entries))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_AllSeq() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		entries := exampleCollectKeyValues(transaction.AllSeq("config"))
		core.Println(len(entries))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_Count() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		count, countResult := transaction.Count("config")
		exampleRequireOK(countResult)
		core.Println(count)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_CountAll() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		count, countResult := transaction.CountAll("conf")
		exampleRequireOK(countResult)
		core.Println(count)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_Groups() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		groups, groupsResult := transaction.Groups()
		exampleRequireOK(groupsResult)
		core.Println(groups)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_GroupsSeq() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		groups := exampleCollectGroups(transaction.GroupsSeq())
		core.Println(groups)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_Render() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "colour", "blue"))
		rendered, renderResult := transaction.Render("colour={{.colour}}", "config")
		exampleRequireOK(renderResult)
		core.Println(rendered)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_GetSplit() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "tags", "alpha,beta"))
		seq, splitResult := transaction.GetSplit("config", "tags", ",")
		exampleRequireOK(splitResult)
		core.Println(exampleCollectStrings(seq))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_GetFields() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetIn("config", "text", "alpha beta"))
		seq, fieldsResult := transaction.GetFields("config", "text")
		exampleRequireOK(fieldsResult)
		core.Println(exampleCollectStrings(seq))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleScopedStoreTransaction_PurgeExpired() {
	storeInstance, scopedStore := exampleScopedStore()
	defer exampleCloseStore(storeInstance)
	result := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) core.Result {
		exampleRequireOK(transaction.SetWithTTL(defaultScopedGroupName, "token", "abc", time.Nanosecond))
		removed, purgeResult := transaction.PurgeExpired()
		exampleRequireOK(purgeResult)
		core.Println(removed)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}
