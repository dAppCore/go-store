package store

import (
	"time"

	core "dappco.re/go"
)

func exampleRequireOK(result core.Result) {
	if !result.OK {
		panic(result.Value)
	}
}

func exampleOpenStore() *Store {
	storeInstance, result := New(":memory:")
	exampleRequireOK(result)
	return storeInstance
}

func exampleOpenConfiguredStore() *Store {
	storeInstance, result := NewConfigured(StoreConfig{
		DatabasePath:            ":memory:",
		PurgeInterval:           time.Minute,
		WorkspaceStateDirectory: ".core/example-state",
	})
	exampleRequireOK(result)
	return storeInstance
}

func exampleCloseStore(storeInstance *Store) {
	if storeInstance == nil {
		return
	}
	result := storeInstance.Close()
	exampleRequireOK(result)
}

func exampleStoreWithConfig() *Store {
	storeInstance := exampleOpenStore()
	exampleRequireOK(storeInstance.Set("config", "colour", "blue"))
	exampleRequireOK(storeInstance.Set("config", "tags", "alpha,beta"))
	return storeInstance
}

func exampleWorkspace() (*Store, *Workspace) {
	storeInstance := exampleOpenConfiguredStore()
	workspace, result := storeInstance.NewWorkspace("example-workspace")
	exampleRequireOK(result)
	return storeInstance, workspace
}

func exampleCollectKeyValues(seq core.Seq2[KeyValue, error]) []KeyValue {
	entries := []KeyValue{}
	for entry, err := range seq {
		if err != nil {
			panic(err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func exampleCollectGroups(seq core.Seq2[string, error]) []string {
	groups := []string{}
	for group, err := range seq {
		if err != nil {
			panic(err)
		}
		groups = append(groups, group)
	}
	return groups
}

func exampleCollectStrings(seq core.Seq[string]) []string {
	values := []string{}
	for value := range seq {
		values = append(values, value)
	}
	return values
}

func ExampleStoreConfig_Normalised() {
	config := StoreConfig{DatabasePath: ":memory:"}
	normalised := config.Normalised()
	core.Println(normalised.PurgeInterval > 0)
}

func ExampleStoreConfig_Validate() {
	config := StoreConfig{DatabasePath: ":memory:"}
	result := config.Validate()
	core.Println(result.OK)
}

func ExampleJournalConfiguration_Validate() {
	config := JournalConfiguration{
		EndpointURL:  "http://127.0.0.1:8086",
		Organisation: "core",
		BucketName:   "events",
	}
	result := config.Validate()
	core.Println(result.OK)
}

func ExampleWithJournal() {
	storeInstance, result := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.JournalConfigured())
}

func ExampleWithWorkspaceStateDirectory() {
	storeInstance, result := New(":memory:", WithWorkspaceStateDirectory(".core/workspaces"))
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.WorkspaceStateDirectory())
}

func ExampleStore_JournalConfiguration() {
	storeInstance, result := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.JournalConfiguration().BucketName)
}

func ExampleStore_JournalConfigured() {
	storeInstance, result := New(":memory:", WithJournal("http://127.0.0.1:8086", "core", "events"))
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.JournalConfigured())
}

func ExampleStore_Config() {
	storeInstance := exampleOpenConfiguredStore()
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.Config().WorkspaceStateDirectory)
}

func ExampleStore_DatabasePath() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.DatabasePath())
}

func ExampleStore_WorkspaceStateDirectory() {
	storeInstance := exampleOpenConfiguredStore()
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.WorkspaceStateDirectory())
}

func ExampleStore_IsClosed() {
	storeInstance := exampleOpenStore()
	result := storeInstance.Close()
	exampleRequireOK(result)
	core.Println(storeInstance.IsClosed())
}

func ExampleWithPurgeInterval() {
	storeInstance, result := New(":memory:", WithPurgeInterval(time.Minute))
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.Config().PurgeInterval)
}

func ExampleNewConfigured() {
	storeInstance, result := NewConfigured(StoreConfig{DatabasePath: ":memory:"})
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.DatabasePath())
}

func ExampleNew() {
	storeInstance, result := New(":memory:")
	exampleRequireOK(result)
	defer exampleCloseStore(storeInstance)
	core.Println(storeInstance.IsClosed())
}

func ExampleStore_Close() {
	storeInstance := exampleOpenStore()
	result := storeInstance.Close()
	exampleRequireOK(result)
	core.Println(storeInstance.IsClosed())
}

func ExampleStore_Get() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	value, result := storeInstance.Get("config", "colour")
	exampleRequireOK(result)
	core.Println(value)
}

func ExampleStore_Set() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Set("config", "colour", "blue")
	exampleRequireOK(result)
}

func ExampleStore_SetWithTTL() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.SetWithTTL("cache", "token", "abc", time.Minute)
	exampleRequireOK(result)
}

func ExampleStore_Delete() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Delete("config", "colour")
	exampleRequireOK(result)
}

func ExampleStore_Exists() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	exists, result := storeInstance.Exists("config", "colour")
	exampleRequireOK(result)
	core.Println(exists)
}

func ExampleStore_GroupExists() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	exists, result := storeInstance.GroupExists("config")
	exampleRequireOK(result)
	core.Println(exists)
}

func ExampleStore_Count() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	count, result := storeInstance.Count("config")
	exampleRequireOK(result)
	core.Println(count)
}

func ExampleStore_DeleteGroup() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.DeleteGroup("config")
	exampleRequireOK(result)
}

func ExampleStore_DeletePrefix() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.DeletePrefix("conf")
	exampleRequireOK(result)
}

func ExampleStore_GetAll() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	values, result := storeInstance.GetAll("config")
	exampleRequireOK(result)
	core.Println(values["colour"])
}

func ExampleStore_GetPage() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	entries, result := storeInstance.GetPage("config", 0, 10)
	exampleRequireOK(result)
	core.Println(len(entries))
}

func ExampleStore_AllSeq() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	entries := exampleCollectKeyValues(storeInstance.AllSeq("config"))
	core.Println(len(entries))
}

func ExampleStore_All() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	entries := exampleCollectKeyValues(storeInstance.All("config"))
	core.Println(len(entries))
}

func ExampleStore_GetSplit() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	seq, result := storeInstance.GetSplit("config", "tags", ",")
	exampleRequireOK(result)
	core.Println(exampleCollectStrings(seq))
}

func ExampleStore_GetFields() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	exampleRequireOK(storeInstance.Set("config", "text", "alpha beta"))
	seq, result := storeInstance.GetFields("config", "text")
	exampleRequireOK(result)
	core.Println(exampleCollectStrings(seq))
}

func ExampleStore_Render() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	rendered, result := storeInstance.Render("colour={{.colour}}", "config")
	exampleRequireOK(result)
	core.Println(rendered)
}

func ExampleStore_CountAll() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	count, result := storeInstance.CountAll("conf")
	exampleRequireOK(result)
	core.Println(count)
}

func ExampleStore_Groups() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	groups, result := storeInstance.Groups()
	exampleRequireOK(result)
	core.Println(groups)
}

func ExampleStore_GroupsSeq() {
	storeInstance := exampleStoreWithConfig()
	defer exampleCloseStore(storeInstance)
	groups := exampleCollectGroups(storeInstance.GroupsSeq())
	core.Println(groups)
}

func ExampleStore_PurgeExpired() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	exampleRequireOK(storeInstance.SetWithTTL("cache", "token", "abc", time.Nanosecond))
	removed, result := storeInstance.PurgeExpired()
	exampleRequireOK(result)
	core.Println(removed)
}
