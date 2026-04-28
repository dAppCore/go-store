package store

import (
	"testing"
	"time"

	core "dappco.re/go"
)

// ---------------------------------------------------------------------------
// NewScoped — constructor validation
// ---------------------------------------------------------------------------

func TestScope_NewScoped_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-1")
	assertNotNil(t, scopedStore)
	assertEqual(t, "tenant-1", scopedStore.Namespace())
}

func TestScope_ScopedStore_Good_Config(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	assertNoError(t, err)

	assertEqual(t, ScopedStoreConfig{Namespace: "tenant-a", Quota: QuotaConfig{MaxKeys: 4, MaxGroups: 2}}, scopedStore.Config())
}

func TestScope_ScopedStore_Good_ConfigZeroValueFromNil(t *testing.T) {
	var scopedStore *ScopedStore
	config := scopedStore.Config()

	assertEqual(t, ScopedStoreConfig{}, config)
	assertEqual(t, "", config.Namespace)
}

func TestScope_NewScoped_Good_AlphanumericHyphens(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	valid := []string{"abc", "ABC", "123", "a-b-c", "tenant-42", "A1-B2"}
	for _, namespace := range valid {
		scopedStore := NewScoped(storeInstance, namespace)
		assertNotNil(t, scopedStore)
	}
}

func TestScope_NewScoped_Bad_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNil(t, NewScoped(storeInstance, ""))
}

func TestScope_NewScoped_Bad_NilStore(t *testing.T) {
	scopedStore := NewScoped(nil, "tenant-a")
	assertNil(t, scopedStore)
	assertEqual(t, "", scopedStore.Namespace())
}

func TestScope_NewScoped_Bad_InvalidChars(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	invalid := []string{"foo.bar", "foo:bar", "foo bar", "foo/bar", "foo_bar", "tenant!", "@ns"}
	for _, namespace := range invalid {
		assertNilf(t, NewScoped(storeInstance, namespace), "namespace %q should be invalid", namespace)
	}
}

func TestScope_NewScopedConfigured_Bad_InvalidNamespaceFromQuotaConfig(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant_a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	assertError(t, err)
	assertContainsString(t, err.Error(), "store.NewScoped")
}

func TestScope_NewScopedConfigured_Bad_NilStoreFromQuotaConfig(t *testing.T) {
	_, err := NewScopedConfigured(nil, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	assertError(t, err)
	assertContainsString(t, err.Error(), "store instance is nil")
}

func TestScope_NewScopedConfigured_Bad_NegativeMaxKeys(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: -1},
	})
	assertError(t, err)
	assertContainsString(t, err.Error(), "zero or positive")
}

func TestScope_NewScopedConfigured_Bad_NegativeMaxGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: -1},
	})
	assertError(t, err)
	assertContainsString(t, err.Error(), "zero or positive")
}

func TestScope_NewScopedConfigured_Good_InlineQuotaFields(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	assertNoError(t, err)

	assertEqual(t, 4, scopedStore.MaxKeys)
	assertEqual(t, 2, scopedStore.MaxGroups)
}

func TestScope_ScopedStoreConfig_Good_Validate(t *testing.T) {
	err := (ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	}).Validate()
	assertNoError(t, err)
}

func TestScope_NewScopedConfigured_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	assertNoError(t, err)
	assertNotNil(t, scopedStore)
	assertEqual(t, 4, scopedStore.MaxKeys)
	assertEqual(t, 2, scopedStore.MaxGroups)
}

func TestScope_NewScopedWithQuota_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 4, MaxGroups: 2})
	assertNoError(t, err)
	assertNotNil(t, scopedStore)

	assertEqual(t, "tenant-a", scopedStore.Namespace())
	assertEqual(t, 4, scopedStore.MaxKeys)
	assertEqual(t, 2, scopedStore.MaxGroups)
}

func TestScope_NewScopedConfigured_Bad_InvalidNamespace(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant_a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	assertError(t, err)
	assertContainsString(t, err.Error(), "namespace")
}

func TestScope_ScopedStore_Good_NilReceiverReturnsErrors(t *testing.T) {
	var scopedStore *ScopedStore

	_, err := scopedStore.Get("theme")
	assertError(t, err)
	assertContainsString(t, err.Error(), "scoped store is nil")

	err = scopedStore.Set("theme", "dark")
	assertError(t, err)
	assertContainsString(t, err.Error(), "scoped store is nil")

	_, err = scopedStore.Count("config")
	assertError(t, err)
	assertContainsString(t, err.Error(), "scoped store is nil")

	_, err = scopedStore.Groups()
	assertError(t, err)
	assertContainsString(t, err.Error(), "scoped store is nil")

	for entry, iterationErr := range scopedStore.All("config") {
		_ = entry
		assertError(t, iterationErr)
		assertContainsString(t, iterationErr.Error(), "scoped store is nil")
		break
	}

	for groupName, iterationErr := range scopedStore.GroupsSeq() {
		_ = groupName
		assertError(t, iterationErr)
		assertContainsString(t, iterationErr.Error(), "scoped store is nil")
		break
	}
}

// ---------------------------------------------------------------------------
// ScopedStore — basic CRUD
// ---------------------------------------------------------------------------

func TestScope_ScopedStore_Good_SetGet(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("config", "theme", "dark"))

	value, err := scopedStore.GetFrom("config", "theme")
	assertNoError(t, err)
	assertEqual(t, "dark", value)
}

func TestScope_ScopedStore_Good_DefaultGroupHelpers(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.Set("theme", "dark"))

	value, err := scopedStore.Get("theme")
	assertNoError(t, err)
	assertEqual(t, "dark", value)

	rawValue, err := storeInstance.Get("tenant-a:default", "theme")
	assertNoError(t, err)
	assertEqual(t, "dark", rawValue)
}

func TestScope_ScopedStore_Good_SetInGetFrom(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("config", "theme", "dark"))

	value, err := scopedStore.GetFrom("config", "theme")
	assertNoError(t, err)
	assertEqual(t, "dark", value)
}

func TestScope_ScopedStore_Good_PrefixedInUnderlyingStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("config", "key", "val"))

	// The underlying store should have the prefixed group name.
	value, err := storeInstance.Get("tenant-a:config", "key")
	assertNoError(t, err)
	assertEqual(t, "val", value)

	// Direct access without prefix should fail.
	_, err = storeInstance.Get("config", "key")
	assertTrue(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_NamespaceIsolation(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	alphaStore := NewScoped(storeInstance, "tenant-a")
	betaStore := NewScoped(storeInstance, "tenant-b")

	assertNoError(t, alphaStore.SetIn("config", "colour", "blue"))
	assertNoError(t, betaStore.SetIn("config", "colour", "red"))

	alphaValue, err := alphaStore.GetFrom("config", "colour")
	assertNoError(t, err)
	assertEqual(t, "blue", alphaValue)

	betaValue, err := betaStore.GetFrom("config", "colour")
	assertNoError(t, err)
	assertEqual(t, "red", betaValue)
}

// ---------------------------------------------------------------------------
// ScopedStore — Exists / ExistsIn / GroupExists
// ---------------------------------------------------------------------------

func TestScope_ScopedStore_Good_ExistsInDefaultGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.Set("colour", "blue"))

	exists, err := scopedStore.Exists("colour")
	assertNoError(t, err)
	assertTrue(t, exists)

	exists, err = scopedStore.Exists("missing")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Good_ExistsInExplicitGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))

	exists, err := scopedStore.ExistsIn("config", "colour")
	assertNoError(t, err)
	assertTrue(t, exists)

	exists, err = scopedStore.ExistsIn("config", "missing")
	assertNoError(t, err)
	assertFalse(t, exists)

	exists, err = scopedStore.ExistsIn("other-group", "colour")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Good_ExistsExpiredKeyReturnsFalse(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetWithTTL("session", "token", "abc123", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	exists, err := scopedStore.ExistsIn("session", "token")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Good_GroupExists(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))

	exists, err := scopedStore.GroupExists("config")
	assertNoError(t, err)
	assertTrue(t, exists)

	exists, err = scopedStore.GroupExists("missing-group")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Good_GroupExistsAfterDelete(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	assertNoError(t, scopedStore.DeleteGroup("config"))

	exists, err := scopedStore.GroupExists("config")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Bad_ExistsClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	_ = storeInstance.Close()
	scopedStore := NewScoped(storeInstance, "tenant-a")

	_, err := scopedStore.Exists("colour")
	assertError(t, err)

	_, err = scopedStore.ExistsIn("config", "colour")
	assertError(t, err)

	_, err = scopedStore.GroupExists("config")
	assertError(t, err)
}

func TestScope_ScopedStore_Good_Delete(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("g", "k", "v"))
	assertNoError(t, scopedStore.Delete("g", "k"))

	_, err := scopedStore.GetFrom("g", "k")
	assertTrue(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_DeleteGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("g", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g", "b", "2"))
	assertNoError(t, scopedStore.DeleteGroup("g"))

	count, err := scopedStore.Count("g")
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestScope_ScopedStore_Good_DeletePrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	otherScopedStore := NewScoped(storeInstance, "tenant-b")

	assertNoError(t, scopedStore.SetIn("config", "theme", "dark"))
	assertNoError(t, scopedStore.SetIn("cache", "page", "home"))
	assertNoError(t, scopedStore.SetIn("cache-warm", "status", "ready"))
	assertNoError(t, otherScopedStore.SetIn("cache", "page", "keep"))

	assertNoError(t, scopedStore.DeletePrefix("cache"))

	_, err := scopedStore.GetFrom("cache", "page")
	assertTrue(t, core.Is(err, NotFoundError))
	_, err = scopedStore.GetFrom("cache-warm", "status")
	assertTrue(t, core.Is(err, NotFoundError))

	value, err := scopedStore.GetFrom("config", "theme")
	assertNoError(t, err)
	assertEqual(t, "dark", value)

	otherValue, err := otherScopedStore.GetFrom("cache", "page")
	assertNoError(t, err)
	assertEqual(t, "keep", otherValue)
}

func TestScope_ScopedStore_Good_OnChange_NamespaceLocal(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	otherScopedStore := NewScoped(storeInstance, "tenant-b")

	var events []Event
	unregister := scopedStore.OnChange(func(event Event) {
		events = append(events, event)
	})
	defer unregister()

	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	assertNoError(t, otherScopedStore.SetIn("config", "colour", "red"))
	assertNoError(t, scopedStore.Delete("config", "colour"))

	assertLen(t, events, 2)
	assertEqual(t, "config", events[0].Group)
	assertEqual(t, "colour", events[0].Key)
	assertEqual(t, "blue", events[0].Value)
	assertEqual(t, "config", events[1].Group)
	assertEqual(t, "colour", events[1].Key)
	assertEqual(t, "", events[1].Value)
}

func TestScope_ScopedStore_Good_Watch_NamespaceLocal(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	otherScopedStore := NewScoped(storeInstance, "tenant-b")

	events := scopedStore.Watch("config")
	defer scopedStore.Unwatch("config", events)

	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	assertNoError(t, otherScopedStore.SetIn("config", "colour", "red"))

	select {
	case event, ok := <-events:
		assertTrue(t, ok)
		assertEqual(t, EventSet, event.Type)
		assertEqual(t, "config", event.Group)
		assertEqual(t, "colour", event.Key)
		assertEqual(t, "blue", event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scoped watch event")
	}

	select {
	case event := <-events:
		t.Fatalf("unexpected event from another namespace: %#v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestScope_ScopedStore_Good_Watch_All_NamespaceLocal(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	otherScopedStore := NewScoped(storeInstance, "tenant-b")

	events := scopedStore.Watch("*")
	defer scopedStore.Unwatch("*", events)

	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	assertNoError(t, scopedStore.SetIn("cache", "page", "home"))
	assertNoError(t, otherScopedStore.SetIn("config", "colour", "red"))

	select {
	case event, ok := <-events:
		assertTrue(t, ok)
		assertEqual(t, "config", event.Group)
		assertEqual(t, "colour", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first wildcard scoped watch event")
	}

	select {
	case event, ok := <-events:
		assertTrue(t, ok)
		assertEqual(t, "cache", event.Group)
		assertEqual(t, "page", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second wildcard scoped watch event")
	}

	select {
	case event := <-events:
		t.Fatalf("unexpected wildcard event from another namespace: %#v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestScope_ScopedStore_Good_Unwatch_ClosesLocalChannel(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")

	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)

	select {
	case _, ok := <-events:
		assertFalse(t, ok)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scoped watch channel to close")
	}
}

func TestScope_ScopedStore_Good_GetAll(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	alphaStore := NewScoped(storeInstance, "tenant-a")
	betaStore := NewScoped(storeInstance, "tenant-b")

	assertNoError(t, alphaStore.SetIn("items", "x", "1"))
	assertNoError(t, alphaStore.SetIn("items", "y", "2"))
	assertNoError(t, betaStore.SetIn("items", "z", "3"))

	all, err := alphaStore.GetAll("items")
	assertNoError(t, err)
	assertEqual(t, map[string]string{"x": "1", "y": "2"}, all)

	betaEntries, err := betaStore.GetAll("items")
	assertNoError(t, err)
	assertEqual(t, map[string]string{"z": "3"}, betaEntries)
}

func TestScope_ScopedStore_Good_GetPage(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("items", "charlie", "3"))
	assertNoError(t, scopedStore.SetIn("items", "alpha", "1"))
	assertNoError(t, scopedStore.SetIn("items", "bravo", "2"))

	page, err := scopedStore.GetPage("items", 1, 1)
	assertNoError(t, err)
	assertLen(t, page, 1)
	assertEqual(t, KeyValue{Key: "bravo", Value: "2"}, page[0])
}

func TestScope_ScopedStore_Good_All(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("items", "first", "1"))
	assertNoError(t, scopedStore.SetIn("items", "second", "2"))

	var keys []string
	for entry, err := range scopedStore.All("items") {
		assertNoError(t, err)
		keys = append(keys, entry.Key)
	}

	assertElementsMatch(t, []string{"first", "second"}, keys)
}

func TestScope_ScopedStore_Good_All_SortedByKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("items", "charlie", "3"))
	assertNoError(t, scopedStore.SetIn("items", "alpha", "1"))
	assertNoError(t, scopedStore.SetIn("items", "bravo", "2"))

	var keys []string
	for entry, err := range scopedStore.All("items") {
		assertNoError(t, err)
		keys = append(keys, entry.Key)
	}

	assertEqual(t, []string{"alpha", "bravo", "charlie"}, keys)
}

func TestScope_ScopedStore_Good_Count(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("g", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g", "b", "2"))

	count, err := scopedStore.Count("g")
	assertNoError(t, err)
	assertEqual(t, 2, count)
}

func TestScope_ScopedStore_Good_SetWithTTL(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetWithTTL("g", "k", "v", time.Hour))

	value, err := scopedStore.GetFrom("g", "k")
	assertNoError(t, err)
	assertEqual(t, "v", value)
}

func TestScope_ScopedStore_Good_SetWithTTL_Expires(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetWithTTL("g", "k", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	_, err := scopedStore.GetFrom("g", "k")
	assertTrue(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_Render(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("user", "name", "Alice"))

	renderedTemplate, err := scopedStore.Render("Hello {{ .name }}", "user")
	assertNoError(t, err)
	assertEqual(t, "Hello Alice", renderedTemplate)
}

func TestScope_ScopedStore_Good_BulkHelpers(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	alphaStore := NewScoped(storeInstance, "tenant-a")
	betaStore := NewScoped(storeInstance, "tenant-b")

	assertNoError(t, alphaStore.SetIn("config", "colour", "blue"))
	assertNoError(t, alphaStore.SetIn("sessions", "token", "abc123"))
	assertNoError(t, betaStore.SetIn("config", "colour", "red"))

	count, err := alphaStore.CountAll("")
	assertNoError(t, err)
	assertEqual(t, 2, count)

	count, err = alphaStore.CountAll("config")
	assertNoError(t, err)
	assertEqual(t, 1, count)

	groupNames, err := alphaStore.Groups("")
	assertNoError(t, err)
	assertElementsMatch(t, []string{"config", "sessions"}, groupNames)

	groupNames, err = alphaStore.Groups("conf")
	assertNoError(t, err)
	assertEqual(t, []string{"config"}, groupNames)

	var streamedGroupNames []string
	for groupName, iterationErr := range alphaStore.GroupsSeq("") {
		assertNoError(t, iterationErr)
		streamedGroupNames = append(streamedGroupNames, groupName)
	}
	assertElementsMatch(t, []string{"config", "sessions"}, streamedGroupNames)

	var filteredGroupNames []string
	for groupName, iterationErr := range alphaStore.GroupsSeq("config") {
		assertNoError(t, iterationErr)
		filteredGroupNames = append(filteredGroupNames, groupName)
	}
	assertEqual(t, []string{"config"}, filteredGroupNames)
}

func TestScope_ScopedStore_Good_GroupsSeqStopsEarly(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("alpha", "a", "1"))
	assertNoError(t, scopedStore.SetIn("beta", "b", "2"))

	groups := scopedStore.GroupsSeq("")
	var seen []string
	for groupName, iterationErr := range groups {
		assertNoError(t, iterationErr)
		seen = append(seen, groupName)
		break
	}

	assertLen(t, seen, 1)
}

func TestScope_ScopedStore_Good_GroupsSeqSorted(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("charlie", "c", "3"))
	assertNoError(t, scopedStore.SetIn("alpha", "a", "1"))
	assertNoError(t, scopedStore.SetIn("bravo", "b", "2"))

	var groupNames []string
	for groupName, iterationErr := range scopedStore.GroupsSeq("") {
		assertNoError(t, iterationErr)
		groupNames = append(groupNames, groupName)
	}

	assertEqual(t, []string{"alpha", "bravo", "charlie"}, groupNames)
}

func TestScope_ScopedStore_Good_GetSplitAndGetFields(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetIn("config", "hosts", "alpha,beta,gamma"))
	assertNoError(t, scopedStore.SetIn("config", "flags", "one two\tthree\n"))

	parts, err := scopedStore.GetSplit("config", "hosts", ",")
	assertNoError(t, err)

	var splitValues []string
	for value := range parts {
		splitValues = append(splitValues, value)
	}
	assertEqual(t, []string{"alpha", "beta", "gamma"}, splitValues)

	fields, err := scopedStore.GetFields("config", "flags")
	assertNoError(t, err)

	var fieldValues []string
	for value := range fields {
		fieldValues = append(fieldValues, value)
	}
	assertEqual(t, []string{"one", "two", "three"}, fieldValues)
}

func TestScope_ScopedStore_Good_PurgeExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNoError(t, scopedStore.SetWithTTL("session", "token", "abc123", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	removedRows, err := scopedStore.PurgeExpired()
	assertNoError(t, err)
	assertEqual(t, int64(1), removedRows)

	_, err = scopedStore.GetFrom("session", "token")
	assertTrue(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_PurgeExpired_NamespaceLocal(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	alphaStore := NewScoped(storeInstance, "tenant-a")
	betaStore := NewScoped(storeInstance, "tenant-b")

	assertNoError(t, alphaStore.SetWithTTL("session", "alpha-token", "alpha", 1*time.Millisecond))
	assertNoError(t, betaStore.SetWithTTL("session", "beta-token", "beta", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	assertEqual(t, 1, rawEntryCount(t, storeInstance, "tenant-a:session"))
	assertEqual(t, 1, rawEntryCount(t, storeInstance, "tenant-b:session"))

	removedRows, err := alphaStore.PurgeExpired()
	assertNoError(t, err)
	assertEqual(t, int64(1), removedRows)

	assertEqual(t, 0, rawEntryCount(t, storeInstance, "tenant-a:session"))
	assertEqual(t, 1, rawEntryCount(t, storeInstance, "tenant-b:session"))
}

// ---------------------------------------------------------------------------
// Quota enforcement — MaxKeys
// ---------------------------------------------------------------------------

func TestScope_Quota_Good_MaxKeys(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 5},
	})
	assertNoError(t, err)

	// Insert 5 keys across different groups — should be fine.
	for i := range 5 {
		assertNoError(t, scopedStore.SetIn("g", keyName(i), "v"))
	}

	// 6th key should fail.
	err = scopedStore.SetIn("g", "overflow", "v")
	assertError(t, err)
	assertTruef(t, core.Is(err, QuotaExceededError), "expected QuotaExceededError, got: %v", err)
}

func TestScope_Quota_Bad_QuotaCheckQueryError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{})
	defer func() { _ = database.Close() }()

	storeInstance := &Store{
		sqliteDatabase: database,
		cancelPurge:    func() {},
	}

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	assertNoError(t, err)

	err = scopedStore.SetIn("config", "theme", "dark")
	assertError(t, err)
	assertContainsString(t, err.Error(), "quota check")
}

func TestScope_Quota_Good_MaxKeys_AcrossGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 3},
	})

	assertNoError(t, scopedStore.SetIn("g1", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g2", "b", "2"))
	assertNoError(t, scopedStore.SetIn("g3", "c", "3"))

	// Total is now 3 — any new key should fail regardless of group.
	err := scopedStore.SetIn("g4", "d", "4")
	assertTrue(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_UpsertDoesNotCount(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 3},
	})

	assertNoError(t, scopedStore.SetIn("g", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g", "b", "2"))
	assertNoError(t, scopedStore.SetIn("g", "c", "3"))

	// Upserting existing key should succeed.
	assertNoError(t, scopedStore.SetIn("g", "a", "updated"))

	value, err := scopedStore.GetFrom("g", "a")
	assertNoError(t, err)
	assertEqual(t, "updated", value)
}

func TestScope_Quota_Good_ExpiredUpsertDoesNotEmitDeleteEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})

	events := storeInstance.Watch("tenant-a:g")
	defer storeInstance.Unwatch("tenant-a:g", events)

	assertNoError(t, scopedStore.SetWithTTL("g", "token", "old", 1*time.Millisecond))
	select {
	case event := <-events:
		assertEqual(t, EventSet, event.Type)
		assertEqual(t, "old", event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial set event")
	}
	time.Sleep(5 * time.Millisecond)

	assertNoError(t, scopedStore.SetIn("g", "token", "new"))

	select {
	case event := <-events:
		assertEqual(t, EventSet, event.Type)
		assertEqual(t, "new", event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for upsert event")
	}

	select {
	case event := <-events:
		t.Fatalf("unexpected extra event: %#v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestScope_Quota_Good_DeleteAndReInsert(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 3},
	})

	assertNoError(t, scopedStore.SetIn("g", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g", "b", "2"))
	assertNoError(t, scopedStore.SetIn("g", "c", "3"))

	// Delete one key, then insert a new one — should work.
	assertNoError(t, scopedStore.Delete("g", "c"))
	assertNoError(t, scopedStore.SetIn("g", "d", "4"))
}

func TestScope_Quota_Good_ZeroMeansUnlimited(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 0, MaxGroups: 0},
	})

	// Should be able to insert many keys and groups without error.
	for i := range 100 {
		assertNoError(t, scopedStore.SetIn("g", keyName(i), "v"))
	}
}

func TestScope_Quota_Good_ExpiredKeysExcluded(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 3},
	})

	// Insert 3 keys, 2 with short TTL.
	assertNoError(t, scopedStore.SetWithTTL("g", "temp1", "v", 1*time.Millisecond))
	assertNoError(t, scopedStore.SetWithTTL("g", "temp2", "v", 1*time.Millisecond))
	assertNoError(t, scopedStore.SetIn("g", "permanent", "v"))

	time.Sleep(5 * time.Millisecond)

	// After expiry, only 1 key counts — should be able to insert 2 more.
	assertNoError(t, scopedStore.SetIn("g", "new1", "v"))
	assertNoError(t, scopedStore.SetIn("g", "new2", "v"))

	// Now at 3 — next should fail.
	err := scopedStore.SetIn("g", "new3", "v")
	assertTrue(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_SetWithTTL_Enforced(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 2},
	})

	assertNoError(t, scopedStore.SetWithTTL("g", "a", "1", time.Hour))
	assertNoError(t, scopedStore.SetWithTTL("g", "b", "2", time.Hour))

	err := scopedStore.SetWithTTL("g", "c", "3", time.Hour)
	assertTrue(t, core.Is(err, QuotaExceededError))
}

// ---------------------------------------------------------------------------
// Quota enforcement — MaxGroups
// ---------------------------------------------------------------------------

func TestScope_Quota_Good_MaxGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 3},
	})

	assertNoError(t, scopedStore.SetIn("g1", "k", "v"))
	assertNoError(t, scopedStore.SetIn("g2", "k", "v"))
	assertNoError(t, scopedStore.SetIn("g3", "k", "v"))

	// 4th group should fail.
	err := scopedStore.SetIn("g4", "k", "v")
	assertError(t, err)
	assertTrue(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_MaxGroups_ExistingGroupOK(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 2},
	})

	assertNoError(t, scopedStore.SetIn("g1", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g2", "b", "2"))

	// Adding more keys to existing groups should be fine.
	assertNoError(t, scopedStore.SetIn("g1", "c", "3"))
	assertNoError(t, scopedStore.SetIn("g2", "d", "4"))
}

func TestScope_Quota_Good_MaxGroups_DeleteAndRecreate(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 2},
	})

	assertNoError(t, scopedStore.SetIn("g1", "k", "v"))
	assertNoError(t, scopedStore.SetIn("g2", "k", "v"))

	// Delete a group, then create a new one.
	assertNoError(t, scopedStore.DeleteGroup("g1"))
	assertNoError(t, scopedStore.SetIn("g3", "k", "v"))
}

func TestScope_Quota_Good_MaxGroups_ZeroUnlimited(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 0},
	})

	for i := range 50 {
		assertNoError(t, scopedStore.SetIn(keyName(i), "k", "v"))
	}
}

func TestScope_Quota_Good_MaxGroups_ExpiredGroupExcluded(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 2},
	})

	// Create 2 groups, one with only TTL keys.
	assertNoError(t, scopedStore.SetWithTTL("g1", "k", "v", 1*time.Millisecond))
	assertNoError(t, scopedStore.SetIn("g2", "k", "v"))

	time.Sleep(5 * time.Millisecond)

	// g1's only key has expired, so group count should be 1 — we can create a new one.
	assertNoError(t, scopedStore.SetIn("g3", "k", "v"))
}

func TestScope_Quota_Good_BothLimits(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 10, MaxGroups: 2},
	})

	assertNoError(t, scopedStore.SetIn("g1", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g2", "b", "2"))

	// Group limit hit.
	err := scopedStore.SetIn("g3", "c", "3")
	assertTrue(t, core.Is(err, QuotaExceededError))

	// But adding to existing groups is fine (within key limit).
	assertNoError(t, scopedStore.SetIn("g1", "d", "4"))
}

func TestScope_Quota_Good_DoesNotAffectOtherNamespaces(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	alphaStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 2},
	})
	betaStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-b",
		Quota:     QuotaConfig{MaxKeys: 2},
	})

	assertNoError(t, alphaStore.SetIn("g", "a1", "v"))
	assertNoError(t, alphaStore.SetIn("g", "a2", "v"))
	assertNoError(t, betaStore.SetIn("g", "b1", "v"))
	assertNoError(t, betaStore.SetIn("g", "b2", "v"))

	// alphaStore is at limit — but betaStore's keys don't count against alphaStore.
	err := alphaStore.SetIn("g", "a3", "v")
	assertTrue(t, core.Is(err, QuotaExceededError))

	// betaStore is also at limit independently.
	err = betaStore.SetIn("g", "b3", "v")
	assertTrue(t, core.Is(err, QuotaExceededError))
}

// ---------------------------------------------------------------------------
// CountAll
// ---------------------------------------------------------------------------

func TestScope_CountAll_Good_WithPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("ns-a:g1", "k1", "v"))
	assertNoError(t, storeInstance.Set("ns-a:g1", "k2", "v"))
	assertNoError(t, storeInstance.Set("ns-a:g2", "k1", "v"))
	assertNoError(t, storeInstance.Set("ns-b:g1", "k1", "v"))

	count, err := storeInstance.CountAll("ns-a:")
	assertNoError(t, err)
	assertEqual(t, 3, count)

	count, err = storeInstance.CountAll("ns-b:")
	assertNoError(t, err)
	assertEqual(t, 1, count)
}

func TestScope_CountAll_Good_WithPrefix_Wildcards(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	// Add keys in groups that look like wildcards.
	assertNoError(t, storeInstance.Set("user_1", "k", "v"))
	assertNoError(t, storeInstance.Set("user_2", "k", "v"))
	assertNoError(t, storeInstance.Set("user%test", "k", "v"))
	assertNoError(t, storeInstance.Set("user_test", "k", "v"))

	// Prefix "user_" should ONLY match groups starting with "user_".
	// Since we escape "_", it matches literal "_".
	// Groups: "user_1", "user_2", "user_test" (3 total).
	// "user%test" is NOT matched because "_" is literal.
	count, err := storeInstance.CountAll("user_")
	assertNoError(t, err)
	assertEqual(t, 3, count)

	// Prefix "user%" should ONLY match "user%test".
	count, err = storeInstance.CountAll("user%")
	assertNoError(t, err)
	assertEqual(t, 1, count)
}

func TestScope_CountAll_Good_EmptyPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("g1", "k1", "v"))
	assertNoError(t, storeInstance.Set("g2", "k2", "v"))

	count, err := storeInstance.CountAll("")
	assertNoError(t, err)
	assertEqual(t, 2, count)
}

func TestScope_CountAll_Good_ExcludesExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("ns:g", "permanent", "v"))
	assertNoError(t, storeInstance.SetWithTTL("ns:g", "temp", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	count, err := storeInstance.CountAll("ns:")
	assertNoError(t, err)
	assertEqualf(t, 1, count, "expired keys should not be counted")
}

func TestScope_CountAll_Good_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	count, err := storeInstance.CountAll("nonexistent:")
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestScope_CountAll_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	_ = storeInstance.Close()
	_, err := storeInstance.CountAll("")
	assertError(t, err)
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

func TestScope_Groups_Good_WithPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("ns-a:g1", "k", "v"))
	assertNoError(t, storeInstance.Set("ns-a:g2", "k", "v"))
	assertNoError(t, storeInstance.Set("ns-a:g2", "k2", "v")) // duplicate group
	assertNoError(t, storeInstance.Set("ns-b:g1", "k", "v"))

	groups, err := storeInstance.Groups("ns-a:")
	assertNoError(t, err)
	assertLen(t, groups, 2)
	assertContainsElement(t, groups, "ns-a:g1")
	assertContainsElement(t, groups, "ns-a:g2")
}

func TestScope_Groups_Good_EmptyPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("g1", "k", "v"))
	assertNoError(t, storeInstance.Set("g2", "k", "v"))
	assertNoError(t, storeInstance.Set("g3", "k", "v"))

	groups, err := storeInstance.Groups("")
	assertNoError(t, err)
	assertLen(t, groups, 3)
}

func TestScope_Groups_Good_Distinct(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	// Multiple keys in the same group should produce one entry.
	assertNoError(t, storeInstance.Set("g1", "a", "v"))
	assertNoError(t, storeInstance.Set("g1", "b", "v"))
	assertNoError(t, storeInstance.Set("g1", "c", "v"))

	groups, err := storeInstance.Groups("")
	assertNoError(t, err)
	assertLen(t, groups, 1)
	assertEqual(t, "g1", groups[0])
}

func TestScope_Groups_Good_ExcludesExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("ns:g1", "permanent", "v"))
	assertNoError(t, storeInstance.SetWithTTL("ns:g2", "temp", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	groups, err := storeInstance.Groups("ns:")
	assertNoError(t, err)
	assertLenf(t, groups, 1, "group with only expired keys should be excluded")
	assertEqual(t, "ns:g1", groups[0])
}

func TestScope_Groups_Good_SortedByGroupName(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("charlie", "c", "3"))
	assertNoError(t, storeInstance.Set("alpha", "a", "1"))
	assertNoError(t, storeInstance.Set("bravo", "b", "2"))

	groups, err := storeInstance.Groups("")
	assertNoError(t, err)
	assertEqual(t, []string{"alpha", "bravo", "charlie"}, groups)
}

func TestScope_Groups_Good_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	groups, err := storeInstance.Groups("nonexistent:")
	assertNoError(t, err)
	assertEmpty(t, groups)
}

func TestScope_Groups_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	_ = storeInstance.Close()
	_, err := storeInstance.Groups("")
	assertError(t, err)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func keyName(i int) string {
	return core.Concat("key-", core.Sprint(i))
}

func rawEntryCount(t *testing.T, storeInstance *Store, group string) int {
	t.Helper()

	var count int
	err := storeInstance.sqliteDatabase.QueryRow(
		"SELECT COUNT(*) FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ?",
		group,
	).Scan(&count)
	assertNoError(t, err)
	return count
}
