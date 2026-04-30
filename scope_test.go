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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-1")
	assertNotNil(t, scopedStore)
	assertEqual(t, "tenant-1", scopedStore.Namespace())
}

func TestScope_ScopedStore_Good_Config(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	assertNoError(t, err)

	assertEqual(t, ScopedStoreConfig{Namespace: testTenantA, Quota: QuotaConfig{MaxKeys: 4, MaxGroups: 2}}, scopedStore.Config())
}

func TestScope_ScopedStore_Good_ConfigZeroValueFromNil(t *testing.T) {
	var scopedStore *ScopedStore
	config := scopedStore.Config()

	assertEqual(t, ScopedStoreConfig{}, config)
	assertEqual(t, "", config.Namespace)
}

func TestScope_NewScoped_Good_AlphanumericHyphens(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	valid := []string{"abc", "ABC", "123", "a-b-c", testTenant42, "A1-B2"}
	for _, namespace := range valid {
		scopedStore := NewScoped(storeInstance, namespace)
		assertNotNil(t, scopedStore)
	}
}

func TestScope_NewScoped_Bad_Empty(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	assertNil(t, NewScoped(storeInstance, ""))
}

func TestScope_NewScoped_Bad_NilStore(t *testing.T) {
	scopedStore := NewScoped(nil, testTenantA)
	assertNil(t, scopedStore)
	assertEqual(t, "", scopedStore.Namespace())
}

func TestScope_NewScoped_Bad_InvalidChars(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	invalid := []string{"foo.bar", "foo:bar", "foo bar", "foo/bar", "foo_bar", "tenant!", "@ns"}
	for _, namespace := range invalid {
		assertNilf(t, NewScoped(storeInstance, namespace), "namespace %q should be invalid", namespace)
	}
}

func TestScope_NewScopedConfigured_Bad_InvalidNamespaceFromQuotaConfig(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
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
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	assertError(t, err)
	assertContainsString(t, err.Error(), "store instance is nil")
}

func TestScope_NewScopedConfigured_Bad_NegativeMaxKeys(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: -1},
	})
	assertError(t, err)
	assertContainsString(t, err.Error(), "zero or positive")
}

func TestScope_NewScopedConfigured_Bad_NegativeMaxGroups(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxGroups: -1},
	})
	assertError(t, err)
	assertContainsString(t, err.Error(), "zero or positive")
}

func TestScope_NewScopedConfigured_Good_InlineQuotaFields(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	assertNoError(t, err)

	assertEqual(t, 4, scopedStore.MaxKeys)
	assertEqual(t, 2, scopedStore.MaxGroups)
}

func TestScope_ScopedStoreConfig_Good_Validate(t *testing.T) {
	err := (ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	}).Validate()
	assertNoError(t, err)
}

func TestScope_NewScopedConfigured_Good(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	assertNoError(t, err)
	assertNotNil(t, scopedStore)
	assertEqual(t, 4, scopedStore.MaxKeys)
	assertEqual(t, 2, scopedStore.MaxGroups)
}

func TestScope_NewScopedWithQuota_Good(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedWithQuota(storeInstance, testTenantA, QuotaConfig{MaxKeys: 4, MaxGroups: 2})
	assertNoError(t, err)
	assertNotNil(t, scopedStore)

	assertEqual(t, testTenantA, scopedStore.Namespace())
	assertEqual(t, 4, scopedStore.MaxKeys)
	assertEqual(t, 2, scopedStore.MaxGroups)
}

func TestScope_NewScopedConfigured_Bad_InvalidNamespace(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
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
	assertContainsString(t, err.Error(), testScopedStoreNilMessage)

	err = scopedStore.Set("theme", "dark")
	assertError(t, err)
	assertContainsString(t, err.Error(), testScopedStoreNilMessage)

	_, err = scopedStore.Count("config")
	assertError(t, err)
	assertContainsString(t, err.Error(), testScopedStoreNilMessage)

	_, err = scopedStore.Groups()
	assertError(t, err)
	assertContainsString(t, err.Error(), testScopedStoreNilMessage)

	for entry, iterationErr := range scopedStore.All("config") {
		_ = entry
		assertError(t, iterationErr)
		assertContainsString(t, iterationErr.Error(), testScopedStoreNilMessage)
		break
	}

	for groupName, iterationErr := range scopedStore.GroupsSeq() {
		_ = groupName
		assertError(t, iterationErr)
		assertContainsString(t, iterationErr.Error(), testScopedStoreNilMessage)
		break
	}
}

// ---------------------------------------------------------------------------
// ScopedStore — basic CRUD
// ---------------------------------------------------------------------------

func TestScope_ScopedStore_Good_SetGet(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("config", "theme", "dark"))

	value, err := scopedStore.GetFrom("config", "theme")
	assertNoError(t, err)
	assertEqual(t, "dark", value)
}

func TestScope_ScopedStore_Good_DefaultGroupHelpers(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.Set("theme", "dark"))

	value, err := scopedStore.Get("theme")
	assertNoError(t, err)
	assertEqual(t, "dark", value)

	rawValue, err := storeInstance.Get("tenant-a:default", "theme")
	assertNoError(t, err)
	assertEqual(t, "dark", rawValue)
}

func TestScope_ScopedStore_Good_SetInGetFrom(t *testing.T) {
	assertScopedStoreSetInGetFrom(t)
}

func assertScopedStoreSetInGetFrom(t *testing.T) {
	t.Helper()

	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("config", "theme", "dark"))

	value, err := scopedStore.GetFrom("config", "theme")
	assertNoError(t, err)
	assertEqual(t, "dark", value)
}

func TestScope_ScopedStore_Good_PrefixedInUnderlyingStore(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("config", "key", "val"))

	// The underlying store should have the prefixed group name.
	value, err := storeInstance.Get(testTenantAConfigGroup, "key")
	assertNoError(t, err)
	assertEqual(t, "val", value)

	// Direct access without prefix should fail.
	_, err = storeInstance.Get("config", "key")
	assertTrue(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_NamespaceIsolation(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	alphaStore := NewScoped(storeInstance, testTenantA)
	betaStore := NewScoped(storeInstance, testTenantB)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.Set("colour", "blue"))

	exists, err := scopedStore.Exists("colour")
	assertNoError(t, err)
	assertTrue(t, exists)

	exists, err = scopedStore.Exists("missing")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Good_ExistsInExplicitGroup(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetWithTTL("session", "token", "abc123", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	exists, err := scopedStore.ExistsIn("session", "token")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Good_GroupExists(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))

	exists, err := scopedStore.GroupExists("config")
	assertNoError(t, err)
	assertTrue(t, exists)

	exists, err = scopedStore.GroupExists("missing-group")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Good_GroupExistsAfterDelete(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	assertNoError(t, scopedStore.DeleteGroup("config"))

	exists, err := scopedStore.GroupExists("config")
	assertNoError(t, err)
	assertFalse(t, exists)
}

func TestScope_ScopedStore_Bad_ExistsClosedStore(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	_ = storeInstance.Close()
	scopedStore := NewScoped(storeInstance, testTenantA)

	_, err := scopedStore.Exists("colour")
	assertError(t, err)

	_, err = scopedStore.ExistsIn("config", "colour")
	assertError(t, err)

	_, err = scopedStore.GroupExists("config")
	assertError(t, err)
}

func TestScope_ScopedStore_Good_Delete(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("g", "k", "v"))
	assertNoError(t, scopedStore.Delete("g", "k"))

	_, err := scopedStore.GetFrom("g", "k")
	assertTrue(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_DeleteGroup(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("g", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g", "b", "2"))
	assertNoError(t, scopedStore.DeleteGroup("g"))

	count, err := scopedStore.Count("g")
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestScope_ScopedStore_Good_DeletePrefix(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	otherScopedStore := NewScoped(storeInstance, testTenantB)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	otherScopedStore := NewScoped(storeInstance, testTenantB)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	otherScopedStore := NewScoped(storeInstance, testTenantB)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	otherScopedStore := NewScoped(storeInstance, testTenantB)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	alphaStore := NewScoped(storeInstance, testTenantA)
	betaStore := NewScoped(storeInstance, testTenantB)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("items", "charlie", "3"))
	assertNoError(t, scopedStore.SetIn("items", "alpha", "1"))
	assertNoError(t, scopedStore.SetIn("items", "bravo", "2"))

	page, err := scopedStore.GetPage("items", 1, 1)
	assertNoError(t, err)
	assertLen(t, page, 1)
	assertEqual(t, KeyValue{Key: "bravo", Value: "2"}, page[0])
}

func TestScope_ScopedStore_Good_All(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("g", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g", "b", "2"))

	count, err := scopedStore.Count("g")
	assertNoError(t, err)
	assertEqual(t, 2, count)
}

func TestScope_ScopedStore_Good_SetWithTTL(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetWithTTL("g", "k", "v", time.Hour))

	value, err := scopedStore.GetFrom("g", "k")
	assertNoError(t, err)
	assertEqual(t, "v", value)
}

func TestScope_ScopedStore_Good_SetWithTTL_Expires(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetWithTTL("g", "k", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	_, err := scopedStore.GetFrom("g", "k")
	assertTrue(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_Render(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetIn("user", "name", "Alice"))

	renderedTemplate, err := scopedStore.Render("Hello {{ .name }}", "user")
	assertNoError(t, err)
	assertEqual(t, "Hello Alice", renderedTemplate)
}

func TestScope_ScopedStore_Good_BulkHelpers(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	alphaStore := NewScoped(storeInstance, testTenantA)
	betaStore := NewScoped(storeInstance, testTenantB)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, testTenantA)
	assertNoError(t, scopedStore.SetWithTTL("session", "token", "abc123", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	removedRows, err := scopedStore.PurgeExpired()
	assertNoError(t, err)
	assertEqual(t, int64(1), removedRows)

	_, err = scopedStore.GetFrom("session", "token")
	assertTrue(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_PurgeExpired_NamespaceLocal(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	alphaStore := NewScoped(storeInstance, testTenantA)
	betaStore := NewScoped(storeInstance, testTenantB)

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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
		cancelPurge:    noopCancelPurge,
	}

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	assertNoError(t, err)

	err = scopedStore.SetIn("config", "theme", "dark")
	assertError(t, err)
	assertContainsString(t, err.Error(), quotaCheckContext)
}

func TestScope_Quota_Good_MaxKeys_AcrossGroups(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: 0, MaxGroups: 0},
	})

	// Should be able to insert many keys and groups without error.
	for i := range 100 {
		assertNoError(t, scopedStore.SetIn("g", keyName(i), "v"))
	}
}

func TestScope_Quota_Good_ExpiredKeysExcluded(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxGroups: 2},
	})

	assertNoError(t, scopedStore.SetIn("g1", "a", "1"))
	assertNoError(t, scopedStore.SetIn("g2", "b", "2"))

	// Adding more keys to existing groups should be fine.
	assertNoError(t, scopedStore.SetIn("g1", "c", "3"))
	assertNoError(t, scopedStore.SetIn("g2", "d", "4"))
}

func TestScope_Quota_Good_MaxGroups_DeleteAndRecreate(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxGroups: 2},
	})

	assertNoError(t, scopedStore.SetIn("g1", "k", "v"))
	assertNoError(t, scopedStore.SetIn("g2", "k", "v"))

	// Delete a group, then create a new one.
	assertNoError(t, scopedStore.DeleteGroup("g1"))
	assertNoError(t, scopedStore.SetIn("g3", "k", "v"))
}

func TestScope_Quota_Good_MaxGroups_ZeroUnlimited(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxGroups: 0},
	})

	for i := range 50 {
		assertNoError(t, scopedStore.SetIn(keyName(i), "k", "v"))
	}
}

func TestScope_Quota_Good_MaxGroups_ExpiredGroupExcluded(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	alphaStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantA,
		Quota:     QuotaConfig{MaxKeys: 2},
	})
	betaStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: testTenantB,
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set(testNamespacedGroupOne, "k1", "v"))
	assertNoError(t, storeInstance.Set(testNamespacedGroupOne, "k2", "v"))
	assertNoError(t, storeInstance.Set(testNamespacedGroupTwo, "k1", "v"))
	assertNoError(t, storeInstance.Set("ns-b:g1", "k1", "v"))

	count, err := storeInstance.CountAll("ns-a:")
	assertNoError(t, err)
	assertEqual(t, 3, count)

	count, err = storeInstance.CountAll("ns-b:")
	assertNoError(t, err)
	assertEqual(t, 1, count)
}

func TestScope_CountAll_Good_WithPrefix_Wildcards(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("g1", "k1", "v"))
	assertNoError(t, storeInstance.Set("g2", "k2", "v"))

	count, err := storeInstance.CountAll("")
	assertNoError(t, err)
	assertEqual(t, 2, count)
}

func TestScope_CountAll_Good_ExcludesExpired(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("ns:g", "permanent", "v"))
	assertNoError(t, storeInstance.SetWithTTL("ns:g", "temp", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	count, err := storeInstance.CountAll("ns:")
	assertNoError(t, err)
	assertEqualf(t, 1, count, "expired keys should not be counted")
}

func TestScope_CountAll_Good_Empty(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	count, err := storeInstance.CountAll("nonexistent:")
	assertNoError(t, err)
	assertEqual(t, 0, count)
}

func TestScope_CountAll_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	_ = storeInstance.Close()
	_, err := storeInstance.CountAll("")
	assertError(t, err)
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

func TestScope_Groups_Good_WithPrefix(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set(testNamespacedGroupOne, "k", "v"))
	assertNoError(t, storeInstance.Set(testNamespacedGroupTwo, "k", "v"))
	assertNoError(t, storeInstance.Set(testNamespacedGroupTwo, "k2", "v")) // duplicate group
	assertNoError(t, storeInstance.Set("ns-b:g1", "k", "v"))

	groups, err := storeInstance.Groups("ns-a:")
	assertNoError(t, err)
	assertLen(t, groups, 2)
	assertContainsElement(t, groups, testNamespacedGroupOne)
	assertContainsElement(t, groups, testNamespacedGroupTwo)
}

func TestScope_Groups_Good_EmptyPrefix(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("g1", "k", "v"))
	assertNoError(t, storeInstance.Set("g2", "k", "v"))
	assertNoError(t, storeInstance.Set("g3", "k", "v"))

	groups, err := storeInstance.Groups("")
	assertNoError(t, err)
	assertLen(t, groups, 3)
}

func TestScope_Groups_Good_Distinct(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
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
	storeInstance, _ := New(testMemoryDatabasePath)
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
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("charlie", "c", "3"))
	assertNoError(t, storeInstance.Set("alpha", "a", "1"))
	assertNoError(t, storeInstance.Set("bravo", "b", "2"))

	groups, err := storeInstance.Groups("")
	assertNoError(t, err)
	assertEqual(t, []string{"alpha", "bravo", "charlie"}, groups)
}

func TestScope_Groups_Good_Empty(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
	defer func() { _ = storeInstance.Close() }()

	groups, err := storeInstance.Groups("nonexistent:")
	assertNoError(t, err)
	assertEmpty(t, groups)
}

func TestScope_Groups_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(testMemoryDatabasePath)
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
		sqlSelectCountFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ?",
		group,
	).Scan(&count)
	assertNoError(t, err)
	return count
}

func TestScope_QuotaConfig_Validate_Good(t *T) {
	quota := QuotaConfig{MaxKeys: 2, MaxGroups: 1}
	err := quota.Validate()
	AssertNoError(t, err)
}

func TestScope_QuotaConfig_Validate_Bad(t *T) {
	quota := QuotaConfig{MaxKeys: -1}
	err := quota.Validate()
	AssertError(t, err)
}

func TestScope_QuotaConfig_Validate_Ugly(t *T) {
	quota := QuotaConfig{}
	err := quota.Validate()
	AssertNoError(t, err)
}

func TestScope_ScopedStoreConfig_Validate_Good(t *T) {
	config := ScopedStoreConfig{Namespace: testTenantA, Quota: QuotaConfig{MaxKeys: 2}}
	err := config.Validate()
	AssertNoError(t, err)
}

func TestScope_ScopedStoreConfig_Validate_Bad(t *T) {
	config := ScopedStoreConfig{Namespace: "tenant_a"}
	err := config.Validate()
	AssertError(t, err)
}

func TestScope_ScopedStoreConfig_Validate_Ugly(t *T) {
	config := ScopedStoreConfig{Namespace: testTenantA, Quota: QuotaConfig{MaxGroups: -1}}
	err := config.Validate()
	AssertError(t, err)
}

func TestScope_NewScoped_Bad(t *T) {
	scopedStore := NewScoped(nil, testTenantA)
	AssertNil(t, scopedStore)
	AssertEqual(t, "", scopedStore.Namespace())
}

func TestScope_NewScoped_Ugly(t *T) {
	scopedStore := NewScoped(ax7Store(t), testTenant42)
	AssertNotNil(t, scopedStore)
	AssertEqual(t, testTenant42, scopedStore.Namespace())
}

func TestScope_NewScopedConfigured_Bad(t *T) {
	scopedStore, err := NewScopedConfigured(nil, ScopedStoreConfig{Namespace: testTenantA})
	AssertError(t, err)
	AssertNil(t, scopedStore)
}

func TestScope_NewScopedConfigured_Ugly(t *T) {
	scopedStore, err := NewScopedConfigured(ax7Store(t), ScopedStoreConfig{Namespace: testTenantA, Quota: QuotaConfig{MaxKeys: 1, MaxGroups: 1}})
	RequireNoError(t, err)
	AssertEqual(t, 1, scopedStore.Config().Quota.MaxKeys)
}

func TestScope_NewScopedWithQuota_Bad(t *T) {
	scopedStore, err := NewScopedWithQuota(nil, testTenantA, QuotaConfig{})
	AssertError(t, err)
	AssertNil(t, scopedStore)
}

func TestScope_NewScopedWithQuota_Ugly(t *T) {
	scopedStore, err := NewScopedWithQuota(ax7Store(t), testTenantA, QuotaConfig{MaxGroups: 1})
	RequireNoError(t, err)
	AssertEqual(t, 1, scopedStore.Config().Quota.MaxGroups)
}

func TestScope_ScopedStore_Namespace_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	namespace := scopedStore.Namespace()
	AssertEqual(t, testTenantA, namespace)
}

func TestScope_ScopedStore_Namespace_Bad(t *T) {
	var scopedStore *ScopedStore
	namespace := scopedStore.Namespace()
	AssertEqual(t, "", namespace)
}

func TestScope_ScopedStore_Namespace_Ugly(t *T) {
	scopedStore := ax7QuotaScopedStore(t, 1, 1)
	namespace := scopedStore.Namespace()
	AssertEqual(t, testTenantA, namespace)
}

func TestScope_ScopedStore_Config_Good(t *T) {
	scopedStore := ax7QuotaScopedStore(t, 2, 1)
	config := scopedStore.Config()
	AssertEqual(t, 2, config.Quota.MaxKeys)
}

func TestScope_ScopedStore_Config_Bad(t *T) {
	var scopedStore *ScopedStore
	config := scopedStore.Config()
	AssertEqual(t, ScopedStoreConfig{}, config)
}

func TestScope_ScopedStore_Config_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	config := scopedStore.Config()
	AssertEqual(t, testTenantA, config.Namespace)
}

func TestScope_ScopedStore_Exists_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.Set("colour", "blue"))
	exists, err := scopedStore.Exists("colour")
	AssertNoError(t, err)
	AssertTrue(t, exists)
}

func TestScope_ScopedStore_Exists_Bad(t *T) {
	var scopedStore *ScopedStore
	exists, err := scopedStore.Exists("colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStore_Exists_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	exists, err := scopedStore.Exists("missing")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStore_ExistsIn_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	exists, err := scopedStore.ExistsIn("config", "colour")
	AssertNoError(t, err)
	AssertTrue(t, exists)
}

func TestScope_ScopedStore_ExistsIn_Bad(t *T) {
	var scopedStore *ScopedStore
	exists, err := scopedStore.ExistsIn("config", "colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStore_ExistsIn_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	exists, err := scopedStore.ExistsIn("config", "missing")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStore_GroupExists_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	exists, err := scopedStore.GroupExists("config")
	AssertNoError(t, err)
	AssertTrue(t, exists)
}

func TestScope_ScopedStore_GroupExists_Bad(t *T) {
	var scopedStore *ScopedStore
	exists, err := scopedStore.GroupExists("config")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStore_GroupExists_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	exists, err := scopedStore.GroupExists("missing")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStore_Get_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.Set("colour", "blue"))
	got, err := scopedStore.Get("colour")
	AssertNoError(t, err)
	AssertEqual(t, "blue", got)
}

func TestScope_ScopedStore_Get_Bad(t *T) {
	var scopedStore *ScopedStore
	got, err := scopedStore.Get("colour")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestScope_ScopedStore_Get_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	got, err := scopedStore.Get("missing")
	AssertErrorIs(t, err, NotFoundError)
	AssertEqual(t, "", got)
}

func TestScope_ScopedStore_GetFrom_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	got, err := scopedStore.GetFrom("config", "colour")
	AssertNoError(t, err)
	AssertEqual(t, "blue", got)
}

func TestScope_ScopedStore_GetFrom_Bad(t *T) {
	var scopedStore *ScopedStore
	got, err := scopedStore.GetFrom("config", "colour")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestScope_ScopedStore_GetFrom_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	got, err := scopedStore.GetFrom("config", "missing")
	AssertErrorIs(t, err, NotFoundError)
	AssertEqual(t, "", got)
}

func TestScope_ScopedStore_Set_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Set("colour", "blue")
	AssertNoError(t, err)
	AssertTrue(t, ax7ScopedExists(t, scopedStore, "colour"))
}

func TestScope_ScopedStore_Set_Bad(t *T) {
	var scopedStore *ScopedStore
	err := scopedStore.Set("colour", "blue")
	AssertError(t, err)
}

func TestScope_ScopedStore_Set_Ugly(t *T) {
	scopedStore := ax7QuotaScopedStore(t, 1, 0)
	RequireNoError(t, scopedStore.Set("colour", "blue"))
	err := scopedStore.Set("shape", "circle")
	AssertErrorIs(t, err, QuotaExceededError)
}

func TestScope_ScopedStore_SetIn_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.SetIn("config", "colour", "blue")
	AssertNoError(t, err)
	AssertTrue(t, ax7ScopedExistsIn(t, scopedStore, "config", "colour"))
}

func TestScope_ScopedStore_SetIn_Bad(t *T) {
	var scopedStore *ScopedStore
	err := scopedStore.SetIn("config", "colour", "blue")
	AssertError(t, err)
}

func TestScope_ScopedStore_SetIn_Ugly(t *T) {
	scopedStore := ax7QuotaScopedStore(t, 0, 1)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	err := scopedStore.SetIn("other", "shape", "circle")
	AssertErrorIs(t, err, QuotaExceededError)
}

func TestScope_ScopedStore_SetWithTTL_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.SetWithTTL("sessions", "token", "abc", Hour)
	AssertNoError(t, err)
	AssertTrue(t, ax7ScopedExistsIn(t, scopedStore, "sessions", "token"))
}

func TestScope_ScopedStore_SetWithTTL_Bad(t *T) {
	var scopedStore *ScopedStore
	err := scopedStore.SetWithTTL("sessions", "token", "abc", Hour)
	AssertError(t, err)
}

func TestScope_ScopedStore_SetWithTTL_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetWithTTL("sessions", "token", "abc", -Millisecond))
	exists, err := scopedStore.ExistsIn("sessions", "token")
	AssertNoError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStore_Delete_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	err := scopedStore.Delete("config", "colour")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedExistsIn(t, scopedStore, "config", "colour"))
}

func TestScope_ScopedStore_Delete_Bad(t *T) {
	var scopedStore *ScopedStore
	err := scopedStore.Delete("config", "colour")
	AssertError(t, err)
}

func TestScope_ScopedStore_Delete_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Delete("missing", "key")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedExistsIn(t, scopedStore, "missing", "key"))
}

func TestScope_ScopedStore_DeleteGroup_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	err := scopedStore.DeleteGroup("config")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedGroupExists(t, scopedStore, "config"))
}

func TestScope_ScopedStore_DeleteGroup_Bad(t *T) {
	var scopedStore *ScopedStore
	err := scopedStore.DeleteGroup("config")
	AssertError(t, err)
}

func TestScope_ScopedStore_DeleteGroup_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.DeleteGroup("missing")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedGroupExists(t, scopedStore, "missing"))
}

func TestScope_ScopedStore_DeletePrefix_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn(testCacheA, "k", "v"))
	err := scopedStore.DeletePrefix("cache")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedGroupExists(t, scopedStore, testCacheA))
}

func TestScope_ScopedStore_DeletePrefix_Bad(t *T) {
	var scopedStore *ScopedStore
	err := scopedStore.DeletePrefix("cache")
	AssertError(t, err)
}

func TestScope_ScopedStore_DeletePrefix_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("cache", "k", "v"))
	err := scopedStore.DeletePrefix("")
	AssertNoError(t, err)
	AssertFalse(t, ax7ScopedGroupExists(t, scopedStore, "cache"))
}

func TestScope_ScopedStore_GetAll_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	entries, err := scopedStore.GetAll("config")
	AssertNoError(t, err)
	AssertEqual(t, "blue", entries["colour"])
}

func TestScope_ScopedStore_GetAll_Bad(t *T) {
	var scopedStore *ScopedStore
	entries, err := scopedStore.GetAll("config")
	AssertError(t, err)
	AssertNil(t, entries)
}

func TestScope_ScopedStore_GetAll_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	entries, err := scopedStore.GetAll("missing")
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestScope_ScopedStore_GetPage_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	page, err := scopedStore.GetPage("config", 0, 1)
	AssertNoError(t, err)
	AssertEqual(t, "colour", page[0].Key)
}

func TestScope_ScopedStore_GetPage_Bad(t *T) {
	var scopedStore *ScopedStore
	page, err := scopedStore.GetPage("config", 0, 1)
	AssertError(t, err)
	AssertNil(t, page)
}

func TestScope_ScopedStore_GetPage_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	page, err := scopedStore.GetPage("missing", 0, 1)
	AssertNoError(t, err)
	AssertEmpty(t, page)
}

func TestScope_ScopedStore_All_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	entries, err := ax7CollectKeyValues(scopedStore.All("config"))
	AssertNoError(t, err)
	AssertEqual(t, "colour", entries[0].Key)
}

func TestScope_ScopedStore_All_Bad(t *T) {
	var scopedStore *ScopedStore
	entries, err := ax7CollectKeyValues(scopedStore.All("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestScope_ScopedStore_All_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	entries, err := ax7CollectKeyValues(scopedStore.All("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestScope_ScopedStore_AllSeq_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	entries, err := ax7CollectKeyValues(scopedStore.AllSeq("config"))
	AssertNoError(t, err)
	AssertEqual(t, "blue", entries[0].Value)
}

func TestScope_ScopedStore_AllSeq_Bad(t *T) {
	var scopedStore *ScopedStore
	entries, err := ax7CollectKeyValues(scopedStore.AllSeq("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestScope_ScopedStore_AllSeq_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	entries, err := ax7CollectKeyValues(scopedStore.AllSeq("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, entries)
}

func TestScope_ScopedStore_Count_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	count, err := scopedStore.Count("config")
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestScope_ScopedStore_Count_Bad(t *T) {
	var scopedStore *ScopedStore
	count, err := scopedStore.Count("config")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestScope_ScopedStore_Count_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	count, err := scopedStore.Count("missing")
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestScope_ScopedStore_CountAll_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	count, err := scopedStore.CountAll()
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestScope_ScopedStore_CountAll_Bad(t *T) {
	var scopedStore *ScopedStore
	count, err := scopedStore.CountAll()
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestScope_ScopedStore_CountAll_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	count, err := scopedStore.CountAll("missing")
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestScope_ScopedStore_Groups_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	groups, err := scopedStore.Groups()
	AssertNoError(t, err)
	AssertEqual(t, []string{"config"}, groups)
}

func TestScope_ScopedStore_Groups_Bad(t *T) {
	var scopedStore *ScopedStore
	groups, err := scopedStore.Groups()
	AssertError(t, err)
	AssertNil(t, groups)
}

func TestScope_ScopedStore_Groups_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	groups, err := scopedStore.Groups("missing")
	AssertNoError(t, err)
	AssertEmpty(t, groups)
}

func TestScope_ScopedStore_GroupsSeq_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	groups, err := ax7CollectGroups(scopedStore.GroupsSeq())
	AssertNoError(t, err)
	AssertEqual(t, []string{"config"}, groups)
}

func TestScope_ScopedStore_GroupsSeq_Bad(t *T) {
	var scopedStore *ScopedStore
	groups, err := ax7CollectGroups(scopedStore.GroupsSeq())
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestScope_ScopedStore_GroupsSeq_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	groups, err := ax7CollectGroups(scopedStore.GroupsSeq("missing"))
	AssertNoError(t, err)
	AssertEmpty(t, groups)
}

func TestScope_ScopedStore_Render_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "name", "alice"))
	rendered, err := scopedStore.Render("hello {{ .name }}", "config")
	AssertNoError(t, err)
	AssertEqual(t, "hello alice", rendered)
}

func TestScope_ScopedStore_Render_Bad(t *T) {
	var scopedStore *ScopedStore
	rendered, err := scopedStore.Render("hello", "config")
	AssertError(t, err)
	AssertEqual(t, "", rendered)
}

func TestScope_ScopedStore_Render_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	rendered, err := scopedStore.Render("empty", "missing")
	AssertNoError(t, err)
	AssertEqual(t, "empty", rendered)
}

func TestScope_ScopedStore_GetSplit_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "hosts", "a,b"))
	seq, err := scopedStore.GetSplit("config", "hosts", ",")
	AssertNoError(t, err)
	AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
}

func TestScope_ScopedStore_GetSplit_Bad(t *T) {
	var scopedStore *ScopedStore
	seq, err := scopedStore.GetSplit("config", "hosts", ",")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestScope_ScopedStore_GetSplit_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	seq, err := scopedStore.GetSplit("config", "missing", ",")
	AssertErrorIs(t, err, NotFoundError)
	AssertNil(t, seq)
}

func TestScope_ScopedStore_GetFields_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetIn("config", "flags", "a b"))
	seq, err := scopedStore.GetFields("config", "flags")
	AssertNoError(t, err)
	AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
}

func TestScope_ScopedStore_GetFields_Bad(t *T) {
	var scopedStore *ScopedStore
	seq, err := scopedStore.GetFields("config", "flags")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestScope_ScopedStore_GetFields_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	seq, err := scopedStore.GetFields("config", "missing")
	AssertErrorIs(t, err, NotFoundError)
	AssertNil(t, seq)
}

func TestScope_ScopedStore_PurgeExpired_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	RequireNoError(t, scopedStore.SetWithTTL("sessions", "token", "abc", -Millisecond))
	removed, err := scopedStore.PurgeExpired()
	AssertNoError(t, err)
	AssertEqual(t, int64(1), removed)
}

func TestScope_ScopedStore_PurgeExpired_Bad(t *T) {
	var scopedStore *ScopedStore
	removed, err := scopedStore.PurgeExpired()
	AssertError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestScope_ScopedStore_PurgeExpired_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	removed, err := scopedStore.PurgeExpired()
	AssertNoError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestScope_ScopedStore_Watch_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	events := scopedStore.Watch("config")
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	event := <-events
	AssertEqual(t, "config", event.Group)
}

func TestScope_ScopedStore_Watch_Bad(t *T) {
	var scopedStore *ScopedStore
	events := scopedStore.Watch("config")
	_, ok := <-events
	AssertFalse(t, ok)
}

func TestScope_ScopedStore_Watch_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	events := scopedStore.Watch("*")
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	event := <-events
	AssertEqual(t, "config", event.Group)
}

func TestScope_ScopedStore_Unwatch_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)
	_, ok := <-events
	AssertFalse(t, ok)
}

func TestScope_ScopedStore_Unwatch_Bad(t *T) {
	var scopedStore *ScopedStore
	AssertNotPanics(t, func() { scopedStore.Unwatch("config", nil) })
	AssertEqual(t, "", scopedStore.Namespace())
}

func TestScope_ScopedStore_Unwatch_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)
	AssertNotPanics(t, func() { scopedStore.Unwatch("config", events) })
}

func TestScope_ScopedStore_OnChange_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	called := false
	unregister := scopedStore.OnChange(func(event Event) { called = event.Group == "config" })
	defer unregister()
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	AssertTrue(t, called)
}

func TestScope_ScopedStore_OnChange_Bad(t *T) {
	var scopedStore *ScopedStore
	unregister := scopedStore.OnChange(func(Event) {
		// Intentionally empty: nil scoped store should return a no-op unregister.
	})
	unregister()
	AssertEqual(t, "", scopedStore.Namespace())
}

func TestScope_ScopedStore_OnChange_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	count := 0
	unregister := scopedStore.OnChange(func(Event) { count++ })
	unregister()
	RequireNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	AssertEqual(t, 0, count)
}

func TestScope_ScopedStore_Transaction_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error { return transaction.Set("colour", "blue") })
	AssertNoError(t, err)
	AssertTrue(t, ax7ScopedExists(t, scopedStore, "colour"))
}

func TestScope_ScopedStore_Transaction_Bad(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(nil)
	AssertError(t, err)
	AssertFalse(t, ax7ScopedExists(t, scopedStore, "colour"))
}

func TestScope_ScopedStore_Transaction_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error { return NewError("rollback") })
	AssertError(t, err)
	AssertFalse(t, ax7ScopedExists(t, scopedStore, "colour"))
}

func TestScope_ScopedStoreTransaction_Exists_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.Set("colour", "blue"))
		exists, err := transaction.Exists("colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Exists_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	exists, err := transaction.Exists("colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStoreTransaction_Exists_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		exists, err := transaction.Exists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_ExistsIn_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		exists, err := transaction.ExistsIn("config", "colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_ExistsIn_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	exists, err := transaction.ExistsIn("config", "colour")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStoreTransaction_ExistsIn_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		exists, err := transaction.ExistsIn("config", "missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GroupExists_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		exists, err := transaction.GroupExists("config")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GroupExists_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	exists, err := transaction.GroupExists("config")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestScope_ScopedStoreTransaction_GroupExists_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		exists, err := transaction.GroupExists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Get_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.Set("colour", "blue"))
		got, err := transaction.Get("colour")
		AssertNoError(t, err)
		AssertEqual(t, "blue", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Get_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	got, err := transaction.Get("colour")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestScope_ScopedStoreTransaction_Get_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		got, err := transaction.Get("missing")
		AssertErrorIs(t, err, NotFoundError)
		AssertEqual(t, "", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetFrom_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		got, err := transaction.GetFrom("config", "colour")
		AssertNoError(t, err)
		AssertEqual(t, "blue", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetFrom_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	got, err := transaction.GetFrom("config", "colour")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestScope_ScopedStoreTransaction_GetFrom_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		got, err := transaction.GetFrom("config", "missing")
		AssertErrorIs(t, err, NotFoundError)
		AssertEqual(t, "", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Set_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		err := transaction.Set("colour", "blue")
		AssertNoError(t, err)
		exists, err := transaction.Exists("colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Set_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	err := transaction.Set("colour", "blue")
	AssertError(t, err)
}

func TestScope_ScopedStoreTransaction_Set_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.Set("colour", "blue"))
		err := transaction.Set("shape", "circle")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_SetIn_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		err := transaction.SetIn("config", "colour", "blue")
		AssertNoError(t, err)
		exists, err := transaction.ExistsIn("config", "colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_SetIn_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	err := transaction.SetIn("config", "colour", "blue")
	AssertError(t, err)
}

func TestScope_ScopedStoreTransaction_SetIn_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		err := transaction.SetIn("config", "colour", "green")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_SetWithTTL_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		err := transaction.SetWithTTL("sessions", "token", "abc", Hour)
		AssertNoError(t, err)
		exists, err := transaction.ExistsIn("sessions", "token")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_SetWithTTL_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	err := transaction.SetWithTTL("sessions", "token", "abc", Hour)
	AssertError(t, err)
}

func TestScope_ScopedStoreTransaction_SetWithTTL_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetWithTTL("sessions", "token", "abc", -Millisecond))
		exists, err := transaction.ExistsIn("sessions", "token")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Delete_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
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

func TestScope_ScopedStoreTransaction_Delete_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	err := transaction.Delete("config", "colour")
	AssertError(t, err)
}

func TestScope_ScopedStoreTransaction_Delete_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		err := transaction.Delete("missing", "key")
		AssertNoError(t, err)
		exists, err := transaction.ExistsIn("missing", "key")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_DeleteGroup_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
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

func TestScope_ScopedStoreTransaction_DeleteGroup_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	err := transaction.DeleteGroup("config")
	AssertError(t, err)
}

func TestScope_ScopedStoreTransaction_DeleteGroup_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		err := transaction.DeleteGroup("missing")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_DeletePrefix_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn(testCacheA, "colour", "blue"))
		err := transaction.DeletePrefix("cache")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists(testCacheA)
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_DeletePrefix_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	err := transaction.DeletePrefix("cache")
	AssertError(t, err)
}

func TestScope_ScopedStoreTransaction_DeletePrefix_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		err := transaction.DeletePrefix("missing")
		AssertNoError(t, err)
		groups, err := transaction.Groups("missing")
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetAll_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		entries, err := transaction.GetAll("config")
		AssertNoError(t, err)
		AssertEqual(t, "blue", entries["colour"])
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetAll_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	entries, err := transaction.GetAll("config")
	AssertError(t, err)
	AssertNil(t, entries)
}

func TestScope_ScopedStoreTransaction_GetAll_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		entries, err := transaction.GetAll("missing")
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetPage_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		page, err := transaction.GetPage("config", 0, 1)
		AssertNoError(t, err)
		AssertEqual(t, "colour", page[0].Key)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetPage_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	page, err := transaction.GetPage("config", 0, 1)
	AssertError(t, err)
	AssertNil(t, page)
}

func TestScope_ScopedStoreTransaction_GetPage_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		page, err := transaction.GetPage("missing", 0, 1)
		AssertNoError(t, err)
		AssertEmpty(t, page)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_All_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		entries, err := ax7CollectKeyValues(transaction.All("config"))
		AssertNoError(t, err)
		AssertEqual(t, "colour", entries[0].Key)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_All_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	entries, err := ax7CollectKeyValues(transaction.All("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestScope_ScopedStoreTransaction_All_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		entries, err := ax7CollectKeyValues(transaction.All("missing"))
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_AllSeq_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		entries, err := ax7CollectKeyValues(transaction.AllSeq("config"))
		AssertNoError(t, err)
		AssertEqual(t, "blue", entries[0].Value)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_AllSeq_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	entries, err := ax7CollectKeyValues(transaction.AllSeq("config"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestScope_ScopedStoreTransaction_AllSeq_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		entries, err := ax7CollectKeyValues(transaction.AllSeq("missing"))
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Count_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		count, err := transaction.Count("config")
		AssertNoError(t, err)
		AssertEqual(t, 1, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Count_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	count, err := transaction.Count("config")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestScope_ScopedStoreTransaction_Count_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		count, err := transaction.Count("missing")
		AssertNoError(t, err)
		AssertEqual(t, 0, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_CountAll_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		count, err := transaction.CountAll()
		AssertNoError(t, err)
		AssertEqual(t, 1, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_CountAll_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	count, err := transaction.CountAll()
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestScope_ScopedStoreTransaction_CountAll_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		count, err := transaction.CountAll("missing")
		AssertNoError(t, err)
		AssertEqual(t, 0, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Groups_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		groups, err := transaction.Groups()
		AssertNoError(t, err)
		AssertEqual(t, []string{"config"}, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Groups_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	groups, err := transaction.Groups()
	AssertError(t, err)
	AssertNil(t, groups)
}

func TestScope_ScopedStoreTransaction_Groups_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		groups, err := transaction.Groups("missing")
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GroupsSeq_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "colour", "blue"))
		groups, err := ax7CollectGroups(transaction.GroupsSeq())
		AssertNoError(t, err)
		AssertEqual(t, []string{"config"}, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GroupsSeq_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	groups, err := ax7CollectGroups(transaction.GroupsSeq())
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestScope_ScopedStoreTransaction_GroupsSeq_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		groups, err := ax7CollectGroups(transaction.GroupsSeq("missing"))
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Render_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "name", "alice"))
		rendered, err := transaction.Render("hello {{ .name }}", "config")
		AssertNoError(t, err)
		AssertEqual(t, "hello alice", rendered)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_Render_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	rendered, err := transaction.Render("hello", "config")
	AssertError(t, err)
	AssertEqual(t, "", rendered)
}

func TestScope_ScopedStoreTransaction_Render_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		rendered, err := transaction.Render("empty", "missing")
		AssertNoError(t, err)
		AssertEqual(t, "empty", rendered)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetSplit_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "hosts", "a,b"))
		seq, err := transaction.GetSplit("config", "hosts", ",")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetSplit_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	seq, err := transaction.GetSplit("config", "hosts", ",")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestScope_ScopedStoreTransaction_GetSplit_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		seq, err := transaction.GetSplit("missing", "hosts", ",")
		AssertErrorIs(t, err, NotFoundError)
		AssertNil(t, seq)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetFields_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetIn("config", "flags", "a b"))
		seq, err := transaction.GetFields("config", "flags")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_GetFields_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	seq, err := transaction.GetFields("config", "flags")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestScope_ScopedStoreTransaction_GetFields_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		seq, err := transaction.GetFields("missing", "flags")
		AssertErrorIs(t, err, NotFoundError)
		AssertNil(t, seq)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_PurgeExpired_Good(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		RequireNoError(t, transaction.SetWithTTL("sessions", "token", "abc", -Millisecond))
		removed, err := transaction.PurgeExpired()
		AssertNoError(t, err)
		AssertEqual(t, int64(1), removed)
		return nil
	})
	AssertNoError(t, err)
}

func TestScope_ScopedStoreTransaction_PurgeExpired_Bad(t *T) {
	var transaction *ScopedStoreTransaction
	removed, err := transaction.PurgeExpired()
	AssertError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestScope_ScopedStoreTransaction_PurgeExpired_Ugly(t *T) {
	scopedStore := ax7ScopedStore(t)
	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		removed, err := transaction.PurgeExpired()
		AssertNoError(t, err)
		AssertEqual(t, int64(0), removed)
		return nil
	})
	AssertNoError(t, err)
}
