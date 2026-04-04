package store

import (
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// NewScoped — constructor validation
// ---------------------------------------------------------------------------

func TestScope_NewScoped_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScoped(storeInstance, "tenant-1")
	require.NoError(t, err)
	require.NotNil(t, scopedStore)
	assert.Equal(t, "tenant-1", scopedStore.Namespace())
}

func TestScope_ScopedStore_Good_Config(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	require.NoError(t, err)

	assert.Equal(t, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	}, scopedStore.Config())
}

func TestScope_ScopedStore_Good_ConfigZeroValueFromNil(t *testing.T) {
	var scopedStore *ScopedStore

	assert.Equal(t, ScopedStoreConfig{}, scopedStore.Config())
}

func TestScope_NewScoped_Good_AlphanumericHyphens(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	valid := []string{"abc", "ABC", "123", "a-b-c", "tenant-42", "A1-B2"}
	for _, namespace := range valid {
		scopedStore, err := NewScoped(storeInstance, namespace)
		require.NoError(t, err, "namespace %q should be valid", namespace)
		require.NotNil(t, scopedStore)
	}
}

func TestScope_NewScoped_Bad_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScoped(storeInstance, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid")
}

func TestScope_NewScoped_Bad_NilStore(t *testing.T) {
	_, err := NewScoped(nil, "tenant-a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store instance is nil")
}

func TestScope_NewScoped_Bad_InvalidChars(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	invalid := []string{"foo.bar", "foo:bar", "foo bar", "foo/bar", "foo_bar", "tenant!", "@ns"}
	for _, namespace := range invalid {
		_, err := NewScoped(storeInstance, namespace)
		require.Error(t, err, "namespace %q should be invalid", namespace)
	}
}

func TestScope_NewScopedConfigured_Bad_InvalidNamespaceFromQuotaConfig(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant_a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.NewScoped")
}

func TestScope_NewScopedConfigured_Bad_NilStoreFromQuotaConfig(t *testing.T) {
	_, err := NewScopedConfigured(nil, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store instance is nil")
}

func TestScope_NewScopedConfigured_Bad_NegativeMaxKeys(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: -1},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zero or positive")
}

func TestScope_NewScopedConfigured_Bad_NegativeMaxGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: -1},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zero or positive")
}

func TestScope_NewScopedConfigured_Good_InlineQuotaFields(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	require.NoError(t, err)

	assert.Equal(t, 4, scopedStore.MaxKeys)
	assert.Equal(t, 2, scopedStore.MaxGroups)
}

func TestScope_ScopedStoreConfig_Good_Validate(t *testing.T) {
	err := (ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	}).Validate()
	require.NoError(t, err)
}

func TestScope_NewScopedConfigured_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	require.NoError(t, err)
	require.NotNil(t, scopedStore)
	assert.Equal(t, 4, scopedStore.MaxKeys)
	assert.Equal(t, 2, scopedStore.MaxGroups)
}

func TestScope_NewScopedConfigured_Bad_InvalidNamespace(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant_a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "namespace")
}

// ---------------------------------------------------------------------------
// ScopedStore — basic CRUD
// ---------------------------------------------------------------------------

func TestScope_ScopedStore_Good_SetGet(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("config", "theme", "dark"))

	value, err := scopedStore.GetFrom("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", value)
}

func TestScope_ScopedStore_Good_DefaultGroupHelpers(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("theme", "dark"))

	value, err := scopedStore.Get("theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", value)

	rawValue, err := storeInstance.Get("tenant-a:default", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", rawValue)
}

func TestScope_ScopedStore_Good_SetInGetFrom(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("config", "theme", "dark"))

	value, err := scopedStore.GetFrom("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", value)
}

func TestScope_ScopedStore_Good_PrefixedInUnderlyingStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("config", "key", "val"))

	// The underlying store should have the prefixed group name.
	value, err := storeInstance.Get("tenant-a:config", "key")
	require.NoError(t, err)
	assert.Equal(t, "val", value)

	// Direct access without prefix should fail.
	_, err = storeInstance.Get("config", "key")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_NamespaceIsolation(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore, _ := NewScoped(storeInstance, "tenant-a")
	betaStore, _ := NewScoped(storeInstance, "tenant-b")

	require.NoError(t, alphaStore.SetIn("config", "colour", "blue"))
	require.NoError(t, betaStore.SetIn("config", "colour", "red"))

	alphaValue, err := alphaStore.GetFrom("config", "colour")
	require.NoError(t, err)
	assert.Equal(t, "blue", alphaValue)

	betaValue, err := betaStore.GetFrom("config", "colour")
	require.NoError(t, err)
	assert.Equal(t, "red", betaValue)
}

func TestScope_ScopedStore_Good_Delete(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("g", "k", "v"))
	require.NoError(t, scopedStore.Delete("g", "k"))

	_, err := scopedStore.GetFrom("g", "k")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_DeleteGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("g", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g", "b", "2"))
	require.NoError(t, scopedStore.DeleteGroup("g"))

	count, err := scopedStore.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestScope_ScopedStore_Good_DeletePrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	otherScopedStore, _ := NewScoped(storeInstance, "tenant-b")

	require.NoError(t, scopedStore.SetIn("config", "theme", "dark"))
	require.NoError(t, scopedStore.SetIn("cache", "page", "home"))
	require.NoError(t, scopedStore.SetIn("cache-warm", "status", "ready"))
	require.NoError(t, otherScopedStore.SetIn("cache", "page", "keep"))

	require.NoError(t, scopedStore.DeletePrefix("cache"))

	_, err := scopedStore.GetFrom("cache", "page")
	assert.True(t, core.Is(err, NotFoundError))
	_, err = scopedStore.GetFrom("cache-warm", "status")
	assert.True(t, core.Is(err, NotFoundError))

	value, err := scopedStore.GetFrom("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", value)

	otherValue, err := otherScopedStore.GetFrom("cache", "page")
	require.NoError(t, err)
	assert.Equal(t, "keep", otherValue)
}

func TestScope_ScopedStore_Good_OnChange_NamespaceLocal(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	otherScopedStore, _ := NewScoped(storeInstance, "tenant-b")

	var events []Event
	unregister := scopedStore.OnChange(func(event Event) {
		events = append(events, event)
	})
	defer unregister()

	require.NoError(t, scopedStore.SetIn("config", "colour", "blue"))
	require.NoError(t, otherScopedStore.SetIn("config", "colour", "red"))
	require.NoError(t, scopedStore.Delete("config", "colour"))

	require.Len(t, events, 2)
	assert.Equal(t, "config", events[0].Group)
	assert.Equal(t, "colour", events[0].Key)
	assert.Equal(t, "blue", events[0].Value)
	assert.Equal(t, "config", events[1].Group)
	assert.Equal(t, "colour", events[1].Key)
	assert.Equal(t, "", events[1].Value)
}

func TestScope_ScopedStore_Good_Watch_NamespaceLocal(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	otherScopedStore, _ := NewScoped(storeInstance, "tenant-b")

	events := scopedStore.Watch("config")
	defer scopedStore.Unwatch("config", events)

	require.NoError(t, scopedStore.SetIn("config", "colour", "blue"))
	require.NoError(t, otherScopedStore.SetIn("config", "colour", "red"))

	select {
	case event, ok := <-events:
		require.True(t, ok)
		assert.Equal(t, EventSet, event.Type)
		assert.Equal(t, "config", event.Group)
		assert.Equal(t, "colour", event.Key)
		assert.Equal(t, "blue", event.Value)
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
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	otherScopedStore, _ := NewScoped(storeInstance, "tenant-b")

	events := scopedStore.Watch("*")
	defer scopedStore.Unwatch("*", events)

	require.NoError(t, scopedStore.SetIn("config", "colour", "blue"))
	require.NoError(t, scopedStore.SetIn("cache", "page", "home"))
	require.NoError(t, otherScopedStore.SetIn("config", "colour", "red"))

	select {
	case event, ok := <-events:
		require.True(t, ok)
		assert.Equal(t, "config", event.Group)
		assert.Equal(t, "colour", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first wildcard scoped watch event")
	}

	select {
	case event, ok := <-events:
		require.True(t, ok)
		assert.Equal(t, "cache", event.Group)
		assert.Equal(t, "page", event.Key)
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
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")

	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)

	select {
	case _, ok := <-events:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scoped watch channel to close")
	}
}

func TestScope_ScopedStore_Good_GetAll(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore, _ := NewScoped(storeInstance, "tenant-a")
	betaStore, _ := NewScoped(storeInstance, "tenant-b")

	require.NoError(t, alphaStore.SetIn("items", "x", "1"))
	require.NoError(t, alphaStore.SetIn("items", "y", "2"))
	require.NoError(t, betaStore.SetIn("items", "z", "3"))

	all, err := alphaStore.GetAll("items")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"x": "1", "y": "2"}, all)

	betaEntries, err := betaStore.GetAll("items")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"z": "3"}, betaEntries)
}

func TestScope_ScopedStore_Good_GetPage(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("items", "charlie", "3"))
	require.NoError(t, scopedStore.SetIn("items", "alpha", "1"))
	require.NoError(t, scopedStore.SetIn("items", "bravo", "2"))

	page, err := scopedStore.GetPage("items", 1, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, KeyValue{Key: "bravo", Value: "2"}, page[0])
}

func TestScope_ScopedStore_Good_All(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("items", "first", "1"))
	require.NoError(t, scopedStore.SetIn("items", "second", "2"))

	var keys []string
	for entry, err := range scopedStore.All("items") {
		require.NoError(t, err)
		keys = append(keys, entry.Key)
	}

	assert.ElementsMatch(t, []string{"first", "second"}, keys)
}

func TestScope_ScopedStore_Good_All_SortedByKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("items", "charlie", "3"))
	require.NoError(t, scopedStore.SetIn("items", "alpha", "1"))
	require.NoError(t, scopedStore.SetIn("items", "bravo", "2"))

	var keys []string
	for entry, err := range scopedStore.All("items") {
		require.NoError(t, err)
		keys = append(keys, entry.Key)
	}

	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, keys)
}

func TestScope_ScopedStore_Good_Count(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("g", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g", "b", "2"))

	count, err := scopedStore.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestScope_ScopedStore_Good_SetWithTTL(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetWithTTL("g", "k", "v", time.Hour))

	value, err := scopedStore.GetFrom("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)
}

func TestScope_ScopedStore_Good_SetWithTTL_Expires(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetWithTTL("g", "k", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	_, err := scopedStore.GetFrom("g", "k")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_Render(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("user", "name", "Alice"))

	renderedTemplate, err := scopedStore.Render("Hello {{ .name }}", "user")
	require.NoError(t, err)
	assert.Equal(t, "Hello Alice", renderedTemplate)
}

func TestScope_ScopedStore_Good_BulkHelpers(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore, _ := NewScoped(storeInstance, "tenant-a")
	betaStore, _ := NewScoped(storeInstance, "tenant-b")

	require.NoError(t, alphaStore.SetIn("config", "colour", "blue"))
	require.NoError(t, alphaStore.SetIn("sessions", "token", "abc123"))
	require.NoError(t, betaStore.SetIn("config", "colour", "red"))

	count, err := alphaStore.CountAll("")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	count, err = alphaStore.CountAll("config")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	groupNames, err := alphaStore.Groups("")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"config", "sessions"}, groupNames)

	groupNames, err = alphaStore.Groups("conf")
	require.NoError(t, err)
	assert.Equal(t, []string{"config"}, groupNames)

	var streamedGroupNames []string
	for groupName, iterationErr := range alphaStore.GroupsSeq("") {
		require.NoError(t, iterationErr)
		streamedGroupNames = append(streamedGroupNames, groupName)
	}
	assert.ElementsMatch(t, []string{"config", "sessions"}, streamedGroupNames)

	var filteredGroupNames []string
	for groupName, iterationErr := range alphaStore.GroupsSeq("config") {
		require.NoError(t, iterationErr)
		filteredGroupNames = append(filteredGroupNames, groupName)
	}
	assert.Equal(t, []string{"config"}, filteredGroupNames)
}

func TestScope_ScopedStore_Good_GroupsSeqStopsEarly(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("alpha", "a", "1"))
	require.NoError(t, scopedStore.SetIn("beta", "b", "2"))

	groups := scopedStore.GroupsSeq("")
	var seen []string
	for groupName, iterationErr := range groups {
		require.NoError(t, iterationErr)
		seen = append(seen, groupName)
		break
	}

	assert.Len(t, seen, 1)
}

func TestScope_ScopedStore_Good_GroupsSeqSorted(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("charlie", "c", "3"))
	require.NoError(t, scopedStore.SetIn("alpha", "a", "1"))
	require.NoError(t, scopedStore.SetIn("bravo", "b", "2"))

	var groupNames []string
	for groupName, iterationErr := range scopedStore.GroupsSeq("") {
		require.NoError(t, iterationErr)
		groupNames = append(groupNames, groupName)
	}

	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, groupNames)
}

func TestScope_ScopedStore_Good_GetSplitAndGetFields(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("config", "hosts", "alpha,beta,gamma"))
	require.NoError(t, scopedStore.SetIn("config", "flags", "one two\tthree\n"))

	parts, err := scopedStore.GetSplit("config", "hosts", ",")
	require.NoError(t, err)

	var splitValues []string
	for value := range parts {
		splitValues = append(splitValues, value)
	}
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, splitValues)

	fields, err := scopedStore.GetFields("config", "flags")
	require.NoError(t, err)

	var fieldValues []string
	for value := range fields {
		fieldValues = append(fieldValues, value)
	}
	assert.Equal(t, []string{"one", "two", "three"}, fieldValues)
}

func TestScope_ScopedStore_Good_PurgeExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetWithTTL("session", "token", "abc123", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	removedRows, err := scopedStore.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(1), removedRows)

	_, err = scopedStore.GetFrom("session", "token")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_PurgeExpired_NamespaceLocal(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore, _ := NewScoped(storeInstance, "tenant-a")
	betaStore, _ := NewScoped(storeInstance, "tenant-b")

	require.NoError(t, alphaStore.SetWithTTL("session", "alpha-token", "alpha", 1*time.Millisecond))
	require.NoError(t, betaStore.SetWithTTL("session", "beta-token", "beta", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	assert.Equal(t, 1, rawEntryCount(t, storeInstance, "tenant-a:session"))
	assert.Equal(t, 1, rawEntryCount(t, storeInstance, "tenant-b:session"))

	removedRows, err := alphaStore.PurgeExpired()
	require.NoError(t, err)
	assert.Equal(t, int64(1), removedRows)

	assert.Equal(t, 0, rawEntryCount(t, storeInstance, "tenant-a:session"))
	assert.Equal(t, 1, rawEntryCount(t, storeInstance, "tenant-b:session"))
}

// ---------------------------------------------------------------------------
// Quota enforcement — MaxKeys
// ---------------------------------------------------------------------------

func TestScope_Quota_Good_MaxKeys(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 5},
	})
	require.NoError(t, err)

	// Insert 5 keys across different groups — should be fine.
	for i := range 5 {
		require.NoError(t, scopedStore.SetIn("g", keyName(i), "v"))
	}

	// 6th key should fail.
	err = scopedStore.SetIn("g", "overflow", "v")
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError), "expected QuotaExceededError, got: %v", err)
}

func TestScope_Quota_Bad_QuotaCheckQueryError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{})
	defer database.Close()

	storeInstance := &Store{
		sqliteDatabase: database,
		cancelPurge:    func() {},
	}

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})
	require.NoError(t, err)

	err = scopedStore.SetIn("config", "theme", "dark")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quota check")
}

func TestScope_Quota_Good_MaxKeys_AcrossGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 3},
	})

	require.NoError(t, scopedStore.SetIn("g1", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g2", "b", "2"))
	require.NoError(t, scopedStore.SetIn("g3", "c", "3"))

	// Total is now 3 — any new key should fail regardless of group.
	err := scopedStore.SetIn("g4", "d", "4")
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_UpsertDoesNotCount(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 3},
	})

	require.NoError(t, scopedStore.SetIn("g", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g", "b", "2"))
	require.NoError(t, scopedStore.SetIn("g", "c", "3"))

	// Upserting existing key should succeed.
	require.NoError(t, scopedStore.SetIn("g", "a", "updated"))

	value, err := scopedStore.GetFrom("g", "a")
	require.NoError(t, err)
	assert.Equal(t, "updated", value)
}

func TestScope_Quota_Good_ExpiredUpsertDoesNotEmitDeleteEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 1},
	})

	events := storeInstance.Watch("tenant-a:g")
	defer storeInstance.Unwatch("tenant-a:g", events)

	require.NoError(t, scopedStore.SetWithTTL("g", "token", "old", 1*time.Millisecond))
	select {
	case event := <-events:
		assert.Equal(t, EventSet, event.Type)
		assert.Equal(t, "old", event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial set event")
	}
	time.Sleep(5 * time.Millisecond)

	require.NoError(t, scopedStore.SetIn("g", "token", "new"))

	select {
	case event := <-events:
		assert.Equal(t, EventSet, event.Type)
		assert.Equal(t, "new", event.Value)
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
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 3},
	})

	require.NoError(t, scopedStore.SetIn("g", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g", "b", "2"))
	require.NoError(t, scopedStore.SetIn("g", "c", "3"))

	// Delete one key, then insert a new one — should work.
	require.NoError(t, scopedStore.Delete("g", "c"))
	require.NoError(t, scopedStore.SetIn("g", "d", "4"))
}

func TestScope_Quota_Good_ZeroMeansUnlimited(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 0, MaxGroups: 0},
	})

	// Should be able to insert many keys and groups without error.
	for i := range 100 {
		require.NoError(t, scopedStore.SetIn("g", keyName(i), "v"))
	}
}

func TestScope_Quota_Good_ExpiredKeysExcluded(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 3},
	})

	// Insert 3 keys, 2 with short TTL.
	require.NoError(t, scopedStore.SetWithTTL("g", "temp1", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.SetWithTTL("g", "temp2", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.SetIn("g", "permanent", "v"))

	time.Sleep(5 * time.Millisecond)

	// After expiry, only 1 key counts — should be able to insert 2 more.
	require.NoError(t, scopedStore.SetIn("g", "new1", "v"))
	require.NoError(t, scopedStore.SetIn("g", "new2", "v"))

	// Now at 3 — next should fail.
	err := scopedStore.SetIn("g", "new3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_SetWithTTL_Enforced(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 2},
	})

	require.NoError(t, scopedStore.SetWithTTL("g", "a", "1", time.Hour))
	require.NoError(t, scopedStore.SetWithTTL("g", "b", "2", time.Hour))

	err := scopedStore.SetWithTTL("g", "c", "3", time.Hour)
	assert.True(t, core.Is(err, QuotaExceededError))
}

// ---------------------------------------------------------------------------
// Quota enforcement — MaxGroups
// ---------------------------------------------------------------------------

func TestScope_Quota_Good_MaxGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 3},
	})

	require.NoError(t, scopedStore.SetIn("g1", "k", "v"))
	require.NoError(t, scopedStore.SetIn("g2", "k", "v"))
	require.NoError(t, scopedStore.SetIn("g3", "k", "v"))

	// 4th group should fail.
	err := scopedStore.SetIn("g4", "k", "v")
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_MaxGroups_ExistingGroupOK(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 2},
	})

	require.NoError(t, scopedStore.SetIn("g1", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g2", "b", "2"))

	// Adding more keys to existing groups should be fine.
	require.NoError(t, scopedStore.SetIn("g1", "c", "3"))
	require.NoError(t, scopedStore.SetIn("g2", "d", "4"))
}

func TestScope_Quota_Good_MaxGroups_DeleteAndRecreate(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 2},
	})

	require.NoError(t, scopedStore.SetIn("g1", "k", "v"))
	require.NoError(t, scopedStore.SetIn("g2", "k", "v"))

	// Delete a group, then create a new one.
	require.NoError(t, scopedStore.DeleteGroup("g1"))
	require.NoError(t, scopedStore.SetIn("g3", "k", "v"))
}

func TestScope_Quota_Good_MaxGroups_ZeroUnlimited(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 0},
	})

	for i := range 50 {
		require.NoError(t, scopedStore.SetIn(keyName(i), "k", "v"))
	}
}

func TestScope_Quota_Good_MaxGroups_ExpiredGroupExcluded(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxGroups: 2},
	})

	// Create 2 groups, one with only TTL keys.
	require.NoError(t, scopedStore.SetWithTTL("g1", "k", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.SetIn("g2", "k", "v"))

	time.Sleep(5 * time.Millisecond)

	// g1's only key has expired, so group count should be 1 — we can create a new one.
	require.NoError(t, scopedStore.SetIn("g3", "k", "v"))
}

func TestScope_Quota_Good_BothLimits(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 10, MaxGroups: 2},
	})

	require.NoError(t, scopedStore.SetIn("g1", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g2", "b", "2"))

	// Group limit hit.
	err := scopedStore.SetIn("g3", "c", "3")
	assert.True(t, core.Is(err, QuotaExceededError))

	// But adding to existing groups is fine (within key limit).
	require.NoError(t, scopedStore.SetIn("g1", "d", "4"))
}

func TestScope_Quota_Good_DoesNotAffectOtherNamespaces(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 2},
	})
	betaStore, _ := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-b",
		Quota:     QuotaConfig{MaxKeys: 2},
	})

	require.NoError(t, alphaStore.SetIn("g", "a1", "v"))
	require.NoError(t, alphaStore.SetIn("g", "a2", "v"))
	require.NoError(t, betaStore.SetIn("g", "b1", "v"))
	require.NoError(t, betaStore.SetIn("g", "b2", "v"))

	// alphaStore is at limit — but betaStore's keys don't count against alphaStore.
	err := alphaStore.SetIn("g", "a3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))

	// betaStore is also at limit independently.
	err = betaStore.SetIn("g", "b3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))
}

// ---------------------------------------------------------------------------
// CountAll
// ---------------------------------------------------------------------------

func TestScope_CountAll_Good_WithPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("ns-a:g1", "k1", "v"))
	require.NoError(t, storeInstance.Set("ns-a:g1", "k2", "v"))
	require.NoError(t, storeInstance.Set("ns-a:g2", "k1", "v"))
	require.NoError(t, storeInstance.Set("ns-b:g1", "k1", "v"))

	count, err := storeInstance.CountAll("ns-a:")
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	count, err = storeInstance.CountAll("ns-b:")
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestScope_CountAll_Good_WithPrefix_Wildcards(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	// Add keys in groups that look like wildcards.
	require.NoError(t, storeInstance.Set("user_1", "k", "v"))
	require.NoError(t, storeInstance.Set("user_2", "k", "v"))
	require.NoError(t, storeInstance.Set("user%test", "k", "v"))
	require.NoError(t, storeInstance.Set("user_test", "k", "v"))

	// Prefix "user_" should ONLY match groups starting with "user_".
	// Since we escape "_", it matches literal "_".
	// Groups: "user_1", "user_2", "user_test" (3 total).
	// "user%test" is NOT matched because "_" is literal.
	count, err := storeInstance.CountAll("user_")
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Prefix "user%" should ONLY match "user%test".
	count, err = storeInstance.CountAll("user%")
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestScope_CountAll_Good_EmptyPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g1", "k1", "v"))
	require.NoError(t, storeInstance.Set("g2", "k2", "v"))

	count, err := storeInstance.CountAll("")
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestScope_CountAll_Good_ExcludesExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("ns:g", "permanent", "v"))
	require.NoError(t, storeInstance.SetWithTTL("ns:g", "temp", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	count, err := storeInstance.CountAll("ns:")
	require.NoError(t, err)
	assert.Equal(t, 1, count, "expired keys should not be counted")
}

func TestScope_CountAll_Good_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	count, err := storeInstance.CountAll("nonexistent:")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestScope_CountAll_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	_, err := storeInstance.CountAll("")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

func TestScope_Groups_Good_WithPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("ns-a:g1", "k", "v"))
	require.NoError(t, storeInstance.Set("ns-a:g2", "k", "v"))
	require.NoError(t, storeInstance.Set("ns-a:g2", "k2", "v")) // duplicate group
	require.NoError(t, storeInstance.Set("ns-b:g1", "k", "v"))

	groups, err := storeInstance.Groups("ns-a:")
	require.NoError(t, err)
	assert.Len(t, groups, 2)
	assert.Contains(t, groups, "ns-a:g1")
	assert.Contains(t, groups, "ns-a:g2")
}

func TestScope_Groups_Good_EmptyPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g1", "k", "v"))
	require.NoError(t, storeInstance.Set("g2", "k", "v"))
	require.NoError(t, storeInstance.Set("g3", "k", "v"))

	groups, err := storeInstance.Groups("")
	require.NoError(t, err)
	assert.Len(t, groups, 3)
}

func TestScope_Groups_Good_Distinct(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	// Multiple keys in the same group should produce one entry.
	require.NoError(t, storeInstance.Set("g1", "a", "v"))
	require.NoError(t, storeInstance.Set("g1", "b", "v"))
	require.NoError(t, storeInstance.Set("g1", "c", "v"))

	groups, err := storeInstance.Groups("")
	require.NoError(t, err)
	assert.Len(t, groups, 1)
	assert.Equal(t, "g1", groups[0])
}

func TestScope_Groups_Good_ExcludesExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("ns:g1", "permanent", "v"))
	require.NoError(t, storeInstance.SetWithTTL("ns:g2", "temp", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	groups, err := storeInstance.Groups("ns:")
	require.NoError(t, err)
	assert.Len(t, groups, 1, "group with only expired keys should be excluded")
	assert.Equal(t, "ns:g1", groups[0])
}

func TestScope_Groups_Good_SortedByGroupName(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("charlie", "c", "3"))
	require.NoError(t, storeInstance.Set("alpha", "a", "1"))
	require.NoError(t, storeInstance.Set("bravo", "b", "2"))

	groups, err := storeInstance.Groups("")
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, groups)
}

func TestScope_Groups_Good_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	groups, err := storeInstance.Groups("nonexistent:")
	require.NoError(t, err)
	assert.Empty(t, groups)
}

func TestScope_Groups_Bad_ClosedStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	storeInstance.Close()

	_, err := storeInstance.Groups("")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func keyName(i int) string {
	return "key-" + string(rune('a'+i%26))
}

func rawEntryCount(t *testing.T, storeInstance *Store, group string) int {
	t.Helper()

	var count int
	err := storeInstance.sqliteDatabase.QueryRow(
		"SELECT COUNT(*) FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ?",
		group,
	).Scan(&count)
	require.NoError(t, err)
	return count
}
