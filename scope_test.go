package store

import (
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustScoped(t *testing.T, storeInstance *Store, namespace string) *ScopedStore {
	t.Helper()

	scopedStore := NewScoped(storeInstance, namespace)
	require.NotNil(t, scopedStore)
	return scopedStore
}

// ---------------------------------------------------------------------------
// NewScoped — constructor validation
// ---------------------------------------------------------------------------

func TestScope_NewScoped_Good(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-1")
	assert.Equal(t, "tenant-1", scopedStore.Namespace())
}

func TestScope_NewScoped_Good_AlphanumericHyphens(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	for _, namespace := range []string{"abc", "ABC", "123", "a-b-c", "tenant-42", "A1-B2"} {
		require.NotNil(t, NewScoped(storeInstance, namespace), "namespace %q should be valid", namespace)
	}
}

func TestScope_NewScoped_Bad_Empty(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	assert.Nil(t, NewScoped(storeInstance, ""))
}

func TestScope_NewScoped_Bad_NilStore(t *testing.T) {
	assert.Nil(t, NewScoped(nil, "tenant-a"))
}

func TestScope_NewScoped_Bad_InvalidChars(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	for _, namespace := range []string{"foo.bar", "foo:bar", "foo bar", "foo/bar", "foo_bar", "tenant!", "@ns"} {
		assert.Nil(t, NewScoped(storeInstance, namespace), "namespace %q should be invalid", namespace)
	}
}

func TestScope_NewScopedWithQuota_Bad_InvalidNamespace(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScopedWithQuota(storeInstance, "tenant_a", QuotaConfig{MaxKeys: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "namespace")
}

func TestScope_NewScopedWithQuota_Bad_NilStore(t *testing.T) {
	_, err := NewScopedWithQuota(nil, "tenant-a", QuotaConfig{MaxKeys: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store instance is nil")
}

func TestScope_NewScopedWithQuota_Bad_NegativeMaxKeys(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: -1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zero or positive")
}

func TestScope_NewScopedWithQuota_Bad_NegativeMaxGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: -1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zero or positive")
}

func TestScope_NewScopedWithQuota_Good_InlineQuotaFields(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 4, MaxGroups: 2})
	require.NoError(t, err)

	assert.Equal(t, 4, scopedStore.MaxKeys)
	assert.Equal(t, 2, scopedStore.MaxGroups)
}

// ---------------------------------------------------------------------------
// ScopedStore — basic CRUD
// ---------------------------------------------------------------------------

func TestScope_ScopedStore_Good_SetGet(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("config", "theme", "dark"))

	value, err := scopedStore.GetFrom("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", value)
}

func TestScope_ScopedStore_Good_DefaultGroupHelpers(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("theme", "dark"))

	value, err := scopedStore.Get("theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", value)

	rawValue, err := storeInstance.Get("tenant-a:default", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", rawValue)
}

func TestScope_ScopedStore_Good_SetInAndGetFrom(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("config", "colour", "blue"))

	value, err := scopedStore.GetFrom("config", "colour")
	require.NoError(t, err)
	assert.Equal(t, "blue", value)
}

func TestScope_ScopedStore_Good_AllSeq(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("items", "first", "1"))
	require.NoError(t, scopedStore.SetIn("items", "second", "2"))

	var keys []string
	for entry, err := range scopedStore.AllSeq("items") {
		require.NoError(t, err)
		keys = append(keys, entry.Key)
	}

	assert.ElementsMatch(t, []string{"first", "second"}, keys)
}

func TestScope_ScopedStore_Good_PrefixedInUnderlyingStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("config", "key", "val"))

	value, err := storeInstance.Get("tenant-a:config", "key")
	require.NoError(t, err)
	assert.Equal(t, "val", value)

	_, err = storeInstance.Get("config", "key")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_NamespaceIsolation(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore := mustScoped(t, storeInstance, "tenant-a")
	betaStore := mustScoped(t, storeInstance, "tenant-b")

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

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("g", "k", "v"))
	require.NoError(t, scopedStore.Delete("g", "k"))

	_, err := scopedStore.GetFrom("g", "k")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_DeleteGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
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

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("config", "colour", "blue"))
	require.NoError(t, scopedStore.SetIn("sessions", "token", "abc123"))
	require.NoError(t, storeInstance.Set("tenant-b:config", "colour", "green"))

	require.NoError(t, scopedStore.DeletePrefix(""))

	_, err := scopedStore.GetFrom("config", "colour")
	assert.Error(t, err)
	_, err = scopedStore.GetFrom("sessions", "token")
	assert.Error(t, err)

	value, err := storeInstance.Get("tenant-b:config", "colour")
	require.NoError(t, err)
	assert.Equal(t, "green", value)
}

func TestScope_ScopedStore_Good_GetAll(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore := mustScoped(t, storeInstance, "tenant-a")
	betaStore := mustScoped(t, storeInstance, "tenant-b")

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

func TestScope_ScopedStore_Good_All(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
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

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
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

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("g", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g", "b", "2"))

	count, err := scopedStore.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestScope_ScopedStore_Good_SetWithTTL(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetWithTTL("g", "k", "v", time.Hour))

	value, err := scopedStore.GetFrom("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)
}

func TestScope_ScopedStore_Good_SetWithTTL_Expires(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetWithTTL("g", "k", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	_, err := scopedStore.GetFrom("g", "k")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_Render(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetIn("user", "name", "Alice"))

	renderedTemplate, err := scopedStore.Render("Hello {{ .name }}", "user")
	require.NoError(t, err)
	assert.Equal(t, "Hello Alice", renderedTemplate)
}

func TestScope_ScopedStore_Good_BulkHelpers(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore := mustScoped(t, storeInstance, "tenant-a")
	betaStore := mustScoped(t, storeInstance, "tenant-b")

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

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
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

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
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

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
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

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
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

	alphaStore := mustScoped(t, storeInstance, "tenant-a")
	betaStore := mustScoped(t, storeInstance, "tenant-b")

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

func TestScope_ScopedStore_Good_WatchAndUnwatch(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	events := scopedStore.Watch("config")
	scopedStore.Unwatch("config", events)

	_, open := <-events
	assert.False(t, open, "channel should be closed after Unwatch")

	require.NoError(t, scopedStore.SetIn("config", "theme", "dark"))
}

func TestScope_ScopedStore_Good_WatchWildcardGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")
	events := scopedStore.Watch("*")

	require.NoError(t, scopedStore.SetIn("config", "theme", "dark"))
	require.NoError(t, storeInstance.Set("other", "theme", "light"))

	received := drainEvents(events, 1, time.Second)
	require.Len(t, received, 1)
	assert.Equal(t, "tenant-a:config", received[0].Group)
	assert.Equal(t, "theme", received[0].Key)
	assert.Equal(t, "dark", received[0].Value)

	scopedStore.Unwatch("*", events)
	_, open := <-events
	assert.False(t, open, "channel should be closed after wildcard Unwatch")
}

func TestScope_ScopedStore_Good_OnChange(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := mustScoped(t, storeInstance, "tenant-a")

	var seen []Event
	unregister := scopedStore.OnChange(func(event Event) {
		seen = append(seen, event)
	})
	defer unregister()

	require.NoError(t, scopedStore.SetIn("config", "theme", "dark"))
	require.NoError(t, storeInstance.Set("other", "key", "value"))

	require.Len(t, seen, 1)
	assert.Equal(t, "tenant-a:config", seen[0].Group)
	assert.Equal(t, "theme", seen[0].Key)
	assert.Equal(t, "dark", seen[0].Value)
}

// ---------------------------------------------------------------------------
// Quota enforcement — MaxKeys
// ---------------------------------------------------------------------------

func TestScope_Quota_Good_MaxKeys(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 5})
	require.NoError(t, err)

	for i := range 5 {
		require.NoError(t, scopedStore.SetIn("g", keyName(i), "v"))
	}

	err = scopedStore.SetIn("g", "overflow", "v")
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Bad_QuotaCheckQueryError(t *testing.T) {
	database, _ := openStubSQLiteDatabase(t, stubSQLiteScenario{})
	defer database.Close()

	storeInstance := &Store{
		database:    database,
		cancelPurge: func() {},
	}

	scopedStore, err := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 1})
	require.NoError(t, err)

	err = scopedStore.SetIn("config", "theme", "dark")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quota check")
}

func TestScope_Quota_Good_MaxKeys_AcrossGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, scopedStore.SetIn("g1", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g2", "b", "2"))
	require.NoError(t, scopedStore.SetIn("g3", "c", "3"))

	err := scopedStore.SetIn("g4", "d", "4")
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_UpsertDoesNotCount(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, scopedStore.SetIn("g", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g", "b", "2"))
	require.NoError(t, scopedStore.SetIn("g", "c", "3"))
	require.NoError(t, scopedStore.SetIn("g", "a", "updated"))

	value, err := scopedStore.GetFrom("g", "a")
	require.NoError(t, err)
	assert.Equal(t, "updated", value)
}

func TestScope_Quota_Good_DeleteAndReInsert(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, scopedStore.SetIn("g", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g", "b", "2"))
	require.NoError(t, scopedStore.SetIn("g", "c", "3"))
	require.NoError(t, scopedStore.Delete("g", "c"))
	require.NoError(t, scopedStore.SetIn("g", "d", "4"))
}

func TestScope_Quota_Good_ZeroMeansUnlimited(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 0, MaxGroups: 0})

	for i := range 100 {
		require.NoError(t, scopedStore.SetIn("g", keyName(i), "v"))
	}
}

func TestScope_Quota_Good_ExpiredKeysExcluded(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, scopedStore.SetWithTTL("g", "temp1", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.SetWithTTL("g", "temp2", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.SetIn("g", "permanent", "v"))

	time.Sleep(5 * time.Millisecond)

	require.NoError(t, scopedStore.SetIn("g", "new1", "v"))
	require.NoError(t, scopedStore.SetIn("g", "new2", "v"))

	err := scopedStore.SetIn("g", "new3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_SetWithTTL_Enforced(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 2})

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

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 3})

	require.NoError(t, scopedStore.SetIn("g1", "k", "v"))
	require.NoError(t, scopedStore.SetIn("g2", "k", "v"))
	require.NoError(t, scopedStore.SetIn("g3", "k", "v"))

	err := scopedStore.SetIn("g4", "k", "v")
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_MaxGroups_ExistingGroupOK(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 2})

	require.NoError(t, scopedStore.SetIn("g1", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g2", "b", "2"))
	require.NoError(t, scopedStore.SetIn("g1", "c", "3"))
	require.NoError(t, scopedStore.SetIn("g2", "d", "4"))
}

func TestScope_Quota_Good_MaxGroups_DeleteAndRecreate(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 2})

	require.NoError(t, scopedStore.SetIn("g1", "k", "v"))
	require.NoError(t, scopedStore.SetIn("g2", "k", "v"))
	require.NoError(t, scopedStore.DeleteGroup("g1"))
	require.NoError(t, scopedStore.SetIn("g3", "k", "v"))
}

func TestScope_Quota_Good_MaxGroups_ZeroUnlimited(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 0})

	for i := range 50 {
		require.NoError(t, scopedStore.SetIn(keyName(i), "k", "v"))
	}
}

func TestScope_Quota_Good_MaxGroups_ExpiredGroupExcluded(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 2})

	require.NoError(t, scopedStore.SetWithTTL("g1", "k", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.SetIn("g2", "k", "v"))

	time.Sleep(5 * time.Millisecond)

	require.NoError(t, scopedStore.SetIn("g3", "k", "v"))
}

func TestScope_Quota_Good_BothLimits(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 10, MaxGroups: 2})

	require.NoError(t, scopedStore.SetIn("g1", "a", "1"))
	require.NoError(t, scopedStore.SetIn("g2", "b", "2"))

	err := scopedStore.SetIn("g3", "c", "3")
	assert.True(t, core.Is(err, QuotaExceededError))

	require.NoError(t, scopedStore.SetIn("g1", "d", "4"))
}

func TestScope_Quota_Good_DoesNotAffectOtherNamespaces(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 2})
	betaStore, _ := NewScopedWithQuota(storeInstance, "tenant-b", QuotaConfig{MaxKeys: 2})

	require.NoError(t, alphaStore.SetIn("g", "a1", "v"))
	require.NoError(t, alphaStore.SetIn("g", "a2", "v"))
	require.NoError(t, betaStore.SetIn("g", "b1", "v"))
	require.NoError(t, betaStore.SetIn("g", "b2", "v"))

	err := alphaStore.SetIn("g", "a3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))

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

	require.NoError(t, storeInstance.Set("user_1", "k", "v"))
	require.NoError(t, storeInstance.Set("user_2", "k", "v"))
	require.NoError(t, storeInstance.Set("user%test", "k", "v"))
	require.NoError(t, storeInstance.Set("user_test", "k", "v"))

	count, err := storeInstance.CountAll("user_")
	require.NoError(t, err)
	assert.Equal(t, 3, count)

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

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

func TestScope_Groups_Good_WithPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("ns-a:group-1", "k", "v"))
	require.NoError(t, storeInstance.Set("ns-a:group-2", "k", "v"))
	require.NoError(t, storeInstance.Set("ns-b:group-1", "k", "v"))

	groups, err := storeInstance.Groups("ns-a:")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns-a:group-1", "ns-a:group-2"}, groups)
}

func TestScope_GroupsSeq_Good_EmptyPrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g1", "k1", "v"))
	require.NoError(t, storeInstance.Set("g2", "k2", "v"))

	var groups []string
	for groupName, err := range storeInstance.GroupsSeq("") {
		require.NoError(t, err)
		groups = append(groups, groupName)
	}
	assert.Equal(t, []string{"g1", "g2"}, groups)
}

func TestScope_GroupsSeq_Good_StopsEarly(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("g1", "k1", "v"))
	require.NoError(t, storeInstance.Set("g2", "k2", "v"))

	count := 0
	for range storeInstance.GroupsSeq("") {
		count++
		break
	}
	assert.Equal(t, 1, count)
}

func keyName(index int) string {
	return core.Sprintf("key-%02d", index)
}

func rawEntryCount(tb testing.TB, storeInstance *Store, group string) int {
	tb.Helper()

	var count int
	err := storeInstance.database.QueryRow(
		"SELECT COUNT(*) FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ?",
		group,
	).Scan(&count)
	require.NoError(tb, err)
	return count
}
