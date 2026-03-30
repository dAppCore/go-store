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

func TestScope_NewScoped_Bad_InvalidChars(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	invalid := []string{"foo.bar", "foo:bar", "foo bar", "foo/bar", "foo_bar", "tenant!", "@ns"}
	for _, namespace := range invalid {
		_, err := NewScoped(storeInstance, namespace)
		require.Error(t, err, "namespace %q should be invalid", namespace)
	}
}

func TestScope_NewScopedWithQuota_Bad_InvalidNamespace(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	_, err := NewScopedWithQuota(storeInstance, "tenant_a", QuotaConfig{MaxKeys: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store.NewScoped")
}

// ---------------------------------------------------------------------------
// ScopedStore — basic CRUD
// ---------------------------------------------------------------------------

func TestScope_ScopedStore_Good_SetGet(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("config", "theme", "dark"))

	value, err := scopedStore.Get("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", value)
}

func TestScope_ScopedStore_Good_PrefixedInUnderlyingStore(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("config", "key", "val"))

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

	require.NoError(t, alphaStore.Set("config", "colour", "blue"))
	require.NoError(t, betaStore.Set("config", "colour", "red"))

	alphaValue, err := alphaStore.Get("config", "colour")
	require.NoError(t, err)
	assert.Equal(t, "blue", alphaValue)

	betaValue, err := betaStore.Get("config", "colour")
	require.NoError(t, err)
	assert.Equal(t, "red", betaValue)
}

func TestScope_ScopedStore_Good_Delete(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("g", "k", "v"))
	require.NoError(t, scopedStore.Delete("g", "k"))

	_, err := scopedStore.Get("g", "k")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_DeleteGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("g", "a", "1"))
	require.NoError(t, scopedStore.Set("g", "b", "2"))
	require.NoError(t, scopedStore.DeleteGroup("g"))

	count, err := scopedStore.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestScope_ScopedStore_Good_GetAll(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore, _ := NewScoped(storeInstance, "tenant-a")
	betaStore, _ := NewScoped(storeInstance, "tenant-b")

	require.NoError(t, alphaStore.Set("items", "x", "1"))
	require.NoError(t, alphaStore.Set("items", "y", "2"))
	require.NoError(t, betaStore.Set("items", "z", "3"))

	all, err := alphaStore.GetAll("items")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"x": "1", "y": "2"}, all)

	allB, err := betaStore.GetAll("items")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"z": "3"}, allB)
}

func TestScope_ScopedStore_Good_All(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("items", "first", "1"))
	require.NoError(t, scopedStore.Set("items", "second", "2"))

	var keys []string
	for entry, err := range scopedStore.All("items") {
		require.NoError(t, err)
		keys = append(keys, entry.Key)
	}

	assert.ElementsMatch(t, []string{"first", "second"}, keys)
}

func TestScope_ScopedStore_Good_Count(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("g", "a", "1"))
	require.NoError(t, scopedStore.Set("g", "b", "2"))

	count, err := scopedStore.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestScope_ScopedStore_Good_SetWithTTL(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetWithTTL("g", "k", "v", time.Hour))

	value, err := scopedStore.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", value)
}

func TestScope_ScopedStore_Good_SetWithTTL_Expires(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.SetWithTTL("g", "k", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	_, err := scopedStore.Get("g", "k")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_Render(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, scopedStore.Set("user", "name", "Alice"))

	renderedTemplate, err := scopedStore.Render("Hello {{ .name }}", "user")
	require.NoError(t, err)
	assert.Equal(t, "Hello Alice", renderedTemplate)
}

// ---------------------------------------------------------------------------
// Quota enforcement — MaxKeys
// ---------------------------------------------------------------------------

func TestScope_Quota_Good_MaxKeys(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 5})
	require.NoError(t, err)

	// Insert 5 keys across different groups — should be fine.
	for i := range 5 {
		require.NoError(t, scopedStore.Set("g", keyName(i), "v"))
	}

	// 6th key should fail.
	err = scopedStore.Set("g", "overflow", "v")
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError), "expected QuotaExceededError, got: %v", err)
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

	err = scopedStore.Set("config", "theme", "dark")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quota check")
}

func TestScope_Quota_Good_MaxKeys_AcrossGroups(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, scopedStore.Set("g1", "a", "1"))
	require.NoError(t, scopedStore.Set("g2", "b", "2"))
	require.NoError(t, scopedStore.Set("g3", "c", "3"))

	// Total is now 3 — any new key should fail regardless of group.
	err := scopedStore.Set("g4", "d", "4")
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_UpsertDoesNotCount(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, scopedStore.Set("g", "a", "1"))
	require.NoError(t, scopedStore.Set("g", "b", "2"))
	require.NoError(t, scopedStore.Set("g", "c", "3"))

	// Upserting existing key should succeed.
	require.NoError(t, scopedStore.Set("g", "a", "updated"))

	value, err := scopedStore.Get("g", "a")
	require.NoError(t, err)
	assert.Equal(t, "updated", value)
}

func TestScope_Quota_Good_DeleteAndReInsert(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, scopedStore.Set("g", "a", "1"))
	require.NoError(t, scopedStore.Set("g", "b", "2"))
	require.NoError(t, scopedStore.Set("g", "c", "3"))

	// Delete one key, then insert a new one — should work.
	require.NoError(t, scopedStore.Delete("g", "c"))
	require.NoError(t, scopedStore.Set("g", "d", "4"))
}

func TestScope_Quota_Good_ZeroMeansUnlimited(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 0, MaxGroups: 0})

	// Should be able to insert many keys and groups without error.
	for i := range 100 {
		require.NoError(t, scopedStore.Set("g", keyName(i), "v"))
	}
}

func TestScope_Quota_Good_ExpiredKeysExcluded(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 3})

	// Insert 3 keys, 2 with short TTL.
	require.NoError(t, scopedStore.SetWithTTL("g", "temp1", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.SetWithTTL("g", "temp2", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.Set("g", "permanent", "v"))

	time.Sleep(5 * time.Millisecond)

	// After expiry, only 1 key counts — should be able to insert 2 more.
	require.NoError(t, scopedStore.Set("g", "new1", "v"))
	require.NoError(t, scopedStore.Set("g", "new2", "v"))

	// Now at 3 — next should fail.
	err := scopedStore.Set("g", "new3", "v")
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

	require.NoError(t, scopedStore.Set("g1", "k", "v"))
	require.NoError(t, scopedStore.Set("g2", "k", "v"))
	require.NoError(t, scopedStore.Set("g3", "k", "v"))

	// 4th group should fail.
	err := scopedStore.Set("g4", "k", "v")
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_MaxGroups_ExistingGroupOK(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 2})

	require.NoError(t, scopedStore.Set("g1", "a", "1"))
	require.NoError(t, scopedStore.Set("g2", "b", "2"))

	// Adding more keys to existing groups should be fine.
	require.NoError(t, scopedStore.Set("g1", "c", "3"))
	require.NoError(t, scopedStore.Set("g2", "d", "4"))
}

func TestScope_Quota_Good_MaxGroups_DeleteAndRecreate(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 2})

	require.NoError(t, scopedStore.Set("g1", "k", "v"))
	require.NoError(t, scopedStore.Set("g2", "k", "v"))

	// Delete a group, then create a new one.
	require.NoError(t, scopedStore.DeleteGroup("g1"))
	require.NoError(t, scopedStore.Set("g3", "k", "v"))
}

func TestScope_Quota_Good_MaxGroups_ZeroUnlimited(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 0})

	for i := range 50 {
		require.NoError(t, scopedStore.Set(keyName(i), "k", "v"))
	}
}

func TestScope_Quota_Good_MaxGroups_ExpiredGroupExcluded(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxGroups: 2})

	// Create 2 groups, one with only TTL keys.
	require.NoError(t, scopedStore.SetWithTTL("g1", "k", "v", 1*time.Millisecond))
	require.NoError(t, scopedStore.Set("g2", "k", "v"))

	time.Sleep(5 * time.Millisecond)

	// g1's only key has expired, so group count should be 1 — we can create a new one.
	require.NoError(t, scopedStore.Set("g3", "k", "v"))
}

func TestScope_Quota_Good_BothLimits(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 10, MaxGroups: 2})

	require.NoError(t, scopedStore.Set("g1", "a", "1"))
	require.NoError(t, scopedStore.Set("g2", "b", "2"))

	// Group limit hit.
	err := scopedStore.Set("g3", "c", "3")
	assert.True(t, core.Is(err, QuotaExceededError))

	// But adding to existing groups is fine (within key limit).
	require.NoError(t, scopedStore.Set("g1", "d", "4"))
}

func TestScope_Quota_Good_DoesNotAffectOtherNamespaces(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	alphaStore, _ := NewScopedWithQuota(storeInstance, "tenant-a", QuotaConfig{MaxKeys: 2})
	betaStore, _ := NewScopedWithQuota(storeInstance, "tenant-b", QuotaConfig{MaxKeys: 2})

	require.NoError(t, alphaStore.Set("g", "a1", "v"))
	require.NoError(t, alphaStore.Set("g", "a2", "v"))
	require.NoError(t, betaStore.Set("g", "b1", "v"))
	require.NoError(t, betaStore.Set("g", "b2", "v"))

	// alphaStore is at limit — but betaStore's keys don't count against alphaStore.
	err := alphaStore.Set("g", "a3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))

	// betaStore is also at limit independently.
	err = betaStore.Set("g", "b3", "v")
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
