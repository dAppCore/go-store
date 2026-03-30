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
	s, _ := New(":memory:")
	defer s.Close()

	sc, err := NewScoped(s, "tenant-1")
	require.NoError(t, err)
	require.NotNil(t, sc)
	assert.Equal(t, "tenant-1", sc.Namespace())
}

func TestScope_NewScoped_Good_AlphanumericHyphens(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	valid := []string{"abc", "ABC", "123", "a-b-c", "tenant-42", "A1-B2"}
	for _, ns := range valid {
		sc, err := NewScoped(s, ns)
		require.NoError(t, err, "namespace %q should be valid", ns)
		require.NotNil(t, sc)
	}
}

func TestScope_NewScoped_Bad_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	_, err := NewScoped(s, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid")
}

func TestScope_NewScoped_Bad_InvalidChars(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	invalid := []string{"foo.bar", "foo:bar", "foo bar", "foo/bar", "foo_bar", "tenant!", "@ns"}
	for _, ns := range invalid {
		_, err := NewScoped(s, ns)
		require.Error(t, err, "namespace %q should be invalid", ns)
	}
}

// ---------------------------------------------------------------------------
// ScopedStore — basic CRUD
// ---------------------------------------------------------------------------

func TestScope_ScopedStore_Good_SetGet(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScoped(s, "tenant-a")
	require.NoError(t, sc.Set("config", "theme", "dark"))

	val, err := sc.Get("config", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", val)
}

func TestScope_ScopedStore_Good_PrefixedInUnderlyingStore(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScoped(s, "tenant-a")
	require.NoError(t, sc.Set("config", "key", "val"))

	// The underlying store should have the prefixed group name.
	val, err := s.Get("tenant-a:config", "key")
	require.NoError(t, err)
	assert.Equal(t, "val", val)

	// Direct access without prefix should fail.
	_, err = s.Get("config", "key")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_NamespaceIsolation(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	a, _ := NewScoped(s, "tenant-a")
	b, _ := NewScoped(s, "tenant-b")

	require.NoError(t, a.Set("config", "colour", "blue"))
	require.NoError(t, b.Set("config", "colour", "red"))

	va, err := a.Get("config", "colour")
	require.NoError(t, err)
	assert.Equal(t, "blue", va)

	vb, err := b.Get("config", "colour")
	require.NoError(t, err)
	assert.Equal(t, "red", vb)
}

func TestScope_ScopedStore_Good_Delete(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScoped(s, "tenant-a")
	require.NoError(t, sc.Set("g", "k", "v"))
	require.NoError(t, sc.Delete("g", "k"))

	_, err := sc.Get("g", "k")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_DeleteGroup(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScoped(s, "tenant-a")
	require.NoError(t, sc.Set("g", "a", "1"))
	require.NoError(t, sc.Set("g", "b", "2"))
	require.NoError(t, sc.DeleteGroup("g"))

	n, err := sc.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestScope_ScopedStore_Good_GetAll(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	a, _ := NewScoped(s, "tenant-a")
	b, _ := NewScoped(s, "tenant-b")

	require.NoError(t, a.Set("items", "x", "1"))
	require.NoError(t, a.Set("items", "y", "2"))
	require.NoError(t, b.Set("items", "z", "3"))

	all, err := a.GetAll("items")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"x": "1", "y": "2"}, all)

	allB, err := b.GetAll("items")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"z": "3"}, allB)
}

func TestScope_ScopedStore_Good_Count(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScoped(s, "tenant-a")
	require.NoError(t, sc.Set("g", "a", "1"))
	require.NoError(t, sc.Set("g", "b", "2"))

	n, err := sc.Count("g")
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestScope_ScopedStore_Good_SetWithTTL(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScoped(s, "tenant-a")
	require.NoError(t, sc.SetWithTTL("g", "k", "v", time.Hour))

	val, err := sc.Get("g", "k")
	require.NoError(t, err)
	assert.Equal(t, "v", val)
}

func TestScope_ScopedStore_Good_SetWithTTL_Expires(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScoped(s, "tenant-a")
	require.NoError(t, sc.SetWithTTL("g", "k", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	_, err := sc.Get("g", "k")
	assert.True(t, core.Is(err, NotFoundError))
}

func TestScope_ScopedStore_Good_Render(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScoped(s, "tenant-a")
	require.NoError(t, sc.Set("user", "name", "Alice"))

	out, err := sc.Render("Hello {{ .name }}", "user")
	require.NoError(t, err)
	assert.Equal(t, "Hello Alice", out)
}

// ---------------------------------------------------------------------------
// Quota enforcement — MaxKeys
// ---------------------------------------------------------------------------

func TestScope_Quota_Good_MaxKeys(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, err := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 5})
	require.NoError(t, err)

	// Insert 5 keys across different groups — should be fine.
	for i := range 5 {
		require.NoError(t, sc.Set("g", keyName(i), "v"))
	}

	// 6th key should fail.
	err = sc.Set("g", "overflow", "v")
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError), "expected QuotaExceededError, got: %v", err)
}

func TestScope_Quota_Good_MaxKeys_AcrossGroups(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, sc.Set("g1", "a", "1"))
	require.NoError(t, sc.Set("g2", "b", "2"))
	require.NoError(t, sc.Set("g3", "c", "3"))

	// Total is now 3 — any new key should fail regardless of group.
	err := sc.Set("g4", "d", "4")
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_UpsertDoesNotCount(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, sc.Set("g", "a", "1"))
	require.NoError(t, sc.Set("g", "b", "2"))
	require.NoError(t, sc.Set("g", "c", "3"))

	// Upserting existing key should succeed.
	require.NoError(t, sc.Set("g", "a", "updated"))

	val, err := sc.Get("g", "a")
	require.NoError(t, err)
	assert.Equal(t, "updated", val)
}

func TestScope_Quota_Good_DeleteAndReInsert(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 3})

	require.NoError(t, sc.Set("g", "a", "1"))
	require.NoError(t, sc.Set("g", "b", "2"))
	require.NoError(t, sc.Set("g", "c", "3"))

	// Delete one key, then insert a new one — should work.
	require.NoError(t, sc.Delete("g", "c"))
	require.NoError(t, sc.Set("g", "d", "4"))
}

func TestScope_Quota_Good_ZeroMeansUnlimited(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 0, MaxGroups: 0})

	// Should be able to insert many keys and groups without error.
	for i := range 100 {
		require.NoError(t, sc.Set("g", keyName(i), "v"))
	}
}

func TestScope_Quota_Good_ExpiredKeysExcluded(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 3})

	// Insert 3 keys, 2 with short TTL.
	require.NoError(t, sc.SetWithTTL("g", "temp1", "v", 1*time.Millisecond))
	require.NoError(t, sc.SetWithTTL("g", "temp2", "v", 1*time.Millisecond))
	require.NoError(t, sc.Set("g", "permanent", "v"))

	time.Sleep(5 * time.Millisecond)

	// After expiry, only 1 key counts — should be able to insert 2 more.
	require.NoError(t, sc.Set("g", "new1", "v"))
	require.NoError(t, sc.Set("g", "new2", "v"))

	// Now at 3 — next should fail.
	err := sc.Set("g", "new3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_SetWithTTL_Enforced(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 2})

	require.NoError(t, sc.SetWithTTL("g", "a", "1", time.Hour))
	require.NoError(t, sc.SetWithTTL("g", "b", "2", time.Hour))

	err := sc.SetWithTTL("g", "c", "3", time.Hour)
	assert.True(t, core.Is(err, QuotaExceededError))
}

// ---------------------------------------------------------------------------
// Quota enforcement — MaxGroups
// ---------------------------------------------------------------------------

func TestScope_Quota_Good_MaxGroups(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxGroups: 3})

	require.NoError(t, sc.Set("g1", "k", "v"))
	require.NoError(t, sc.Set("g2", "k", "v"))
	require.NoError(t, sc.Set("g3", "k", "v"))

	// 4th group should fail.
	err := sc.Set("g4", "k", "v")
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError))
}

func TestScope_Quota_Good_MaxGroups_ExistingGroupOK(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxGroups: 2})

	require.NoError(t, sc.Set("g1", "a", "1"))
	require.NoError(t, sc.Set("g2", "b", "2"))

	// Adding more keys to existing groups should be fine.
	require.NoError(t, sc.Set("g1", "c", "3"))
	require.NoError(t, sc.Set("g2", "d", "4"))
}

func TestScope_Quota_Good_MaxGroups_DeleteAndRecreate(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxGroups: 2})

	require.NoError(t, sc.Set("g1", "k", "v"))
	require.NoError(t, sc.Set("g2", "k", "v"))

	// Delete a group, then create a new one.
	require.NoError(t, sc.DeleteGroup("g1"))
	require.NoError(t, sc.Set("g3", "k", "v"))
}

func TestScope_Quota_Good_MaxGroups_ZeroUnlimited(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxGroups: 0})

	for i := range 50 {
		require.NoError(t, sc.Set(keyName(i), "k", "v"))
	}
}

func TestScope_Quota_Good_MaxGroups_ExpiredGroupExcluded(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxGroups: 2})

	// Create 2 groups, one with only TTL keys.
	require.NoError(t, sc.SetWithTTL("g1", "k", "v", 1*time.Millisecond))
	require.NoError(t, sc.Set("g2", "k", "v"))

	time.Sleep(5 * time.Millisecond)

	// g1's only key has expired, so group count should be 1 — we can create a new one.
	require.NoError(t, sc.Set("g3", "k", "v"))
}

func TestScope_Quota_Good_BothLimits(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 10, MaxGroups: 2})

	require.NoError(t, sc.Set("g1", "a", "1"))
	require.NoError(t, sc.Set("g2", "b", "2"))

	// Group limit hit.
	err := sc.Set("g3", "c", "3")
	assert.True(t, core.Is(err, QuotaExceededError))

	// But adding to existing groups is fine (within key limit).
	require.NoError(t, sc.Set("g1", "d", "4"))
}

func TestScope_Quota_Good_DoesNotAffectOtherNamespaces(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	a, _ := NewScopedWithQuota(s, "tenant-a", QuotaConfig{MaxKeys: 2})
	b, _ := NewScopedWithQuota(s, "tenant-b", QuotaConfig{MaxKeys: 2})

	require.NoError(t, a.Set("g", "a1", "v"))
	require.NoError(t, a.Set("g", "a2", "v"))
	require.NoError(t, b.Set("g", "b1", "v"))
	require.NoError(t, b.Set("g", "b2", "v"))

	// a is at limit — but b's keys don't count against a.
	err := a.Set("g", "a3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))

	// b is also at limit independently.
	err = b.Set("g", "b3", "v")
	assert.True(t, core.Is(err, QuotaExceededError))
}

// ---------------------------------------------------------------------------
// CountAll
// ---------------------------------------------------------------------------

func TestScope_CountAll_Good_WithPrefix(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("ns-a:g1", "k1", "v"))
	require.NoError(t, s.Set("ns-a:g1", "k2", "v"))
	require.NoError(t, s.Set("ns-a:g2", "k1", "v"))
	require.NoError(t, s.Set("ns-b:g1", "k1", "v"))

	n, err := s.CountAll("ns-a:")
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = s.CountAll("ns-b:")
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestScope_CountAll_Good_WithPrefix_Wildcards(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// Add keys in groups that look like wildcards.
	require.NoError(t, s.Set("user_1", "k", "v"))
	require.NoError(t, s.Set("user_2", "k", "v"))
	require.NoError(t, s.Set("user%test", "k", "v"))
	require.NoError(t, s.Set("user_test", "k", "v"))

	// Prefix "user_" should ONLY match groups starting with "user_".
	// Since we escape "_", it matches literal "_".
	// Groups: "user_1", "user_2", "user_test" (3 total).
	// "user%test" is NOT matched because "_" is literal.
	n, err := s.CountAll("user_")
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	// Prefix "user%" should ONLY match "user%test".
	n, err = s.CountAll("user%")
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestScope_CountAll_Good_EmptyPrefix(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g1", "k1", "v"))
	require.NoError(t, s.Set("g2", "k2", "v"))

	n, err := s.CountAll("")
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestScope_CountAll_Good_ExcludesExpired(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("ns:g", "permanent", "v"))
	require.NoError(t, s.SetWithTTL("ns:g", "temp", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	n, err := s.CountAll("ns:")
	require.NoError(t, err)
	assert.Equal(t, 1, n, "expired keys should not be counted")
}

func TestScope_CountAll_Good_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	n, err := s.CountAll("nonexistent:")
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestScope_CountAll_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.CountAll("")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

func TestScope_Groups_Good_WithPrefix(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("ns-a:g1", "k", "v"))
	require.NoError(t, s.Set("ns-a:g2", "k", "v"))
	require.NoError(t, s.Set("ns-a:g2", "k2", "v")) // duplicate group
	require.NoError(t, s.Set("ns-b:g1", "k", "v"))

	groups, err := s.Groups("ns-a:")
	require.NoError(t, err)
	assert.Len(t, groups, 2)
	assert.Contains(t, groups, "ns-a:g1")
	assert.Contains(t, groups, "ns-a:g2")
}

func TestScope_Groups_Good_EmptyPrefix(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("g1", "k", "v"))
	require.NoError(t, s.Set("g2", "k", "v"))
	require.NoError(t, s.Set("g3", "k", "v"))

	groups, err := s.Groups("")
	require.NoError(t, err)
	assert.Len(t, groups, 3)
}

func TestScope_Groups_Good_Distinct(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// Multiple keys in the same group should produce one entry.
	require.NoError(t, s.Set("g1", "a", "v"))
	require.NoError(t, s.Set("g1", "b", "v"))
	require.NoError(t, s.Set("g1", "c", "v"))

	groups, err := s.Groups("")
	require.NoError(t, err)
	assert.Len(t, groups, 1)
	assert.Equal(t, "g1", groups[0])
}

func TestScope_Groups_Good_ExcludesExpired(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	require.NoError(t, s.Set("ns:g1", "permanent", "v"))
	require.NoError(t, s.SetWithTTL("ns:g2", "temp", "v", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	groups, err := s.Groups("ns:")
	require.NoError(t, err)
	assert.Len(t, groups, 1, "group with only expired keys should be excluded")
	assert.Equal(t, "ns:g1", groups[0])
}

func TestScope_Groups_Good_Empty(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	groups, err := s.Groups("nonexistent:")
	require.NoError(t, err)
	assert.Empty(t, groups)
}

func TestScope_Groups_Bad_ClosedStore(t *testing.T) {
	s, _ := New(":memory:")
	s.Close()

	_, err := s.Groups("")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func keyName(i int) string {
	return "key-" + string(rune('a'+i%26))
}
