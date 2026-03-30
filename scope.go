package store

import (
	"iter"
	"regexp"
	"time"

	core "dappco.re/go/core"
)

// validNamespace matches alphanumeric characters and hyphens (non-empty).
var validNamespace = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// Zero values mean unlimited.
// Usage example: `quota := store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}`
type QuotaConfig struct {
	MaxKeys   int // maximum total keys across all groups in the namespace
	MaxGroups int // maximum distinct groups in the namespace
}

// Group names are prefixed with namespace + ":" before reaching the underlying store.
// Usage example: `scopedStore, _ := store.NewScoped(storeInstance, "tenant-a"); _ = scopedStore.Set("config", "theme", "dark")`
type ScopedStore struct {
	storeInstance *Store
	namespace     string
	quota         QuotaConfig
}

// Namespaces must be non-empty and contain only alphanumeric characters and hyphens.
// Usage example: `scopedStore, _ := store.NewScoped(storeInstance, "tenant-a")`
func NewScoped(storeInstance *Store, namespace string) (*ScopedStore, error) {
	if !validNamespace.MatchString(namespace) {
		return nil, core.E("store.NewScoped", core.Sprintf("namespace %q is invalid (must be non-empty, alphanumeric + hyphens)", namespace), nil)
	}
	scopedStore := &ScopedStore{storeInstance: storeInstance, namespace: namespace}
	return scopedStore, nil
}

// Quotas are checked before new keys or groups are created.
// Usage example: `scopedStore, _ := store.NewScopedWithQuota(storeInstance, "tenant-a", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10})`
func NewScopedWithQuota(storeInstance *Store, namespace string, quota QuotaConfig) (*ScopedStore, error) {
	scopedStore, err := NewScoped(storeInstance, namespace)
	if err != nil {
		return nil, err
	}
	scopedStore.quota = quota
	return scopedStore, nil
}

// namespacedGroup returns the group name with the namespace prefix applied.
func (scopedStore *ScopedStore) namespacedGroup(group string) string {
	return scopedStore.namespace + ":" + group
}

// Usage example: `scopedStore, _ := store.NewScoped(storeInstance, "tenant-a"); namespace := scopedStore.Namespace()`
func (scopedStore *ScopedStore) Namespace() string {
	return scopedStore.namespace
}

// Usage example: `themeValue, err := scopedStore.Get("config", "theme")`
func (scopedStore *ScopedStore) Get(group, key string) (string, error) {
	return scopedStore.storeInstance.Get(scopedStore.namespacedGroup(group), key)
}

// Quota checks happen before inserting new keys or groups.
// Usage example: `err := scopedStore.Set("config", "theme", "dark")`
func (scopedStore *ScopedStore) Set(group, key, value string) error {
	if err := scopedStore.checkQuota(group, key); err != nil {
		return err
	}
	return scopedStore.storeInstance.Set(scopedStore.namespacedGroup(group), key, value)
}

// Quota checks happen before inserting new keys or groups, even when the value expires later.
// Usage example: `err := scopedStore.SetWithTTL("sessions", "token", "abc", time.Hour)`
func (scopedStore *ScopedStore) SetWithTTL(group, key, value string, ttl time.Duration) error {
	if err := scopedStore.checkQuota(group, key); err != nil {
		return err
	}
	return scopedStore.storeInstance.SetWithTTL(scopedStore.namespacedGroup(group), key, value, ttl)
}

// Usage example: `err := scopedStore.Delete("config", "theme")`
func (scopedStore *ScopedStore) Delete(group, key string) error {
	return scopedStore.storeInstance.Delete(scopedStore.namespacedGroup(group), key)
}

// Usage example: `err := scopedStore.DeleteGroup("cache")`
func (scopedStore *ScopedStore) DeleteGroup(group string) error {
	return scopedStore.storeInstance.DeleteGroup(scopedStore.namespacedGroup(group))
}

// Usage example: `configEntries, err := scopedStore.GetAll("config")`
func (scopedStore *ScopedStore) GetAll(group string) (map[string]string, error) {
	return scopedStore.storeInstance.GetAll(scopedStore.namespacedGroup(group))
}

// Usage example: `for entry, err := range scopedStore.All("config") { if err != nil { break }; _ = entry }`
func (scopedStore *ScopedStore) All(group string) iter.Seq2[KeyValue, error] {
	return scopedStore.storeInstance.All(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.Count("config")`
func (scopedStore *ScopedStore) Count(group string) (int, error) {
	return scopedStore.storeInstance.Count(scopedStore.namespacedGroup(group))
}

// Usage example: `renderedTemplate, err := scopedStore.Render("Hello {{ .name }}", "user")`
func (scopedStore *ScopedStore) Render(templateSource, group string) (string, error) {
	return scopedStore.storeInstance.Render(templateSource, scopedStore.namespacedGroup(group))
}

// checkQuota verifies that inserting key into group would not exceed the
// namespace's quota limits. It returns nil if no quota is set or the operation
// is within bounds. Existing keys (upserts) are not counted as new.
func (scopedStore *ScopedStore) checkQuota(group, key string) error {
	if scopedStore.quota.MaxKeys == 0 && scopedStore.quota.MaxGroups == 0 {
		return nil
	}

	namespacedGroup := scopedStore.namespacedGroup(group)
	namespacePrefix := scopedStore.namespace + ":"

	// Check if this is an upsert (key already exists) — upserts never exceed quota.
	_, err := scopedStore.storeInstance.Get(namespacedGroup, key)
	if err == nil {
		// Key exists — this is an upsert, no quota check needed.
		return nil
	}
	if !core.Is(err, NotFoundError) {
		// A database error occurred, not just a "not found" result.
		return core.E("store.ScopedStore", "quota check", err)
	}

	// Check MaxKeys quota.
	if scopedStore.quota.MaxKeys > 0 {
		keyCount, err := scopedStore.storeInstance.CountAll(namespacePrefix)
		if err != nil {
			return core.E("store.ScopedStore", "quota check", err)
		}
		if keyCount >= scopedStore.quota.MaxKeys {
			return core.E("store.ScopedStore", core.Sprintf("key limit (%d)", scopedStore.quota.MaxKeys), QuotaExceededError)
		}
	}

	// Check MaxGroups quota — only if this would create a new group.
	if scopedStore.quota.MaxGroups > 0 {
		existingGroupCount, err := scopedStore.storeInstance.Count(namespacedGroup)
		if err != nil {
			return core.E("store.ScopedStore", "quota check", err)
		}
		if existingGroupCount == 0 {
			// This group is new — check if adding it would exceed the group limit.
			knownGroupCount := 0
			for _, iterationErr := range scopedStore.storeInstance.GroupsSeq(namespacePrefix) {
				if iterationErr != nil {
					return core.E("store.ScopedStore", "quota check", iterationErr)
				}
				knownGroupCount++
			}
			if knownGroupCount >= scopedStore.quota.MaxGroups {
				return core.E("store.ScopedStore", core.Sprintf("group limit (%d)", scopedStore.quota.MaxGroups), QuotaExceededError)
			}
		}
	}

	return nil
}
