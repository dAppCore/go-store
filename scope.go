package store

import (
	"iter"
	"regexp"
	"time"

	core "dappco.re/go/core"
)

// validNamespace matches alphanumeric characters and hyphens (non-empty).
var validNamespace = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// QuotaConfig defines optional limits for a ScopedStore namespace.
// Zero values mean unlimited.
// Usage example: `quota := store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}`
type QuotaConfig struct {
	MaxKeys   int // maximum total keys across all groups in the namespace
	MaxGroups int // maximum distinct groups in the namespace
}

// ScopedStore wraps a *Store and auto-prefixes all group names with a
// namespace to prevent key collisions across tenants.
// Usage example: `scopedStore, _ := store.NewScoped(storeInstance, "tenant-a")`
type ScopedStore struct {
	store     *Store
	namespace string
	quota     QuotaConfig
}

// NewScoped creates a ScopedStore that prefixes all groups with the given
// namespace. The namespace must be non-empty and contain only alphanumeric
// characters and hyphens.
// Usage example: `scopedStore, _ := store.NewScoped(storeInstance, "tenant-a")`
func NewScoped(store *Store, namespace string) (*ScopedStore, error) {
	if !validNamespace.MatchString(namespace) {
		return nil, core.E("store.NewScoped", core.Sprintf("namespace %q is invalid (must be non-empty, alphanumeric + hyphens)", namespace), nil)
	}
	scopedStore := &ScopedStore{store: store, namespace: namespace}
	return scopedStore, nil
}

// NewScopedWithQuota creates a ScopedStore with quota enforcement. Quotas are
// checked on Set and SetWithTTL before inserting new keys or creating new
// groups.
// Usage example: `scopedStore, _ := store.NewScopedWithQuota(storeInstance, "tenant-a", quota)`
func NewScopedWithQuota(store *Store, namespace string, quota QuotaConfig) (*ScopedStore, error) {
	scopedStore, err := NewScoped(store, namespace)
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

// Namespace returns the namespace string for this scoped store.
// Usage example: `namespace := scopedStore.Namespace()`
func (scopedStore *ScopedStore) Namespace() string {
	return scopedStore.namespace
}

// Get retrieves a value by group and key within the namespace.
// Usage example: `value, err := scopedStore.Get("config", "theme")`
func (scopedStore *ScopedStore) Get(group, key string) (string, error) {
	return scopedStore.store.Get(scopedStore.namespacedGroup(group), key)
}

// Set stores a value by group and key within the namespace. If quotas are
// configured, they are checked before inserting new keys or groups.
// Usage example: `err := scopedStore.Set("config", "theme", "dark")`
func (scopedStore *ScopedStore) Set(group, key, value string) error {
	if err := scopedStore.checkQuota(group, key); err != nil {
		return err
	}
	return scopedStore.store.Set(scopedStore.namespacedGroup(group), key, value)
}

// SetWithTTL stores a value with a time-to-live within the namespace. Quota
// checks are applied for new keys and groups.
// Usage example: `err := scopedStore.SetWithTTL("sessions", "token", "abc", time.Hour)`
func (scopedStore *ScopedStore) SetWithTTL(group, key, value string, ttl time.Duration) error {
	if err := scopedStore.checkQuota(group, key); err != nil {
		return err
	}
	return scopedStore.store.SetWithTTL(scopedStore.namespacedGroup(group), key, value, ttl)
}

// Delete removes a single key from a group within the namespace.
// Usage example: `err := scopedStore.Delete("config", "theme")`
func (scopedStore *ScopedStore) Delete(group, key string) error {
	return scopedStore.store.Delete(scopedStore.namespacedGroup(group), key)
}

// DeleteGroup removes all keys in a group within the namespace.
// Usage example: `err := scopedStore.DeleteGroup("cache")`
func (scopedStore *ScopedStore) DeleteGroup(group string) error {
	return scopedStore.store.DeleteGroup(scopedStore.namespacedGroup(group))
}

// GetAll returns all non-expired key-value pairs in a group within the
// namespace.
// Usage example: `entries, err := scopedStore.GetAll("config")`
func (scopedStore *ScopedStore) GetAll(group string) (map[string]string, error) {
	return scopedStore.store.GetAll(scopedStore.namespacedGroup(group))
}

// All returns an iterator over all non-expired key-value pairs in a group
// within the namespace.
// Usage example: `for entry, err := range scopedStore.All("config") { _ = entry; _ = err }`
func (scopedStore *ScopedStore) All(group string) iter.Seq2[KeyValue, error] {
	return scopedStore.store.All(scopedStore.namespacedGroup(group))
}

// Count returns the number of non-expired keys in a group within the namespace.
// Usage example: `count, err := scopedStore.Count("config")`
func (scopedStore *ScopedStore) Count(group string) (int, error) {
	return scopedStore.store.Count(scopedStore.namespacedGroup(group))
}

// Render loads all non-expired key-value pairs from a namespaced group and
// renders a Go template.
// Usage example: `output, err := scopedStore.Render("Hello {{ .name }}", "user")`
func (scopedStore *ScopedStore) Render(templateSource, group string) (string, error) {
	return scopedStore.store.Render(templateSource, scopedStore.namespacedGroup(group))
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
	_, err := scopedStore.store.Get(namespacedGroup, key)
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
		keyCount, err := scopedStore.store.CountAll(namespacePrefix)
		if err != nil {
			return core.E("store.ScopedStore", "quota check", err)
		}
		if keyCount >= scopedStore.quota.MaxKeys {
			return core.E("store.ScopedStore", core.Sprintf("key limit (%d)", scopedStore.quota.MaxKeys), QuotaExceededError)
		}
	}

	// Check MaxGroups quota — only if this would create a new group.
	if scopedStore.quota.MaxGroups > 0 {
		existingGroupCount, err := scopedStore.store.Count(namespacedGroup)
		if err != nil {
			return core.E("store.ScopedStore", "quota check", err)
		}
		if existingGroupCount == 0 {
			// This group is new — check if adding it would exceed the group limit.
			knownGroupCount := 0
			for _, iterationErr := range scopedStore.store.GroupsSeq(namespacePrefix) {
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
