package store

import (
	"iter"
	"regexp"
	"time"

	core "dappco.re/go/core"
)

// validNamespace.MatchString("tenant-a") is true; validNamespace.MatchString("tenant_a") is false.
var validNamespace = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// QuotaConfig sets per-namespace key and group limits.
// Usage example: `quota := store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}`
type QuotaConfig struct {
	// Usage example: `store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}` limits a namespace to 100 keys.
	MaxKeys int
	// Usage example: `store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}` limits a namespace to 10 groups.
	MaxGroups int
}

// ScopedStore prefixes group names with namespace + ":" before delegating to Store.
// Usage example: `scopedStore, err := store.NewScoped(storeInstance, "tenant-a"); if err != nil { return }; if err := scopedStore.Set("config", "colour", "blue"); err != nil { return }`
type ScopedStore struct {
	storeInstance *Store
	namespace     string
	quota         QuotaConfig
}

// NewScoped validates a namespace and prefixes groups with namespace + ":".
// Usage example: `scopedStore, err := store.NewScoped(storeInstance, "tenant-a"); if err != nil { return }`
func NewScoped(storeInstance *Store, namespace string) (*ScopedStore, error) {
	if storeInstance == nil {
		return nil, core.E("store.NewScoped", "store instance is nil", nil)
	}
	if !validNamespace.MatchString(namespace) {
		return nil, core.E("store.NewScoped", core.Sprintf("namespace %q is invalid; use names like %q or %q", namespace, "tenant-a", "tenant-42"), nil)
	}
	scopedStore := &ScopedStore{storeInstance: storeInstance, namespace: namespace}
	return scopedStore, nil
}

// NewScopedWithQuota adds per-namespace key and group limits.
// Usage example: `scopedStore, err := store.NewScopedWithQuota(storeInstance, "tenant-a", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}); if err != nil { return }`
func NewScopedWithQuota(storeInstance *Store, namespace string, quota QuotaConfig) (*ScopedStore, error) {
	scopedStore, err := NewScoped(storeInstance, namespace)
	if err != nil {
		return nil, err
	}
	scopedStore.quota = quota
	return scopedStore, nil
}

func (scopedStore *ScopedStore) namespacedGroup(group string) string {
	return scopedStore.namespace + ":" + group
}

func (scopedStore *ScopedStore) namespacePrefix() string {
	return scopedStore.namespace + ":"
}

func (scopedStore *ScopedStore) trimNamespacePrefix(groupName string) string {
	return core.TrimPrefix(groupName, scopedStore.namespacePrefix())
}

// Namespace returns the namespace string.
// Usage example: `scopedStore, err := store.NewScoped(storeInstance, "tenant-a"); if err != nil { return }; namespace := scopedStore.Namespace(); fmt.Println(namespace)`
func (scopedStore *ScopedStore) Namespace() string {
	return scopedStore.namespace
}

// Usage example: `colourValue, err := scopedStore.Get("config", "colour")`
func (scopedStore *ScopedStore) Get(group, key string) (string, error) {
	return scopedStore.storeInstance.Get(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.Set("config", "colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) Set(group, key, value string) error {
	if err := scopedStore.checkQuota("store.ScopedStore.Set", group, key); err != nil {
		return err
	}
	return scopedStore.storeInstance.Set(scopedStore.namespacedGroup(group), key, value)
}

// Usage example: `if err := scopedStore.SetWithTTL("sessions", "token", "abc123", time.Hour); err != nil { return }`
func (scopedStore *ScopedStore) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	if err := scopedStore.checkQuota("store.ScopedStore.SetWithTTL", group, key); err != nil {
		return err
	}
	return scopedStore.storeInstance.SetWithTTL(scopedStore.namespacedGroup(group), key, value, timeToLive)
}

// Usage example: `if err := scopedStore.Delete("config", "colour"); err != nil { return }`
func (scopedStore *ScopedStore) Delete(group, key string) error {
	return scopedStore.storeInstance.Delete(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.DeleteGroup("cache"); err != nil { return }`
func (scopedStore *ScopedStore) DeleteGroup(group string) error {
	return scopedStore.storeInstance.DeleteGroup(scopedStore.namespacedGroup(group))
}

// Usage example: `colourEntries, err := scopedStore.GetAll("config")`
func (scopedStore *ScopedStore) GetAll(group string) (map[string]string, error) {
	return scopedStore.storeInstance.GetAll(scopedStore.namespacedGroup(group))
}

// Usage example: `for entry, err := range scopedStore.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) All(group string) iter.Seq2[KeyValue, error] {
	return scopedStore.storeInstance.All(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.Count("config")`
func (scopedStore *ScopedStore) Count(group string) (int, error) {
	return scopedStore.storeInstance.Count(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.CountAll("config")`
func (scopedStore *ScopedStore) CountAll(groupPrefix string) (int, error) {
	return scopedStore.storeInstance.CountAll(scopedStore.namespacedGroup(groupPrefix))
}

// Usage example: `groupNames, err := scopedStore.Groups("config")`
func (scopedStore *ScopedStore) Groups(groupPrefix string) ([]string, error) {
	groupNames, err := scopedStore.storeInstance.Groups(scopedStore.namespacedGroup(groupPrefix))
	if err != nil {
		return nil, err
	}
	for i, groupName := range groupNames {
		groupNames[i] = scopedStore.trimNamespacePrefix(groupName)
	}
	return groupNames, nil
}

// Usage example: `for groupName, err := range scopedStore.GroupsSeq("config") { if err != nil { break }; fmt.Println(groupName) }`
func (scopedStore *ScopedStore) GroupsSeq(groupPrefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		namespacePrefix := scopedStore.namespacePrefix()
		for groupName, err := range scopedStore.storeInstance.GroupsSeq(scopedStore.namespacedGroup(groupPrefix)) {
			if err != nil {
				if !yield("", err) {
					return
				}
				continue
			}
			if !yield(core.TrimPrefix(groupName, namespacePrefix), nil) {
				return
			}
		}
	}
}

// Usage example: `renderedTemplate, err := scopedStore.Render("Hello {{ .name }}", "user")`
func (scopedStore *ScopedStore) Render(templateSource, group string) (string, error) {
	return scopedStore.storeInstance.Render(templateSource, scopedStore.namespacedGroup(group))
}

// Usage example: `parts, err := scopedStore.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (scopedStore *ScopedStore) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	return scopedStore.storeInstance.GetSplit(scopedStore.namespacedGroup(group), key, separator)
}

// Usage example: `fields, err := scopedStore.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (scopedStore *ScopedStore) GetFields(group, key string) (iter.Seq[string], error) {
	return scopedStore.storeInstance.GetFields(scopedStore.namespacedGroup(group), key)
}

// Usage example: `removedRows, err := scopedStore.PurgeExpired(); if err != nil { return }; fmt.Println(removedRows)`
func (scopedStore *ScopedStore) PurgeExpired() (int64, error) {
	return scopedStore.storeInstance.PurgeExpired()
}

// checkQuota("store.ScopedStore.Set", "config", "colour") returns nil when the
// namespace still has quota available and QuotaExceededError when a new key or
// group would exceed the configured limit. Existing keys are treated as
// upserts and do not consume quota.
func (scopedStore *ScopedStore) checkQuota(operation, group, key string) error {
	if scopedStore.quota.MaxKeys == 0 && scopedStore.quota.MaxGroups == 0 {
		return nil
	}

	namespacedGroup := scopedStore.namespacedGroup(group)
	namespacePrefix := scopedStore.namespacePrefix()

	// Check if this is an upsert (key already exists) — upserts never exceed quota.
	_, err := scopedStore.storeInstance.Get(namespacedGroup, key)
	if err == nil {
		// Key exists — this is an upsert, no quota check needed.
		return nil
	}
	if !core.Is(err, NotFoundError) {
		// A database error occurred, not just a "not found" result.
		return core.E(operation, "quota check", err)
	}

	// Check MaxKeys quota.
	if scopedStore.quota.MaxKeys > 0 {
		keyCount, err := scopedStore.storeInstance.CountAll(namespacePrefix)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if keyCount >= scopedStore.quota.MaxKeys {
			return core.E(operation, core.Sprintf("key limit (%d)", scopedStore.quota.MaxKeys), QuotaExceededError)
		}
	}

	// Check MaxGroups quota — only if this would create a new group.
	if scopedStore.quota.MaxGroups > 0 {
		existingGroupCount, err := scopedStore.storeInstance.Count(namespacedGroup)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if existingGroupCount == 0 {
			// This group is new — check if adding it would exceed the group limit.
			knownGroupCount := 0
			for _, iterationErr := range scopedStore.storeInstance.GroupsSeq(namespacePrefix) {
				if iterationErr != nil {
					return core.E(operation, "quota check", iterationErr)
				}
				knownGroupCount++
			}
			if knownGroupCount >= scopedStore.quota.MaxGroups {
				return core.E(operation, core.Sprintf("group limit (%d)", scopedStore.quota.MaxGroups), QuotaExceededError)
			}
		}
	}

	return nil
}
