package store

import (
	"iter"
	"regexp"
	"time"

	core "dappco.re/go/core"
)

// validNamespace.MatchString("tenant-a") is true; validNamespace.MatchString("tenant_a") is false.
var validNamespace = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

const defaultScopedGroupName = "default"

// QuotaConfig sets per-namespace key and group limits.
// Usage example: `quota := store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}`
type QuotaConfig struct {
	// Usage example: `store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}` limits a namespace to 100 keys.
	MaxKeys int
	// Usage example: `store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}` limits a namespace to 10 groups.
	MaxGroups int
}

// Usage example: `if err := (store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}).Validate(); err != nil { return }`
func (quotaConfig QuotaConfig) Validate() error {
	if quotaConfig.MaxKeys < 0 || quotaConfig.MaxGroups < 0 {
		return core.E(
			"store.QuotaConfig.Validate",
			core.Sprintf("quota values must be zero or positive; got MaxKeys=%d MaxGroups=%d", quotaConfig.MaxKeys, quotaConfig.MaxGroups),
			nil,
		)
	}
	return nil
}

// ScopedStoreConfig combines namespace selection with optional quota limits.
// Usage example: `config := store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}}`
type ScopedStoreConfig struct {
	// Usage example: `config := store.ScopedStoreConfig{Namespace: "tenant-a"}`
	Namespace string
	// Usage example: `config := store.ScopedStoreConfig{Quota: store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}}`
	Quota QuotaConfig
}

// Usage example: `if err := (store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}}).Validate(); err != nil { return }`
func (scopedConfig ScopedStoreConfig) Validate() error {
	if !validNamespace.MatchString(scopedConfig.Namespace) {
		return core.E(
			"store.ScopedStoreConfig.Validate",
			core.Sprintf("namespace %q is invalid; use names like %q or %q", scopedConfig.Namespace, "tenant-a", "tenant-42"),
			nil,
		)
	}
	if err := scopedConfig.Quota.Validate(); err != nil {
		return core.E("store.ScopedStoreConfig.Validate", "quota", err)
	}
	return nil
}

// ScopedStore prefixes group names with namespace + ":" before delegating to Store.
//
// Usage example: `scopedStore, err := store.NewScoped(storeInstance, "tenant-a"); if err != nil { return }; if err := scopedStore.SetIn("config", "colour", "blue"); err != nil { return }`
type ScopedStore struct {
	storeInstance *Store
	namespace     string
	// Usage example: `scopedStore.MaxKeys = 100`
	MaxKeys int
	// Usage example: `scopedStore.MaxGroups = 10`
	MaxGroups int
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

// NewScopedConfigured validates the namespace and optional quota settings before constructing a ScopedStore.
// Usage example: `scopedStore, err := store.NewScopedConfigured(storeInstance, store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}}); if err != nil { return }`
func NewScopedConfigured(storeInstance *Store, scopedConfig ScopedStoreConfig) (*ScopedStore, error) {
	if storeInstance == nil {
		return nil, core.E("store.NewScopedConfigured", "store instance is nil", nil)
	}
	if err := scopedConfig.Validate(); err != nil {
		return nil, core.E("store.NewScopedConfigured", "validate config", err)
	}
	scopedStore, err := NewScoped(storeInstance, scopedConfig.Namespace)
	if err != nil {
		return nil, err
	}
	scopedStore.MaxKeys = scopedConfig.Quota.MaxKeys
	scopedStore.MaxGroups = scopedConfig.Quota.MaxGroups
	return scopedStore, nil
}

// NewScopedWithQuota adds per-namespace key and group limits.
// Usage example: `scopedStore, err := store.NewScopedWithQuota(storeInstance, "tenant-a", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}); if err != nil { return }`
func NewScopedWithQuota(storeInstance *Store, namespace string, quota QuotaConfig) (*ScopedStore, error) {
	return NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: namespace,
		Quota:     quota,
	})
}

func (scopedStore *ScopedStore) namespacedGroup(group string) string {
	return scopedStore.namespace + ":" + group
}

func (scopedStore *ScopedStore) namespacePrefix() string {
	return scopedStore.namespace + ":"
}

func (scopedStore *ScopedStore) defaultGroup() string {
	return defaultScopedGroupName
}

func (scopedStore *ScopedStore) trimNamespacePrefix(groupName string) string {
	return core.TrimPrefix(groupName, scopedStore.namespacePrefix())
}

// Namespace returns the namespace string.
// Usage example: `scopedStore, err := store.NewScoped(storeInstance, "tenant-a"); if err != nil { return }; namespace := scopedStore.Namespace(); fmt.Println(namespace)`
func (scopedStore *ScopedStore) Namespace() string {
	return scopedStore.namespace
}

// Usage example: `colourValue, err := scopedStore.Get("colour")`
func (scopedStore *ScopedStore) Get(key string) (string, error) {
	return scopedStore.storeInstance.Get(scopedStore.namespacedGroup(scopedStore.defaultGroup()), key)
}

// GetFrom reads a key from an explicit namespaced group.
// Usage example: `colourValue, err := scopedStore.GetFrom("config", "colour")`
func (scopedStore *ScopedStore) GetFrom(group, key string) (string, error) {
	return scopedStore.storeInstance.Get(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.Set("colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) Set(key, value string) error {
	defaultGroup := scopedStore.defaultGroup()
	if err := scopedStore.checkQuota("store.ScopedStore.Set", defaultGroup, key); err != nil {
		return err
	}
	return scopedStore.storeInstance.Set(scopedStore.namespacedGroup(defaultGroup), key, value)
}

// SetIn writes a key to an explicit namespaced group.
// Usage example: `if err := scopedStore.SetIn("config", "colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) SetIn(group, key, value string) error {
	if err := scopedStore.checkQuota("store.ScopedStore.SetIn", group, key); err != nil {
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

// Usage example: `if err := scopedStore.DeletePrefix("cache"); err != nil { return }`
// Usage example: `if err := scopedStore.DeletePrefix(""); err != nil { return }`
func (scopedStore *ScopedStore) DeletePrefix(groupPrefix string) error {
	return scopedStore.storeInstance.DeletePrefix(scopedStore.namespacedGroup(groupPrefix))
}

// Usage example: `colourEntries, err := scopedStore.GetAll("config")`
func (scopedStore *ScopedStore) GetAll(group string) (map[string]string, error) {
	return scopedStore.storeInstance.GetAll(scopedStore.namespacedGroup(group))
}

// Usage example: `page, err := scopedStore.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) GetPage(group string, offset, limit int) ([]KeyValue, error) {
	return scopedStore.storeInstance.GetPage(scopedStore.namespacedGroup(group), offset, limit)
}

// Usage example: `for entry, err := range scopedStore.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) All(group string) iter.Seq2[KeyValue, error] {
	return scopedStore.storeInstance.All(scopedStore.namespacedGroup(group))
}

// Usage example: `for entry, err := range scopedStore.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return scopedStore.All(group)
}

// Usage example: `keyCount, err := scopedStore.Count("config")`
func (scopedStore *ScopedStore) Count(group string) (int, error) {
	return scopedStore.storeInstance.Count(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.CountAll("config")`
// Usage example: `keyCount, err := scopedStore.CountAll()`
func (scopedStore *ScopedStore) CountAll(groupPrefix ...string) (int, error) {
	return scopedStore.storeInstance.CountAll(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
}

// Usage example: `groupNames, err := scopedStore.Groups("config")`
// Usage example: `groupNames, err := scopedStore.Groups()`
func (scopedStore *ScopedStore) Groups(groupPrefix ...string) ([]string, error) {
	groupNames, err := scopedStore.storeInstance.Groups(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
	if err != nil {
		return nil, err
	}
	for i, groupName := range groupNames {
		groupNames[i] = scopedStore.trimNamespacePrefix(groupName)
	}
	return groupNames, nil
}

// Usage example: `for groupName, err := range scopedStore.GroupsSeq("config") { if err != nil { break }; fmt.Println(groupName) }`
// Usage example: `for groupName, err := range scopedStore.GroupsSeq() { if err != nil { break }; fmt.Println(groupName) }`
func (scopedStore *ScopedStore) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		namespacePrefix := scopedStore.namespacePrefix()
		for groupName, err := range scopedStore.storeInstance.GroupsSeq(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix))) {
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
	if scopedStore == nil {
		return 0, core.E("store.ScopedStore.PurgeExpired", "scoped store is nil", nil)
	}
	if err := scopedStore.storeInstance.ensureReady("store.ScopedStore.PurgeExpired"); err != nil {
		return 0, err
	}

	removedRows, err := purgeExpiredMatchingGroupPrefix(scopedStore.storeInstance.sqliteDatabase, scopedStore.namespacePrefix())
	if err != nil {
		return 0, core.E("store.ScopedStore.PurgeExpired", "delete expired rows", err)
	}
	return removedRows, nil
}

// ScopedStoreTransaction exposes namespace-local transaction helpers so callers
// can work inside a scoped namespace without manually prefixing group names.
//
// Usage example: `err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error { return transaction.Set("theme", "dark") })`
type ScopedStoreTransaction struct {
	scopedStore      *ScopedStore
	storeTransaction *StoreTransaction
}

// Usage example: `err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error { return transaction.Set("theme", "dark") })`
func (scopedStore *ScopedStore) Transaction(operation func(*ScopedStoreTransaction) error) error {
	if scopedStore == nil {
		return core.E("store.ScopedStore.Transaction", "scoped store is nil", nil)
	}
	if operation == nil {
		return core.E("store.ScopedStore.Transaction", "operation is nil", nil)
	}

	return scopedStore.storeInstance.Transaction(func(storeTransaction *StoreTransaction) error {
		return operation(&ScopedStoreTransaction{
			scopedStore:      scopedStore,
			storeTransaction: storeTransaction,
		})
	})
}

func (scopedStoreTransaction *ScopedStoreTransaction) ensureReady(operation string) error {
	if scopedStoreTransaction == nil {
		return core.E(operation, "scoped transaction is nil", nil)
	}
	if scopedStoreTransaction.scopedStore == nil {
		return core.E(operation, "scoped transaction store is nil", nil)
	}
	if scopedStoreTransaction.storeTransaction == nil {
		return core.E(operation, "scoped transaction database is nil", nil)
	}
	if err := scopedStoreTransaction.scopedStore.storeInstance.ensureReady(operation); err != nil {
		return err
	}
	return scopedStoreTransaction.storeTransaction.ensureReady(operation)
}

// Usage example: `colourValue, err := scopedStoreTransaction.Get("colour")`
func (scopedStoreTransaction *ScopedStoreTransaction) Get(key string) (string, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Get"); err != nil {
		return "", err
	}
	return scopedStoreTransaction.storeTransaction.Get(
		scopedStoreTransaction.scopedStore.namespacedGroup(scopedStoreTransaction.scopedStore.defaultGroup()),
		key,
	)
}

// Usage example: `colourValue, err := scopedStoreTransaction.GetFrom("config", "colour")`
func (scopedStoreTransaction *ScopedStoreTransaction) GetFrom(group, key string) (string, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetFrom"); err != nil {
		return "", err
	}
	return scopedStoreTransaction.storeTransaction.Get(scopedStoreTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStoreTransaction.Set("theme", "dark"); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) Set(key, value string) error {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Set"); err != nil {
		return err
	}
	defaultGroup := scopedStoreTransaction.scopedStore.defaultGroup()
	if err := scopedStoreTransaction.checkQuota("store.ScopedStoreTransaction.Set", defaultGroup, key); err != nil {
		return err
	}
	return scopedStoreTransaction.storeTransaction.Set(
		scopedStoreTransaction.scopedStore.namespacedGroup(defaultGroup),
		key,
		value,
	)
}

// Usage example: `if err := scopedStoreTransaction.SetIn("config", "colour", "blue"); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) SetIn(group, key, value string) error {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.SetIn"); err != nil {
		return err
	}
	if err := scopedStoreTransaction.checkQuota("store.ScopedStoreTransaction.SetIn", group, key); err != nil {
		return err
	}
	return scopedStoreTransaction.storeTransaction.Set(scopedStoreTransaction.scopedStore.namespacedGroup(group), key, value)
}

// Usage example: `if err := scopedStoreTransaction.SetWithTTL("sessions", "token", "abc123", time.Hour); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.SetWithTTL"); err != nil {
		return err
	}
	if err := scopedStoreTransaction.checkQuota("store.ScopedStoreTransaction.SetWithTTL", group, key); err != nil {
		return err
	}
	return scopedStoreTransaction.storeTransaction.SetWithTTL(scopedStoreTransaction.scopedStore.namespacedGroup(group), key, value, timeToLive)
}

// Usage example: `if err := scopedStoreTransaction.Delete("config", "colour"); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) Delete(group, key string) error {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Delete"); err != nil {
		return err
	}
	return scopedStoreTransaction.storeTransaction.Delete(scopedStoreTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStoreTransaction.DeleteGroup("cache"); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) DeleteGroup(group string) error {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.DeleteGroup"); err != nil {
		return err
	}
	return scopedStoreTransaction.storeTransaction.DeleteGroup(scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `if err := scopedStoreTransaction.DeletePrefix("cache"); err != nil { return err }`
// Usage example: `if err := scopedStoreTransaction.DeletePrefix(""); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) DeletePrefix(groupPrefix string) error {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.DeletePrefix"); err != nil {
		return err
	}
	return scopedStoreTransaction.storeTransaction.DeletePrefix(scopedStoreTransaction.scopedStore.namespacedGroup(groupPrefix))
}

// Usage example: `colourEntries, err := scopedStoreTransaction.GetAll("config")`
func (scopedStoreTransaction *ScopedStoreTransaction) GetAll(group string) (map[string]string, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetAll"); err != nil {
		return nil, err
	}
	return scopedStoreTransaction.storeTransaction.GetAll(scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `page, err := scopedStoreTransaction.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (scopedStoreTransaction *ScopedStoreTransaction) GetPage(group string, offset, limit int) ([]KeyValue, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetPage"); err != nil {
		return nil, err
	}
	return scopedStoreTransaction.storeTransaction.GetPage(scopedStoreTransaction.scopedStore.namespacedGroup(group), offset, limit)
}

// Usage example: `for entry, err := range scopedStoreTransaction.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStoreTransaction *ScopedStoreTransaction) All(group string) iter.Seq2[KeyValue, error] {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.All"); err != nil {
		return func(yield func(KeyValue, error) bool) {
			yield(KeyValue{}, err)
		}
	}
	return scopedStoreTransaction.storeTransaction.All(scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `for entry, err := range scopedStoreTransaction.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStoreTransaction *ScopedStoreTransaction) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return scopedStoreTransaction.All(group)
}

// Usage example: `keyCount, err := scopedStoreTransaction.Count("config")`
func (scopedStoreTransaction *ScopedStoreTransaction) Count(group string) (int, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Count"); err != nil {
		return 0, err
	}
	return scopedStoreTransaction.storeTransaction.Count(scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStoreTransaction.CountAll("config")`
// Usage example: `keyCount, err := scopedStoreTransaction.CountAll()`
func (scopedStoreTransaction *ScopedStoreTransaction) CountAll(groupPrefix ...string) (int, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.CountAll"); err != nil {
		return 0, err
	}
	return scopedStoreTransaction.storeTransaction.CountAll(scopedStoreTransaction.scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
}

// Usage example: `groupNames, err := scopedStoreTransaction.Groups("config")`
// Usage example: `groupNames, err := scopedStoreTransaction.Groups()`
func (scopedStoreTransaction *ScopedStoreTransaction) Groups(groupPrefix ...string) ([]string, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Groups"); err != nil {
		return nil, err
	}

	groupNames, err := scopedStoreTransaction.storeTransaction.Groups(scopedStoreTransaction.scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
	if err != nil {
		return nil, err
	}
	for i, groupName := range groupNames {
		groupNames[i] = scopedStoreTransaction.scopedStore.trimNamespacePrefix(groupName)
	}
	return groupNames, nil
}

// Usage example: `for groupName, err := range scopedStoreTransaction.GroupsSeq("config") { if err != nil { break }; fmt.Println(groupName) }`
// Usage example: `for groupName, err := range scopedStoreTransaction.GroupsSeq() { if err != nil { break }; fmt.Println(groupName) }`
func (scopedStoreTransaction *ScopedStoreTransaction) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GroupsSeq"); err != nil {
			yield("", err)
			return
		}

		namespacePrefix := scopedStoreTransaction.scopedStore.namespacePrefix()
		for groupName, err := range scopedStoreTransaction.storeTransaction.GroupsSeq(scopedStoreTransaction.scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix))) {
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

// Usage example: `renderedTemplate, err := scopedStoreTransaction.Render("Hello {{ .name }}", "user")`
func (scopedStoreTransaction *ScopedStoreTransaction) Render(templateSource, group string) (string, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Render"); err != nil {
		return "", err
	}
	return scopedStoreTransaction.storeTransaction.Render(templateSource, scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `parts, err := scopedStoreTransaction.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (scopedStoreTransaction *ScopedStoreTransaction) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetSplit"); err != nil {
		return nil, err
	}
	return scopedStoreTransaction.storeTransaction.GetSplit(scopedStoreTransaction.scopedStore.namespacedGroup(group), key, separator)
}

// Usage example: `fields, err := scopedStoreTransaction.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (scopedStoreTransaction *ScopedStoreTransaction) GetFields(group, key string) (iter.Seq[string], error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetFields"); err != nil {
		return nil, err
	}
	return scopedStoreTransaction.storeTransaction.GetFields(scopedStoreTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `removedRows, err := scopedStoreTransaction.PurgeExpired(); if err != nil { return err }; fmt.Println(removedRows)`
func (scopedStoreTransaction *ScopedStoreTransaction) PurgeExpired() (int64, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.PurgeExpired"); err != nil {
		return 0, err
	}

	removedRows, err := purgeExpiredMatchingGroupPrefix(scopedStoreTransaction.storeTransaction.sqliteTransaction, scopedStoreTransaction.scopedStore.namespacePrefix())
	if err != nil {
		return 0, core.E("store.ScopedStoreTransaction.PurgeExpired", "delete expired rows", err)
	}
	return removedRows, nil
}

func (scopedStoreTransaction *ScopedStoreTransaction) checkQuota(operation, group, key string) error {
	if scopedStoreTransaction.scopedStore.MaxKeys == 0 && scopedStoreTransaction.scopedStore.MaxGroups == 0 {
		return nil
	}

	namespacedGroup := scopedStoreTransaction.scopedStore.namespacedGroup(group)
	namespacePrefix := scopedStoreTransaction.scopedStore.namespacePrefix()

	_, err := scopedStoreTransaction.storeTransaction.Get(namespacedGroup, key)
	if err == nil {
		return nil
	}
	if !core.Is(err, NotFoundError) {
		return core.E(operation, "quota check", err)
	}

	if scopedStoreTransaction.scopedStore.MaxKeys > 0 {
		keyCount, err := scopedStoreTransaction.storeTransaction.CountAll(namespacePrefix)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if keyCount >= scopedStoreTransaction.scopedStore.MaxKeys {
			return core.E(operation, core.Sprintf("key limit (%d)", scopedStoreTransaction.scopedStore.MaxKeys), QuotaExceededError)
		}
	}

	if scopedStoreTransaction.scopedStore.MaxGroups > 0 {
		existingGroupCount, err := scopedStoreTransaction.storeTransaction.Count(namespacedGroup)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if existingGroupCount == 0 {
			knownGroupCount := 0
			for _, iterationErr := range scopedStoreTransaction.storeTransaction.GroupsSeq(namespacePrefix) {
				if iterationErr != nil {
					return core.E(operation, "quota check", iterationErr)
				}
				knownGroupCount++
			}
			if knownGroupCount >= scopedStoreTransaction.scopedStore.MaxGroups {
				return core.E(operation, core.Sprintf("group limit (%d)", scopedStoreTransaction.scopedStore.MaxGroups), QuotaExceededError)
			}
		}
	}

	return nil
}

// checkQuota("store.ScopedStore.Set", "config", "colour") returns nil when the
// namespace still has quota available and QuotaExceededError when a new key or
// group would exceed the configured limit. Existing keys are treated as
// upserts and do not consume quota.
func (scopedStore *ScopedStore) checkQuota(operation, group, key string) error {
	if scopedStore.MaxKeys == 0 && scopedStore.MaxGroups == 0 {
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
	if scopedStore.MaxKeys > 0 {
		keyCount, err := scopedStore.storeInstance.CountAll(namespacePrefix)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if keyCount >= scopedStore.MaxKeys {
			return core.E(operation, core.Sprintf("key limit (%d)", scopedStore.MaxKeys), QuotaExceededError)
		}
	}

	// Check MaxGroups quota — only if this would create a new group.
	if scopedStore.MaxGroups > 0 {
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
			if knownGroupCount >= scopedStore.MaxGroups {
				return core.E(operation, core.Sprintf("group limit (%d)", scopedStore.MaxGroups), QuotaExceededError)
			}
		}
	}

	return nil
}
