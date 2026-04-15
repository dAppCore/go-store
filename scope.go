package store

import (
	"database/sql"
	"iter"
	"regexp"
	"sync"
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

// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a")`
// Usage example: `if err := scopedStore.Set("colour", "blue"); err != nil { return } // writes tenant-a:default/colour`
// Usage example: `if err := scopedStore.SetIn("config", "colour", "blue"); err != nil { return } // writes tenant-a:config/colour`
type ScopedStore struct {
	store     *Store
	namespace string
	// Usage example: `scopedStore.MaxKeys = 100`
	MaxKeys int
	// Usage example: `scopedStore.MaxGroups = 10`
	MaxGroups int

	watcherBridgeLock sync.Mutex
	watcherBridges    map[uintptr]scopedWatcherBridge
}

type scopedWatcherBridge struct {
	sourceGroup  string
	sourceEvents <-chan Event
	done         chan struct{}
}

// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a")`
// Prefer `NewScopedConfigured(storeInstance, store.ScopedStoreConfig{Namespace: "tenant-a"})`
// when the namespace and quota are already known at the call site.
func NewScoped(storeInstance *Store, namespace string) *ScopedStore {
	if storeInstance == nil || !validNamespace.MatchString(namespace) {
		return nil
	}
	scopedStore := &ScopedStore{
		store:          storeInstance,
		namespace:      namespace,
		watcherBridges: make(map[uintptr]scopedWatcherBridge),
	}
	return scopedStore
}

// Usage example: `scopedStore, err := store.NewScopedConfigured(storeInstance, store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}}); if err != nil { return }`
// This keeps the namespace and quota in one declarative literal instead of an
// option chain.
func NewScopedConfigured(storeInstance *Store, scopedConfig ScopedStoreConfig) (*ScopedStore, error) {
	if storeInstance == nil {
		return nil, core.E("store.NewScopedConfigured", "store instance is nil", nil)
	}
	if err := scopedConfig.Validate(); err != nil {
		return nil, core.E("store.NewScopedConfigured", "validate config", err)
	}
	scopedStore := NewScoped(storeInstance, scopedConfig.Namespace)
	if scopedStore == nil {
		return nil, core.E("store.NewScopedConfigured", "construct scoped store", nil)
	}
	scopedStore.MaxKeys = scopedConfig.Quota.MaxKeys
	scopedStore.MaxGroups = scopedConfig.Quota.MaxGroups
	return scopedStore, nil
}

// Usage example: `scopedStore, err := store.NewScopedWithQuota(storeInstance, "tenant-a", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}); if err != nil { return }`
// This is a convenience constructor for callers that already have the namespace
// and quota values split across separate inputs.
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

func (scopedStore *ScopedStore) ensureReady(operation string) error {
	if scopedStore == nil {
		return core.E(operation, "scoped store is nil", nil)
	}
	if scopedStore.store == nil {
		return core.E(operation, "scoped store store is nil", nil)
	}
	if err := scopedStore.store.ensureReady(operation); err != nil {
		return err
	}
	return nil
}

// Namespace returns the namespace string.
// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a"); namespace := scopedStore.Namespace(); fmt.Println(namespace)`
func (scopedStore *ScopedStore) Namespace() string {
	return scopedStore.namespace
}

// Config returns the namespace and quota settings as a single declarative struct.
// Usage example: `config := scopedStore.Config(); fmt.Println(config.Namespace, config.Quota.MaxKeys, config.Quota.MaxGroups)`
func (scopedStore *ScopedStore) Config() ScopedStoreConfig {
	if scopedStore == nil {
		return ScopedStoreConfig{}
	}
	return ScopedStoreConfig{
		Namespace: scopedStore.namespace,
		Quota: QuotaConfig{
			MaxKeys:   scopedStore.MaxKeys,
			MaxGroups: scopedStore.MaxGroups,
		},
	}
}

// Usage example: `exists, err := scopedStore.Exists("colour")`
// Usage example: `if exists, _ := scopedStore.Exists("token"); !exists { fmt.Println("session expired") }`
func (scopedStore *ScopedStore) Exists(key string) (bool, error) {
	if err := scopedStore.ensureReady("store.Exists"); err != nil {
		return false, err
	}
	return scopedStore.store.Exists(scopedStore.namespacedGroup(scopedStore.defaultGroup()), key)
}

// Usage example: `exists, err := scopedStore.ExistsIn("config", "colour")`
// Usage example: `if exists, _ := scopedStore.ExistsIn("session", "token"); !exists { fmt.Println("session expired") }`
func (scopedStore *ScopedStore) ExistsIn(group, key string) (bool, error) {
	if err := scopedStore.ensureReady("store.Exists"); err != nil {
		return false, err
	}
	return scopedStore.store.Exists(scopedStore.namespacedGroup(group), key)
}

// Usage example: `exists, err := scopedStore.GroupExists("config")`
// Usage example: `if exists, _ := scopedStore.GroupExists("cache"); !exists { fmt.Println("group is empty") }`
func (scopedStore *ScopedStore) GroupExists(group string) (bool, error) {
	if err := scopedStore.ensureReady("store.GroupExists"); err != nil {
		return false, err
	}
	return scopedStore.store.GroupExists(scopedStore.namespacedGroup(group))
}

// Usage example: `colourValue, err := scopedStore.Get("colour")`
func (scopedStore *ScopedStore) Get(key string) (string, error) {
	if err := scopedStore.ensureReady("store.Get"); err != nil {
		return "", err
	}
	return scopedStore.store.Get(scopedStore.namespacedGroup(scopedStore.defaultGroup()), key)
}

// GetFrom reads a key from an explicit namespaced group.
// Usage example: `colourValue, err := scopedStore.GetFrom("config", "colour")`
func (scopedStore *ScopedStore) GetFrom(group, key string) (string, error) {
	if err := scopedStore.ensureReady("store.Get"); err != nil {
		return "", err
	}
	return scopedStore.store.Get(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.Set("colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) Set(key, value string) error {
	if err := scopedStore.ensureReady("store.Set"); err != nil {
		return err
	}
	defaultGroup := scopedStore.defaultGroup()
	if err := scopedStore.checkQuota("store.ScopedStore.Set", defaultGroup, key); err != nil {
		return err
	}
	return scopedStore.store.Set(scopedStore.namespacedGroup(defaultGroup), key, value)
}

// SetIn writes a key to an explicit namespaced group.
// Usage example: `if err := scopedStore.SetIn("config", "colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) SetIn(group, key, value string) error {
	if err := scopedStore.ensureReady("store.Set"); err != nil {
		return err
	}
	if err := scopedStore.checkQuota("store.ScopedStore.SetIn", group, key); err != nil {
		return err
	}
	return scopedStore.store.Set(scopedStore.namespacedGroup(group), key, value)
}

// Usage example: `if err := scopedStore.SetWithTTL("sessions", "token", "abc123", time.Hour); err != nil { return }`
func (scopedStore *ScopedStore) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	if err := scopedStore.ensureReady("store.SetWithTTL"); err != nil {
		return err
	}
	if err := scopedStore.checkQuota("store.ScopedStore.SetWithTTL", group, key); err != nil {
		return err
	}
	return scopedStore.store.SetWithTTL(scopedStore.namespacedGroup(group), key, value, timeToLive)
}

// Usage example: `if err := scopedStore.Delete("config", "colour"); err != nil { return }`
func (scopedStore *ScopedStore) Delete(group, key string) error {
	if err := scopedStore.ensureReady("store.Delete"); err != nil {
		return err
	}
	return scopedStore.store.Delete(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.DeleteGroup("cache"); err != nil { return }`
func (scopedStore *ScopedStore) DeleteGroup(group string) error {
	if err := scopedStore.ensureReady("store.DeleteGroup"); err != nil {
		return err
	}
	return scopedStore.store.DeleteGroup(scopedStore.namespacedGroup(group))
}

// Usage example: `if err := scopedStore.DeletePrefix("cache"); err != nil { return }`
// Usage example: `if err := scopedStore.DeletePrefix(""); err != nil { return }`
func (scopedStore *ScopedStore) DeletePrefix(groupPrefix string) error {
	if err := scopedStore.ensureReady("store.DeletePrefix"); err != nil {
		return err
	}
	return scopedStore.store.DeletePrefix(scopedStore.namespacedGroup(groupPrefix))
}

// Usage example: `colourEntries, err := scopedStore.GetAll("config")`
func (scopedStore *ScopedStore) GetAll(group string) (map[string]string, error) {
	if err := scopedStore.ensureReady("store.GetAll"); err != nil {
		return nil, err
	}
	return scopedStore.store.GetAll(scopedStore.namespacedGroup(group))
}

// Usage example: `page, err := scopedStore.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) GetPage(group string, offset, limit int) ([]KeyValue, error) {
	if err := scopedStore.ensureReady("store.GetPage"); err != nil {
		return nil, err
	}
	return scopedStore.store.GetPage(scopedStore.namespacedGroup(group), offset, limit)
}

// Usage example: `for entry, err := range scopedStore.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) All(group string) iter.Seq2[KeyValue, error] {
	if err := scopedStore.ensureReady("store.All"); err != nil {
		return func(yield func(KeyValue, error) bool) {
			yield(KeyValue{}, err)
		}
	}
	return scopedStore.store.All(scopedStore.namespacedGroup(group))
}

// Usage example: `for entry, err := range scopedStore.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return scopedStore.All(group)
}

// Usage example: `keyCount, err := scopedStore.Count("config")`
func (scopedStore *ScopedStore) Count(group string) (int, error) {
	if err := scopedStore.ensureReady("store.Count"); err != nil {
		return 0, err
	}
	return scopedStore.store.Count(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.CountAll("config")`
// Usage example: `keyCount, err := scopedStore.CountAll()`
func (scopedStore *ScopedStore) CountAll(groupPrefix ...string) (int, error) {
	if err := scopedStore.ensureReady("store.CountAll"); err != nil {
		return 0, err
	}
	return scopedStore.store.CountAll(scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix)))
}

// Usage example: `groupNames, err := scopedStore.Groups("config")`
// Usage example: `groupNames, err := scopedStore.Groups()`
func (scopedStore *ScopedStore) Groups(groupPrefix ...string) ([]string, error) {
	if err := scopedStore.ensureReady("store.Groups"); err != nil {
		return nil, err
	}
	groupNames, err := scopedStore.store.Groups(scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix)))
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
		if err := scopedStore.ensureReady("store.GroupsSeq"); err != nil {
			yield("", err)
			return
		}
		namespacePrefix := scopedStore.namespacePrefix()
		for groupName, err := range scopedStore.store.GroupsSeq(scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix))) {
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
	if err := scopedStore.ensureReady("store.Render"); err != nil {
		return "", err
	}
	return scopedStore.store.Render(templateSource, scopedStore.namespacedGroup(group))
}

// Usage example: `parts, err := scopedStore.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (scopedStore *ScopedStore) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	if err := scopedStore.ensureReady("store.GetSplit"); err != nil {
		return nil, err
	}
	return scopedStore.store.GetSplit(scopedStore.namespacedGroup(group), key, separator)
}

// Usage example: `fields, err := scopedStore.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (scopedStore *ScopedStore) GetFields(group, key string) (iter.Seq[string], error) {
	if err := scopedStore.ensureReady("store.GetFields"); err != nil {
		return nil, err
	}
	return scopedStore.store.GetFields(scopedStore.namespacedGroup(group), key)
}

// Usage example: `removedRows, err := scopedStore.PurgeExpired(); if err != nil { return }; fmt.Println(removedRows)`
func (scopedStore *ScopedStore) PurgeExpired() (int64, error) {
	if err := scopedStore.ensureReady("store.PurgeExpired"); err != nil {
		return 0, err
	}

	cutoffUnixMilli := time.Now().UnixMilli()
	expiredEntries, err := listExpiredEntriesMatchingGroupPrefix(scopedStore.store.sqliteDatabase, scopedStore.namespacePrefix(), cutoffUnixMilli)
	if err != nil {
		return 0, core.E("store.ScopedStore.PurgeExpired", "list expired rows", err)
	}

	removedRows, err := purgeExpiredMatchingGroupPrefix(scopedStore.store.sqliteDatabase, scopedStore.namespacePrefix(), cutoffUnixMilli)
	if err != nil {
		return 0, core.E("store.ScopedStore.PurgeExpired", "delete expired rows", err)
	}
	if removedRows > 0 {
		for _, expiredEntry := range expiredEntries {
			scopedStore.store.notify(Event{
				Type:      EventDelete,
				Group:     expiredEntry.group,
				Key:       expiredEntry.key,
				Timestamp: time.Now(),
			})
		}
	}
	return removedRows, nil
}

// Usage example: `events := scopedStore.Watch("config")`
// Usage example: `events := scopedStore.Watch("*")`
// A write to `tenant-a:config` is delivered back to this scoped watcher as
// `config`, so callers never have to strip the namespace themselves.
func (scopedStore *ScopedStore) Watch(group string) <-chan Event {
	if scopedStore == nil || scopedStore.store == nil {
		return closedEventChannel()
	}

	sourceGroup := scopedStore.namespacedGroup(group)
	if group == "*" {
		sourceGroup = "*"
	}

	sourceEvents := scopedStore.store.Watch(sourceGroup)
	localEvents := make(chan Event, watcherEventBufferCapacity)
	done := make(chan struct{})
	localEventsPointer := channelPointer(localEvents)

	scopedStore.watcherBridgeLock.Lock()
	if scopedStore.watcherBridges == nil {
		scopedStore.watcherBridges = make(map[uintptr]scopedWatcherBridge)
	}
	scopedStore.watcherBridges[localEventsPointer] = scopedWatcherBridge{
		sourceGroup:  sourceGroup,
		sourceEvents: sourceEvents,
		done:         done,
	}
	scopedStore.watcherBridgeLock.Unlock()

	go func() {
		defer close(localEvents)
		defer scopedStore.removeWatcherBridge(localEventsPointer)

		for {
			select {
			case <-done:
				return
			case event, ok := <-sourceEvents:
				if !ok {
					return
				}

				localEvent, allowed := scopedStore.localiseWatchedEvent(event)
				if !allowed {
					continue
				}

				select {
				case localEvents <- localEvent:
				default:
				}
			}
		}
	}()

	return localEvents
}

// Usage example: `events := scopedStore.Watch("config"); scopedStore.Unwatch("config", events)`
// Usage example: `events := scopedStore.Watch("*"); scopedStore.Unwatch("*", events)`
func (scopedStore *ScopedStore) Unwatch(group string, events <-chan Event) {
	if scopedStore == nil || events == nil {
		return
	}

	scopedStore.watcherBridgeLock.Lock()
	watcherBridge, ok := scopedStore.watcherBridges[channelPointer(events)]
	if ok {
		delete(scopedStore.watcherBridges, channelPointer(events))
	}
	scopedStore.watcherBridgeLock.Unlock()

	if !ok {
		return
	}

	close(watcherBridge.done)
	scopedStore.store.Unwatch(watcherBridge.sourceGroup, watcherBridge.sourceEvents)
}

func (scopedStore *ScopedStore) removeWatcherBridge(pointer uintptr) {
	if scopedStore == nil {
		return
	}

	scopedStore.watcherBridgeLock.Lock()
	delete(scopedStore.watcherBridges, pointer)
	scopedStore.watcherBridgeLock.Unlock()
}

func (scopedStore *ScopedStore) localiseWatchedEvent(event Event) (Event, bool) {
	if scopedStore == nil {
		return Event{}, false
	}

	namespacePrefix := scopedStore.namespacePrefix()
	if event.Group == "*" {
		return event, true
	}
	if !core.HasPrefix(event.Group, namespacePrefix) {
		return Event{}, false
	}

	event.Group = core.TrimPrefix(event.Group, namespacePrefix)
	return event, true
}

// Usage example: `unregister := scopedStore.OnChange(func(event store.Event) { fmt.Println(event.Group, event.Key, event.Value) })`
// A callback registered on `tenant-a` receives `config` rather than
// `tenant-a:config`.
func (scopedStore *ScopedStore) OnChange(callback func(Event)) func() {
	if scopedStore == nil || callback == nil {
		return func() {}
	}
	if scopedStore.store == nil {
		return func() {}
	}

	namespacePrefix := scopedStore.namespacePrefix()
	return scopedStore.store.OnChange(func(event Event) {
		if !core.HasPrefix(event.Group, namespacePrefix) {
			return
		}
		event.Group = core.TrimPrefix(event.Group, namespacePrefix)
		callback(event)
	})
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

	return scopedStore.store.Transaction(func(storeTransaction *StoreTransaction) error {
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
	if err := scopedStoreTransaction.scopedStore.store.ensureReady(operation); err != nil {
		return err
	}
	return scopedStoreTransaction.storeTransaction.ensureReady(operation)
}

// Usage example: `exists, err := scopedStoreTransaction.Exists("colour")`
func (scopedStoreTransaction *ScopedStoreTransaction) Exists(key string) (bool, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Exists"); err != nil {
		return false, err
	}
	return scopedStoreTransaction.storeTransaction.Exists(
		scopedStoreTransaction.scopedStore.namespacedGroup(scopedStoreTransaction.scopedStore.defaultGroup()),
		key,
	)
}

// Usage example: `exists, err := scopedStoreTransaction.ExistsIn("config", "colour")`
func (scopedStoreTransaction *ScopedStoreTransaction) ExistsIn(group, key string) (bool, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.ExistsIn"); err != nil {
		return false, err
	}
	return scopedStoreTransaction.storeTransaction.Exists(scopedStoreTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `exists, err := scopedStoreTransaction.GroupExists("config")`
func (scopedStoreTransaction *ScopedStoreTransaction) GroupExists(group string) (bool, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GroupExists"); err != nil {
		return false, err
	}
	return scopedStoreTransaction.storeTransaction.GroupExists(scopedStoreTransaction.scopedStore.namespacedGroup(group))
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
	return scopedStoreTransaction.storeTransaction.CountAll(scopedStoreTransaction.scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix)))
}

// Usage example: `groupNames, err := scopedStoreTransaction.Groups("config")`
// Usage example: `groupNames, err := scopedStoreTransaction.Groups()`
func (scopedStoreTransaction *ScopedStoreTransaction) Groups(groupPrefix ...string) ([]string, error) {
	if err := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Groups"); err != nil {
		return nil, err
	}

	groupNames, err := scopedStoreTransaction.storeTransaction.Groups(scopedStoreTransaction.scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix)))
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
		for groupName, err := range scopedStoreTransaction.storeTransaction.GroupsSeq(scopedStoreTransaction.scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix))) {
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

	cutoffUnixMilli := time.Now().UnixMilli()
	expiredEntries, err := listExpiredEntriesMatchingGroupPrefix(scopedStoreTransaction.storeTransaction.sqliteTransaction, scopedStoreTransaction.scopedStore.namespacePrefix(), cutoffUnixMilli)
	if err != nil {
		return 0, core.E("store.ScopedStoreTransaction.PurgeExpired", "list expired rows", err)
	}

	removedRows, err := purgeExpiredMatchingGroupPrefix(scopedStoreTransaction.storeTransaction.sqliteTransaction, scopedStoreTransaction.scopedStore.namespacePrefix(), cutoffUnixMilli)
	if err != nil {
		return 0, core.E("store.ScopedStoreTransaction.PurgeExpired", "delete expired rows", err)
	}
	if removedRows > 0 {
		for _, expiredEntry := range expiredEntries {
			scopedStoreTransaction.storeTransaction.recordEvent(Event{
				Type:      EventDelete,
				Group:     expiredEntry.group,
				Key:       expiredEntry.key,
				Timestamp: time.Now(),
			})
		}
	}
	return removedRows, nil
}

func (scopedStoreTransaction *ScopedStoreTransaction) checkQuota(operation, group, key string) error {
	return enforceQuota(
		operation,
		group,
		key,
		scopedStoreTransaction.scopedStore.namespacePrefix(),
		scopedStoreTransaction.scopedStore.namespacedGroup(group),
		scopedStoreTransaction.scopedStore.MaxKeys,
		scopedStoreTransaction.scopedStore.MaxGroups,
		scopedStoreTransaction.storeTransaction.sqliteTransaction,
		scopedStoreTransaction.storeTransaction,
	)
}

// checkQuota("store.ScopedStore.Set", "config", "colour") returns nil when the
// namespace still has quota available and QuotaExceededError when a new key or
// group would exceed the configured limit. Existing keys are treated as
// upserts and do not consume quota.
func (scopedStore *ScopedStore) checkQuota(operation, group, key string) error {
	return enforceQuota(
		operation,
		group,
		key,
		scopedStore.namespacePrefix(),
		scopedStore.namespacedGroup(group),
		scopedStore.MaxKeys,
		scopedStore.MaxGroups,
		scopedStore.store.sqliteDatabase,
		scopedStore.store,
	)
}

type quotaCounter interface {
	CountAll(groupPrefix string) (int, error)
	Count(group string) (int, error)
	GroupsSeq(groupPrefix ...string) iter.Seq2[string, error]
}

func enforceQuota(
	operation, group, key, namespacePrefix, namespacedGroup string,
	maxKeys, maxGroups int,
	queryable keyExistenceQuery,
	counter quotaCounter,
) error {
	if maxKeys == 0 && maxGroups == 0 {
		return nil
	}

	exists, err := liveEntryExists(queryable, namespacedGroup, key)
	if err != nil {
		// A database error occurred, not just a "not found" result.
		return core.E(operation, "quota check", err)
	}
	if exists {
		// Key exists - this is an upsert, no quota check needed.
		return nil
	}

	if maxKeys > 0 {
		keyCount, err := counter.CountAll(namespacePrefix)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if keyCount >= maxKeys {
			return core.E(operation, core.Sprintf("key limit (%d)", maxKeys), QuotaExceededError)
		}
	}

	if maxGroups > 0 {
		existingGroupCount, err := counter.Count(namespacedGroup)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if existingGroupCount == 0 {
			knownGroupCount := 0
			for _, iterationErr := range counter.GroupsSeq(namespacePrefix) {
				if iterationErr != nil {
					return core.E(operation, "quota check", iterationErr)
				}
				knownGroupCount++
			}
			if knownGroupCount >= maxGroups {
				return core.E(operation, core.Sprintf("group limit (%d)", maxGroups), QuotaExceededError)
			}
		}
	}

	return nil
}

func liveEntryExists(queryable keyExistenceQuery, group, key string) (bool, error) {
	var exists int
	err := queryable.QueryRow(
		"SELECT 1 FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1",
		group,
		key,
		time.Now().UnixMilli(),
	).Scan(&exists)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, err
}

type keyExistenceQuery interface {
	QueryRow(query string, args ...any) *sql.Row
}
