package store

import (
	"database/sql"
	"iter"
	"regexp"
	"sync" // Note: AX-6 — internal concurrency primitive; structural for store infrastructure (RFC §4 explicitly mandates).
	"time"

	core "dappco.re/go"
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
func (quotaConfig QuotaConfig) Validate() core.Result {
	if quotaConfig.MaxKeys < 0 || quotaConfig.MaxGroups < 0 {
		return core.Fail(core.E(
			"store.QuotaConfig.Validate",
			core.Sprintf("quota values must be zero or positive; got MaxKeys=%d MaxGroups=%d", quotaConfig.MaxKeys, quotaConfig.MaxGroups),
			nil,
		))
	}
	return core.Ok(nil)
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
func (scopedConfig ScopedStoreConfig) Validate() core.Result {
	if !validNamespace.MatchString(scopedConfig.Namespace) {
		return core.Fail(core.E(
			"store.ScopedStoreConfig.Validate",
			core.Sprintf("namespace %q is invalid; use names like %q or %q", scopedConfig.Namespace, exampleTenantA, exampleTenant42),
			nil,
		))
	}
	if result := scopedConfig.Quota.Validate(); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E("store.ScopedStoreConfig.Validate", "quota", err))
	}
	return core.Ok(nil)
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
func NewScopedConfigured(storeInstance *Store, scopedConfig ScopedStoreConfig) (*ScopedStore, core.Result) {
	if storeInstance == nil {
		return nil, core.Fail(core.E(opNewScopedConfigured, "store instance is nil", nil))
	}
	if result := scopedConfig.Validate(); !result.OK {
		err, _ := result.Value.(error)
		return nil, core.Fail(core.E(opNewScopedConfigured, "validate config", err))
	}
	scopedStore := NewScoped(storeInstance, scopedConfig.Namespace)
	if scopedStore == nil {
		return nil, core.Fail(core.E(opNewScopedConfigured, "construct scoped store", nil))
	}
	scopedStore.MaxKeys = scopedConfig.Quota.MaxKeys
	scopedStore.MaxGroups = scopedConfig.Quota.MaxGroups
	return scopedStore, core.Ok(nil)
}

// Usage example: `scopedStore, err := store.NewScopedWithQuota(storeInstance, "tenant-a", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}); if err != nil { return }`
// This is a convenience constructor for callers that already have the namespace
// and quota values split across separate inputs.
func NewScopedWithQuota(storeInstance *Store, namespace string, quota QuotaConfig) (*ScopedStore, core.Result) {
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

func (scopedStore *ScopedStore) ensureReady(operation string) core.Result {
	if scopedStore == nil {
		return core.Fail(core.E(operation, scopedStoreNilMessage, nil))
	}
	if scopedStore.store == nil {
		return core.Fail(core.E(operation, "scoped store store is nil", nil))
	}
	if result := scopedStore.store.ensureReady(operation); !result.OK {
		return result
	}
	return core.Ok(nil)
}

// Namespace returns the namespace string.
// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a"); namespace := scopedStore.Namespace(); fmt.Println(namespace)`
func (scopedStore *ScopedStore) Namespace() string {
	if scopedStore == nil {
		return ""
	}
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
func (scopedStore *ScopedStore) Exists(key string) (bool, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.Exists"); !result.OK {
		return false, result
	}
	return scopedStore.store.Exists(scopedStore.namespacedGroup(scopedStore.defaultGroup()), key)
}

// Usage example: `exists, err := scopedStore.ExistsIn("config", "colour")`
// Usage example: `if exists, _ := scopedStore.ExistsIn("session", "token"); !exists { fmt.Println("session expired") }`
func (scopedStore *ScopedStore) ExistsIn(group, key string) (bool, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.ExistsIn"); !result.OK {
		return false, result
	}
	return scopedStore.store.Exists(scopedStore.namespacedGroup(group), key)
}

// Usage example: `exists, err := scopedStore.GroupExists("config")`
// Usage example: `if exists, _ := scopedStore.GroupExists("cache"); !exists { fmt.Println("group is empty") }`
func (scopedStore *ScopedStore) GroupExists(group string) (bool, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.GroupExists"); !result.OK {
		return false, result
	}
	return scopedStore.store.GroupExists(scopedStore.namespacedGroup(group))
}

// Usage example: `colourValue, err := scopedStore.Get("colour")`
func (scopedStore *ScopedStore) Get(key string) (string, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.Get"); !result.OK {
		return "", result
	}
	return scopedStore.store.Get(scopedStore.namespacedGroup(scopedStore.defaultGroup()), key)
}

// GetFrom reads a key from an explicit namespaced group.
// Usage example: `colourValue, err := scopedStore.GetFrom("config", "colour")`
func (scopedStore *ScopedStore) GetFrom(group, key string) (string, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.GetFrom"); !result.OK {
		return "", result
	}
	return scopedStore.store.Get(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.Set("colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) Set(key, value string) core.Result {
	if result := scopedStore.ensureReady("store.ScopedStore.Set"); !result.OK {
		return result
	}
	if result := scopedStore.Transaction(func(scopedTransaction *ScopedStoreTransaction) core.Result {
		return scopedTransaction.Set(key, value)
	}); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E("store.ScopedStore.Set", "write scoped key", err))
	}
	return core.Ok(nil)
}

// SetIn writes a key to an explicit namespaced group.
// Usage example: `if err := scopedStore.SetIn("config", "colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) SetIn(group, key, value string) core.Result {
	if result := scopedStore.ensureReady("store.ScopedStore.SetIn"); !result.OK {
		return result
	}
	if result := scopedStore.Transaction(func(scopedTransaction *ScopedStoreTransaction) core.Result {
		return scopedTransaction.SetIn(group, key, value)
	}); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E("store.ScopedStore.SetIn", "write scoped group key", err))
	}
	return core.Ok(nil)
}

// Usage example: `if err := scopedStore.SetWithTTL("sessions", "token", "abc123", time.Hour); err != nil { return }`
func (scopedStore *ScopedStore) SetWithTTL(group, key, value string, timeToLive time.Duration) core.Result {
	if result := scopedStore.ensureReady("store.ScopedStore.SetWithTTL"); !result.OK {
		return result
	}
	if result := scopedStore.Transaction(func(scopedTransaction *ScopedStoreTransaction) core.Result {
		return scopedTransaction.SetWithTTL(group, key, value, timeToLive)
	}); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E("store.ScopedStore.SetWithTTL", "write scoped group key with TTL", err))
	}
	return core.Ok(nil)
}

// Usage example: `if err := scopedStore.Delete("config", "colour"); err != nil { return }`
func (scopedStore *ScopedStore) Delete(group, key string) core.Result {
	if result := scopedStore.ensureReady("store.ScopedStore.Delete"); !result.OK {
		return result
	}
	return scopedStore.store.Delete(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.DeleteGroup("cache"); err != nil { return }`
func (scopedStore *ScopedStore) DeleteGroup(group string) core.Result {
	if result := scopedStore.ensureReady("store.ScopedStore.DeleteGroup"); !result.OK {
		return result
	}
	return scopedStore.store.DeleteGroup(scopedStore.namespacedGroup(group))
}

// Usage example: `if err := scopedStore.DeletePrefix("cache"); err != nil { return }`
// Usage example: `if err := scopedStore.DeletePrefix(""); err != nil { return }`
func (scopedStore *ScopedStore) DeletePrefix(groupPrefix string) core.Result {
	if result := scopedStore.ensureReady("store.ScopedStore.DeletePrefix"); !result.OK {
		return result
	}
	return scopedStore.store.DeletePrefix(scopedStore.namespacedGroup(groupPrefix))
}

// Usage example: `colourEntries, err := scopedStore.GetAll("config")`
func (scopedStore *ScopedStore) GetAll(group string) (map[string]string, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.GetAll"); !result.OK {
		return nil, result
	}
	return scopedStore.store.GetAll(scopedStore.namespacedGroup(group))
}

// Usage example: `page, err := scopedStore.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) GetPage(group string, offset, limit int) ([]KeyValue, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.GetPage"); !result.OK {
		return nil, result
	}
	return scopedStore.store.GetPage(scopedStore.namespacedGroup(group), offset, limit)
}

// Usage example: `for entry, err := range scopedStore.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) All(group string) iter.Seq2[KeyValue, error] {
	if result := scopedStore.ensureReady("store.ScopedStore.All"); !result.OK {
		return func(yield func(KeyValue, error) bool) {
			err, _ := result.Value.(error)
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
func (scopedStore *ScopedStore) Count(group string) (int, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.Count"); !result.OK {
		return 0, result
	}
	return scopedStore.store.Count(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.CountAll("config")`
// Usage example: `keyCount, err := scopedStore.CountAll()`
func (scopedStore *ScopedStore) CountAll(groupPrefix ...string) (int, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.CountAll"); !result.OK {
		return 0, result
	}
	return scopedStore.store.CountAll(scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix)))
}

// Usage example: `groupNames, err := scopedStore.Groups("config")`
// Usage example: `groupNames, err := scopedStore.Groups()`
func (scopedStore *ScopedStore) Groups(groupPrefix ...string) ([]string, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.Groups"); !result.OK {
		return nil, result
	}
	groupNames, result := scopedStore.store.Groups(scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix)))
	if !result.OK {
		return nil, result
	}
	for i, groupName := range groupNames {
		groupNames[i] = scopedStore.trimNamespacePrefix(groupName)
	}
	return groupNames, core.Ok(nil)
}

// Usage example: `for groupName, err := range scopedStore.GroupsSeq("config") { if err != nil { break }; fmt.Println(groupName) }`
// Usage example: `for groupName, err := range scopedStore.GroupsSeq() { if err != nil { break }; fmt.Println(groupName) }`
func (scopedStore *ScopedStore) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if result := scopedStore.ensureReady("store.ScopedStore.GroupsSeq"); !result.OK {
			err, _ := result.Value.(error)
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
func (scopedStore *ScopedStore) Render(templateSource, group string) (string, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.Render"); !result.OK {
		return "", result
	}
	return scopedStore.store.Render(templateSource, scopedStore.namespacedGroup(group))
}

// Usage example: `parts, err := scopedStore.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (scopedStore *ScopedStore) GetSplit(group, key, separator string) (iter.Seq[string], core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.GetSplit"); !result.OK {
		return nil, result
	}
	return scopedStore.store.GetSplit(scopedStore.namespacedGroup(group), key, separator)
}

// Usage example: `fields, err := scopedStore.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (scopedStore *ScopedStore) GetFields(group, key string) (iter.Seq[string], core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.GetFields"); !result.OK {
		return nil, result
	}
	return scopedStore.store.GetFields(scopedStore.namespacedGroup(group), key)
}

// Usage example: `removedRows, err := scopedStore.PurgeExpired(); if err != nil { return }; fmt.Println(removedRows)`
func (scopedStore *ScopedStore) PurgeExpired() (int64, core.Result) {
	if result := scopedStore.ensureReady("store.ScopedStore.PurgeExpired"); !result.OK {
		return 0, result
	}

	cutoffUnixMilli := time.Now().UnixMilli()
	expiredEntries, result := deleteExpiredEntriesMatchingGroupPrefix(scopedStore.store.sqliteDatabase, scopedStore.namespacePrefix(), cutoffUnixMilli)
	if !result.OK {
		err, _ := result.Value.(error)
		return 0, core.Fail(core.E("store.ScopedStore.PurgeExpired", "delete expired rows", err))
	}
	scopedStore.store.notifyExpiredEntries(expiredEntries)
	return int64(len(expiredEntries)), core.Ok(nil)
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
		return noopUnregister
	}
	if scopedStore.store == nil {
		return noopUnregister
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
func (scopedStore *ScopedStore) Transaction(operation func(*ScopedStoreTransaction) core.Result) core.Result {
	if scopedStore == nil {
		return core.Fail(core.E(opScopedStoreTransaction, scopedStoreNilMessage, nil))
	}
	if operation == nil {
		return core.Fail(core.E(opScopedStoreTransaction, "operation is nil", nil))
	}
	if scopedStore.store == nil {
		return core.Fail(core.E(opScopedStoreTransaction, "scoped store store is nil", nil))
	}

	return scopedStore.store.Transaction(func(storeTransaction *StoreTransaction) core.Result {
		return operation(&ScopedStoreTransaction{
			scopedStore:      scopedStore,
			storeTransaction: storeTransaction,
		})
	})
}

func (scopedStoreTransaction *ScopedStoreTransaction) ensureReady(operation string) core.Result {
	if scopedStoreTransaction == nil {
		return core.Fail(core.E(operation, "scoped transaction is nil", nil))
	}
	if scopedStoreTransaction.scopedStore == nil {
		return core.Fail(core.E(operation, "scoped transaction store is nil", nil))
	}
	if scopedStoreTransaction.storeTransaction == nil {
		return core.Fail(core.E(operation, "scoped transaction database is nil", nil))
	}
	if result := scopedStoreTransaction.scopedStore.store.ensureReady(operation); !result.OK {
		return result
	}
	return scopedStoreTransaction.storeTransaction.ensureReady(operation)
}

// Usage example: `exists, err := scopedStoreTransaction.Exists("colour")`
func (scopedStoreTransaction *ScopedStoreTransaction) Exists(key string) (bool, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Exists"); !result.OK {
		return false, result
	}
	return scopedStoreTransaction.storeTransaction.Exists(
		scopedStoreTransaction.scopedStore.namespacedGroup(scopedStoreTransaction.scopedStore.defaultGroup()),
		key,
	)
}

// Usage example: `exists, err := scopedStoreTransaction.ExistsIn("config", "colour")`
func (scopedStoreTransaction *ScopedStoreTransaction) ExistsIn(group, key string) (bool, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.ExistsIn"); !result.OK {
		return false, result
	}
	return scopedStoreTransaction.storeTransaction.Exists(scopedStoreTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `exists, err := scopedStoreTransaction.GroupExists("config")`
func (scopedStoreTransaction *ScopedStoreTransaction) GroupExists(group string) (bool, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GroupExists"); !result.OK {
		return false, result
	}
	return scopedStoreTransaction.storeTransaction.GroupExists(scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `colourValue, err := scopedStoreTransaction.Get("colour")`
func (scopedStoreTransaction *ScopedStoreTransaction) Get(key string) (string, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Get"); !result.OK {
		return "", result
	}
	return scopedStoreTransaction.storeTransaction.Get(
		scopedStoreTransaction.scopedStore.namespacedGroup(scopedStoreTransaction.scopedStore.defaultGroup()),
		key,
	)
}

// Usage example: `colourValue, err := scopedStoreTransaction.GetFrom("config", "colour")`
func (scopedStoreTransaction *ScopedStoreTransaction) GetFrom(group, key string) (string, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetFrom"); !result.OK {
		return "", result
	}
	return scopedStoreTransaction.storeTransaction.Get(scopedStoreTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStoreTransaction.Set("theme", "dark"); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) Set(key, value string) core.Result {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Set"); !result.OK {
		return result
	}
	defaultGroup := scopedStoreTransaction.scopedStore.defaultGroup()
	if result := scopedStoreTransaction.checkQuota("store.ScopedStoreTransaction.Set", defaultGroup, key); !result.OK {
		return result
	}
	return scopedStoreTransaction.storeTransaction.Set(
		scopedStoreTransaction.scopedStore.namespacedGroup(defaultGroup),
		key,
		value,
	)
}

// Usage example: `if err := scopedStoreTransaction.SetIn("config", "colour", "blue"); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) SetIn(group, key, value string) core.Result {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.SetIn"); !result.OK {
		return result
	}
	if result := scopedStoreTransaction.checkQuota("store.ScopedStoreTransaction.SetIn", group, key); !result.OK {
		return result
	}
	return scopedStoreTransaction.storeTransaction.Set(scopedStoreTransaction.scopedStore.namespacedGroup(group), key, value)
}

// Usage example: `if err := scopedStoreTransaction.SetWithTTL("sessions", "token", "abc123", time.Hour); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) SetWithTTL(group, key, value string, timeToLive time.Duration) core.Result {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.SetWithTTL"); !result.OK {
		return result
	}
	if result := scopedStoreTransaction.checkQuota("store.ScopedStoreTransaction.SetWithTTL", group, key); !result.OK {
		return result
	}
	return scopedStoreTransaction.storeTransaction.SetWithTTL(scopedStoreTransaction.scopedStore.namespacedGroup(group), key, value, timeToLive)
}

// Usage example: `if err := scopedStoreTransaction.Delete("config", "colour"); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) Delete(group, key string) core.Result {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Delete"); !result.OK {
		return result
	}
	return scopedStoreTransaction.storeTransaction.Delete(scopedStoreTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStoreTransaction.DeleteGroup("cache"); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) DeleteGroup(group string) core.Result {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.DeleteGroup"); !result.OK {
		return result
	}
	return scopedStoreTransaction.storeTransaction.DeleteGroup(scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `if err := scopedStoreTransaction.DeletePrefix("cache"); err != nil { return err }`
// Usage example: `if err := scopedStoreTransaction.DeletePrefix(""); err != nil { return err }`
func (scopedStoreTransaction *ScopedStoreTransaction) DeletePrefix(groupPrefix string) core.Result {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.DeletePrefix"); !result.OK {
		return result
	}
	return scopedStoreTransaction.storeTransaction.DeletePrefix(scopedStoreTransaction.scopedStore.namespacedGroup(groupPrefix))
}

// Usage example: `colourEntries, err := scopedStoreTransaction.GetAll("config")`
func (scopedStoreTransaction *ScopedStoreTransaction) GetAll(group string) (map[string]string, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetAll"); !result.OK {
		return nil, result
	}
	return scopedStoreTransaction.storeTransaction.GetAll(scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `page, err := scopedStoreTransaction.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (scopedStoreTransaction *ScopedStoreTransaction) GetPage(group string, offset, limit int) ([]KeyValue, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetPage"); !result.OK {
		return nil, result
	}
	return scopedStoreTransaction.storeTransaction.GetPage(scopedStoreTransaction.scopedStore.namespacedGroup(group), offset, limit)
}

// Usage example: `for entry, err := range scopedStoreTransaction.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStoreTransaction *ScopedStoreTransaction) All(group string) iter.Seq2[KeyValue, error] {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.All"); !result.OK {
		return func(yield func(KeyValue, error) bool) {
			err, _ := result.Value.(error)
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
func (scopedStoreTransaction *ScopedStoreTransaction) Count(group string) (int, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Count"); !result.OK {
		return 0, result
	}
	return scopedStoreTransaction.storeTransaction.Count(scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStoreTransaction.CountAll("config")`
// Usage example: `keyCount, err := scopedStoreTransaction.CountAll()`
func (scopedStoreTransaction *ScopedStoreTransaction) CountAll(groupPrefix ...string) (int, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.CountAll"); !result.OK {
		return 0, result
	}
	return scopedStoreTransaction.storeTransaction.CountAll(scopedStoreTransaction.scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix)))
}

// Usage example: `groupNames, err := scopedStoreTransaction.Groups("config")`
// Usage example: `groupNames, err := scopedStoreTransaction.Groups()`
func (scopedStoreTransaction *ScopedStoreTransaction) Groups(groupPrefix ...string) ([]string, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Groups"); !result.OK {
		return nil, result
	}

	groupNames, result := scopedStoreTransaction.storeTransaction.Groups(scopedStoreTransaction.scopedStore.namespacedGroup(firstStringOrEmpty(groupPrefix)))
	if !result.OK {
		return nil, result
	}
	for i, groupName := range groupNames {
		groupNames[i] = scopedStoreTransaction.scopedStore.trimNamespacePrefix(groupName)
	}
	return groupNames, core.Ok(nil)
}

// Usage example: `for groupName, err := range scopedStoreTransaction.GroupsSeq("config") { if err != nil { break }; fmt.Println(groupName) }`
// Usage example: `for groupName, err := range scopedStoreTransaction.GroupsSeq() { if err != nil { break }; fmt.Println(groupName) }`
func (scopedStoreTransaction *ScopedStoreTransaction) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GroupsSeq"); !result.OK {
			err, _ := result.Value.(error)
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
func (scopedStoreTransaction *ScopedStoreTransaction) Render(templateSource, group string) (string, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.Render"); !result.OK {
		return "", result
	}
	return scopedStoreTransaction.storeTransaction.Render(templateSource, scopedStoreTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `parts, err := scopedStoreTransaction.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (scopedStoreTransaction *ScopedStoreTransaction) GetSplit(group, key, separator string) (iter.Seq[string], core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetSplit"); !result.OK {
		return nil, result
	}
	return scopedStoreTransaction.storeTransaction.GetSplit(scopedStoreTransaction.scopedStore.namespacedGroup(group), key, separator)
}

// Usage example: `fields, err := scopedStoreTransaction.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (scopedStoreTransaction *ScopedStoreTransaction) GetFields(group, key string) (iter.Seq[string], core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.GetFields"); !result.OK {
		return nil, result
	}
	return scopedStoreTransaction.storeTransaction.GetFields(scopedStoreTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `removedRows, err := scopedStoreTransaction.PurgeExpired(); if err != nil { return err }; fmt.Println(removedRows)`
func (scopedStoreTransaction *ScopedStoreTransaction) PurgeExpired() (int64, core.Result) {
	if result := scopedStoreTransaction.ensureReady("store.ScopedStoreTransaction.PurgeExpired"); !result.OK {
		return 0, result
	}

	cutoffUnixMilli := time.Now().UnixMilli()
	expiredEntries, result := deleteExpiredEntriesMatchingGroupPrefix(scopedStoreTransaction.storeTransaction.sqliteTransaction, scopedStoreTransaction.scopedStore.namespacePrefix(), cutoffUnixMilli)
	if !result.OK {
		err, _ := result.Value.(error)
		return 0, core.Fail(core.E("store.ScopedStoreTransaction.PurgeExpired", "delete expired rows", err))
	}
	scopedStoreTransaction.storeTransaction.recordExpiredEntries(expiredEntries)
	return int64(len(expiredEntries)), core.Ok(nil)
}

func (scopedStoreTransaction *ScopedStoreTransaction) checkQuota(operation, group, key string) core.Result {
	return enforceQuota(quotaCheck{
		operation:       operation,
		key:             key,
		namespacePrefix: scopedStoreTransaction.scopedStore.namespacePrefix(),
		namespacedGroup: scopedStoreTransaction.scopedStore.namespacedGroup(group),
		maxKeys:         scopedStoreTransaction.scopedStore.MaxKeys,
		maxGroups:       scopedStoreTransaction.scopedStore.MaxGroups,
		queryable:       scopedStoreTransaction.storeTransaction.sqliteTransaction,
		counter:         scopedStoreTransaction.storeTransaction,
	})
}

type quotaCounter interface {
	CountAll(groupPrefix string) (int, core.Result)
	Count(group string) (int, core.Result)
	GroupsSeq(groupPrefix ...string) iter.Seq2[string, error]
}

type quotaCheck struct {
	operation       string
	key             string
	namespacePrefix string
	namespacedGroup string
	maxKeys         int
	maxGroups       int
	queryable       keyExistenceQuery
	counter         quotaCounter
}

func enforceQuota(check quotaCheck) core.Result {
	if check.maxKeys == 0 && check.maxGroups == 0 {
		return core.Ok(nil)
	}

	exists, result := liveEntryExists(check.queryable, check.namespacedGroup, check.key)
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(check.operation, quotaCheckContext, err))
	}
	if exists {
		return core.Ok(nil)
	}

	if result := enforceKeyQuota(check); !result.OK {
		return result
	}
	return enforceGroupQuota(check)
}

func enforceKeyQuota(check quotaCheck) core.Result {
	if check.maxKeys <= 0 {
		return core.Ok(nil)
	}
	keyCount, result := check.counter.CountAll(check.namespacePrefix)
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(check.operation, quotaCheckContext, err))
	}
	if keyCount >= check.maxKeys {
		return core.Fail(core.E(check.operation, core.Sprintf("key limit (%d)", check.maxKeys), QuotaExceededError))
	}
	return core.Ok(nil)
}

func enforceGroupQuota(check quotaCheck) core.Result {
	if check.maxGroups <= 0 {
		return core.Ok(nil)
	}
	existingGroupCount, result := check.counter.Count(check.namespacedGroup)
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(check.operation, quotaCheckContext, err))
	}
	if existingGroupCount > 0 {
		return core.Ok(nil)
	}
	knownGroupCount, result := countKnownGroups(check.counter, check.namespacePrefix)
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(check.operation, quotaCheckContext, err))
	}
	if knownGroupCount >= check.maxGroups {
		return core.Fail(core.E(check.operation, core.Sprintf("group limit (%d)", check.maxGroups), QuotaExceededError))
	}
	return core.Ok(nil)
}

func countKnownGroups(counter quotaCounter, namespacePrefix string) (int, core.Result) {
	knownGroupCount := 0
	for _, iterationErr := range counter.GroupsSeq(namespacePrefix) {
		if iterationErr != nil {
			return 0, core.Fail(iterationErr)
		}
		knownGroupCount++
	}
	return knownGroupCount, core.Ok(nil)
}

func liveEntryExists(queryable keyExistenceQuery, group, key string) (bool, core.Result) {
	var exists int
	err := queryable.QueryRow(
		"SELECT 1 FROM "+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1",
		group,
		key,
		time.Now().UnixMilli(),
	).Scan(&exists)
	if err == nil {
		return true, core.Ok(nil)
	}
	if err == sql.ErrNoRows {
		return false, core.Ok(nil)
	}
	return false, core.Fail(err)
}

type keyExistenceQuery interface {
	QueryRow(query string, args ...any) *sql.Row
}
