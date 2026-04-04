package store

import (
	"iter"
	"regexp"
	"sync"
	"time"

	core "dappco.re/go/core"
)

// validNamespace.MatchString("tenant-a") is true; validNamespace.MatchString("tenant_a") is false.
var validNamespace = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

const defaultScopedGroupName = "default"

// Usage example: `quota := store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}`
type QuotaConfig struct {
	// Usage example: `store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}` limits a namespace to 100 keys.
	MaxKeys int
	// Usage example: `store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}` limits a namespace to 10 groups.
	MaxGroups int
}

// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a"); if scopedStore == nil { return }; if err := scopedStore.Set("colour", "blue"); err != nil { return }; if err := scopedStore.SetIn("config", "language", "en-GB"); err != nil { return }`
type ScopedStore struct {
	backingStore *Store
	namespace    string
	// Usage example: `scopedStore.MaxKeys = 100`
	MaxKeys int
	// Usage example: `scopedStore.MaxGroups = 10`
	MaxGroups int

	scopedWatchersLock sync.Mutex
	scopedWatchers     map[uintptr]*scopedWatcherBinding
}

// Usage example: `err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error { return transaction.Set("colour", "blue") })`
type ScopedStoreTransaction struct {
	scopedStore      *ScopedStore
	storeTransaction *StoreTransaction
}

// Usage example: `config := store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}}`
type ScopedStoreConfig struct {
	// Usage example: `config := store.ScopedStoreConfig{Namespace: "tenant-a"}`
	Namespace string
	// Usage example: `config := store.ScopedStoreConfig{Quota: store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}}`
	Quota QuotaConfig
}

type scopedWatcherBinding struct {
	backingStore     *Store
	underlyingEvents <-chan Event
	done             chan struct{}
	stop             chan struct{}
	stopOnce         sync.Once
}

func (scopedStore *ScopedStore) resolvedStore(operation string) (*Store, error) {
	if scopedStore == nil {
		return nil, core.E(operation, "scoped store is nil", nil)
	}
	if scopedStore.backingStore == nil {
		return nil, core.E(operation, "underlying store is nil", nil)
	}
	if err := scopedStore.backingStore.ensureReady(operation); err != nil {
		return nil, err
	}
	return scopedStore.backingStore, nil
}

// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a"); if scopedStore == nil { return }`
func NewScoped(storeInstance *Store, namespace string) *ScopedStore {
	if storeInstance == nil {
		return nil
	}
	if !validNamespace.MatchString(namespace) {
		return nil
	}
	scopedStore := &ScopedStore{backingStore: storeInstance, namespace: namespace}
	return scopedStore
}

// Usage example: `scopedStore, err := store.NewScopedConfigured(storeInstance, store.ScopedStoreConfig{Namespace: "tenant-a", Quota: store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}}); if err != nil { return }`
func NewScopedConfigured(storeInstance *Store, config ScopedStoreConfig) (*ScopedStore, error) {
	scopedStore := NewScoped(storeInstance, config.Namespace)
	if scopedStore == nil {
		if storeInstance == nil {
			return nil, core.E("store.NewScopedConfigured", "store instance is nil", nil)
		}
		return nil, core.E("store.NewScopedConfigured", core.Sprintf("namespace %q is invalid; use names like %q or %q", config.Namespace, "tenant-a", "tenant-42"), nil)
	}
	if config.Quota.MaxKeys < 0 || config.Quota.MaxGroups < 0 {
		return nil, core.E(
			"store.NewScopedConfigured",
			core.Sprintf("quota values must be zero or positive; got MaxKeys=%d MaxGroups=%d", config.Quota.MaxKeys, config.Quota.MaxGroups),
			nil,
		)
	}
	scopedStore.MaxKeys = config.Quota.MaxKeys
	scopedStore.MaxGroups = config.Quota.MaxGroups
	return scopedStore, nil
}

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

func (scopedStore *ScopedStore) trimNamespacePrefix(groupName string) string {
	return core.TrimPrefix(groupName, scopedStore.namespacePrefix())
}

// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a"); if scopedStore == nil { return }; fmt.Println(scopedStore.Namespace())`
func (scopedStore *ScopedStore) Namespace() string {
	if scopedStore == nil {
		return ""
	}
	return scopedStore.namespace
}

// Usage example: `colourValue, err := scopedStore.Get("colour")`
func (scopedStore *ScopedStore) Get(key string) (string, error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.Get")
	if err != nil {
		return "", err
	}
	return backingStore.Get(scopedStore.namespacedGroup(defaultScopedGroupName), key)
}

// Usage example: `colourValue, err := scopedStore.GetFrom("config", "colour")`
func (scopedStore *ScopedStore) GetFrom(group, key string) (string, error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.GetFrom")
	if err != nil {
		return "", err
	}
	return backingStore.Get(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.Set("colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) Set(key, value string) error {
	return scopedStore.SetIn(defaultScopedGroupName, key, value)
}

// Usage example: `if err := scopedStore.SetIn("config", "colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) SetIn(group, key, value string) error {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.SetIn")
	if err != nil {
		return err
	}
	if err := scopedStore.checkQuota("store.ScopedStore.SetIn", group, key); err != nil {
		return err
	}
	return backingStore.Set(scopedStore.namespacedGroup(group), key, value)
}

// Usage example: `if err := scopedStore.SetWithTTL("sessions", "token", "abc123", time.Hour); err != nil { return }`
func (scopedStore *ScopedStore) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.SetWithTTL")
	if err != nil {
		return err
	}
	if err := scopedStore.checkQuota("store.ScopedStore.SetWithTTL", group, key); err != nil {
		return err
	}
	return backingStore.SetWithTTL(scopedStore.namespacedGroup(group), key, value, timeToLive)
}

// Usage example: `if err := scopedStore.Delete("config", "colour"); err != nil { return }`
func (scopedStore *ScopedStore) Delete(group, key string) error {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.Delete")
	if err != nil {
		return err
	}
	return backingStore.Delete(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.DeleteGroup("cache"); err != nil { return }`
func (scopedStore *ScopedStore) DeleteGroup(group string) error {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.DeleteGroup")
	if err != nil {
		return err
	}
	return backingStore.DeleteGroup(scopedStore.namespacedGroup(group))
}

// Usage example: `if err := scopedStore.DeletePrefix("config"); err != nil { return }`
func (scopedStore *ScopedStore) DeletePrefix(groupPrefix string) error {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.DeletePrefix")
	if err != nil {
		return err
	}
	return backingStore.DeletePrefix(scopedStore.namespacedGroup(groupPrefix))
}

// Usage example: `colourEntries, err := scopedStore.GetAll("config")`
func (scopedStore *ScopedStore) GetAll(group string) (map[string]string, error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.GetAll")
	if err != nil {
		return nil, err
	}
	return backingStore.GetAll(scopedStore.namespacedGroup(group))
}

// Usage example: `page, err := scopedStore.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) GetPage(group string, offset, limit int) ([]KeyValue, error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.GetPage")
	if err != nil {
		return nil, err
	}
	return backingStore.GetPage(scopedStore.namespacedGroup(group), offset, limit)
}

// Usage example: `for entry, err := range scopedStore.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) All(group string) iter.Seq2[KeyValue, error] {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.All")
	if err != nil {
		return func(yield func(KeyValue, error) bool) {
			yield(KeyValue{}, err)
		}
	}
	return backingStore.All(scopedStore.namespacedGroup(group))
}

// Usage example: `for entry, err := range scopedStore.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) AllSeq(group string) iter.Seq2[KeyValue, error] {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.AllSeq")
	if err != nil {
		return func(yield func(KeyValue, error) bool) {
			yield(KeyValue{}, err)
		}
	}
	return backingStore.AllSeq(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.Count("config")`
func (scopedStore *ScopedStore) Count(group string) (int, error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.Count")
	if err != nil {
		return 0, err
	}
	return backingStore.Count(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.CountAll("config")`
// Usage example: `keyCount, err := scopedStore.CountAll()`
func (scopedStore *ScopedStore) CountAll(groupPrefix ...string) (int, error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.CountAll")
	if err != nil {
		return 0, err
	}
	return backingStore.CountAll(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
}

// Usage example: `groupNames, err := scopedStore.Groups("config")`
// Usage example: `groupNames, err := scopedStore.Groups()`
func (scopedStore *ScopedStore) Groups(groupPrefix ...string) ([]string, error) {
	backingStore, err := scopedStore.resolvedStore("store.Groups")
	if err != nil {
		return nil, err
	}

	groupNames, err := backingStore.Groups(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
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
		backingStore, err := scopedStore.resolvedStore("store.ScopedStore.GroupsSeq")
		if err != nil {
			yield("", err)
			return
		}
		namespacePrefix := scopedStore.namespacePrefix()
		for groupName, err := range backingStore.GroupsSeq(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix))) {
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
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.Render")
	if err != nil {
		return "", err
	}
	return backingStore.Render(templateSource, scopedStore.namespacedGroup(group))
}

// Usage example: `parts, err := scopedStore.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (scopedStore *ScopedStore) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.GetSplit")
	if err != nil {
		return nil, err
	}
	return backingStore.GetSplit(scopedStore.namespacedGroup(group), key, separator)
}

// Usage example: `fields, err := scopedStore.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (scopedStore *ScopedStore) GetFields(group, key string) (iter.Seq[string], error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.GetFields")
	if err != nil {
		return nil, err
	}
	return backingStore.GetFields(scopedStore.namespacedGroup(group), key)
}

// Usage example: `events := scopedStore.Watch("config")`
func (scopedStore *ScopedStore) Watch(group string) <-chan Event {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.Watch")
	if err != nil {
		return closedEventChannel()
	}
	if group != "*" {
		return backingStore.Watch(scopedStore.namespacedGroup(group))
	}

	forwardedEvents := make(chan Event, watcherEventBufferCapacity)
	binding := &scopedWatcherBinding{
		backingStore:     backingStore,
		underlyingEvents: backingStore.Watch("*"),
		done:             make(chan struct{}),
		stop:             make(chan struct{}),
	}

	scopedStore.scopedWatchersLock.Lock()
	if scopedStore.scopedWatchers == nil {
		scopedStore.scopedWatchers = make(map[uintptr]*scopedWatcherBinding)
	}
	scopedStore.scopedWatchers[channelPointer(forwardedEvents)] = binding
	scopedStore.scopedWatchersLock.Unlock()

	namespacePrefix := scopedStore.namespacePrefix()
	go func() {
		defer close(forwardedEvents)
		defer close(binding.done)
		defer scopedStore.forgetScopedWatcher(forwardedEvents)

		for {
			select {
			case event, ok := <-binding.underlyingEvents:
				if !ok {
					return
				}
				if !core.HasPrefix(event.Group, namespacePrefix) {
					continue
				}
				select {
				case forwardedEvents <- event:
				default:
				}
			case <-binding.stop:
				return
			case <-backingStore.purgeContext.Done():
				return
			}
		}
	}()

	return forwardedEvents
}

// Usage example: `scopedStore.Unwatch("config", events)`
func (scopedStore *ScopedStore) Unwatch(group string, events <-chan Event) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.Unwatch")
	if err != nil {
		return
	}
	if group == "*" {
		scopedStore.forgetAndStopScopedWatcher(events)
		return
	}
	backingStore.Unwatch(scopedStore.namespacedGroup(group), events)
}

// Usage example: `unregister := scopedStore.OnChange(func(event store.Event) { fmt.Println(event.Group, event.Key) })`
func (scopedStore *ScopedStore) OnChange(callback func(Event)) func() {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.OnChange")
	if err != nil {
		return func() {}
	}
	if callback == nil {
		return func() {}
	}

	namespacePrefix := scopedStore.namespacePrefix()
	return backingStore.OnChange(func(event Event) {
		if !core.HasPrefix(event.Group, namespacePrefix) {
			return
		}
		callback(event)
	})
}

// Usage example: `removedRows, err := scopedStore.PurgeExpired(); if err != nil { return }; fmt.Println(removedRows)`
func (scopedStore *ScopedStore) PurgeExpired() (int64, error) {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.PurgeExpired")
	if err != nil {
		return 0, err
	}
	removedRows, err := backingStore.purgeExpiredMatchingGroupPrefix(scopedStore.namespacePrefix())
	if err != nil {
		return 0, core.E("store.ScopedStore.PurgeExpired", "delete expired rows", err)
	}
	return removedRows, nil
}

// Usage example: `err := scopedStore.Transaction(func(transaction *store.ScopedStoreTransaction) error { return transaction.SetIn("config", "colour", "blue") })`
func (scopedStore *ScopedStore) Transaction(operation func(*ScopedStoreTransaction) error) error {
	backingStore, err := scopedStore.resolvedStore("store.ScopedStore.Transaction")
	if err != nil {
		return err
	}
	if operation == nil {
		return core.E("store.ScopedStore.Transaction", "operation is nil", nil)
	}

	return backingStore.Transaction(func(transaction *StoreTransaction) error {
		scopedTransaction := &ScopedStoreTransaction{
			scopedStore:      scopedStore,
			storeTransaction: transaction,
		}
		return operation(scopedTransaction)
	})
}

func (scopedTransaction *ScopedStoreTransaction) resolvedTransaction(operation string) (*StoreTransaction, error) {
	if scopedTransaction == nil {
		return nil, core.E(operation, "scoped transaction is nil", nil)
	}
	if scopedTransaction.scopedStore == nil {
		return nil, core.E(operation, "scoped store is nil", nil)
	}
	if scopedTransaction.storeTransaction == nil {
		return nil, core.E(operation, "transaction is nil", nil)
	}
	if _, err := scopedTransaction.scopedStore.resolvedStore(operation); err != nil {
		return nil, err
	}
	return scopedTransaction.storeTransaction, nil
}

// Usage example: `value, err := transaction.Get("colour")`
func (scopedTransaction *ScopedStoreTransaction) Get(key string) (string, error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.Get")
	if err != nil {
		return "", err
	}
	return storeTransaction.Get(scopedTransaction.scopedStore.namespacedGroup(defaultScopedGroupName), key)
}

// Usage example: `value, err := transaction.GetFrom("config", "colour")`
func (scopedTransaction *ScopedStoreTransaction) GetFrom(group, key string) (string, error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.GetFrom")
	if err != nil {
		return "", err
	}
	return storeTransaction.Get(scopedTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := transaction.Set("colour", "blue"); err != nil { return err }`
func (scopedTransaction *ScopedStoreTransaction) Set(key, value string) error {
	return scopedTransaction.SetIn(defaultScopedGroupName, key, value)
}

// Usage example: `if err := transaction.SetIn("config", "colour", "blue"); err != nil { return err }`
func (scopedTransaction *ScopedStoreTransaction) SetIn(group, key, value string) error {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.SetIn")
	if err != nil {
		return err
	}
	if err := scopedTransaction.checkQuota("store.ScopedStoreTransaction.SetIn", group, key); err != nil {
		return err
	}
	return storeTransaction.Set(scopedTransaction.scopedStore.namespacedGroup(group), key, value)
}

// Usage example: `if err := transaction.SetWithTTL("sessions", "token", "abc123", time.Hour); err != nil { return err }`
func (scopedTransaction *ScopedStoreTransaction) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.SetWithTTL")
	if err != nil {
		return err
	}
	if err := scopedTransaction.checkQuota("store.ScopedStoreTransaction.SetWithTTL", group, key); err != nil {
		return err
	}
	return storeTransaction.SetWithTTL(scopedTransaction.scopedStore.namespacedGroup(group), key, value, timeToLive)
}

// Usage example: `if err := transaction.Delete("config", "colour"); err != nil { return err }`
func (scopedTransaction *ScopedStoreTransaction) Delete(group, key string) error {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.Delete")
	if err != nil {
		return err
	}
	return storeTransaction.Delete(scopedTransaction.scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := transaction.DeleteGroup("cache"); err != nil { return err }`
func (scopedTransaction *ScopedStoreTransaction) DeleteGroup(group string) error {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.DeleteGroup")
	if err != nil {
		return err
	}
	return storeTransaction.DeleteGroup(scopedTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `if err := transaction.DeletePrefix("config"); err != nil { return err }`
func (scopedTransaction *ScopedStoreTransaction) DeletePrefix(groupPrefix string) error {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.DeletePrefix")
	if err != nil {
		return err
	}
	return storeTransaction.DeletePrefix(scopedTransaction.scopedStore.namespacedGroup(groupPrefix))
}

// Usage example: `entries, err := transaction.GetAll("config")`
func (scopedTransaction *ScopedStoreTransaction) GetAll(group string) (map[string]string, error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.GetAll")
	if err != nil {
		return nil, err
	}
	return storeTransaction.GetAll(scopedTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `page, err := transaction.GetPage("config", 0, 25)`
func (scopedTransaction *ScopedStoreTransaction) GetPage(group string, offset, limit int) ([]KeyValue, error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.GetPage")
	if err != nil {
		return nil, err
	}
	return storeTransaction.GetPage(scopedTransaction.scopedStore.namespacedGroup(group), offset, limit)
}

// Usage example: `for entry, err := range transaction.All("config") { if err != nil { return }; fmt.Println(entry.Key, entry.Value) }`
func (scopedTransaction *ScopedStoreTransaction) All(group string) iter.Seq2[KeyValue, error] {
	return scopedTransaction.AllSeq(group)
}

// Usage example: `for entry, err := range transaction.AllSeq("config") { if err != nil { return }; fmt.Println(entry.Key, entry.Value) }`
func (scopedTransaction *ScopedStoreTransaction) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.AllSeq")
		if err != nil {
			yield(KeyValue{}, err)
			return
		}
		for entry, iterationErr := range storeTransaction.AllSeq(scopedTransaction.scopedStore.namespacedGroup(group)) {
			if iterationErr != nil {
				if !yield(KeyValue{}, iterationErr) {
					return
				}
				continue
			}
			if !yield(entry, nil) {
				return
			}
		}
	}
}

// Usage example: `count, err := transaction.Count("config")`
func (scopedTransaction *ScopedStoreTransaction) Count(group string) (int, error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.Count")
	if err != nil {
		return 0, err
	}
	return storeTransaction.Count(scopedTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `count, err := transaction.CountAll("config")`
// Usage example: `count, err := transaction.CountAll()`
func (scopedTransaction *ScopedStoreTransaction) CountAll(groupPrefix ...string) (int, error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.CountAll")
	if err != nil {
		return 0, err
	}
	return storeTransaction.CountAll(scopedTransaction.scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
}

// Usage example: `groups, err := transaction.Groups("config")`
// Usage example: `groups, err := transaction.Groups()`
func (scopedTransaction *ScopedStoreTransaction) Groups(groupPrefix ...string) ([]string, error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.Groups")
	if err != nil {
		return nil, err
	}

	groupNames, err := storeTransaction.Groups(scopedTransaction.scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
	if err != nil {
		return nil, err
	}
	for index, groupName := range groupNames {
		groupNames[index] = scopedTransaction.scopedStore.trimNamespacePrefix(groupName)
	}
	return groupNames, nil
}

// Usage example: `for groupName, err := range transaction.GroupsSeq("config") { if err != nil { return }; fmt.Println(groupName) }`
// Usage example: `for groupName, err := range transaction.GroupsSeq() { if err != nil { return }; fmt.Println(groupName) }`
func (scopedTransaction *ScopedStoreTransaction) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.GroupsSeq")
		if err != nil {
			yield("", err)
			return
		}
		namespacePrefix := scopedTransaction.scopedStore.namespacePrefix()
		for groupName, iterationErr := range storeTransaction.GroupsSeq(scopedTransaction.scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix))) {
			if iterationErr != nil {
				if !yield("", iterationErr) {
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

// Usage example: `renderedTemplate, err := transaction.Render("Hello {{ .name }}", "user")`
func (scopedTransaction *ScopedStoreTransaction) Render(templateSource, group string) (string, error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.Render")
	if err != nil {
		return "", err
	}
	return storeTransaction.Render(templateSource, scopedTransaction.scopedStore.namespacedGroup(group))
}

// Usage example: `parts, err := transaction.GetSplit("config", "hosts", ",")`
func (scopedTransaction *ScopedStoreTransaction) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.GetSplit")
	if err != nil {
		return nil, err
	}
	return storeTransaction.GetSplit(scopedTransaction.scopedStore.namespacedGroup(group), key, separator)
}

// Usage example: `fields, err := transaction.GetFields("config", "flags")`
func (scopedTransaction *ScopedStoreTransaction) GetFields(group, key string) (iter.Seq[string], error) {
	storeTransaction, err := scopedTransaction.resolvedTransaction("store.ScopedStoreTransaction.GetFields")
	if err != nil {
		return nil, err
	}
	return storeTransaction.GetFields(scopedTransaction.scopedStore.namespacedGroup(group), key)
}

// checkQuota("store.ScopedStoreTransaction.SetIn", "config", "colour") uses
// the transaction's own read state so staged writes inside the same
// transaction count towards the namespace limits.
func (scopedTransaction *ScopedStoreTransaction) checkQuota(operation, group, key string) error {
	if scopedTransaction == nil {
		return core.E(operation, "scoped transaction is nil", nil)
	}
	if scopedTransaction.scopedStore == nil {
		return core.E(operation, "scoped store is nil", nil)
	}
	if scopedTransaction.scopedStore.MaxKeys == 0 && scopedTransaction.scopedStore.MaxGroups == 0 {
		return nil
	}

	storeTransaction, err := scopedTransaction.resolvedTransaction(operation)
	if err != nil {
		return err
	}

	namespacedGroup := scopedTransaction.scopedStore.namespacedGroup(group)
	namespacePrefix := scopedTransaction.scopedStore.namespacePrefix()

	// Upserts never consume quota.
	_, err = storeTransaction.Get(namespacedGroup, key)
	if err == nil {
		return nil
	}
	if !core.Is(err, NotFoundError) {
		return core.E(operation, "quota check", err)
	}

	if scopedTransaction.scopedStore.MaxKeys > 0 {
		keyCount, err := storeTransaction.CountAll(namespacePrefix)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if keyCount >= scopedTransaction.scopedStore.MaxKeys {
			return core.E(operation, core.Sprintf("key limit (%d)", scopedTransaction.scopedStore.MaxKeys), QuotaExceededError)
		}
	}

	if scopedTransaction.scopedStore.MaxGroups > 0 {
		existingGroupCount, err := storeTransaction.Count(namespacedGroup)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if existingGroupCount == 0 {
			groupNames, err := storeTransaction.Groups(namespacePrefix)
			if err != nil {
				return core.E(operation, "quota check", err)
			}
			if len(groupNames) >= scopedTransaction.scopedStore.MaxGroups {
				return core.E(operation, core.Sprintf("group limit (%d)", scopedTransaction.scopedStore.MaxGroups), QuotaExceededError)
			}
		}
	}

	return nil
}

func (scopedStore *ScopedStore) forgetScopedWatcher(events <-chan Event) {
	if scopedStore == nil || events == nil {
		return
	}

	scopedStore.scopedWatchersLock.Lock()
	defer scopedStore.scopedWatchersLock.Unlock()
	if scopedStore.scopedWatchers == nil {
		return
	}
	delete(scopedStore.scopedWatchers, channelPointer(events))
}

func (scopedStore *ScopedStore) forgetAndStopScopedWatcher(events <-chan Event) {
	if scopedStore == nil || events == nil {
		return
	}

	scopedStore.scopedWatchersLock.Lock()
	binding := scopedStore.scopedWatchers[channelPointer(events)]
	if binding != nil {
		delete(scopedStore.scopedWatchers, channelPointer(events))
	}
	scopedStore.scopedWatchersLock.Unlock()

	if binding == nil {
		return
	}

	binding.stopOnce.Do(func() {
		close(binding.stop)
	})
	if binding.backingStore != nil {
		binding.backingStore.Unwatch("*", binding.underlyingEvents)
	}
	<-binding.done
}

// checkQuota("store.ScopedStore.Set", "config", "colour") returns nil when the
// namespace still has quota available and QuotaExceededError when a new key or
// group would exceed the configured limit. Existing keys are treated as
// upserts and do not consume quota.
func (scopedStore *ScopedStore) checkQuota(operation, group, key string) error {
	if scopedStore == nil {
		return core.E(operation, "scoped store is nil", nil)
	}
	if scopedStore.MaxKeys == 0 && scopedStore.MaxGroups == 0 {
		return nil
	}

	namespacedGroup := scopedStore.namespacedGroup(group)
	namespacePrefix := scopedStore.namespacePrefix()

	// Check if this is an upsert (key already exists) — upserts never exceed quota.
	_, err := scopedStore.backingStore.Get(namespacedGroup, key)
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
		keyCount, err := scopedStore.backingStore.CountAll(namespacePrefix)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if keyCount >= scopedStore.MaxKeys {
			return core.E(operation, core.Sprintf("key limit (%d)", scopedStore.MaxKeys), QuotaExceededError)
		}
	}

	// Check MaxGroups quota — only if this would create a new group.
	if scopedStore.MaxGroups > 0 {
		existingGroupCount, err := scopedStore.backingStore.Count(namespacedGroup)
		if err != nil {
			return core.E(operation, "quota check", err)
		}
		if existingGroupCount == 0 {
			// This group is new, so count existing namespace groups with the public helper.
			groupNames, err := scopedStore.backingStore.Groups(namespacePrefix)
			if err != nil {
				return core.E(operation, "quota check", err)
			}
			knownGroupCount := len(groupNames)
			if knownGroupCount >= scopedStore.MaxGroups {
				return core.E(operation, core.Sprintf("group limit (%d)", scopedStore.MaxGroups), QuotaExceededError)
			}
		}
	}

	return nil
}
