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

// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a"); if scopedStore == nil { return }; if err := scopedStore.Set("config", "colour", "blue"); err != nil { return }`
type ScopedStore struct {
	backingStore *Store
	namespace    string
	MaxKeys      int
	MaxGroups    int

	scopedWatchersLock sync.Mutex
	scopedWatchers     map[uintptr]*scopedWatcherBinding
}

type scopedWatcherBinding struct {
	storeInstance    *Store
	underlyingEvents <-chan Event
	done             chan struct{}
	stop             chan struct{}
	stopOnce         sync.Once
}

func (scopedStore *ScopedStore) storeInstance(operation string) (*Store, error) {
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

// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a")`
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

// Usage example: `scopedStore, err := store.NewScopedWithQuota(storeInstance, "tenant-a", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}); if err != nil { return }`
func NewScopedWithQuota(storeInstance *Store, namespace string, quota QuotaConfig) (*ScopedStore, error) {
	scopedStore := NewScoped(storeInstance, namespace)
	if scopedStore == nil {
		if storeInstance == nil {
			return nil, core.E("store.NewScopedWithQuota", "store instance is nil", nil)
		}
		return nil, core.E("store.NewScopedWithQuota", core.Sprintf("namespace %q is invalid; use names like %q or %q", namespace, "tenant-a", "tenant-42"), nil)
	}
	if quota.MaxKeys < 0 || quota.MaxGroups < 0 {
		return nil, core.E(
			"store.NewScopedWithQuota",
			core.Sprintf("quota values must be zero or positive; got MaxKeys=%d MaxGroups=%d", quota.MaxKeys, quota.MaxGroups),
			nil,
		)
	}
	scopedStore.MaxKeys = quota.MaxKeys
	scopedStore.MaxGroups = quota.MaxGroups
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

// Usage example: `scopedStore := store.NewScoped(storeInstance, "tenant-a"); if scopedStore == nil { return }; namespace := scopedStore.Namespace(); fmt.Println(namespace)`
func (scopedStore *ScopedStore) Namespace() string {
	if scopedStore == nil {
		return ""
	}
	return scopedStore.namespace
}

// Usage example: `colourValue, err := scopedStore.Get("colour")`
func (scopedStore *ScopedStore) Get(key string) (string, error) {
	storeInstance, err := scopedStore.storeInstance("store.Get")
	if err != nil {
		return "", err
	}
	return storeInstance.Get(scopedStore.namespacedGroup(defaultScopedGroupName), key)
}

// Usage example: `colourValue, err := scopedStore.GetFrom("config", "colour")`
func (scopedStore *ScopedStore) GetFrom(group, key string) (string, error) {
	storeInstance, err := scopedStore.storeInstance("store.Get")
	if err != nil {
		return "", err
	}
	return storeInstance.Get(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.Set("colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) Set(key, value string) error {
	return scopedStore.SetIn(defaultScopedGroupName, key, value)
}

// Usage example: `if err := scopedStore.SetIn("config", "colour", "blue"); err != nil { return }`
func (scopedStore *ScopedStore) SetIn(group, key, value string) error {
	storeInstance, err := scopedStore.storeInstance("store.Set")
	if err != nil {
		return err
	}
	if err := scopedStore.checkQuota("store.ScopedStore.SetIn", group, key); err != nil {
		return err
	}
	return storeInstance.Set(scopedStore.namespacedGroup(group), key, value)
}

// Usage example: `if err := scopedStore.SetWithTTL("sessions", "token", "abc123", time.Hour); err != nil { return }`
func (scopedStore *ScopedStore) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	storeInstance, err := scopedStore.storeInstance("store.SetWithTTL")
	if err != nil {
		return err
	}
	if err := scopedStore.checkQuota("store.ScopedStore.SetWithTTL", group, key); err != nil {
		return err
	}
	return storeInstance.SetWithTTL(scopedStore.namespacedGroup(group), key, value, timeToLive)
}

// Usage example: `if err := scopedStore.Delete("config", "colour"); err != nil { return }`
func (scopedStore *ScopedStore) Delete(group, key string) error {
	storeInstance, err := scopedStore.storeInstance("store.Delete")
	if err != nil {
		return err
	}
	return storeInstance.Delete(scopedStore.namespacedGroup(group), key)
}

// Usage example: `if err := scopedStore.DeleteGroup("cache"); err != nil { return }`
func (scopedStore *ScopedStore) DeleteGroup(group string) error {
	storeInstance, err := scopedStore.storeInstance("store.DeleteGroup")
	if err != nil {
		return err
	}
	return storeInstance.DeleteGroup(scopedStore.namespacedGroup(group))
}

// Usage example: `colourEntries, err := scopedStore.GetAll("config")`
func (scopedStore *ScopedStore) GetAll(group string) (map[string]string, error) {
	storeInstance, err := scopedStore.storeInstance("store.GetAll")
	if err != nil {
		return nil, err
	}
	return storeInstance.GetAll(scopedStore.namespacedGroup(group))
}

// Usage example: `for entry, err := range scopedStore.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) All(group string) iter.Seq2[KeyValue, error] {
	storeInstance, err := scopedStore.storeInstance("store.All")
	if err != nil {
		return func(yield func(KeyValue, error) bool) {
			yield(KeyValue{}, err)
		}
	}
	return storeInstance.All(scopedStore.namespacedGroup(group))
}

// Usage example: `for entry, err := range scopedStore.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (scopedStore *ScopedStore) AllSeq(group string) iter.Seq2[KeyValue, error] {
	storeInstance, err := scopedStore.storeInstance("store.All")
	if err != nil {
		return func(yield func(KeyValue, error) bool) {
			yield(KeyValue{}, err)
		}
	}
	return storeInstance.AllSeq(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.Count("config")`
func (scopedStore *ScopedStore) Count(group string) (int, error) {
	storeInstance, err := scopedStore.storeInstance("store.Count")
	if err != nil {
		return 0, err
	}
	return storeInstance.Count(scopedStore.namespacedGroup(group))
}

// Usage example: `keyCount, err := scopedStore.CountAll("config")`
// Usage example: `keyCount, err := scopedStore.CountAll()`
func (scopedStore *ScopedStore) CountAll(groupPrefix ...string) (int, error) {
	storeInstance, err := scopedStore.storeInstance("store.CountAll")
	if err != nil {
		return 0, err
	}
	return storeInstance.CountAll(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
}

// Usage example: `groupNames, err := scopedStore.Groups("config")`
// Usage example: `groupNames, err := scopedStore.Groups()`
func (scopedStore *ScopedStore) Groups(groupPrefix ...string) ([]string, error) {
	storeInstance, err := scopedStore.storeInstance("store.Groups")
	if err != nil {
		return nil, err
	}

	groupNames, err := storeInstance.Groups(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix)))
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
		storeInstance, err := scopedStore.storeInstance("store.GroupsSeq")
		if err != nil {
			yield("", err)
			return
		}
		namespacePrefix := scopedStore.namespacePrefix()
		for groupName, err := range storeInstance.GroupsSeq(scopedStore.namespacedGroup(firstOrEmptyString(groupPrefix))) {
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
	storeInstance, err := scopedStore.storeInstance("store.Render")
	if err != nil {
		return "", err
	}
	return storeInstance.Render(templateSource, scopedStore.namespacedGroup(group))
}

// Usage example: `parts, err := scopedStore.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (scopedStore *ScopedStore) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	storeInstance, err := scopedStore.storeInstance("store.GetSplit")
	if err != nil {
		return nil, err
	}
	return storeInstance.GetSplit(scopedStore.namespacedGroup(group), key, separator)
}

// Usage example: `fields, err := scopedStore.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (scopedStore *ScopedStore) GetFields(group, key string) (iter.Seq[string], error) {
	storeInstance, err := scopedStore.storeInstance("store.GetFields")
	if err != nil {
		return nil, err
	}
	return storeInstance.GetFields(scopedStore.namespacedGroup(group), key)
}

// Usage example: `events := scopedStore.Watch("config")`
func (scopedStore *ScopedStore) Watch(group string) <-chan Event {
	storeInstance, err := scopedStore.storeInstance("store.Watch")
	if err != nil {
		return closedEventChannel()
	}
	if group != "*" {
		return storeInstance.Watch(scopedStore.namespacedGroup(group))
	}

	forwardedEvents := make(chan Event, watcherEventBufferCapacity)
	binding := &scopedWatcherBinding{
		storeInstance:    storeInstance,
		underlyingEvents: storeInstance.Watch("*"),
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
			case <-storeInstance.purgeContext.Done():
				return
			}
		}
	}()

	return forwardedEvents
}

// Usage example: `scopedStore.Unwatch("config", events)`
func (scopedStore *ScopedStore) Unwatch(group string, events <-chan Event) {
	storeInstance, err := scopedStore.storeInstance("store.Unwatch")
	if err != nil {
		return
	}
	if group == "*" {
		scopedStore.forgetAndStopScopedWatcher(events)
		return
	}
	storeInstance.Unwatch(scopedStore.namespacedGroup(group), events)
}

// Usage example: `unregister := scopedStore.OnChange(func(event store.Event) { fmt.Println(event.Group, event.Key) })`
func (scopedStore *ScopedStore) OnChange(callback func(Event)) func() {
	storeInstance, err := scopedStore.storeInstance("store.OnChange")
	if err != nil {
		return func() {}
	}
	if callback == nil {
		return func() {}
	}

	namespacePrefix := scopedStore.namespacePrefix()
	return storeInstance.OnChange(func(event Event) {
		if !core.HasPrefix(event.Group, namespacePrefix) {
			return
		}
		callback(event)
	})
}

// Usage example: `removedRows, err := scopedStore.PurgeExpired(); if err != nil { return }; fmt.Println(removedRows)`
func (scopedStore *ScopedStore) PurgeExpired() (int64, error) {
	storeInstance, err := scopedStore.storeInstance("store.PurgeExpired")
	if err != nil {
		return 0, err
	}
	removedRows, err := storeInstance.purgeExpiredMatchingGroupPrefix(scopedStore.namespacePrefix())
	if err != nil {
		return 0, core.E("store.ScopedStore.PurgeExpired", "delete expired rows", err)
	}
	return removedRows, nil
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
	if binding.storeInstance != nil {
		binding.storeInstance.Unwatch("*", binding.underlyingEvents)
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
			// This group is new — check if adding it would exceed the group limit.
			knownGroupCount := 0
			for _, iterationErr := range scopedStore.backingStore.GroupsSeq(namespacePrefix) {
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
