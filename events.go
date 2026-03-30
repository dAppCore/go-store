package store

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// EventType identifies the kind of mutation emitted by Store.
// Usage example: `if event.Type == store.EventSet { return }`
type EventType int

const (
	// Usage example: `if event.Type == store.EventSet { return }`
	EventSet EventType = iota
	// Usage example: `if event.Type == store.EventDelete { return }`
	EventDelete
	// Usage example: `if event.Type == store.EventDeleteGroup { return }`
	EventDeleteGroup
)

// Usage example: `label := store.EventDeleteGroup.String()`
func (t EventType) String() string {
	switch t {
	case EventSet:
		return "set"
	case EventDelete:
		return "delete"
	case EventDeleteGroup:
		return "delete_group"
	default:
		return "unknown"
	}
}

// Event describes one mutation delivered to watchers and callbacks.
// Usage example: `event := store.Event{Type: store.EventSet, Group: "config", Key: "colour", Value: "blue"}`
// Usage example: `event := store.Event{Type: store.EventDeleteGroup, Group: "config"}`
type Event struct {
	// Usage example: `if event.Type == store.EventDeleteGroup { return }`
	Type EventType
	// Usage example: `if event.Group == "config" { return }`
	Group string
	// Usage example: `if event.Key == "colour" { return }`
	Key string
	// Usage example: `if event.Value == "blue" { return }`
	Value string
	// Usage example: `if event.Timestamp.IsZero() { return }`
	Timestamp time.Time
}

// Watcher exposes the read-only event stream returned by Watch.
// Usage example: `watcher := storeInstance.Watch("config", "*"); defer storeInstance.Unwatch(watcher); for event := range watcher.Events { if event.Type == EventDeleteGroup { return } }`
type Watcher struct {
	// Usage example: `for event := range watcher.Events { if event.Key == "colour" { return } }`
	Events <-chan Event

	// eventsChannel is the internal write channel (same underlying channel as Events).
	eventsChannel chan Event

	groupPattern   string
	keyPattern     string
	registrationID uint64
}

// changeCallbackRegistration keeps the registration ID so unregister can remove
// the exact callback later.
type changeCallbackRegistration struct {
	registrationID uint64
	callback       func(Event)
}

// Watch("config", "*") can hold 16 pending events before non-blocking sends
// start dropping new ones.
const watcherEventBufferCapacity = 16

// Watch registers a buffered subscription for matching mutations.
// Usage example: `watcher := storeInstance.Watch("*", "*")`
func (storeInstance *Store) Watch(group, key string) *Watcher {
	eventChannel := make(chan Event, watcherEventBufferCapacity)
	watcher := &Watcher{
		Events:         eventChannel,
		eventsChannel:  eventChannel,
		groupPattern:   group,
		keyPattern:     key,
		registrationID: atomic.AddUint64(&storeInstance.nextWatcherRegistrationID, 1),
	}

	storeInstance.watchersLock.Lock()
	storeInstance.watchers = append(storeInstance.watchers, watcher)
	storeInstance.watchersLock.Unlock()

	return watcher
}

// Unwatch removes a watcher and closes its event stream.
// Usage example: `storeInstance.Unwatch(watcher)`
func (storeInstance *Store) Unwatch(watcher *Watcher) {
	if watcher == nil {
		return
	}

	storeInstance.watchersLock.Lock()
	defer storeInstance.watchersLock.Unlock()

	storeInstance.watchers = slices.DeleteFunc(storeInstance.watchers, func(existing *Watcher) bool {
		if existing.registrationID == watcher.registrationID {
			close(watcher.eventsChannel)
			return true
		}
		return false
	})
}

// OnChange registers a synchronous mutation callback.
// Usage example: `events := make(chan store.Event, 1); unregister := storeInstance.OnChange(func(event store.Event) { events <- event }); defer unregister()`
func (storeInstance *Store) OnChange(callback func(Event)) func() {
	registrationID := atomic.AddUint64(&storeInstance.nextCallbackRegistrationID, 1)
	callbackRegistration := changeCallbackRegistration{registrationID: registrationID, callback: callback}

	storeInstance.callbacksLock.Lock()
	storeInstance.callbacks = append(storeInstance.callbacks, callbackRegistration)
	storeInstance.callbacksLock.Unlock()

	// Return an idempotent unregister function.
	var once sync.Once
	return func() {
		once.Do(func() {
			storeInstance.callbacksLock.Lock()
			defer storeInstance.callbacksLock.Unlock()
			storeInstance.callbacks = slices.DeleteFunc(storeInstance.callbacks, func(existing changeCallbackRegistration) bool {
				return existing.registrationID == registrationID
			})
		})
	}
}

// notify(Event{Type: EventSet, Group: "config", Key: "colour", Value: "blue"})
// dispatches matching watchers and callbacks after a successful write. If a
// watcher buffer is full, the event is dropped instead of blocking the writer.
// Callbacks are copied under a separate lock and invoked after the lock is
// released, so they can register or unregister subscriptions without
// deadlocking.
func (storeInstance *Store) notify(event Event) {
	storeInstance.watchersLock.RLock()
	for _, watcher := range storeInstance.watchers {
		if !watcherMatches(watcher, event) {
			continue
		}
		// Non-blocking send: drop the event rather than block the writer.
		select {
		case watcher.eventsChannel <- event:
		default:
		}
	}
	storeInstance.watchersLock.RUnlock()

	storeInstance.callbacksLock.RLock()
	callbacks := append([]changeCallbackRegistration(nil), storeInstance.callbacks...)
	storeInstance.callbacksLock.RUnlock()

	for _, callback := range callbacks {
		callback.callback(event)
	}
}

// watcherMatches reports whether Watch("config", "*") should receive
// Event{Group: "config", Key: "colour"}.
func watcherMatches(watcher *Watcher, event Event) bool {
	if watcher.groupPattern != "*" && watcher.groupPattern != event.Group {
		return false
	}
	if watcher.keyPattern != "*" && watcher.keyPattern != event.Key {
		// EventDeleteGroup has an empty Key — only wildcard watchers or
		// group-level watchers (key="*") should receive it.
		return false
	}
	return true
}
