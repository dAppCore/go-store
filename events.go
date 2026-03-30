package store

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

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

// Usage example: `event := store.Event{Type: store.EventSet, Group: "config", Key: "theme", Value: "dark"}`
// Usage example: `event := store.Event{Type: store.EventDeleteGroup, Group: "config"}`
type Event struct {
	Type      EventType
	Group     string
	Key       string
	Value     string
	Timestamp time.Time
}

// Usage example: `watcher := storeInstance.Watch("config", "*"); defer storeInstance.Unwatch(watcher); for event := range watcher.Events { _ = event }`
type Watcher struct {
	// Usage example: `for event := range watcher.Events { _ = event }`
	Events <-chan Event

	// eventChannel is the internal write channel (same underlying channel as Events).
	eventChannel chan Event

	group string
	key   string
	id    uint64
}

// changeCallbackRegistration keeps a callback and its registration ID together
// so OnChange can unregister it later.
type changeCallbackRegistration struct {
	id       uint64
	callback func(Event)
}

// Each watcher keeps 16 pending events before non-blocking sends start
// dropping new ones.
const watcherEventBufferCapacity = 16

// Usage example: `watcher := storeInstance.Watch("*", "*")`
func (storeInstance *Store) Watch(group, key string) *Watcher {
	eventChannel := make(chan Event, watcherEventBufferCapacity)
	watcher := &Watcher{
		Events:       eventChannel,
		eventChannel: eventChannel,
		group:        group,
		key:          key,
		id:           atomic.AddUint64(&storeInstance.nextRegistrationID, 1),
	}

	storeInstance.watchersLock.Lock()
	storeInstance.watchers = append(storeInstance.watchers, watcher)
	storeInstance.watchersLock.Unlock()

	return watcher
}

// Usage example: `storeInstance.Unwatch(watcher)`
func (storeInstance *Store) Unwatch(watcher *Watcher) {
	if watcher == nil {
		return
	}

	storeInstance.watchersLock.Lock()
	defer storeInstance.watchersLock.Unlock()

	storeInstance.watchers = slices.DeleteFunc(storeInstance.watchers, func(existing *Watcher) bool {
		if existing.id == watcher.id {
			close(watcher.eventChannel)
			return true
		}
		return false
	})
}

// Usage example: `events := make(chan store.Event, 1); unregister := storeInstance.OnChange(func(event store.Event) { events <- event }); defer unregister()`
func (storeInstance *Store) OnChange(callback func(Event)) func() {
	registrationID := atomic.AddUint64(&storeInstance.nextRegistrationID, 1)
	callbackRegistration := changeCallbackRegistration{id: registrationID, callback: callback}

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
				return existing.id == registrationID
			})
		})
	}
}

// notify dispatches an event to all matching watchers and callbacks. It must be
// called after a successful DB write. Watcher sends are non-blocking — if a
// channel buffer is full the event is silently dropped to avoid blocking the
// writer. Callbacks are copied under a separate lock and invoked after the
// lock is released, so a callback can register or unregister other watchers or
// callbacks without deadlocking.
func (storeInstance *Store) notify(event Event) {
	storeInstance.watchersLock.RLock()
	for _, watcher := range storeInstance.watchers {
		if !watcherMatches(watcher, event) {
			continue
		}
		// Non-blocking send: drop the event rather than block the writer.
		select {
		case watcher.eventChannel <- event:
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

// watcherMatches reports whether a watcher's filter matches the given event.
func watcherMatches(watcher *Watcher, event Event) bool {
	if watcher.group != "*" && watcher.group != event.Group {
		return false
	}
	if watcher.key != "*" && watcher.key != event.Key {
		// EventDeleteGroup has an empty Key — only wildcard watchers or
		// group-level watchers (key="*") should receive it.
		return false
	}
	return true
}
