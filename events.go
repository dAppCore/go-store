package store

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// EventType describes the kind of store mutation that occurred.
// Usage example: `if event.Type == store.EventSet { return }`
type EventType int

const (
	// EventSet indicates a key was created or updated.
	// Usage example: `if event.Type == store.EventSet { return }`
	EventSet EventType = iota
	// EventDelete indicates a single key was removed.
	// Usage example: `if event.Type == store.EventDelete { return }`
	EventDelete
	// EventDeleteGroup indicates all keys in a group were removed.
	// Usage example: `if event.Type == store.EventDeleteGroup { return }`
	EventDeleteGroup
)

// String returns a human-readable label for the event type.
// Usage example: `label := store.EventSet.String()`
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

// Event describes a single store mutation. Key is empty for EventDeleteGroup.
// Value is only populated for EventSet.
// Usage example: `func handle(event store.Event) { _ = event.Group }`
type Event struct {
	Type      EventType
	Group     string
	Key       string
	Value     string
	Timestamp time.Time
}

// Watcher receives events matching a group/key filter. Use Store.Watch to
// create one and Store.Unwatch to stop delivery. Events is the primary
// read-only channel.
// Usage example: `watcher := storeInstance.Watch("config", "*"); for event := range watcher.Events { _ = event }`
type Watcher struct {
	// Events is the public read-only channel that consumers select on.
	Events <-chan Event

	// eventsChannel is the internal write channel (same underlying channel as Events).
	eventsChannel chan Event

	group string
	key   string
	id    uint64
}

// callbackEntry pairs a change callback with its unique ID for unregistration.
type callbackEntry struct {
	id       uint64
	callback func(Event)
}

// watcherBufferSize is the capacity of each watcher's buffered channel.
const watcherBufferSize = 16

// Watch creates a new watcher that receives events matching the given group and
// key. Use "*" as a wildcard: ("mygroup", "*") matches all keys in that group,
// ("*", "*") matches every mutation. The returned Watcher has a buffered
// channel (cap 16); events are dropped if the consumer falls behind.
// Usage example: `watcher := storeInstance.Watch("config", "*")`
func (storeInstance *Store) Watch(group, key string) *Watcher {
	eventsChannel := make(chan Event, watcherBufferSize)
	watcher := &Watcher{
		Events:        eventsChannel,
		eventsChannel: eventsChannel,
		group:         group,
		key:           key,
		id:            atomic.AddUint64(&storeInstance.nextRegistrationID, 1),
	}

	storeInstance.registryLock.Lock()
	storeInstance.watchers = append(storeInstance.watchers, watcher)
	storeInstance.registryLock.Unlock()

	return watcher
}

// Unwatch removes a watcher and closes its channel. Safe to call multiple
// times; subsequent calls are no-ops.
// Usage example: `storeInstance.Unwatch(watcher)`
func (storeInstance *Store) Unwatch(watcher *Watcher) {
	if watcher == nil {
		return
	}

	storeInstance.registryLock.Lock()
	defer storeInstance.registryLock.Unlock()

	storeInstance.watchers = slices.DeleteFunc(storeInstance.watchers, func(existing *Watcher) bool {
		if existing.id == watcher.id {
			close(watcher.eventsChannel)
			return true
		}
		return false
	})
}

// OnChange registers a callback that fires on every store mutation. Callbacks
// are called synchronously in the goroutine that performed the write, so the
// caller controls concurrency. Returns an unregister function; calling it stops
// future invocations.
// Usage example: `unregister := storeInstance.OnChange(func(event store.Event) {})`
//
// This is the integration point for go-ws and similar consumers:
//
//	unregister := storeInstance.OnChange(func(event store.Event) {
//	    hub.SendToChannel("store-events", event)
//	})
//	defer unregister()
func (storeInstance *Store) OnChange(callback func(Event)) func() {
	registrationID := atomic.AddUint64(&storeInstance.nextRegistrationID, 1)
	registrationRecord := callbackEntry{id: registrationID, callback: callback}

	storeInstance.registryLock.Lock()
	storeInstance.callbacks = append(storeInstance.callbacks, registrationRecord)
	storeInstance.registryLock.Unlock()

	// Return an idempotent unregister function.
	var once sync.Once
	return func() {
		once.Do(func() {
			storeInstance.registryLock.Lock()
			defer storeInstance.registryLock.Unlock()
			storeInstance.callbacks = slices.DeleteFunc(storeInstance.callbacks, func(existing callbackEntry) bool {
				return existing.id == registrationID
			})
		})
	}
}

// notify dispatches an event to all matching watchers and callbacks. It must be
// called after a successful DB write. Watcher sends are non-blocking — if a
// channel buffer is full the event is silently dropped to avoid blocking the
// writer.
func (storeInstance *Store) notify(event Event) {
	storeInstance.registryLock.RLock()
	defer storeInstance.registryLock.RUnlock()

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

	for _, callback := range storeInstance.callbacks {
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
