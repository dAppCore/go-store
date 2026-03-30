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
// Usage example: `watcher := st.Watch("config", "*"); for event := range watcher.Events { _ = event }`
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
// Usage example: `watcher := st.Watch("config", "*")`
func (s *Store) Watch(group, key string) *Watcher {
	eventsChannel := make(chan Event, watcherBufferSize)
	watcher := &Watcher{
		Events:        eventsChannel,
		eventsChannel: eventsChannel,
		group:         group,
		key:           key,
		id:            atomic.AddUint64(&s.nextRegistrationID, 1),
	}

	s.mu.Lock()
	s.watchers = append(s.watchers, watcher)
	s.mu.Unlock()

	return watcher
}

// Unwatch removes a watcher and closes its channel. Safe to call multiple
// times; subsequent calls are no-ops.
// Usage example: `st.Unwatch(watcher)`
func (s *Store) Unwatch(watcher *Watcher) {
	if watcher == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.watchers = slices.DeleteFunc(s.watchers, func(existing *Watcher) bool {
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
// Usage example: `unreg := st.OnChange(func(event store.Event) {})`
//
// This is the integration point for go-ws and similar consumers:
//
//	unreg := store.OnChange(func(event store.Event) {
//	    hub.SendToChannel("store-events", event)
//	})
//	defer unreg()
func (s *Store) OnChange(callback func(Event)) func() {
	registrationID := atomic.AddUint64(&s.nextRegistrationID, 1)
	registrationRecord := callbackEntry{id: registrationID, callback: callback}

	s.mu.Lock()
	s.callbacks = append(s.callbacks, registrationRecord)
	s.mu.Unlock()

	// Return an idempotent unregister function.
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.callbacks = slices.DeleteFunc(s.callbacks, func(existing callbackEntry) bool {
				return existing.id == registrationID
			})
		})
	}
}

// notify dispatches an event to all matching watchers and callbacks. It must be
// called after a successful DB write. Watcher sends are non-blocking — if a
// channel buffer is full the event is silently dropped to avoid blocking the
// writer.
func (s *Store) notify(e Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, watcher := range s.watchers {
		if !watcherMatches(watcher, e) {
			continue
		}
		// Non-blocking send: drop the event rather than block the writer.
		select {
		case watcher.eventsChannel <- e:
		default:
		}
	}

	for _, callback := range s.callbacks {
		callback.callback(e)
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
