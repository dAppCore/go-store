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
// Usage example: `func handle(e store.Event) { _ = e.Group }`
type Event struct {
	Type      EventType
	Group     string
	Key       string
	Value     string
	Timestamp time.Time
}

// Watcher receives events matching a group/key filter. Use Store.Watch to
// create one and Store.Unwatch to stop delivery.
// Usage example: `watcher := st.Watch("config", "*")`
type Watcher struct {
	// Ch is the public read-only channel that consumers select on.
	Ch <-chan Event

	// ch is the internal write channel (same underlying channel as Ch).
	ch chan Event

	group string
	key   string
	id    uint64
}

// callbackEntry pairs a change callback with its unique ID for unregistration.
type callbackEntry struct {
	id uint64
	fn func(Event)
}

// watcherBufSize is the capacity of each watcher's buffered channel.
const watcherBufSize = 16

// Watch creates a new watcher that receives events matching the given group and
// key. Use "*" as a wildcard: ("mygroup", "*") matches all keys in that group,
// ("*", "*") matches every mutation. The returned Watcher has a buffered
// channel (cap 16); events are dropped if the consumer falls behind.
// Usage example: `watcher := st.Watch("config", "*")`
func (s *Store) Watch(group, key string) *Watcher {
	ch := make(chan Event, watcherBufSize)
	w := &Watcher{
		Ch:    ch,
		ch:    ch,
		group: group,
		key:   key,
		id:    atomic.AddUint64(&s.nextID, 1),
	}

	s.mu.Lock()
	s.watchers = append(s.watchers, w)
	s.mu.Unlock()

	return w
}

// Unwatch removes a watcher and closes its channel. Safe to call multiple
// times; subsequent calls are no-ops.
// Usage example: `st.Unwatch(watcher)`
func (s *Store) Unwatch(w *Watcher) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.watchers = slices.DeleteFunc(s.watchers, func(existing *Watcher) bool {
		if existing.id == w.id {
			close(w.ch)
			return true
		}
		return false
	})
}

// OnChange registers a callback that fires on every store mutation. Callbacks
// are called synchronously in the goroutine that performed the write, so the
// caller controls concurrency. Returns an unregister function; calling it stops
// future invocations.
// Usage example: `unreg := st.OnChange(func(e store.Event) {})`
//
// This is the integration point for go-ws and similar consumers:
//
//	unreg := store.OnChange(func(e store.Event) {
//	    hub.SendToChannel("store-events", e)
//	})
//	defer unreg()
func (s *Store) OnChange(fn func(Event)) func() {
	id := atomic.AddUint64(&s.nextID, 1)
	entry := callbackEntry{id: id, fn: fn}

	s.mu.Lock()
	s.callbacks = append(s.callbacks, entry)
	s.mu.Unlock()

	// Return an idempotent unregister function.
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.callbacks = slices.DeleteFunc(s.callbacks, func(cb callbackEntry) bool {
				return cb.id == id
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

	for _, w := range s.watchers {
		if !watcherMatches(w, e) {
			continue
		}
		// Non-blocking send: drop the event rather than block the writer.
		select {
		case w.ch <- e:
		default:
		}
	}

	for _, cb := range s.callbacks {
		cb.fn(e)
	}
}

// watcherMatches reports whether a watcher's filter matches the given event.
func watcherMatches(w *Watcher, e Event) bool {
	if w.group != "*" && w.group != e.Group {
		return false
	}
	if w.key != "*" && w.key != e.Key {
		// EventDeleteGroup has an empty Key — only wildcard watchers or
		// group-level watchers (key="*") should receive it.
		return false
	}
	return true
}
