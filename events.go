package store

import (
	"reflect"
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

// changeCallbackRegistration keeps the registration ID so unregister can remove
// the exact callback later.
type changeCallbackRegistration struct {
	registrationID uint64
	group          string
	callback       func(Event)
}

// Watch("config") can hold 16 pending events before non-blocking sends start
// dropping new ones.
const watcherEventBufferCapacity = 16

// Watch registers a buffered subscription for one group.
// Usage example: `events := storeInstance.Watch("config")`
// Usage example: `events := storeInstance.Watch("*")`
func (storeInstance *Store) Watch(group string) <-chan Event {
	eventChannel := make(chan Event, watcherEventBufferCapacity)

	storeInstance.watchersLock.Lock()
	if storeInstance.watchers == nil {
		storeInstance.watchers = make(map[string][]chan Event)
	}
	storeInstance.watchers[group] = append(storeInstance.watchers[group], eventChannel)
	storeInstance.watchersLock.Unlock()

	return eventChannel
}

// Unwatch removes a watcher for one group and closes its event stream.
// Usage example: `storeInstance.Unwatch("config", events)`
func (storeInstance *Store) Unwatch(group string, events <-chan Event) {
	if events == nil {
		return
	}

	storeInstance.watchersLock.Lock()
	defer storeInstance.watchersLock.Unlock()

	registeredEvents := storeInstance.watchers[group]
	if len(registeredEvents) == 0 {
		return
	}

	eventsPointer := channelPointer(events)
	nextRegisteredEvents := registeredEvents[:0]
	removed := false
	for _, registeredChannel := range registeredEvents {
		if channelPointer(registeredChannel) == eventsPointer {
			if !removed {
				close(registeredChannel)
				removed = true
			}
			continue
		}
		nextRegisteredEvents = append(nextRegisteredEvents, registeredChannel)
	}
	if !removed {
		return
	}
	if len(nextRegisteredEvents) == 0 {
		delete(storeInstance.watchers, group)
		return
	}
	storeInstance.watchers[group] = nextRegisteredEvents
}

// OnChange registers a synchronous mutation callback.
// Usage example: `unregister := storeInstance.OnChange(func(event store.Event) { fmt.Println(event.Group, event.Key, event.Value) })`
// Usage example: `unregister := storeInstance.OnChange("config", func(key, value string) { fmt.Println(key, value) })`
func (storeInstance *Store) OnChange(arguments ...any) func() {
	if len(arguments) == 0 {
		return func() {}
	}

	var (
		callbackGroup string
		callback      func(Event)
	)

	switch len(arguments) {
	case 1:
		switch typedCallback := arguments[0].(type) {
		case func(Event):
			callback = typedCallback
		default:
			return func() {}
		}
	case 2:
		groupName, ok := arguments[0].(string)
		if !ok {
			return func() {}
		}
		callbackGroup = groupName
		switch typedCallback := arguments[1].(type) {
		case func(Event):
			callback = typedCallback
		case func(string, string):
			callback = func(event Event) {
				typedCallback(event.Key, event.Value)
			}
		default:
			return func() {}
		}
	default:
		return func() {}
	}

	if callback == nil {
		return func() {}
	}

	registrationID := atomic.AddUint64(&storeInstance.nextCallbackRegistrationID, 1)
	callbackRegistration := changeCallbackRegistration{registrationID: registrationID, group: callbackGroup, callback: callback}

	storeInstance.callbacksLock.Lock()
	storeInstance.callbacks = append(storeInstance.callbacks, callbackRegistration)
	storeInstance.callbacksLock.Unlock()

	// Return an idempotent unregister function.
	var once sync.Once
	return func() {
		once.Do(func() {
			storeInstance.callbacksLock.Lock()
			defer storeInstance.callbacksLock.Unlock()
			for i := range storeInstance.callbacks {
				if storeInstance.callbacks[i].registrationID == registrationID {
					storeInstance.callbacks = append(storeInstance.callbacks[:i], storeInstance.callbacks[i+1:]...)
					return
				}
			}
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
	for _, registeredChannel := range storeInstance.watchers["*"] {
		select {
		case registeredChannel <- event:
		default:
		}
	}
	for _, registeredChannel := range storeInstance.watchers[event.Group] {
		select {
		case registeredChannel <- event:
		default:
		}
	}
	storeInstance.watchersLock.RUnlock()

	storeInstance.callbacksLock.RLock()
	callbacks := append([]changeCallbackRegistration(nil), storeInstance.callbacks...)
	storeInstance.callbacksLock.RUnlock()

	for _, callback := range callbacks {
		if callback.group != "" && callback.group != "*" && callback.group != event.Group {
			continue
		}
		callback.callback(event)
	}
}

func channelPointer(eventChannel <-chan Event) uintptr {
	if eventChannel == nil {
		return 0
	}
	return reflect.ValueOf(eventChannel).Pointer()
}
