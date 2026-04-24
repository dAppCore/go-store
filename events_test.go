package store

import (
	"sync"
	"testing"
	"time"

	core "dappco.re/go/core"
)

func TestEvents_Watch_Good_Group(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("config")
	defer storeInstance.Unwatch("config", events)

	assertNoError(t, storeInstance.Set("config", "theme", "dark"))
	assertNoError(t, storeInstance.Set("config", "colour", "blue"))

	received := drainEvents(events, 2, time.Second)
	assertLen(t, received, 2)
	assertEqual(t, "theme", received[0].Key)
	assertEqual(t, "colour", received[1].Key)
	assertEqual(t, "config", received[0].Group)
	assertEqual(t, "config", received[1].Group)
}

func TestEvents_Watch_Good_WildcardGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("*")
	defer storeInstance.Unwatch("*", events)

	assertNoError(t, storeInstance.Set("g1", "k1", "v1"))
	assertNoError(t, storeInstance.Set("g2", "k2", "v2"))
	assertNoError(t, storeInstance.Delete("g1", "k1"))
	assertNoError(t, storeInstance.DeleteGroup("g2"))

	received := drainEvents(events, 4, time.Second)
	assertLen(t, received, 4)
	assertEqual(t, EventSet, received[0].Type)
	assertEqual(t, EventSet, received[1].Type)
	assertEqual(t, EventDelete, received[2].Type)
	assertEqual(t, EventDeleteGroup, received[3].Type)
}

func TestEvents_Unwatch_Good_StopsDelivery(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	storeInstance.Unwatch("g", events)

	_, open := <-events
	assertFalsef(t, open, "channel should be closed after Unwatch")

	assertNoError(t, storeInstance.Set("g", "k", "v"))
}

func TestEvents_Unwatch_Good_Idempotent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	storeInstance.Unwatch("g", events)
	storeInstance.Unwatch("g", events)
}

func TestEvents_Close_Good_ClosesWatcherChannels(t *testing.T) {
	storeInstance, _ := New(":memory:")

	events := storeInstance.Watch("g")
	assertNoError(t, storeInstance.Close())

	_, open := <-events
	assertFalsef(t, open, "channel should be closed after Close")
}

func TestEvents_Unwatch_Good_NilChannel(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	storeInstance.Unwatch("g", nil)
}

func TestEvents_Watch_Good_DeleteEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	defer storeInstance.Unwatch("g", events)

	assertNoError(t, storeInstance.Set("g", "k", "v"))
	<-events

	assertNoError(t, storeInstance.Delete("g", "k"))

	select {
	case event := <-events:
		assertEqual(t, EventDelete, event.Type)
		assertEqual(t, "g", event.Group)
		assertEqual(t, "k", event.Key)
		assertEmpty(t, event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete event")
	}
}

func TestEvents_Watch_Good_DeleteGroupEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	defer storeInstance.Unwatch("g", events)

	assertNoError(t, storeInstance.Set("g", "a", "1"))
	assertNoError(t, storeInstance.Set("g", "b", "2"))
	<-events
	<-events

	assertNoError(t, storeInstance.DeleteGroup("g"))

	select {
	case event := <-events:
		assertEqual(t, EventDeleteGroup, event.Type)
		assertEqual(t, "g", event.Group)
		assertEmpty(t, event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete_group event")
	}
}

func TestEvents_OnChange_Good_Fires(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	var events []Event
	var eventsMutex sync.Mutex

	unregister := storeInstance.OnChange(func(event Event) {
		eventsMutex.Lock()
		events = append(events, event)
		eventsMutex.Unlock()
	})
	defer unregister()

	assertNoError(t, storeInstance.Set("g", "k", "v"))
	assertNoError(t, storeInstance.Delete("g", "k"))

	eventsMutex.Lock()
	defer eventsMutex.Unlock()
	assertLen(t, events, 2)
	assertEqual(t, EventSet, events[0].Type)
	assertEqual(t, EventDelete, events[1].Type)
}

func TestEvents_OnChange_Good_GroupFilteredCallback(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	var seen []string
	unregister := storeInstance.OnChange(func(event Event) {
		if event.Group != "config" {
			return
		}
		seen = append(seen, event.Key+"="+event.Value)
	})
	defer unregister()

	assertNoError(t, storeInstance.Set("config", "theme", "dark"))
	assertNoError(t, storeInstance.Set("other", "theme", "light"))

	assertEqual(t, []string{"theme=dark"}, seen)
}

func TestEvents_OnChange_Good_ReentrantSubscriptionChanges(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	var (
		seen             []string
		seenMutex        sync.Mutex
		nestedEvents     <-chan Event
		nestedActive     bool
		nestedStopped    bool
		unregisterNested = func() {}
	)

	unregisterPrimary := storeInstance.OnChange(func(event Event) {
		seenMutex.Lock()
		seen = append(seen, event.Key)
		seenMutex.Unlock()

		if !nestedActive {
			nestedEvents = storeInstance.Watch("config")
			unregisterNested = storeInstance.OnChange(func(nested Event) {
				seenMutex.Lock()
				seen = append(seen, "nested:"+nested.Key)
				seenMutex.Unlock()
			})
			nestedActive = true
			return
		}

		if !nestedStopped {
			storeInstance.Unwatch("config", nestedEvents)
			unregisterNested()
			nestedStopped = true
		}
	})
	defer unregisterPrimary()

	assertNoError(t, storeInstance.Set("config", "first", "dark"))
	assertNoError(t, storeInstance.Set("config", "second", "light"))
	assertNoError(t, storeInstance.Set("config", "third", "blue"))

	seenMutex.Lock()
	assertEqual(t, []string{"first", "second", "nested:second", "third"}, seen)
	seenMutex.Unlock()

	select {
	case event, open := <-nestedEvents:
		assertTrue(t, open)
		assertEqual(t, "second", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nested watcher event")
	}

	_, open := <-nestedEvents
	assertFalsef(t, open, "nested watcher should be closed after callback-driven unwatch")
}

func TestEvents_Notify_Good_PopulatesTimestamp(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("config")
	defer storeInstance.Unwatch("config", events)

	storeInstance.notify(Event{Type: EventSet, Group: "config", Key: "theme", Value: "dark"})

	select {
	case event := <-events:
		assertFalse(t, event.Timestamp.IsZero())
		assertEqual(t, "config", event.Group)
		assertEqual(t, "theme", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for timestamped event")
	}
}

func TestEvents_Watch_Good_BufferDrops(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	defer storeInstance.Unwatch("g", events)

	for i := 0; i < watcherEventBufferCapacity+8; i++ {
		assertNoError(t, storeInstance.Set("g", core.Sprintf("k-%d", i), "v"))
	}

	received := drainEvents(events, watcherEventBufferCapacity, time.Second)
	assertLessOrEqual(t, len(received), watcherEventBufferCapacity)
}

func TestEvents_Watch_Good_ConcurrentWatchUnwatch(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)

	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			group := core.Sprintf("g-%d", worker)
			events := storeInstance.Watch(group)
			_ = storeInstance.Set(group, "k", "v")
			storeInstance.Unwatch(group, events)
		}(worker)
	}

	wg.Wait()
}

func TestEvents_Watch_Good_ScopedStoreEventGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	assertNotNil(t, scopedStore)

	events := storeInstance.Watch("tenant-a:config")
	defer storeInstance.Unwatch("tenant-a:config", events)

	assertNoError(t, scopedStore.SetIn("config", "theme", "dark"))

	select {
	case event := <-events:
		assertEqual(t, "tenant-a:config", event.Group)
		assertEqual(t, "theme", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scoped event")
	}
}

func TestEvents_Watch_Good_SetWithTTL(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	defer storeInstance.Unwatch("g", events)

	assertNoError(t, storeInstance.SetWithTTL("g", "ephemeral", "v", time.Minute))

	select {
	case event := <-events:
		assertEqual(t, EventSet, event.Type)
		assertEqual(t, "ephemeral", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TTL event")
	}
}

func TestEvents_EventType_Good_String(t *testing.T) {
	assertEqual(t, "set", EventSet.String())
	assertEqual(t, "delete", EventDelete.String())
	assertEqual(t, "deletegroup", EventDeleteGroup.String())
	assertEqual(t, "unknown", EventType(99).String())
}

func drainEvents(events <-chan Event, count int, timeout time.Duration) []Event {
	received := make([]Event, 0, count)
	deadline := time.After(timeout)
	for len(received) < count {
		select {
		case event := <-events:
			received = append(received, event)
		case <-deadline:
			return received
		}
	}
	return received
}
