package store

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Watch — specific key
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_SpecificKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("config", "theme")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.Set("config", "theme", "dark"))

	select {
	case event := <-watcher.Events:
		assert.Equal(t, EventSet, event.Type)
		assert.Equal(t, "config", event.Group)
		assert.Equal(t, "theme", event.Key)
		assert.Equal(t, "dark", event.Value)
		assert.False(t, event.Timestamp.IsZero())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}

	// A Set to a different key in the same group should NOT trigger this watcher.
	require.NoError(t, storeInstance.Set("config", "colour", "blue"))

	select {
	case event := <-watcher.Events:
		t.Fatalf("unexpected event for non-matching key: %+v", event)
	case <-time.After(50 * time.Millisecond):
		// Expected: no event.
	}
}

// ---------------------------------------------------------------------------
// Watch — wildcard key "*"
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_WildcardKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("config", "*")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.Set("config", "theme", "dark"))
	require.NoError(t, storeInstance.Set("config", "colour", "blue"))

	received := drainEvents(watcher.Events, 2, time.Second)
	require.Len(t, received, 2)
	assert.Equal(t, "theme", received[0].Key)
	assert.Equal(t, "colour", received[1].Key)
}

func TestEvents_Watch_Good_GroupMismatch(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("config", "*")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.Set("other", "theme", "dark"))

	select {
	case event := <-watcher.Events:
		t.Fatalf("unexpected event for non-matching group: %+v", event)
	case <-time.After(50 * time.Millisecond):
		// Expected: no event.
	}
}

// ---------------------------------------------------------------------------
// Watch — wildcard ("*", "*") matches everything
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_WildcardAll(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("*", "*")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.Set("g1", "k1", "v1"))
	require.NoError(t, storeInstance.Set("g2", "k2", "v2"))
	require.NoError(t, storeInstance.Delete("g1", "k1"))
	require.NoError(t, storeInstance.DeleteGroup("g2"))

	received := drainEvents(watcher.Events, 4, time.Second)
	require.Len(t, received, 4)
	assert.Equal(t, EventSet, received[0].Type)
	assert.Equal(t, EventSet, received[1].Type)
	assert.Equal(t, EventDelete, received[2].Type)
	assert.Equal(t, EventDeleteGroup, received[3].Type)
}

// ---------------------------------------------------------------------------
// Unwatch — stops delivery, channel closed
// ---------------------------------------------------------------------------

func TestEvents_Unwatch_Good_StopsDelivery(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("g", "k")
	storeInstance.Unwatch(watcher)

	// Channel should be closed.
	_, open := <-watcher.Events
	assert.False(t, open, "channel should be closed after Unwatch")

	// Set after Unwatch should not panic or block.
	require.NoError(t, storeInstance.Set("g", "k", "v"))
}

func TestEvents_Unwatch_Good_Idempotent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("g", "k")

	// Calling Unwatch multiple times should not panic.
	storeInstance.Unwatch(watcher)
	storeInstance.Unwatch(watcher) // second call is a no-op
}

func TestEvents_Unwatch_Good_NilWatcher(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	storeInstance.Unwatch(nil)
}

// ---------------------------------------------------------------------------
// Delete triggers event
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_DeleteEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("g", "k")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.Set("g", "k", "v"))
	// Drain the Set event.
	<-watcher.Events

	require.NoError(t, storeInstance.Delete("g", "k"))

	select {
	case event := <-watcher.Events:
		assert.Equal(t, EventDelete, event.Type)
		assert.Equal(t, "g", event.Group)
		assert.Equal(t, "k", event.Key)
		assert.Empty(t, event.Value, "Delete events should have empty Value")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete event")
	}
}

func TestEvents_Watch_Good_DeleteMissingKeyDoesNotEmitEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("*", "*")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.Delete("g", "missing"))

	select {
	case event := <-watcher.Events:
		t.Fatalf("unexpected event for missing key delete: %+v", event)
	default:
	}
}

// ---------------------------------------------------------------------------
// DeleteGroup triggers event
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_DeleteGroupEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	// A wildcard-key watcher for the group should receive DeleteGroup events.
	watcher := storeInstance.Watch("g", "*")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.Set("g", "a", "1"))
	require.NoError(t, storeInstance.Set("g", "b", "2"))
	// Drain Set events.
	<-watcher.Events
	<-watcher.Events

	require.NoError(t, storeInstance.DeleteGroup("g"))

	select {
	case event := <-watcher.Events:
		assert.Equal(t, EventDeleteGroup, event.Type)
		assert.Equal(t, "g", event.Group)
		assert.Empty(t, event.Key, "DeleteGroup events should have empty Key")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete_group event")
	}
}

func TestEvents_Watch_Good_DeleteMissingGroupDoesNotEmitEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("*", "*")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.DeleteGroup("missing"))

	select {
	case event := <-watcher.Events:
		t.Fatalf("unexpected event for missing group delete: %+v", event)
	default:
	}
}

// ---------------------------------------------------------------------------
// OnChange — callback fires on mutations
// ---------------------------------------------------------------------------

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

	require.NoError(t, storeInstance.Set("g", "k", "v"))
	require.NoError(t, storeInstance.Delete("g", "k"))

	eventsMutex.Lock()
	defer eventsMutex.Unlock()
	require.Len(t, events, 2)
	assert.Equal(t, EventSet, events[0].Type)
	assert.Equal(t, EventDelete, events[1].Type)
}

// ---------------------------------------------------------------------------
// OnChange — unregister stops callback
// ---------------------------------------------------------------------------

func TestEvents_OnChange_Good_Unregister(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	var count atomic.Int32

	unregister := storeInstance.OnChange(func(event Event) {
		count.Add(1)
	})

	require.NoError(t, storeInstance.Set("g", "k", "v1"))
	assert.Equal(t, int32(1), count.Load())

	unregister()

	require.NoError(t, storeInstance.Set("g", "k", "v2"))
	assert.Equal(t, int32(1), count.Load(), "callback should not fire after unregister")

	// Calling unregister again should not panic.
	unregister()
}

func TestEvents_OnChange_Good_NilCallbackNoOp(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	unregister := storeInstance.OnChange(nil)
	require.NotNil(t, unregister)

	unregister()
	require.NoError(t, storeInstance.Set("g", "k", "v"))
	unregister()
}

// ---------------------------------------------------------------------------
// OnChange — callback can manage subscriptions while handling an event
// ---------------------------------------------------------------------------

func TestEvents_OnChange_Good_ReentrantSubscriptions(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	var callbackCount atomic.Int32
	var unregister func()
	unregister = storeInstance.OnChange(func(event Event) {
		callbackCount.Add(1)

		nestedWatcher := storeInstance.Watch("nested", "*")
		storeInstance.Unwatch(nestedWatcher)

		if unregister != nil {
			unregister()
		}
	})

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- storeInstance.Set("g", "k", "v")
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Set to complete")
	}

	assert.Equal(t, int32(1), callbackCount.Load())

	// The callback unregistered itself, so later writes should not increment it.
	require.NoError(t, storeInstance.Set("g", "k", "v2"))
	assert.Equal(t, int32(1), callbackCount.Load())
}

// ---------------------------------------------------------------------------
// Buffer-full doesn't block the writer
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_BufferFullDoesNotBlock(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("g", "*")
	defer storeInstance.Unwatch(watcher)

	// Fill the buffer (cap 16) plus extra writes. None should block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 32 {
			require.NoError(t, storeInstance.Set("g", core.Sprintf("k%d", i), "v"))
		}
	}()

	select {
	case <-done:
		// Success: all writes completed without blocking.
	case <-time.After(5 * time.Second):
		t.Fatal("writes blocked — buffer-full condition caused deadlock")
	}

	// Drain what we can — should get exactly watcherEventBufferCapacity events.
	var received int
	for range watcherEventBufferCapacity {
		select {
		case <-watcher.Events:
			received++
		default:
		}
	}
	assert.Equal(t, watcherEventBufferCapacity, received, "should receive exactly buffer-size events")
}

// ---------------------------------------------------------------------------
// Multiple watchers on same key
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_MultipleWatchersSameKey(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	firstWatcher := storeInstance.Watch("g", "k")
	secondWatcher := storeInstance.Watch("g", "k")
	defer storeInstance.Unwatch(firstWatcher)
	defer storeInstance.Unwatch(secondWatcher)

	require.NoError(t, storeInstance.Set("g", "k", "v"))

	// Both watchers should receive the event independently.
	select {
	case event := <-firstWatcher.Events:
		assert.Equal(t, EventSet, event.Type)
	case <-time.After(time.Second):
		t.Fatal("firstWatcher timed out")
	}

	select {
	case event := <-secondWatcher.Events:
		assert.Equal(t, EventSet, event.Type)
	case <-time.After(time.Second):
		t.Fatal("secondWatcher timed out")
	}
}

// ---------------------------------------------------------------------------
// Concurrent Watch/Unwatch during writes (race test)
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_ConcurrentWatchUnwatch(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	const goroutines = 10
	const ops = 50

	var waitGroup sync.WaitGroup

	// Writers — continuously mutate the store.
	waitGroup.Go(func() {
		for i := range goroutines * ops {
			_ = storeInstance.Set("g", core.Sprintf("k%d", i), "v")
		}
	})

	// Watchers — add and remove watchers concurrently.
	for range goroutines {
		waitGroup.Go(func() {
			for range ops {
				watcher := storeInstance.Watch("g", "*")
				// Drain a few events to exercise the channel path.
				for range 3 {
					select {
					case <-watcher.Events:
					case <-time.After(time.Millisecond):
					}
				}
				storeInstance.Unwatch(watcher)
			}
		})
	}

	waitGroup.Wait()
	// If we got here without a data race or panic, the test passes.
}

// ---------------------------------------------------------------------------
// ScopedStore events — prefixed group name
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_ScopedStoreEvents(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScoped(storeInstance, "tenant-a")
	require.NoError(t, err)

	// Watch on the underlying store with the full prefixed group name.
	watcher := storeInstance.Watch("tenant-a:config", "theme")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, scopedStore.Set("config", "theme", "dark"))

	select {
	case event := <-watcher.Events:
		assert.Equal(t, EventSet, event.Type)
		assert.Equal(t, "tenant-a:config", event.Group)
		assert.Equal(t, "theme", event.Key)
		assert.Equal(t, "dark", event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scoped store event")
	}
}

// ---------------------------------------------------------------------------
// EventType.String()
// ---------------------------------------------------------------------------

func TestEvents_EventType_Good_String(t *testing.T) {
	assert.Equal(t, "set", EventSet.String())
	assert.Equal(t, "delete", EventDelete.String())
	assert.Equal(t, "delete_group", EventDeleteGroup.String())
	assert.Equal(t, "unknown", EventType(99).String())
}

// ---------------------------------------------------------------------------
// SetWithTTL emits events
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_SetWithTTLEmitsEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	watcher := storeInstance.Watch("g", "k")
	defer storeInstance.Unwatch(watcher)

	require.NoError(t, storeInstance.SetWithTTL("g", "k", "ttl-val", time.Hour))

	select {
	case event := <-watcher.Events:
		assert.Equal(t, EventSet, event.Type)
		assert.Equal(t, "g", event.Group)
		assert.Equal(t, "k", event.Key)
		assert.Equal(t, "ttl-val", event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SetWithTTL event")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// drainEvents collects up to n events from ch within the given timeout.
func drainEvents(ch <-chan Event, count int, timeout time.Duration) []Event {
	var events []Event
	deadline := time.After(timeout)
	for range count {
		select {
		case event := <-ch:
			events = append(events, event)
		case <-deadline:
			return events
		}
	}
	return events
}
