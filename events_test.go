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
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("config", "theme")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("config", "theme", "dark"))

	select {
	case e := <-w.Events:
		assert.Equal(t, EventSet, e.Type)
		assert.Equal(t, "config", e.Group)
		assert.Equal(t, "theme", e.Key)
		assert.Equal(t, "dark", e.Value)
		assert.False(t, e.Timestamp.IsZero())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}

	// A Set to a different key in the same group should NOT trigger this watcher.
	require.NoError(t, s.Set("config", "colour", "blue"))

	select {
	case e := <-w.Events:
		t.Fatalf("unexpected event for non-matching key: %+v", e)
	case <-time.After(50 * time.Millisecond):
		// Expected: no event.
	}
}

// ---------------------------------------------------------------------------
// Watch — wildcard key "*"
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_WildcardKey(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("config", "*")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("config", "theme", "dark"))
	require.NoError(t, s.Set("config", "colour", "blue"))

	received := drainEvents(w.Events, 2, time.Second)
	require.Len(t, received, 2)
	assert.Equal(t, "theme", received[0].Key)
	assert.Equal(t, "colour", received[1].Key)
}

// ---------------------------------------------------------------------------
// Watch — wildcard ("*", "*") matches everything
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_WildcardAll(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("*", "*")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("g1", "k1", "v1"))
	require.NoError(t, s.Set("g2", "k2", "v2"))
	require.NoError(t, s.Delete("g1", "k1"))
	require.NoError(t, s.DeleteGroup("g2"))

	received := drainEvents(w.Events, 4, time.Second)
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
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "k")
	s.Unwatch(w)

	// Channel should be closed.
	_, open := <-w.Events
	assert.False(t, open, "channel should be closed after Unwatch")

	// Set after Unwatch should not panic or block.
	require.NoError(t, s.Set("g", "k", "v"))
}

func TestEvents_Unwatch_Good_Idempotent(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "k")

	// Calling Unwatch multiple times should not panic.
	s.Unwatch(w)
	s.Unwatch(w) // second call is a no-op
}

// ---------------------------------------------------------------------------
// Delete triggers event
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_DeleteEvent(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "k")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("g", "k", "v"))
	// Drain the Set event.
	<-w.Events

	require.NoError(t, s.Delete("g", "k"))

	select {
	case e := <-w.Events:
		assert.Equal(t, EventDelete, e.Type)
		assert.Equal(t, "g", e.Group)
		assert.Equal(t, "k", e.Key)
		assert.Empty(t, e.Value, "Delete events should have empty Value")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete event")
	}
}

// ---------------------------------------------------------------------------
// DeleteGroup triggers event
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_DeleteGroupEvent(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// A wildcard-key watcher for the group should receive DeleteGroup events.
	w := s.Watch("g", "*")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("g", "a", "1"))
	require.NoError(t, s.Set("g", "b", "2"))
	// Drain Set events.
	<-w.Events
	<-w.Events

	require.NoError(t, s.DeleteGroup("g"))

	select {
	case e := <-w.Events:
		assert.Equal(t, EventDeleteGroup, e.Type)
		assert.Equal(t, "g", e.Group)
		assert.Empty(t, e.Key, "DeleteGroup events should have empty Key")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete_group event")
	}
}

// ---------------------------------------------------------------------------
// OnChange — callback fires on mutations
// ---------------------------------------------------------------------------

func TestEvents_OnChange_Good_Fires(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	var events []Event
	var mu sync.Mutex

	unreg := s.OnChange(func(e Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	defer unreg()

	require.NoError(t, s.Set("g", "k", "v"))
	require.NoError(t, s.Delete("g", "k"))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, events, 2)
	assert.Equal(t, EventSet, events[0].Type)
	assert.Equal(t, EventDelete, events[1].Type)
}

// ---------------------------------------------------------------------------
// OnChange — unregister stops callback
// ---------------------------------------------------------------------------

func TestEvents_OnChange_Good_Unregister(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	var count atomic.Int32

	unreg := s.OnChange(func(e Event) {
		count.Add(1)
	})

	require.NoError(t, s.Set("g", "k", "v1"))
	assert.Equal(t, int32(1), count.Load())

	unreg()

	require.NoError(t, s.Set("g", "k", "v2"))
	assert.Equal(t, int32(1), count.Load(), "callback should not fire after unregister")

	// Calling unreg again should not panic.
	unreg()
}

// ---------------------------------------------------------------------------
// Buffer-full doesn't block the writer
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_BufferFullDoesNotBlock(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "*")
	defer s.Unwatch(w)

	// Fill the buffer (cap 16) plus extra writes. None should block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 32 {
			require.NoError(t, s.Set("g", core.Sprintf("k%d", i), "v"))
		}
	}()

	select {
	case <-done:
		// Success: all writes completed without blocking.
	case <-time.After(5 * time.Second):
		t.Fatal("writes blocked — buffer-full condition caused deadlock")
	}

	// Drain what we can — should get exactly watcherBufferSize events.
	var received int
	for range watcherBufferSize {
		select {
		case <-w.Events:
			received++
		default:
		}
	}
	assert.Equal(t, watcherBufferSize, received, "should receive exactly buffer-size events")
}

// ---------------------------------------------------------------------------
// Multiple watchers on same key
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_MultipleWatchersSameKey(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w1 := s.Watch("g", "k")
	w2 := s.Watch("g", "k")
	defer s.Unwatch(w1)
	defer s.Unwatch(w2)

	require.NoError(t, s.Set("g", "k", "v"))

	// Both watchers should receive the event independently.
	select {
	case e := <-w1.Events:
		assert.Equal(t, EventSet, e.Type)
	case <-time.After(time.Second):
		t.Fatal("w1 timed out")
	}

	select {
	case e := <-w2.Events:
		assert.Equal(t, EventSet, e.Type)
	case <-time.After(time.Second):
		t.Fatal("w2 timed out")
	}
}

// ---------------------------------------------------------------------------
// Concurrent Watch/Unwatch during writes (race test)
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_ConcurrentWatchUnwatch(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	const goroutines = 10
	const ops = 50

	var wg sync.WaitGroup

	// Writers — continuously mutate the store.
	wg.Go(func() {
		for i := range goroutines * ops {
			_ = s.Set("g", core.Sprintf("k%d", i), "v")
		}
	})

	// Watchers — add and remove watchers concurrently.
	for range goroutines {
		wg.Go(func() {
			for range ops {
				w := s.Watch("g", "*")
				// Drain a few events to exercise the channel path.
				for range 3 {
					select {
					case <-w.Events:
					case <-time.After(time.Millisecond):
					}
				}
				s.Unwatch(w)
			}
		})
	}

	wg.Wait()
	// If we got here without a data race or panic, the test passes.
}

// ---------------------------------------------------------------------------
// ScopedStore events — prefixed group name
// ---------------------------------------------------------------------------

func TestEvents_Watch_Good_ScopedStoreEvents(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, err := NewScoped(s, "tenant-a")
	require.NoError(t, err)

	// Watch on the underlying store with the full prefixed group name.
	w := s.Watch("tenant-a:config", "theme")
	defer s.Unwatch(w)

	require.NoError(t, sc.Set("config", "theme", "dark"))

	select {
	case e := <-w.Events:
		assert.Equal(t, EventSet, e.Type)
		assert.Equal(t, "tenant-a:config", e.Group)
		assert.Equal(t, "theme", e.Key)
		assert.Equal(t, "dark", e.Value)
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
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "k")
	defer s.Unwatch(w)

	require.NoError(t, s.SetWithTTL("g", "k", "ttl-val", time.Hour))

	select {
	case e := <-w.Events:
		assert.Equal(t, EventSet, e.Type)
		assert.Equal(t, "g", e.Group)
		assert.Equal(t, "k", e.Key)
		assert.Equal(t, "ttl-val", e.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SetWithTTL event")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// drainEvents collects up to n events from ch within the given timeout.
func drainEvents(ch <-chan Event, n int, timeout time.Duration) []Event {
	var events []Event
	deadline := time.After(timeout)
	for range n {
		select {
		case e := <-ch:
			events = append(events, e)
		case <-deadline:
			return events
		}
	}
	return events
}
