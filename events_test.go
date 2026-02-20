package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Watch — specific key
// ---------------------------------------------------------------------------

func TestWatch_Good_SpecificKey(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("config", "theme")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("config", "theme", "dark"))

	select {
	case e := <-w.Ch:
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
	case e := <-w.Ch:
		t.Fatalf("unexpected event for non-matching key: %+v", e)
	case <-time.After(50 * time.Millisecond):
		// Expected: no event.
	}
}

// ---------------------------------------------------------------------------
// Watch — wildcard key "*"
// ---------------------------------------------------------------------------

func TestWatch_Good_WildcardKey(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("config", "*")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("config", "theme", "dark"))
	require.NoError(t, s.Set("config", "colour", "blue"))

	received := drainEvents(w.Ch, 2, time.Second)
	require.Len(t, received, 2)
	assert.Equal(t, "theme", received[0].Key)
	assert.Equal(t, "colour", received[1].Key)
}

// ---------------------------------------------------------------------------
// Watch — wildcard ("*", "*") matches everything
// ---------------------------------------------------------------------------

func TestWatch_Good_WildcardAll(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("*", "*")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("g1", "k1", "v1"))
	require.NoError(t, s.Set("g2", "k2", "v2"))
	require.NoError(t, s.Delete("g1", "k1"))
	require.NoError(t, s.DeleteGroup("g2"))

	received := drainEvents(w.Ch, 4, time.Second)
	require.Len(t, received, 4)
	assert.Equal(t, EventSet, received[0].Type)
	assert.Equal(t, EventSet, received[1].Type)
	assert.Equal(t, EventDelete, received[2].Type)
	assert.Equal(t, EventDeleteGroup, received[3].Type)
}

// ---------------------------------------------------------------------------
// Unwatch — stops delivery, channel closed
// ---------------------------------------------------------------------------

func TestUnwatch_Good_StopsDelivery(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "k")
	s.Unwatch(w)

	// Channel should be closed.
	_, open := <-w.Ch
	assert.False(t, open, "channel should be closed after Unwatch")

	// Set after Unwatch should not panic or block.
	require.NoError(t, s.Set("g", "k", "v"))
}

func TestUnwatch_Good_Idempotent(t *testing.T) {
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

func TestWatch_Good_DeleteEvent(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "k")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("g", "k", "v"))
	// Drain the Set event.
	<-w.Ch

	require.NoError(t, s.Delete("g", "k"))

	select {
	case e := <-w.Ch:
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

func TestWatch_Good_DeleteGroupEvent(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	// A wildcard-key watcher for the group should receive DeleteGroup events.
	w := s.Watch("g", "*")
	defer s.Unwatch(w)

	require.NoError(t, s.Set("g", "a", "1"))
	require.NoError(t, s.Set("g", "b", "2"))
	// Drain Set events.
	<-w.Ch
	<-w.Ch

	require.NoError(t, s.DeleteGroup("g"))

	select {
	case e := <-w.Ch:
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

func TestOnChange_Good_Fires(t *testing.T) {
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

func TestOnChange_Good_Unregister(t *testing.T) {
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

func TestWatch_Good_BufferFullDoesNotBlock(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "*")
	defer s.Unwatch(w)

	// Fill the buffer (cap 16) plus extra writes. None should block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 32; i++ {
			require.NoError(t, s.Set("g", fmt.Sprintf("k%d", i), "v"))
		}
	}()

	select {
	case <-done:
		// Success: all writes completed without blocking.
	case <-time.After(5 * time.Second):
		t.Fatal("writes blocked — buffer-full condition caused deadlock")
	}

	// Drain what we can — should get exactly watcherBufSize events.
	var received int
	for range watcherBufSize {
		select {
		case <-w.Ch:
			received++
		default:
		}
	}
	assert.Equal(t, watcherBufSize, received, "should receive exactly buffer-size events")
}

// ---------------------------------------------------------------------------
// Multiple watchers on same key
// ---------------------------------------------------------------------------

func TestWatch_Good_MultipleWatchersSameKey(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w1 := s.Watch("g", "k")
	w2 := s.Watch("g", "k")
	defer s.Unwatch(w1)
	defer s.Unwatch(w2)

	require.NoError(t, s.Set("g", "k", "v"))

	// Both watchers should receive the event independently.
	select {
	case e := <-w1.Ch:
		assert.Equal(t, EventSet, e.Type)
	case <-time.After(time.Second):
		t.Fatal("w1 timed out")
	}

	select {
	case e := <-w2.Ch:
		assert.Equal(t, EventSet, e.Type)
	case <-time.After(time.Second):
		t.Fatal("w2 timed out")
	}
}

// ---------------------------------------------------------------------------
// Concurrent Watch/Unwatch during writes (race test)
// ---------------------------------------------------------------------------

func TestWatch_Good_ConcurrentWatchUnwatch(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	const goroutines = 10
	const ops = 50

	var wg sync.WaitGroup

	// Writers — continuously mutate the store.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < goroutines*ops; i++ {
			_ = s.Set("g", fmt.Sprintf("k%d", i), "v")
		}
	}()

	// Watchers — add and remove watchers concurrently.
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				w := s.Watch("g", "*")
				// Drain a few events to exercise the channel path.
				for range 3 {
					select {
					case <-w.Ch:
					case <-time.After(time.Millisecond):
					}
				}
				s.Unwatch(w)
			}
		}()
	}

	wg.Wait()
	// If we got here without a data race or panic, the test passes.
}

// ---------------------------------------------------------------------------
// ScopedStore events — prefixed group name
// ---------------------------------------------------------------------------

func TestWatch_Good_ScopedStoreEvents(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	sc, err := NewScoped(s, "tenant-a")
	require.NoError(t, err)

	// Watch on the underlying store with the full prefixed group name.
	w := s.Watch("tenant-a:config", "theme")
	defer s.Unwatch(w)

	require.NoError(t, sc.Set("config", "theme", "dark"))

	select {
	case e := <-w.Ch:
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

func TestEventType_String(t *testing.T) {
	assert.Equal(t, "set", EventSet.String())
	assert.Equal(t, "delete", EventDelete.String())
	assert.Equal(t, "delete_group", EventDeleteGroup.String())
	assert.Equal(t, "unknown", EventType(99).String())
}

// ---------------------------------------------------------------------------
// SetWithTTL emits events
// ---------------------------------------------------------------------------

func TestWatch_Good_SetWithTTLEmitsEvent(t *testing.T) {
	s, _ := New(":memory:")
	defer s.Close()

	w := s.Watch("g", "k")
	defer s.Unwatch(w)

	require.NoError(t, s.SetWithTTL("g", "k", "ttl-val", time.Hour))

	select {
	case e := <-w.Ch:
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
