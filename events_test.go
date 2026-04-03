package store

import (
	"sync"
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvents_Watch_Good_Group(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("config")
	defer storeInstance.Unwatch("config", events)

	require.NoError(t, storeInstance.Set("config", "theme", "dark"))
	require.NoError(t, storeInstance.Set("config", "colour", "blue"))

	received := drainEvents(events, 2, time.Second)
	require.Len(t, received, 2)
	assert.Equal(t, "theme", received[0].Key)
	assert.Equal(t, "colour", received[1].Key)
	assert.Equal(t, "config", received[0].Group)
	assert.Equal(t, "config", received[1].Group)
}

func TestEvents_Watch_Good_WildcardGroup(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("*")
	defer storeInstance.Unwatch("*", events)

	require.NoError(t, storeInstance.Set("g1", "k1", "v1"))
	require.NoError(t, storeInstance.Set("g2", "k2", "v2"))
	require.NoError(t, storeInstance.Delete("g1", "k1"))
	require.NoError(t, storeInstance.DeleteGroup("g2"))

	received := drainEvents(events, 4, time.Second)
	require.Len(t, received, 4)
	assert.Equal(t, EventSet, received[0].Type)
	assert.Equal(t, EventSet, received[1].Type)
	assert.Equal(t, EventDelete, received[2].Type)
	assert.Equal(t, EventDeleteGroup, received[3].Type)
}

func TestEvents_Unwatch_Good_StopsDelivery(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	storeInstance.Unwatch("g", events)

	_, open := <-events
	assert.False(t, open, "channel should be closed after Unwatch")

	require.NoError(t, storeInstance.Set("g", "k", "v"))
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
	require.NoError(t, storeInstance.Close())

	_, open := <-events
	assert.False(t, open, "channel should be closed after Close")
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

	require.NoError(t, storeInstance.Set("g", "k", "v"))
	<-events

	require.NoError(t, storeInstance.Delete("g", "k"))

	select {
	case event := <-events:
		assert.Equal(t, EventDelete, event.Type)
		assert.Equal(t, "g", event.Group)
		assert.Equal(t, "k", event.Key)
		assert.Empty(t, event.Value)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delete event")
	}
}

func TestEvents_Watch_Good_DeleteGroupEvent(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	defer storeInstance.Unwatch("g", events)

	require.NoError(t, storeInstance.Set("g", "a", "1"))
	require.NoError(t, storeInstance.Set("g", "b", "2"))
	<-events
	<-events

	require.NoError(t, storeInstance.DeleteGroup("g"))

	select {
	case event := <-events:
		assert.Equal(t, EventDeleteGroup, event.Type)
		assert.Equal(t, "g", event.Group)
		assert.Empty(t, event.Key)
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

	require.NoError(t, storeInstance.Set("g", "k", "v"))
	require.NoError(t, storeInstance.Delete("g", "k"))

	eventsMutex.Lock()
	defer eventsMutex.Unlock()
	require.Len(t, events, 2)
	assert.Equal(t, EventSet, events[0].Type)
	assert.Equal(t, EventDelete, events[1].Type)
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

	require.NoError(t, storeInstance.Set("config", "theme", "dark"))
	require.NoError(t, storeInstance.Set("other", "theme", "light"))

	assert.Equal(t, []string{"theme=dark"}, seen)
}

func TestEvents_Watch_Good_BufferDrops(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	defer storeInstance.Unwatch("g", events)

	for i := 0; i < watcherEventBufferCapacity+8; i++ {
		require.NoError(t, storeInstance.Set("g", core.Sprintf("k-%d", i), "v"))
	}

	received := drainEvents(events, watcherEventBufferCapacity, time.Second)
	assert.LessOrEqual(t, len(received), watcherEventBufferCapacity)
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
	require.NotNil(t, scopedStore)

	events := storeInstance.Watch("tenant-a:config")
	defer storeInstance.Unwatch("tenant-a:config", events)

	require.NoError(t, scopedStore.SetIn("config", "theme", "dark"))

	select {
	case event := <-events:
		assert.Equal(t, "tenant-a:config", event.Group)
		assert.Equal(t, "theme", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scoped event")
	}
}

func TestEvents_Watch_Good_SetWithTTL(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	events := storeInstance.Watch("g")
	defer storeInstance.Unwatch("g", events)

	require.NoError(t, storeInstance.SetWithTTL("g", "ephemeral", "v", time.Minute))

	select {
	case event := <-events:
		assert.Equal(t, EventSet, event.Type)
		assert.Equal(t, "ephemeral", event.Key)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TTL event")
	}
}

func TestEvents_EventType_Good_String(t *testing.T) {
	assert.Equal(t, "set", EventSet.String())
	assert.Equal(t, "delete", EventDelete.String())
	assert.Equal(t, "delete_group", EventDeleteGroup.String())
	assert.Equal(t, "unknown", EventType(99).String())
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
