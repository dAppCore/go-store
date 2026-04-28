package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestEventsV090_EventType_String_Good(t *T) {
	label := store.EventSet.String()
	AssertEqual(t, "set", label)
	AssertEqual(t, "delete", store.EventDelete.String())
}

func TestEventsV090_EventType_String_Bad(t *T) {
	label := store.EventType(99).String()
	AssertEqual(t, "unknown", label)
	AssertNotEqual(t, "", label)
}

func TestEventsV090_EventType_String_Ugly(t *T) {
	label := store.EventDeleteGroup.String()
	AssertEqual(t, "delete_group", label)
	AssertContains(t, label, "group")
}

func TestEventsV090_Store_Watch_Good(t *T) {
	storeInstance := ax7Store(t)
	events := storeInstance.Watch("config")
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	event := <-events
	AssertEqual(t, store.EventSet, event.Type)
}

func TestEventsV090_Store_Watch_Bad(t *T) {
	storeInstance := ax7Store(t)
	RequireNoError(t, storeInstance.Close())
	events := storeInstance.Watch("config")
	_, ok := <-events
	AssertFalse(t, ok)
}

func TestEventsV090_Store_Watch_Ugly(t *T) {
	storeInstance := ax7Store(t)
	events := storeInstance.Watch("*")
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	event := <-events
	AssertEqual(t, "config", event.Group)
}

func TestEventsV090_Store_Unwatch_Good(t *T) {
	storeInstance := ax7Store(t)
	events := storeInstance.Watch("config")
	storeInstance.Unwatch("config", events)
	_, ok := <-events
	AssertFalse(t, ok)
}

func TestEventsV090_Store_Unwatch_Bad(t *T) {
	storeInstance := ax7Store(t)
	AssertNotPanics(t, func() { storeInstance.Unwatch("config", nil) })
	AssertFalse(t, storeInstance.IsClosed())
}

func TestEventsV090_Store_Unwatch_Ugly(t *T) {
	storeInstance := ax7Store(t)
	events := storeInstance.Watch("config")
	storeInstance.Unwatch("config", events)
	AssertNotPanics(t, func() { storeInstance.Unwatch("config", events) })
}

func TestEventsV090_Store_OnChange_Good(t *T) {
	storeInstance := ax7Store(t)
	called := false
	unregister := storeInstance.OnChange(func(event store.Event) { called = event.Group == "config" })
	defer unregister()
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	AssertTrue(t, called)
}

func TestEventsV090_Store_OnChange_Bad(t *T) {
	storeInstance := ax7Store(t)
	unregister := storeInstance.OnChange(nil)
	unregister()
	AssertFalse(t, storeInstance.IsClosed())
}

func TestEventsV090_Store_OnChange_Ugly(t *T) {
	storeInstance := ax7Store(t)
	count := 0
	unregister := storeInstance.OnChange(func(store.Event) { count++ })
	unregister()
	RequireNoError(t, storeInstance.Set("config", "colour", "blue"))
	AssertEqual(t, 0, count)
}
