package store

import core "dappco.re/go"

func ExampleEventType_String() {
	label := EventDeleteGroup.String()
	core.Println(label)
}

func ExampleStore_Watch() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	events := storeInstance.Watch("config")
	storeInstance.Unwatch("config", events)
}

func ExampleStore_Unwatch() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	events := storeInstance.Watch("config")
	storeInstance.Unwatch("config", events)
}

func ExampleStore_OnChange() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	unregister := storeInstance.OnChange(func(event Event) {
		core.Println(event.Type.String())
	})
	unregister()
}
