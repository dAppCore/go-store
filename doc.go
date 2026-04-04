// Package store provides SQLite-backed storage for grouped entries, TTL expiry,
// namespace isolation, quota enforcement, and reactive change notifications.
//
// Usage example:
//
//	func main() {
//		storeInstance, err := store.New(":memory:")
//		if err != nil {
//			return
//		}
//		defer storeInstance.Close()
//
//		if err := storeInstance.Set("config", "colour", "blue"); err != nil {
//			return
//		}
//		if err := storeInstance.SetWithTTL("session", "token", "abc123", 5*time.Minute); err != nil {
//			return
//		}
//
//		colourValue, err := storeInstance.Get("config", "colour")
//		if err != nil {
//			return
//		}
//		fmt.Println(colourValue)
//
//		for entry, err := range storeInstance.All("config") {
//			if err != nil {
//				return
//			}
//			fmt.Println(entry.Key, entry.Value)
//		}
//
//		events := storeInstance.Watch("config")
//		defer storeInstance.Unwatch("config", events)
//		go func() {
//			for event := range events {
//				fmt.Println(event.Type, event.Group, event.Key, event.Value)
//			}
//		}()
//
//		unregister := storeInstance.OnChange(func(event store.Event) {
//			fmt.Println("changed", event.Group, event.Key, event.Value)
//		})
//		defer unregister()
//
//		scopedStore, err := store.NewScopedConfigured(
//			storeInstance,
//			store.ScopedStoreConfig{
//				Namespace: "tenant-a",
//				Quota:     store.QuotaConfig{MaxKeys: 100, MaxGroups: 10},
//			},
//		)
//		if err != nil {
//			return
//		}
//		if err := scopedStore.SetIn("preferences", "locale", "en-GB"); err != nil {
//			return
//		}
//
//		for groupName, err := range storeInstance.GroupsSeq("tenant-a:") {
//			if err != nil {
//				return
//			}
//			fmt.Println(groupName)
//		}
//	}
package store
