// Package store provides SQLite-backed storage for grouped entries, TTL expiry,
// namespace isolation, and reactive change notifications.
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
//		colourValue, err := storeInstance.Get("config", "colour")
//		if err != nil {
//			return
//		}
//		fmt.Println(colourValue)
//
//		scopedStore, err := store.NewScoped(storeInstance, "tenant-a")
//		if err != nil {
//			return
//		}
//		if err := scopedStore.Set("config", "colour", "blue"); err != nil {
//			return
//		}
//
//		quotaScopedStore, err := store.NewScopedWithQuota(storeInstance, "tenant-b", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10})
//		if err != nil {
//			return
//		}
//		if err := quotaScopedStore.Set("preferences", "locale", "en-GB"); err != nil {
//			return
//		}
//
//		watcher := storeInstance.Watch("config", "*")
//		defer storeInstance.Unwatch(watcher)
//		go func() {
//			for event := range watcher.Events {
//				fmt.Println(event.Type, event.Group, event.Key, event.Value)
//			}
//		}()
//
//		unregister := storeInstance.OnChange(func(event store.Event) {
//			fmt.Println("changed", event.Group, event.Key, event.Value)
//		})
//		defer unregister()
//	}
package store
