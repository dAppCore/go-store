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
//		if err := storeInstance.Set("config", "theme", "dark"); err != nil {
//			return
//		}
//		themeValue, err := storeInstance.Get("config", "theme")
//		if err != nil {
//			return
//		}
//		fmt.Println(themeValue)
//
//		scopedStore, err := store.NewScoped(storeInstance, "tenant-a")
//		if err != nil {
//			return
//		}
//		if err := scopedStore.Set("config", "theme", "dark"); err != nil {
//			return
//		}
//
//		quotaScopedStore, err := store.NewScopedWithQuota(storeInstance, "tenant-b", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10})
//		if err != nil {
//			return
//		}
//		if err := quotaScopedStore.Set("prefs", "locale", "en-GB"); err != nil {
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
