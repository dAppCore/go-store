// Package store provides SQLite-backed storage for grouped entries, TTL expiry,
// namespace isolation, and reactive change notifications.
//
// Usage example:
//
//	storeInstance, _ := store.New(":memory:")
//	defer storeInstance.Close()
//
//	_ = storeInstance.Set("config", "theme", "dark")
//	themeValue, _ := storeInstance.Get("config", "theme")
//
//	scopedStore, _ := store.NewScoped(storeInstance, "tenant-a")
//	_ = scopedStore.Set("config", "theme", "dark")
//
//	quotaScopedStore, _ := store.NewScopedWithQuota(storeInstance, "tenant-b", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10})
//	_ = quotaScopedStore.Set("prefs", "locale", "en-GB")
package store
