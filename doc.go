// Package store provides a SQLite-backed key-value store with group namespaces,
// TTL expiry, quota-enforced scoped views, and reactive change notifications.
//
// Usage example:
//
//	storeInstance, _ := store.New(":memory:")
//	defer storeInstance.Close()
//	storeInstance.Set("config", "theme", "dark")
//	themeValue, _ := storeInstance.Get("config", "theme")
//	scopedStore, _ := store.NewScoped(storeInstance, "tenant-a")
//	_ = scopedStore.Set("config", "theme", "dark")
//	quotaScopedStore, _ := store.NewScopedWithQuota(storeInstance, "tenant-b", store.QuotaConfig{MaxKeys: 100, MaxGroups: 10})
//	_ = quotaScopedStore.Set("prefs", "locale", "en-GB")
//
// Use New to open a store, then Set/Get for CRUD operations. Use
// NewScoped/NewScopedWithQuota when group names need tenant isolation or
// per-namespace quotas.
package store
