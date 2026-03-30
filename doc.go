// Package store provides a SQLite-backed key-value store with group namespaces,
// TTL expiry, quota-enforced scoped views, and reactive change notifications.
//
// st, _ := store.New(":memory:")
// value, _ := st.Get("config", "theme")
//
// Use New to open a store, then Set/Get for CRUD operations. Use
// NewScoped/NewScopedWithQuota when group names need tenant isolation or
// per-namespace quotas.
package store
