// Package store provides SQLite-backed key-value storage for grouped entries,
// TTL expiry, namespace isolation, quota enforcement, reactive change
// notifications, SQLite journal writes, workspace journalling, and orphan
// recovery.
//
// Workspace files live under `.core/state/` and can be recovered with
// `RecoverOrphans(".core/state/")`.
//
// Use `store.NewConfigured(store.StoreConfig{...})` when the database path,
// journal, and purge interval are already known. Prefer the struct literal
// over `store.New(..., store.WithJournal(...))` when the full configuration is
// already available, because it reads as data rather than a chain of steps.
//
// Usage example:
//
//	func main() {
//		configuredStore, err := store.NewConfigured(store.StoreConfig{
//			DatabasePath: ":memory:",
//			Journal: store.JournalConfiguration{
//				EndpointURL:  "http://127.0.0.1:8086",
//				Organisation: "core",
//				BucketName:   "events",
//			},
//			PurgeInterval: 20 * time.Millisecond,
//		})
//		if err != nil {
//			return
//		}
//		defer configuredStore.Close()
//
//		if err := configuredStore.Set("config", "colour", "blue"); err != nil {
//			return
//		}
//		if err := configuredStore.SetWithTTL("session", "token", "abc123", 5*time.Minute); err != nil {
//			return
//		}
//
//		colourValue, err := configuredStore.Get("config", "colour")
//		if err != nil {
//			return
//		}
//		fmt.Println(colourValue)
//
//		for entry, err := range configuredStore.All("config") {
//			if err != nil {
//				return
//			}
//			fmt.Println(entry.Key, entry.Value)
//		}
//
//		events := configuredStore.Watch("config")
//		defer configuredStore.Unwatch("config", events)
//		go func() {
//			for event := range events {
//				fmt.Println(event.Type, event.Group, event.Key, event.Value)
//			}
//		}()
//
//		unregister := configuredStore.OnChange(func(event store.Event) {
//			fmt.Println("changed", event.Group, event.Key, event.Value)
//		})
//		defer unregister()
//
//		scopedStore, err := store.NewScopedConfigured(
//			configuredStore,
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
//		for groupName, err := range configuredStore.GroupsSeq("tenant-a:") {
//			if err != nil {
//				return
//			}
//			fmt.Println(groupName)
//		}
//
//		workspace, err := configuredStore.NewWorkspace("scroll-session")
//		if err != nil {
//			return
//		}
//		defer workspace.Discard()
//
//		if err := workspace.Put("like", map[string]any{"user": "@alice"}); err != nil {
//			return
//		}
//		if err := workspace.Put("profile_match", map[string]any{"user": "@charlie"}); err != nil {
//			return
//		}
//		if result := workspace.Commit(); !result.OK {
//			return
//		}
//
//		orphans := configuredStore.RecoverOrphans(".core/state")
//		for _, orphanWorkspace := range orphans {
//			fmt.Println(orphanWorkspace.Name(), orphanWorkspace.Aggregate())
//			orphanWorkspace.Discard()
//		}
//
//		journalResult := configuredStore.QueryJournal(`from(bucket: "events") |> range(start: -24h)`)
//		if !journalResult.OK {
//			return
//		}
//
//		archiveResult := configuredStore.Compact(store.CompactOptions{
//			Before: time.Now().Add(-30 * 24 * time.Hour),
//			Output: "/tmp/archive",
//			Format: "gzip",
//		})
//		if !archiveResult.OK {
//			return
//		}
//	}
package store
