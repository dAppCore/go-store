# go-store RFC - AX-Aligned SQLite Store

> An agent should be able to use this store from this document alone.

**Module:** `dappco.re/go/core/store`
**Repository:** `core/go-store`
**Package:** `store`

---

## 1. Overview

go-store is a single-package SQLite-backed key-value store with TTL expiry, namespace isolation, quota enforcement, and reactive mutation events.

The public surface is intentionally small. Names are descriptive, comments show concrete usage, and the implementation keeps a single SQLite connection so pragma settings stay consistent.

---

## 2. File Layout

| File | Purpose |
|------|---------|
| `doc.go` | Package comment with concrete usage examples |
| `store.go` | `Store`, CRUD, TTL, background purge, bulk reads, prefix counts, group discovery, string splitting helpers, template rendering |
| `events.go` | `EventType`, `Event`, `Watcher`, `Watch`, `Unwatch`, `OnChange`, internal dispatch |
| `scope.go` | `ScopedStore`, `QuotaConfig`, namespace validation, quota enforcement |
| `*_test.go` | Behavioural tests for CRUD, TTL, events, quotas, and defensive error paths |

---

## 3. Core API

### Store

- `New(databasePath string) (*Store, error)` opens a SQLite database, applies WAL and busy-timeout pragmas, and pins access to one connection.
- `Close() error` stops the background purge goroutine and closes the database.
- `Get(group, key string) (string, error)` returns a stored value or `NotFoundError`.
- `Set(group, key, value string) error` stores a value and clears any existing TTL.
- `SetWithTTL(group, key, value string, timeToLive time.Duration) error` stores a value that expires after the supplied duration.
- `Delete(group, key string) error` removes one key.
- `DeleteGroup(group string) error` removes every key in a group.
- `Count(group string) (int, error)` counts non-expired keys in one group.
- `CountAll(groupPrefix string) (int, error)` counts non-expired keys across groups that match a prefix.
- `GetAll(group string) (map[string]string, error)` returns all non-expired keys in one group.
- `All(group string) iter.Seq2[KeyValue, error]` streams all non-expired key-value pairs in one group.
- `Groups(groupPrefix string) ([]string, error)` returns distinct non-expired group names that match a prefix.
- `GroupsSeq(groupPrefix string) iter.Seq2[string, error]` streams matching group names.
- `GetSplit(group, key, separator string) (iter.Seq[string], error)` splits a stored value by a custom separator.
- `GetFields(group, key string) (iter.Seq[string], error)` splits a stored value on whitespace.
- `Render(templateSource, group string) (string, error)` renders a Go `text/template` using group data.
- `PurgeExpired() (int64, error)` removes expired rows immediately.

### ScopedStore

- `NewScoped(storeInstance *Store, namespace string) (*ScopedStore, error)` validates a namespace and prefixes groups with `namespace + ":"`.
- `NewScopedWithQuota(storeInstance *Store, namespace string, quota QuotaConfig) (*ScopedStore, error)` adds per-namespace key and group limits.
- `Namespace() string` returns the namespace string.
- `ScopedStore` exposes the same read and write methods as `Store`, with group names prefixed automatically.

### Events

- `EventType` values: `EventSet`, `EventDelete`, `EventDeleteGroup`.
- `EventType.String()` returns `set`, `delete`, `delete_group`, or `unknown`.
- `Event` carries `Type`, `Group`, `Key`, `Value`, and `Timestamp`.
- `Watcher` exposes `Events <-chan Event`.
- `Watch(group, key string) *Watcher` registers a buffered watcher.
- `Unwatch(watcher *Watcher)` removes a watcher and closes its channel.
- `OnChange(callback func(Event)) func()` registers a synchronous callback and returns an idempotent unregister function.

### Quotas and Errors

- `QuotaConfig{MaxKeys, MaxGroups int}` sets per-namespace limits; zero means unlimited.
- `NotFoundError` is returned when a key does not exist or has expired.
- `QuotaExceededError` is returned when a namespace quota would be exceeded.
- `KeyValue` is the item type returned by `All`.

---

## 4. Behavioural Rules

- Names use full words where practical: `Store`, `ScopedStore`, `QuotaConfig`, `Watcher`, `Namespace`.
- Public comments show concrete usage instead of restating the signature.
- Examples use UK English, for example `colour` and `behaviour`.
- The store keeps a single SQLite connection open for all operations. SQLite pragmas are per-connection, so pooling would make behaviour unpredictable.
- TTL is enforced in three layers: lazy delete on `Get`, query-time filtering on bulk reads, and a background purge goroutine.
- Event delivery to watchers is non-blocking. If a watcher channel is full, the event is dropped rather than blocking the writer.
- Callbacks registered with `OnChange` are invoked synchronously after the database write. Callbacks can safely register or unregister other subscriptions because the watcher and callback registries use separate locks.
- Quota checks happen before writes. Existing keys count as upserts and do not consume quota.
- Namespace strings must match `^[a-zA-Z0-9-]+$`.

---

## 5. Concrete Usage

```go
package main

import (
	"fmt"
	"time"

	"dappco.re/go/core/store"
)

func main() {
	storeInstance, err := store.New(":memory:")
	if err != nil {
		return
	}
	defer storeInstance.Close()

	if err := storeInstance.Set("config", "colour", "blue"); err != nil {
		return
	}
	if err := storeInstance.SetWithTTL("session", "token", "abc123", 5*time.Minute); err != nil {
		return
	}

	colourValue, err := storeInstance.Get("config", "colour")
	if err != nil {
		return
	}
	fmt.Println(colourValue)

	for entry, err := range storeInstance.All("config") {
		if err != nil {
			return
		}
		fmt.Println(entry.Key, entry.Value)
	}

	for groupName, err := range storeInstance.GroupsSeq("tenant-a:") {
		if err != nil {
			return
		}
		fmt.Println(groupName)
	}

	watcher := storeInstance.Watch("config", "*")
	defer storeInstance.Unwatch(watcher)
	go func() {
		for event := range watcher.Events {
			fmt.Println(event.Type, event.Group, event.Key, event.Value)
		}
	}()

	unregister := storeInstance.OnChange(func(event store.Event) {
		fmt.Println("changed", event.Group, event.Key, event.Value)
	})
	defer unregister()

	scopedStore, err := store.NewScopedWithQuota(
		storeInstance,
		"tenant-a",
		store.QuotaConfig{MaxKeys: 100, MaxGroups: 10},
	)
	if err != nil {
		return
	}
	if err := scopedStore.Set("prefs", "locale", "en-GB"); err != nil {
		return
	}
}
```

---

## 6. Reference Paths

| Resource | Location |
|----------|----------|
| Package comment | `doc.go` |
| Core store implementation | `store.go` |
| Events and callbacks | `events.go` |
| Namespace and quota logic | `scope.go` |
| Architecture notes | `docs/architecture.md` |
| Agent conventions | `CODEX.md` |
