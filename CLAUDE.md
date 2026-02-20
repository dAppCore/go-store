# CLAUDE.md

## What This Is

SQLite key-value store wrapper with TTL support and namespace isolation. Module: `forge.lthn.ai/core/go-store`

## Commands

```bash
go test ./...              # Run all tests
go test -v -run Name       # Run single test
go test -race ./...        # Race detector
go test -cover ./...       # Coverage (target: 95%+)
go test -bench=. ./...     # Benchmarks
```

## Key API

```go
st, _ := store.New(":memory:")      // or store.New("/path/to/db")
defer st.Close()

st.Set("group", "key", "value")                         // no expiry
st.SetWithTTL("group", "key", "value", 5*time.Minute)   // expires after TTL
val, _ := st.Get("group", "key")                         // lazy-deletes expired
st.Delete("group", "key")
st.DeleteGroup("group")
all, _ := st.GetAll("group")        // excludes expired
n, _ := st.Count("group")           // excludes expired
out, _ := st.Render(tmpl, "group")  // excludes expired
removed, _ := st.PurgeExpired()     // manual purge
total, _ := st.CountAll("prefix:")  // count keys matching prefix (excludes expired)
groups, _ := st.Groups("prefix:")   // distinct group names matching prefix

// Namespace isolation (auto-prefixes groups with "tenant:")
sc, _ := store.NewScoped(st, "tenant")
sc.Set("config", "key", "val")     // stored as "tenant:config" in underlying store
sc.Get("config", "key")            // reads from "tenant:config"

// With quota enforcement
quota := store.QuotaConfig{MaxKeys: 100, MaxGroups: 10}
sq, _ := store.NewScopedWithQuota(st, "tenant", quota)
sq.Set("g", "k", "v")             // returns ErrQuotaExceeded if limits hit

// Event hooks — reactive notifications for store mutations
w := st.Watch("group", "key")     // watch specific key (buffered chan, cap 16)
w2 := st.Watch("group", "*")      // wildcard: all keys in group
w3 := st.Watch("*", "*")          // wildcard: all mutations
defer st.Unwatch(w)

select {
case e := <-w.Ch:
    fmt.Println(e.Type, e.Group, e.Key, e.Value)
}

// Callback hook (synchronous, caller controls concurrency)
unreg := st.OnChange(func(e store.Event) {
    hub.SendToChannel("store-events", e) // go-ws integration point
})
defer unreg()
```

## Coding Standards

- UK English
- `go test -race ./...` must pass before commit
- Conventional commits: `type(scope): description`
- Co-Author: `Co-Authored-By: Virgil <virgil@lethean.io>`

## Docs

- `docs/architecture.md` — storage layer, group/key model, TTL, events, scoping
- `docs/development.md` — prerequisites, test patterns, benchmarks, adding methods
- `docs/history.md` — completed phases, known limitations, future considerations
