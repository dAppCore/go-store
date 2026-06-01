package store

import (
	"testing"

	core "dappco.re/go"
)

// Real-behaviour coverage for the store Core service: NewService, Register,
// OnStartup action registration, the seven action handlers, and OnShutdown.
// The existing service_test.go holds the AX-7 naming stubs (subject != nil);
// this file drives the registration plumbing end-to-end so the handlers are
// actually executed against a live in-memory store.

// newTestServiceCore builds a Core with the store service registered against an
// in-memory database, runs startup so the action handlers are wired, and
// returns the Core plus the typed Service for direct assertions.
func newTestServiceCore(t *testing.T) (*core.Core, *Service) {
	t.Helper()
	c := core.New(core.WithName("store", NewService(StoreConfig{DatabasePath: testMemoryDatabasePath})))
	requireCoreOK(t, c.ServiceStartup(core.Background(), nil))
	svc := core.MustServiceFor[*Service](c, "store")
	t.Cleanup(func() { _ = c.ServiceShutdown(core.Background()) })
	return c, svc
}

func runStoreAction(t *testing.T, c *core.Core, name string, opts ...core.Option) core.Result {
	t.Helper()
	return c.Action(name).Run(core.Background(), core.NewOptions(opts...))
}

// ---------------------------------------------------------------------------
// NewService / OnStartup — registration wires every action
// ---------------------------------------------------------------------------

func TestServiceHandlers_OnStartup_Good_RegistersAllActions(t *testing.T) {
	c, _ := newTestServiceCore(t)
	for _, name := range []string{
		"store.get", "store.set", "store.delete", "store.get_all",
		"store.groups", "store.delete_group", "store.compact",
	} {
		assertTruef(t, c.Action(name).Exists(), "action %q not registered", name)
	}
}

func TestServiceHandlers_OnStartup_Ugly_DoubleStartupIsIdempotent(t *testing.T) {
	c, svc := newTestServiceCore(t)
	// Second startup must not panic or double-register (core.Once guard).
	assertNoError(t, svc.OnStartup(core.Background()))
	assertTrue(t, c.Action("store.get").Exists())
}

func TestServiceHandlers_OnStartup_Bad_NilServiceIsSafe(t *testing.T) {
	var svc *Service
	assertNoError(t, svc.OnStartup(core.Background()))
}

// ---------------------------------------------------------------------------
// set / get / get_all round-trip
// ---------------------------------------------------------------------------

func TestServiceHandlers_SetGet_Good_RoundTrip(t *testing.T) {
	c, _ := newTestServiceCore(t)
	set := runStoreAction(t, c, "store.set",
		core.Option{Key: "group", Value: "config"},
		core.Option{Key: "key", Value: "host"},
		core.Option{Key: "value", Value: "homelab"},
	)
	assertNoError(t, set)

	get := runStoreAction(t, c, "store.get",
		core.Option{Key: "group", Value: "config"},
		core.Option{Key: "key", Value: "host"},
	)
	assertNoError(t, get)
	assertEqual(t, "homelab", get.Value)
}

func TestServiceHandlers_GetAll_Good_ReturnsNamespaceMap(t *testing.T) {
	c, _ := newTestServiceCore(t)
	assertNoError(t, runStoreAction(t, c, "store.set",
		core.Option{Key: "group", Value: "ns"},
		core.Option{Key: "key", Value: "a"},
		core.Option{Key: "value", Value: "1"},
	))
	assertNoError(t, runStoreAction(t, c, "store.set",
		core.Option{Key: "group", Value: "ns"},
		core.Option{Key: "key", Value: "b"},
		core.Option{Key: "value", Value: "2"},
	))

	all := runStoreAction(t, c, "store.get_all", core.Option{Key: "group", Value: "ns"})
	assertNoError(t, all)
	entries, ok := all.Value.(map[string]string)
	assertTrue(t, ok)
	assertEqual(t, "1", entries["a"])
	assertEqual(t, "2", entries["b"])
}

// ---------------------------------------------------------------------------
// groups — listing + prefix filter
// ---------------------------------------------------------------------------

func TestServiceHandlers_Groups_Good_PrefixFilter(t *testing.T) {
	c, _ := newTestServiceCore(t)
	assertNoError(t, runStoreAction(t, c, "store.set",
		core.Option{Key: "group", Value: "ide.subagent.alpha"},
		core.Option{Key: "key", Value: "k"},
		core.Option{Key: "value", Value: "v"},
	))
	assertNoError(t, runStoreAction(t, c, "store.set",
		core.Option{Key: "group", Value: "other.namespace"},
		core.Option{Key: "key", Value: "k"},
		core.Option{Key: "value", Value: "v"},
	))

	filtered := runStoreAction(t, c, "store.groups", core.Option{Key: "prefix", Value: "ide.subagent."})
	assertNoError(t, filtered)
	groups, ok := filtered.Value.([]string)
	assertTrue(t, ok)
	assertContainsElement(t, groups, "ide.subagent.alpha")

	all := runStoreAction(t, c, "store.groups")
	assertNoError(t, all)
	allGroups, ok := all.Value.([]string)
	assertTrue(t, ok)
	assertContainsElement(t, allGroups, "other.namespace")
}

// ---------------------------------------------------------------------------
// delete / delete_group
// ---------------------------------------------------------------------------

func TestServiceHandlers_Delete_Good_RemovesEntry(t *testing.T) {
	c, _ := newTestServiceCore(t)
	assertNoError(t, runStoreAction(t, c, "store.set",
		core.Option{Key: "group", Value: "config"},
		core.Option{Key: "key", Value: "host"},
		core.Option{Key: "value", Value: "homelab"},
	))
	assertNoError(t, runStoreAction(t, c, "store.delete",
		core.Option{Key: "group", Value: "config"},
		core.Option{Key: "key", Value: "host"},
	))

	get := runStoreAction(t, c, "store.get",
		core.Option{Key: "group", Value: "config"},
		core.Option{Key: "key", Value: "host"},
	)
	// Get on a removed key surfaces NotFoundError, not an empty value.
	assertError(t, get)
	assertContainsString(t, get.Error(), "not found")
}

func TestServiceHandlers_DeleteGroup_Good_ClearsNamespace(t *testing.T) {
	c, _ := newTestServiceCore(t)
	assertNoError(t, runStoreAction(t, c, "store.set",
		core.Option{Key: "group", Value: "ephemeral"},
		core.Option{Key: "key", Value: "k"},
		core.Option{Key: "value", Value: "v"},
	))
	assertNoError(t, runStoreAction(t, c, "store.delete_group", core.Option{Key: "group", Value: "ephemeral"}))

	all := runStoreAction(t, c, "store.get_all", core.Option{Key: "group", Value: "ephemeral"})
	assertNoError(t, all)
	entries, ok := all.Value.(map[string]string)
	assertTrue(t, ok)
	assertLen(t, entries, 0)
}

// ---------------------------------------------------------------------------
// compact — the action handler routes through Store.Compact
// ---------------------------------------------------------------------------

// NOTE (flagged, not fixed): handleCompact only reads "output" and "format"
// options — it never sets CompactOptions.Before. CompactOptions.Validate()
// rejects an empty Before cutoff, so the store.compact action can never reach
// a successful compaction through the action surface as currently wired. The
// option is genuinely missing from the handler, not a test artefact. Tracked
// as a behavioural gap rather than fixed here (out of scope for an additive
// test pass). The test below pins the current, real behaviour: the handler
// surfaces the validation failure rather than panicking.
func TestServiceHandlers_Compact_Bad_MissingBeforeCutoff(t *testing.T) {
	c, _ := newTestServiceCore(t)
	r := runStoreAction(t, c, "store.compact",
		core.Option{Key: "output", Value: t.TempDir()},
		core.Option{Key: "format", Value: "gzip"},
	)
	assertError(t, r)
	assertContainsString(t, r.Error(), "before cutoff time is empty")
}

// ---------------------------------------------------------------------------
// handlers reject an uninitialised service (Bad)
// ---------------------------------------------------------------------------

func TestServiceHandlers_AllHandlers_Bad_NilStore(t *testing.T) {
	svc := &Service{}
	opts := core.NewOptions()
	ctx := nilContext()

	assertError(t, svc.handleGet(ctx, opts))
	assertError(t, svc.handleSet(ctx, opts))
	assertError(t, svc.handleDelete(ctx, opts))
	assertError(t, svc.handleGetAll(ctx, opts))
	assertError(t, svc.handleGroups(ctx, opts))
	assertError(t, svc.handleDeleteGroup(ctx, opts))
	assertError(t, svc.handleCompact(ctx, opts))
}

func nilContext() core.Context { return core.Background() }

// ---------------------------------------------------------------------------
// Register — imperative-style construction + OnShutdown
// ---------------------------------------------------------------------------

// Register wires an empty StoreConfig{}, whose Validate() rejects the empty
// DatabasePath — so the imperative Register path fails until a path is set.
// This pins that real behaviour (Bad path) rather than asserting a success the
// code cannot produce.
func TestServiceHandlers_Register_Bad_EmptyDatabasePath(t *testing.T) {
	c := core.New()
	r := Register(c)
	assertError(t, r)
	assertContainsString(t, r.Error(), "database path is empty")
}

// NewService with an explicit path produces a usable, closable Service.
func TestServiceHandlers_NewService_Good_ProducesClosableService(t *testing.T) {
	c := core.New(core.WithName("store", NewService(StoreConfig{DatabasePath: testMemoryDatabasePath})))
	svc := core.MustServiceFor[*Service](c, "store")
	assertNotNil(t, svc.Store)
	assertNoError(t, svc.OnShutdown(core.Background()))
}

func TestServiceHandlers_OnShutdown_Bad_NilReceiverIsSafe(t *testing.T) {
	var svc *Service
	assertNoError(t, svc.OnShutdown(core.Background()))
}
