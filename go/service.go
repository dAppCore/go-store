// SPDX-License-Identifier: EUPL-1.2

// Service registration for the store package. Exposes the Store surface
// as a Core service with action handlers so consumers can wire store
// operations through the same plumbing as every other core service.
//
//	c, _ := core.New(
//	    core.WithName("store", store.NewService(store.StoreConfig{
//	        DatabasePath: "/var/lib/core/store.db",
//	    })),
//	)
//	r := c.Action("store.get").Run(ctx, core.NewOptions(
//	    core.Option{Key: "group", Value: "config"},
//	    core.Option{Key: "key", Value: "host"},
//	))

package store

import (
	"context"

	core "dappco.re/go"
)

// Service is the registerable handle for the store package — embeds
// *core.ServiceRuntime[StoreConfig] for typed options access and
// holds a live *Store ready for direct method calls or action use.
//
// Usage example: `svc := core.MustServiceFor[*store.Service](c, "store"); svc.Store.Set("config", "host", "homelab")`
type Service struct {
	*core.ServiceRuntime[StoreConfig]
	// Store is the live store.Store the service was constructed with.
	// Usage example: `svc.Store.Set("config", "host", "homelab")`
	Store         *Store
	registrations core.Once
}

// NewService returns a factory that opens the store and produces a
// *Service ready for c.Service() registration. Use through
// core.WithName so the framework wires lifecycle (OnStartup registers
// actions, OnShutdown closes the store).
//
// Usage example: `c, _ := core.New(core.WithName("store", store.NewService(store.StoreConfig{DatabasePath: "/var/lib/core/store.db"})))`
func NewService(config StoreConfig) func(*core.Core) core.Result {
	return func(c *core.Core) core.Result {
		st, r := NewConfigured(config)
		if !r.OK {
			return r
		}
		return core.Ok(&Service{
			ServiceRuntime: core.NewServiceRuntime(c, config),
			Store:          st,
		})
	}
}

// OnStartup registers the store action handlers on the attached Core.
// Implements core.Startable. Idempotent via core.Once — multiple
// startups (e.g. test re-entry) won't double-register.
//
// Usage example: `r := svc.OnStartup(ctx)`
func (s *Service) OnStartup(context.Context) core.Result {
	if s == nil {
		return core.Ok(nil)
	}
	s.registrations.Do(func() {
		c := s.Core()
		if c == nil {
			return
		}
		c.Action("store.get", s.handleGet)
		c.Action("store.set", s.handleSet)
		c.Action("store.delete", s.handleDelete)
		c.Action("store.get_all", s.handleGetAll)
		c.Action("store.groups", s.handleGroups)
		c.Action("store.delete_group", s.handleDeleteGroup)
		c.Action("store.compact", s.handleCompact)
	})
	return core.Ok(nil)
}

// OnShutdown closes the underlying store. Implements core.Stoppable.
//
// Usage example: `r := svc.OnShutdown(ctx)`
func (s *Service) OnShutdown(context.Context) core.Result {
	if s == nil || s.Store == nil {
		return core.Ok(nil)
	}
	return s.Store.Close()
}

// handleGet — `store.get` action handler. Reads opts.group + opts.key
// and returns the stored string in r.Value.
//
//	r := c.Action("store.get").Run(ctx, core.NewOptions(
//	    core.Option{Key: "group", Value: "config"},
//	    core.Option{Key: "key", Value: "host"},
//	))
//	host, _ := r.Value.(string)
func (s *Service) handleGet(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Store == nil {
		return core.Fail(core.E("store.get", "service not initialised", nil))
	}
	val, r := s.Store.Get(opts.String("group"), opts.String("key"))
	if !r.OK {
		return r
	}
	return core.Ok(val)
}

// handleSet — `store.set` action handler. Reads opts.group + opts.key +
// opts.value (string).
//
//	r := c.Action("store.set").Run(ctx, core.NewOptions(
//	    core.Option{Key: "group", Value: "config"},
//	    core.Option{Key: "key", Value: "host"},
//	    core.Option{Key: "value", Value: "homelab"},
//	))
func (s *Service) handleSet(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Store == nil {
		return core.Fail(core.E("store.set", "service not initialised", nil))
	}
	if r := s.Store.Set(opts.String("group"), opts.String("key"), opts.String("value")); !r.OK {
		return r
	}
	return core.Ok(nil)
}

// handleDelete — `store.delete` action handler. Reads opts.group +
// opts.key.
//
//	r := c.Action("store.delete").Run(ctx, core.NewOptions(
//	    core.Option{Key: "group", Value: "config"},
//	    core.Option{Key: "key", Value: "host"},
//	))
func (s *Service) handleDelete(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Store == nil {
		return core.Fail(core.E("store.delete", "service not initialised", nil))
	}
	if r := s.Store.Delete(opts.String("group"), opts.String("key")); !r.OK {
		return r
	}
	return core.Ok(nil)
}

// handleGetAll — `store.get_all` action handler. Reads opts.group and
// returns the namespace's full key/value map in r.Value.
//
//	r := c.Action("store.get_all").Run(ctx, core.NewOptions(
//	    core.Option{Key: "group", Value: "config"},
//	))
//	entries, _ := r.Value.(map[string]string)
func (s *Service) handleGetAll(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Store == nil {
		return core.Fail(core.E("store.get_all", "service not initialised", nil))
	}
	entries, r := s.Store.GetAll(opts.String("group"))
	if !r.OK {
		return r
	}
	return core.Ok(entries)
}

// handleGroups — `store.groups` action handler. Optional opts.prefix
// narrows the listing.
//
//	r := c.Action("store.groups").Run(ctx, core.NewOptions(
//	    core.Option{Key: "prefix", Value: "ide.subagent."},
//	))
//	groups, _ := r.Value.([]string)
func (s *Service) handleGroups(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Store == nil {
		return core.Fail(core.E("store.groups", "service not initialised", nil))
	}
	prefixes := []string{}
	if prefix := opts.String("prefix"); prefix != "" {
		prefixes = append(prefixes, prefix)
	}
	groups, r := s.Store.Groups(prefixes...)
	if !r.OK {
		return r
	}
	return core.Ok(groups)
}

// handleDeleteGroup — `store.delete_group` action handler. Removes
// every entry in opts.group.
//
//	r := c.Action("store.delete_group").Run(ctx, core.NewOptions(
//	    core.Option{Key: "group", Value: "config"},
//	))
func (s *Service) handleDeleteGroup(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Store == nil {
		return core.Fail(core.E("store.delete_group", "service not initialised", nil))
	}
	if r := s.Store.DeleteGroup(opts.String("group")); !r.OK {
		return r
	}
	return core.Ok(nil)
}

// handleCompact — `store.compact` action handler. Reads
// CompactOptions-shaped opts (output, format).
//
//	r := c.Action("store.compact").Run(ctx, core.NewOptions(
//	    core.Option{Key: "output", Value: "/var/lib/core/archive"},
//	    core.Option{Key: "format", Value: "json"},
//	))
func (s *Service) handleCompact(_ core.Context, opts core.Options) core.Result {
	if s == nil || s.Store == nil {
		return core.Fail(core.E("store.compact", "service not initialised", nil))
	}
	options := CompactOptions{
		Output: opts.String("output"),
		Format: opts.String("format"),
	}
	if r := s.Store.Compact(options); !r.OK {
		return r
	}
	return core.Ok(nil)
}
