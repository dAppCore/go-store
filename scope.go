package store

import (
	"errors"
	"fmt"
	"iter"
	"regexp"
	"time"
)

// validNamespace matches alphanumeric characters and hyphens (non-empty).
var validNamespace = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// QuotaConfig defines optional limits for a ScopedStore namespace.
// Zero values mean unlimited.
type QuotaConfig struct {
	MaxKeys   int // maximum total keys across all groups in the namespace
	MaxGroups int // maximum distinct groups in the namespace
}

// ScopedStore wraps a *Store and auto-prefixes all group names with a
// namespace to prevent key collisions across tenants.
type ScopedStore struct {
	store     *Store
	namespace string
	quota     QuotaConfig
}

// NewScoped creates a ScopedStore that prefixes all groups with the given
// namespace. The namespace must be non-empty and contain only alphanumeric
// characters and hyphens.
func NewScoped(store *Store, namespace string) (*ScopedStore, error) {
	if !validNamespace.MatchString(namespace) {
		return nil, fmt.Errorf("store.NewScoped: namespace %q is invalid (must be non-empty, alphanumeric + hyphens)", namespace)
	}
	return &ScopedStore{store: store, namespace: namespace}, nil
}

// NewScopedWithQuota creates a ScopedStore with quota enforcement. Quotas are
// checked on Set and SetWithTTL before inserting new keys or creating new
// groups.
func NewScopedWithQuota(store *Store, namespace string, quota QuotaConfig) (*ScopedStore, error) {
	s, err := NewScoped(store, namespace)
	if err != nil {
		return nil, err
	}
	s.quota = quota
	return s, nil
}

// prefix returns the namespaced group name.
func (s *ScopedStore) prefix(group string) string {
	return s.namespace + ":" + group
}

// Namespace returns the namespace string for this scoped store.
func (s *ScopedStore) Namespace() string {
	return s.namespace
}

// Get retrieves a value by group and key within the namespace.
func (s *ScopedStore) Get(group, key string) (string, error) {
	return s.store.Get(s.prefix(group), key)
}

// Set stores a value by group and key within the namespace. If quotas are
// configured, they are checked before inserting new keys or groups.
func (s *ScopedStore) Set(group, key, value string) error {
	if err := s.checkQuota(group, key); err != nil {
		return err
	}
	return s.store.Set(s.prefix(group), key, value)
}

// SetWithTTL stores a value with a time-to-live within the namespace. Quota
// checks are applied for new keys and groups.
func (s *ScopedStore) SetWithTTL(group, key, value string, ttl time.Duration) error {
	if err := s.checkQuota(group, key); err != nil {
		return err
	}
	return s.store.SetWithTTL(s.prefix(group), key, value, ttl)
}

// Delete removes a single key from a group within the namespace.
func (s *ScopedStore) Delete(group, key string) error {
	return s.store.Delete(s.prefix(group), key)
}

// DeleteGroup removes all keys in a group within the namespace.
func (s *ScopedStore) DeleteGroup(group string) error {
	return s.store.DeleteGroup(s.prefix(group))
}

// GetAll returns all non-expired key-value pairs in a group within the
// namespace.
func (s *ScopedStore) GetAll(group string) (map[string]string, error) {
	return s.store.GetAll(s.prefix(group))
}

// All returns an iterator over all non-expired key-value pairs in a group
// within the namespace.
func (s *ScopedStore) All(group string) iter.Seq2[KV, error] {
	return s.store.All(s.prefix(group))
}

// Count returns the number of non-expired keys in a group within the namespace.
func (s *ScopedStore) Count(group string) (int, error) {
	return s.store.Count(s.prefix(group))
}

// Render loads all non-expired key-value pairs from a namespaced group and
// renders a Go template.
func (s *ScopedStore) Render(tmplStr, group string) (string, error) {
	return s.store.Render(tmplStr, s.prefix(group))
}

// checkQuota verifies that inserting key into group would not exceed the
// namespace's quota limits. It returns nil if no quota is set or the operation
// is within bounds. Existing keys (upserts) are not counted as new.
func (s *ScopedStore) checkQuota(group, key string) error {
	if s.quota.MaxKeys == 0 && s.quota.MaxGroups == 0 {
		return nil
	}

	prefixedGroup := s.prefix(group)
	nsPrefix := s.namespace + ":"

	// Check if this is an upsert (key already exists) — upserts never exceed quota.
	_, err := s.store.Get(prefixedGroup, key)
	if err == nil {
		// Key exists — this is an upsert, no quota check needed.
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		// A database error occurred, not just a "not found" result.
		return fmt.Errorf("store.ScopedStore: quota check: %w", err)
	}

	// Check MaxKeys quota.
	if s.quota.MaxKeys > 0 {
		count, err := s.store.CountAll(nsPrefix)
		if err != nil {
			return fmt.Errorf("store.ScopedStore: quota check: %w", err)
		}
		if count >= s.quota.MaxKeys {
			return fmt.Errorf("store.ScopedStore: key limit (%d): %w", s.quota.MaxKeys, ErrQuotaExceeded)
		}
	}

	// Check MaxGroups quota — only if this would create a new group.
	if s.quota.MaxGroups > 0 {
		groupCount, err := s.store.Count(prefixedGroup)
		if err != nil {
			return fmt.Errorf("store.ScopedStore: quota check: %w", err)
		}
		if groupCount == 0 {
			// This group is new — check if adding it would exceed the group limit.
			count := 0
			for _, err := range s.store.GroupsSeq(nsPrefix) {
				if err != nil {
					return fmt.Errorf("store.ScopedStore: quota check: %w", err)
				}
				count++
			}
			if count >= s.quota.MaxGroups {
				return fmt.Errorf("store.ScopedStore: group limit (%d): %w", s.quota.MaxGroups, ErrQuotaExceeded)
			}
		}
	}

	return nil
}
