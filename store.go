package store

import (
	"context"
	"database/sql"
	"iter"
	"strings"
	"sync"
	"text/template"
	"time"

	coreerr "forge.lthn.ai/core/go-log"
	_ "modernc.org/sqlite"
)

// ErrNotFound is returned when a key does not exist in the store.
var ErrNotFound = coreerr.E("store", "not found", nil)

// ErrQuotaExceeded is returned when a namespace quota limit is reached.
var ErrQuotaExceeded = coreerr.E("store", "quota exceeded", nil)

// Store is a group-namespaced key-value store backed by SQLite.
type Store struct {
	db            *sql.DB
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	purgeInterval time.Duration // interval between background purge cycles

	// Event hooks (Phase 3).
	watchers  []*Watcher
	callbacks []callbackEntry
	mu        sync.RWMutex // protects watchers and callbacks
	nextID    uint64       // monotonic ID for watchers and callbacks
}

// New creates a Store at the given SQLite path. Use ":memory:" for tests.
func New(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, coreerr.E("store.New", "open", err)
	}
	// Serialise all access through a single connection. SQLite only supports
	// one writer at a time; using a pool causes SQLITE_BUSY under contention
	// because pragmas (journal_mode, busy_timeout) are per-connection and the
	// pool hands out different connections for each call.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, coreerr.E("store.New", "WAL", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, coreerr.E("store.New", "busy_timeout", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (
		grp        TEXT NOT NULL,
		key        TEXT NOT NULL,
		value      TEXT NOT NULL,
		expires_at INTEGER,
		PRIMARY KEY (grp, key)
	)`); err != nil {
		db.Close()
		return nil, coreerr.E("store.New", "schema", err)
	}
	// Ensure the expires_at column exists for databases created before TTL support.
	if _, err := db.Exec("ALTER TABLE kv ADD COLUMN expires_at INTEGER"); err != nil {
		// SQLite returns "duplicate column name" if it already exists.
		if !strings.Contains(err.Error(), "duplicate column name") {
			db.Close()
			return nil, coreerr.E("store.New", "migration", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{db: db, cancel: cancel, purgeInterval: 60 * time.Second}
	s.startPurge(ctx)
	return s, nil
}

// Close stops the background purge goroutine and closes the underlying database.
func (s *Store) Close() error {
	s.cancel()
	s.wg.Wait()
	return s.db.Close()
}

// Get retrieves a value by group and key. Expired keys are lazily deleted and
// treated as not found.
func (s *Store) Get(group, key string) (string, error) {
	var val string
	var expiresAt sql.NullInt64
	err := s.db.QueryRow(
		"SELECT value, expires_at FROM kv WHERE grp = ? AND key = ?",
		group, key,
	).Scan(&val, &expiresAt)
	if err == sql.ErrNoRows {
		return "", coreerr.E("store.Get", group+"/"+key, ErrNotFound)
	}
	if err != nil {
		return "", coreerr.E("store.Get", "query", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		// Lazily delete the expired entry.
		if _, err := s.db.Exec("DELETE FROM kv WHERE grp = ? AND key = ?", group, key); err != nil {
			// Log error or ignore; we return ErrNotFound regardless.
			// For now, we wrap the error to provide context if the delete fails
			// for reasons other than "already deleted".
		}
		return "", coreerr.E("store.Get", group+"/"+key, ErrNotFound)
	}
	return val, nil
}

// Set stores a value by group and key, overwriting if exists. The key has no
// expiry (it persists until explicitly deleted).
func (s *Store) Set(group, key, value string) error {
	_, err := s.db.Exec(
		`INSERT INTO kv (grp, key, value, expires_at) VALUES (?, ?, ?, NULL)
		 ON CONFLICT(grp, key) DO UPDATE SET value = excluded.value, expires_at = NULL`,
		group, key, value,
	)
	if err != nil {
		return coreerr.E("store.Set", "exec", err)
	}
	s.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// SetWithTTL stores a value that expires after the given duration. After expiry
// the key is lazily removed on the next Get and periodically by a background
// purge goroutine.
func (s *Store) SetWithTTL(group, key, value string, ttl time.Duration) error {
	expiresAt := time.Now().Add(ttl).UnixMilli()
	_, err := s.db.Exec(
		`INSERT INTO kv (grp, key, value, expires_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(grp, key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at`,
		group, key, value, expiresAt,
	)
	if err != nil {
		return coreerr.E("store.SetWithTTL", "exec", err)
	}
	s.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Delete removes a single key from a group.
func (s *Store) Delete(group, key string) error {
	_, err := s.db.Exec("DELETE FROM kv WHERE grp = ? AND key = ?", group, key)
	if err != nil {
		return coreerr.E("store.Delete", "exec", err)
	}
	s.notify(Event{Type: EventDelete, Group: group, Key: key, Timestamp: time.Now()})
	return nil
}

// Count returns the number of non-expired keys in a group.
func (s *Store) Count(group string) (int, error) {
	var n int
	err := s.db.QueryRow(
		"SELECT COUNT(*) FROM kv WHERE grp = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	).Scan(&n)
	if err != nil {
		return 0, coreerr.E("store.Count", "query", err)
	}
	return n, nil
}

// DeleteGroup removes all keys in a group.
func (s *Store) DeleteGroup(group string) error {
	_, err := s.db.Exec("DELETE FROM kv WHERE grp = ?", group)
	if err != nil {
		return coreerr.E("store.DeleteGroup", "exec", err)
	}
	s.notify(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	return nil
}

// KV represents a key-value pair.
type KV struct {
	Key, Value string
}

// GetAll returns all non-expired key-value pairs in a group.
func (s *Store) GetAll(group string) (map[string]string, error) {
	result := make(map[string]string)
	for kv, err := range s.All(group) {
		if err != nil {
			return nil, coreerr.E("store.GetAll", "iterate", err)
		}
		result[kv.Key] = kv.Value
	}
	return result, nil
}

// All returns an iterator over all non-expired key-value pairs in a group.
func (s *Store) All(group string) iter.Seq2[KV, error] {
	return func(yield func(KV, error) bool) {
		rows, err := s.db.Query(
			"SELECT key, value FROM kv WHERE grp = ? AND (expires_at IS NULL OR expires_at > ?)",
			group, time.Now().UnixMilli(),
		)
		if err != nil {
			yield(KV{}, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var kv KV
			if err := rows.Scan(&kv.Key, &kv.Value); err != nil {
				if !yield(KV{}, coreerr.E("store.All", "scan", err)) {
					return
				}
				continue
			}
			if !yield(kv, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(KV{}, coreerr.E("store.All", "rows", err))
		}
	}
}

// GetSplit retrieves a value and returns an iterator over its parts, split by
// sep.
func (s *Store) GetSplit(group, key, sep string) (iter.Seq[string], error) {
	val, err := s.Get(group, key)
	if err != nil {
		return nil, err
	}
	return strings.SplitSeq(val, sep), nil
}

// GetFields retrieves a value and returns an iterator over its parts, split by
// whitespace.
func (s *Store) GetFields(group, key string) (iter.Seq[string], error) {
	val, err := s.Get(group, key)
	if err != nil {
		return nil, err
	}
	return strings.FieldsSeq(val), nil
}

// Render loads all non-expired key-value pairs from a group and renders a Go
// template.
func (s *Store) Render(tmplStr, group string) (string, error) {
	vars := make(map[string]string)
	for kv, err := range s.All(group) {
		if err != nil {
			return "", coreerr.E("store.Render", "iterate", err)
		}
		vars[kv.Key] = kv.Value
	}

	tmpl, err := template.New("render").Parse(tmplStr)
	if err != nil {
		return "", coreerr.E("store.Render", "parse", err)
	}
	var b strings.Builder
	if err := tmpl.Execute(&b, vars); err != nil {
		return "", coreerr.E("store.Render", "exec", err)
	}
	return b.String(), nil
}

// CountAll returns the total number of non-expired keys across all groups whose
// name starts with the given prefix. Pass an empty string to count everything.
func (s *Store) CountAll(prefix string) (int, error) {
	var n int
	var err error
	if prefix == "" {
		err = s.db.QueryRow(
			"SELECT COUNT(*) FROM kv WHERE (expires_at IS NULL OR expires_at > ?)",
			time.Now().UnixMilli(),
		).Scan(&n)
	} else {
		err = s.db.QueryRow(
			"SELECT COUNT(*) FROM kv WHERE grp LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
			escapeLike(prefix)+"%", time.Now().UnixMilli(),
		).Scan(&n)
	}
	if err != nil {
		return 0, coreerr.E("store.CountAll", "query", err)
	}
	return n, nil
}

// Groups returns the distinct group names of all non-expired keys. If prefix is
// non-empty, only groups starting with that prefix are returned.
func (s *Store) Groups(prefix string) ([]string, error) {
	var groups []string
	for g, err := range s.GroupsSeq(prefix) {
		if err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// GroupsSeq returns an iterator over the distinct group names of all
// non-expired keys.
func (s *Store) GroupsSeq(prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		var rows *sql.Rows
		var err error
		now := time.Now().UnixMilli()
		if prefix == "" {
			rows, err = s.db.Query(
				"SELECT DISTINCT grp FROM kv WHERE (expires_at IS NULL OR expires_at > ?)",
				now,
			)
		} else {
			rows, err = s.db.Query(
				"SELECT DISTINCT grp FROM kv WHERE grp LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
				escapeLike(prefix)+"%", now,
			)
		}
		if err != nil {
			yield("", coreerr.E("store.Groups", "query", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var g string
			if err := rows.Scan(&g); err != nil {
				if !yield("", coreerr.E("store.Groups", "scan", err)) {
					return
				}
				continue
			}
			if !yield(g, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield("", coreerr.E("store.Groups", "rows", err))
		}
	}
}

func escapeLike(s string) string {
	s = strings.ReplaceAll(s, "^", "^^")
	s = strings.ReplaceAll(s, "%", "^%")
	s = strings.ReplaceAll(s, "_", "^_")
	return s
}

// PurgeExpired deletes all expired keys across all groups. Returns the number
// of rows removed.
func (s *Store) PurgeExpired() (int64, error) {
	res, err := s.db.Exec("DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?",
		time.Now().UnixMilli())
	if err != nil {
		return 0, coreerr.E("store.PurgeExpired", "exec", err)
	}
	return res.RowsAffected()
}

// startPurge launches a background goroutine that purges expired entries at the
// store's configured purge interval. It stops when the context is cancelled.
func (s *Store) startPurge(ctx context.Context) {
	s.wg.Go(func() {
		ticker := time.NewTicker(s.purgeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := s.PurgeExpired(); err != nil {
					// We can't return the error as we are in a background goroutine,
					// but we should at least prevent it from being completely silent
					// in a real app (e.g. by logging it). For this module, we keep it
					// running to try again on the next tick.
				}
			}
		}
	})
}
