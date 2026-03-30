package store

import (
	"context"
	"database/sql"
	"iter"
	"sync"
	"text/template"
	"time"
	"unicode"

	core "dappco.re/go/core"
	_ "modernc.org/sqlite"
)

// NotFoundError is returned when a key does not exist in the store.
// Usage example: `if core.Is(err, store.NotFoundError) { return }`
var NotFoundError = core.E("store", "not found", nil)

// QuotaExceededError is returned when a namespace quota limit is reached.
// Usage example: `if core.Is(err, store.QuotaExceededError) { return }`
var QuotaExceededError = core.E("store", "quota exceeded", nil)

// Store is a group-namespaced key-value store backed by SQLite.
// Usage example: `st, _ := store.New(":memory:")`
type Store struct {
	db            *sql.DB
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	purgeInterval time.Duration // interval between background purge cycles

	// Event dispatch state.
	watchers           []*Watcher
	callbacks          []callbackEntry
	mu                 sync.RWMutex // protects watchers and callbacks
	nextRegistrationID uint64       // monotonic ID for watchers and callbacks
}

// New creates a Store at the given SQLite path. Use ":memory:" for tests.
// Usage example: `st, _ := store.New("/tmp/config.db")`
func New(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, core.E("store.New", "open", err)
	}
	// Serialise all access through a single connection. SQLite only supports
	// one writer at a time; using a pool causes SQLITE_BUSY under contention
	// because pragmas (journal_mode, busy_timeout) are per-connection and the
	// pool hands out different connections for each call.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, core.E("store.New", "WAL", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, core.E("store.New", "busy_timeout", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (
		grp        TEXT NOT NULL,
		key        TEXT NOT NULL,
		value      TEXT NOT NULL,
		expires_at INTEGER,
		PRIMARY KEY (grp, key)
	)`); err != nil {
		db.Close()
		return nil, core.E("store.New", "schema", err)
	}
	// Ensure the expires_at column exists for databases created before TTL support.
	if _, err := db.Exec("ALTER TABLE kv ADD COLUMN expires_at INTEGER"); err != nil {
		// SQLite returns "duplicate column name" if it already exists.
		if !core.Contains(err.Error(), "duplicate column name") {
			db.Close()
			return nil, core.E("store.New", "migration", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{db: db, cancel: cancel, purgeInterval: 60 * time.Second}
	s.startPurge(ctx)
	return s, nil
}

// Close stops the background purge goroutine and closes the underlying database.
// Usage example: `defer st.Close()`
func (s *Store) Close() error {
	s.cancel()
	s.wg.Wait()
	return s.db.Close()
}

// Get returns the live value for a group/key pair. Expired keys are lazily
// deleted and treated as not found.
// Usage example: `value, err := st.Get("config", "theme")`
func (s *Store) Get(group, key string) (string, error) {
	var value string
	var expiresAt sql.NullInt64
	err := s.db.QueryRow(
		"SELECT value, expires_at FROM kv WHERE grp = ? AND key = ?",
		group, key,
	).Scan(&value, &expiresAt)
	if err == sql.ErrNoRows {
		return "", core.E("store.Get", core.Concat(group, "/", key), NotFoundError)
	}
	if err != nil {
		return "", core.E("store.Get", "query", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		_, _ = s.db.Exec("DELETE FROM kv WHERE grp = ? AND key = ?", group, key)
		return "", core.E("store.Get", core.Concat(group, "/", key), NotFoundError)
	}
	return value, nil
}

// Set stores a value by group and key, overwriting any existing row and
// clearing its expiry.
// Usage example: `err := st.Set("config", "theme", "dark")`
func (s *Store) Set(group, key, value string) error {
	_, err := s.db.Exec(
		`INSERT INTO kv (grp, key, value, expires_at) VALUES (?, ?, ?, NULL)
		 ON CONFLICT(grp, key) DO UPDATE SET value = excluded.value, expires_at = NULL`,
		group, key, value,
	)
	if err != nil {
		return core.E("store.Set", "exec", err)
	}
	s.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// SetWithTTL stores a value that expires after the given duration. After
// expiry, the key is lazily removed on the next Get and periodically by the
// background purge goroutine.
// Usage example: `err := st.SetWithTTL("session", "token", "abc", time.Hour)`
func (s *Store) SetWithTTL(group, key, value string, ttl time.Duration) error {
	expiresAt := time.Now().Add(ttl).UnixMilli()
	_, err := s.db.Exec(
		`INSERT INTO kv (grp, key, value, expires_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(grp, key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at`,
		group, key, value, expiresAt,
	)
	if err != nil {
		return core.E("store.SetWithTTL", "exec", err)
	}
	s.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Delete removes a single key from a group.
// Usage example: `err := st.Delete("config", "theme")`
func (s *Store) Delete(group, key string) error {
	_, err := s.db.Exec("DELETE FROM kv WHERE grp = ? AND key = ?", group, key)
	if err != nil {
		return core.E("store.Delete", "exec", err)
	}
	s.notify(Event{Type: EventDelete, Group: group, Key: key, Timestamp: time.Now()})
	return nil
}

// Count returns the number of live keys in a group.
// Usage example: `count, err := st.Count("config")`
func (s *Store) Count(group string) (int, error) {
	var count int
	err := s.db.QueryRow(
		"SELECT COUNT(*) FROM kv WHERE grp = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	).Scan(&count)
	if err != nil {
		return 0, core.E("store.Count", "query", err)
	}
	return count, nil
}

// DeleteGroup removes all keys in a group.
// Usage example: `err := st.DeleteGroup("cache")`
func (s *Store) DeleteGroup(group string) error {
	_, err := s.db.Exec("DELETE FROM kv WHERE grp = ?", group)
	if err != nil {
		return core.E("store.DeleteGroup", "exec", err)
	}
	s.notify(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	return nil
}

// KeyValue represents a key-value pair.
// Usage example: `for entry, err := range st.All("config") { _ = entry }`
type KeyValue struct {
	Key, Value string
}

// GetAll returns all non-expired key-value pairs in a group.
// Usage example: `entries, err := st.GetAll("config")`
func (s *Store) GetAll(group string) (map[string]string, error) {
	entriesByKey := make(map[string]string)
	for entry, err := range s.All(group) {
		if err != nil {
			return nil, core.E("store.GetAll", "iterate", err)
		}
		entriesByKey[entry.Key] = entry.Value
	}
	return entriesByKey, nil
}

// All returns an iterator over all non-expired key-value pairs in a group.
// Usage example: `for entry, err := range st.All("config") { _ = entry; _ = err }`
func (s *Store) All(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		rows, err := s.db.Query(
			"SELECT key, value FROM kv WHERE grp = ? AND (expires_at IS NULL OR expires_at > ?)",
			group, time.Now().UnixMilli(),
		)
		if err != nil {
			yield(KeyValue{}, core.E("store.All", "query", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var entry KeyValue
			if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
				if !yield(KeyValue{}, core.E("store.All", "scan", err)) {
					return
				}
				continue
			}
			if !yield(entry, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(KeyValue{}, core.E("store.All", "rows", err))
		}
	}
}

// GetSplit retrieves a value and returns an iterator over its parts, split by
// separator.
// Usage example: `parts, _ := st.GetSplit("config", "hosts", ",")`
func (s *Store) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	value, err := s.Get(group, key)
	if err != nil {
		return nil, err
	}
	return splitSeq(value, separator), nil
}

// GetFields retrieves a value and returns an iterator over its parts, split by
// whitespace.
// Usage example: `fields, _ := st.GetFields("config", "flags")`
func (s *Store) GetFields(group, key string) (iter.Seq[string], error) {
	value, err := s.Get(group, key)
	if err != nil {
		return nil, err
	}
	return fieldsSeq(value), nil
}

// Render loads all non-expired key-value pairs from a group and renders a Go
// template.
// Usage example: `out, err := st.Render("Hello {{ .name }}", "user")`
func (s *Store) Render(templateSource, group string) (string, error) {
	templateData := make(map[string]string)
	for entry, err := range s.All(group) {
		if err != nil {
			return "", core.E("store.Render", "iterate", err)
		}
		templateData[entry.Key] = entry.Value
	}

	tmpl, err := template.New("render").Parse(templateSource)
	if err != nil {
		return "", core.E("store.Render", "parse", err)
	}
	builder := core.NewBuilder()
	if err := tmpl.Execute(builder, templateData); err != nil {
		return "", core.E("store.Render", "exec", err)
	}
	return builder.String(), nil
}

// CountAll returns the total number of non-expired keys across all groups whose
// name starts with the given prefix. Pass an empty string to count everything.
// Usage example: `count, err := st.CountAll("tenant-a:")`
func (s *Store) CountAll(groupPrefix string) (int, error) {
	var count int
	var err error
	if groupPrefix == "" {
		err = s.db.QueryRow(
			"SELECT COUNT(*) FROM kv WHERE (expires_at IS NULL OR expires_at > ?)",
			time.Now().UnixMilli(),
		).Scan(&count)
	} else {
		err = s.db.QueryRow(
			"SELECT COUNT(*) FROM kv WHERE grp LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
			escapeLike(groupPrefix)+"%", time.Now().UnixMilli(),
		).Scan(&count)
	}
	if err != nil {
		return 0, core.E("store.CountAll", "query", err)
	}
	return count, nil
}

// Groups returns the distinct group names of all non-expired keys. If prefix is
// non-empty, only groups starting with that prefix are returned.
// Usage example: `groupNames, err := st.Groups("tenant-a:")`
func (s *Store) Groups(groupPrefix string) ([]string, error) {
	var groupNames []string
	for groupName, err := range s.GroupsSeq(groupPrefix) {
		if err != nil {
			return nil, err
		}
		groupNames = append(groupNames, groupName)
	}
	return groupNames, nil
}

// GroupsSeq returns an iterator over the distinct group names of all
// non-expired keys.
// Usage example: `for groupName, err := range st.GroupsSeq("tenant-a:") { _ = groupName }`
func (s *Store) GroupsSeq(groupPrefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		var rows *sql.Rows
		var err error
		now := time.Now().UnixMilli()
		if groupPrefix == "" {
			rows, err = s.db.Query(
				"SELECT DISTINCT grp FROM kv WHERE (expires_at IS NULL OR expires_at > ?)",
				now,
			)
		} else {
			rows, err = s.db.Query(
				"SELECT DISTINCT grp FROM kv WHERE grp LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
				escapeLike(groupPrefix)+"%", now,
			)
		}
		if err != nil {
			yield("", core.E("store.Groups", "query", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var groupName string
			if err := rows.Scan(&groupName); err != nil {
				if !yield("", core.E("store.Groups", "scan", err)) {
					return
				}
				continue
			}
			if !yield(groupName, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield("", core.E("store.Groups", "rows", err))
		}
	}
}

// escapeLike escapes SQLite LIKE wildcards and the escape character itself.
func escapeLike(s string) string {
	s = core.Replace(s, "^", "^^")
	s = core.Replace(s, "%", "^%")
	s = core.Replace(s, "_", "^_")
	return s
}

// PurgeExpired deletes all expired keys across all groups. Returns the number
// of rows removed.
// Usage example: `removed, err := st.PurgeExpired()`
func (s *Store) PurgeExpired() (int64, error) {
	result, err := s.db.Exec("DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?",
		time.Now().UnixMilli())
	if err != nil {
		return 0, core.E("store.PurgeExpired", "exec", err)
	}
	return result.RowsAffected()
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

// splitSeq preserves the iter.Seq API without importing strings directly.
func splitSeq(value, sep string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, part := range core.Split(value, sep) {
			if !yield(part) {
				return
			}
		}
	}
}

// fieldsSeq yields whitespace-delimited fields without importing strings.
func fieldsSeq(value string) iter.Seq[string] {
	return func(yield func(string) bool) {
		start := -1
		for i, r := range value {
			if unicode.IsSpace(r) {
				if start >= 0 {
					if !yield(value[start:i]) {
						return
					}
					start = -1
				}
				continue
			}
			if start < 0 {
				start = i
			}
		}
		if start >= 0 {
			yield(value[start:])
		}
	}
}
