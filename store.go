package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned when a key does not exist in the store.
var ErrNotFound = errors.New("store: not found")

// ErrQuotaExceeded is returned when a namespace quota limit is reached.
var ErrQuotaExceeded = errors.New("store: quota exceeded")

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
		return nil, fmt.Errorf("store.New: %w", err)
	}
	// Serialise all access through a single connection. SQLite only supports
	// one writer at a time; using a pool causes SQLITE_BUSY under contention
	// because pragmas (journal_mode, busy_timeout) are per-connection and the
	// pool hands out different connections for each call.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("store.New: WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("store.New: busy_timeout: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (
		grp        TEXT NOT NULL,
		key        TEXT NOT NULL,
		value      TEXT NOT NULL,
		expires_at INTEGER,
		PRIMARY KEY (grp, key)
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("store.New: schema: %w", err)
	}
	// Ensure the expires_at column exists for databases created before TTL support.
	// ALTER TABLE ADD COLUMN errors with "duplicate column" if it already exists;
	// this is expected and harmless.
	_, _ = db.Exec("ALTER TABLE kv ADD COLUMN expires_at INTEGER")

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
		return "", fmt.Errorf("store.Get: %s/%s: %w", group, key, ErrNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("store.Get: %w", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		// Lazily delete the expired entry.
		_, _ = s.db.Exec("DELETE FROM kv WHERE grp = ? AND key = ?", group, key)
		return "", fmt.Errorf("store.Get: %s/%s: %w", group, key, ErrNotFound)
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
		return fmt.Errorf("store.Set: %w", err)
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
		return fmt.Errorf("store.SetWithTTL: %w", err)
	}
	s.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Delete removes a single key from a group.
func (s *Store) Delete(group, key string) error {
	_, err := s.db.Exec("DELETE FROM kv WHERE grp = ? AND key = ?", group, key)
	if err != nil {
		return fmt.Errorf("store.Delete: %w", err)
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
		return 0, fmt.Errorf("store.Count: %w", err)
	}
	return n, nil
}

// DeleteGroup removes all keys in a group.
func (s *Store) DeleteGroup(group string) error {
	_, err := s.db.Exec("DELETE FROM kv WHERE grp = ?", group)
	if err != nil {
		return fmt.Errorf("store.DeleteGroup: %w", err)
	}
	s.notify(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	return nil
}

// GetAll returns all non-expired key-value pairs in a group.
func (s *Store) GetAll(group string) (map[string]string, error) {
	rows, err := s.db.Query(
		"SELECT key, value FROM kv WHERE grp = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	)
	if err != nil {
		return nil, fmt.Errorf("store.GetAll: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("store.GetAll: scan: %w", err)
		}
		result[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store.GetAll: rows: %w", err)
	}
	return result, nil
}

// Render loads all non-expired key-value pairs from a group and renders a Go
// template.
func (s *Store) Render(tmplStr, group string) (string, error) {
	rows, err := s.db.Query(
		"SELECT key, value FROM kv WHERE grp = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	)
	if err != nil {
		return "", fmt.Errorf("store.Render: query: %w", err)
	}
	defer rows.Close()

	vars := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return "", fmt.Errorf("store.Render: scan: %w", err)
		}
		vars[k] = v
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("store.Render: rows: %w", err)
	}

	tmpl, err := template.New("render").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("store.Render: parse: %w", err)
	}
	var b strings.Builder
	if err := tmpl.Execute(&b, vars); err != nil {
		return "", fmt.Errorf("store.Render: exec: %w", err)
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
			"SELECT COUNT(*) FROM kv WHERE grp LIKE ? AND (expires_at IS NULL OR expires_at > ?)",
			prefix+"%", time.Now().UnixMilli(),
		).Scan(&n)
	}
	if err != nil {
		return 0, fmt.Errorf("store.CountAll: %w", err)
	}
	return n, nil
}

// Groups returns the distinct group names of all non-expired keys. If prefix is
// non-empty, only groups starting with that prefix are returned.
func (s *Store) Groups(prefix string) ([]string, error) {
	var rows *sql.Rows
	var err error
	if prefix == "" {
		rows, err = s.db.Query(
			"SELECT DISTINCT grp FROM kv WHERE (expires_at IS NULL OR expires_at > ?)",
			time.Now().UnixMilli(),
		)
	} else {
		rows, err = s.db.Query(
			"SELECT DISTINCT grp FROM kv WHERE grp LIKE ? AND (expires_at IS NULL OR expires_at > ?)",
			prefix+"%", time.Now().UnixMilli(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("store.Groups: %w", err)
	}
	defer rows.Close()

	var groups []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return nil, fmt.Errorf("store.Groups: scan: %w", err)
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store.Groups: rows: %w", err)
	}
	return groups, nil
}

// PurgeExpired deletes all expired keys across all groups. Returns the number
// of rows removed.
func (s *Store) PurgeExpired() (int64, error) {
	res, err := s.db.Exec("DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?",
		time.Now().UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("store.PurgeExpired: %w", err)
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
				_, _ = s.PurgeExpired()
			}
		}
	})
}
