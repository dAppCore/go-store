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

// Usage example: `if core.Is(err, store.NotFoundError) { return }`
var NotFoundError = core.E("store", "not found", nil)

// Usage example: `if core.Is(err, store.QuotaExceededError) { return }`
var QuotaExceededError = core.E("store", "quota exceeded", nil)

const (
	entriesTableName       = "entries"
	legacyEntriesTableName = "kv"
	entryGroupColumn       = "group_name"
	entryKeyColumn         = "entry_key"
	entryValueColumn       = "entry_value"
)

// Usage example: `storeInstance, _ := store.New(":memory:"); _ = storeInstance.Set("config", "theme", "dark")`
type Store struct {
	database       *sql.DB
	cancelPurge    context.CancelFunc
	purgeWaitGroup sync.WaitGroup
	purgeInterval  time.Duration // interval between background purge cycles

	// Event dispatch state.
	watchers           []*Watcher
	callbacks          []changeCallbackRegistration
	watchersLock       sync.RWMutex // protects watcher registration and dispatch
	callbacksLock      sync.RWMutex // protects callback registration and dispatch
	nextRegistrationID uint64       // monotonic ID for watchers and callbacks
}

// Usage example: `storeInstance, _ := store.New(":memory:")`
func New(databasePath string) (*Store, error) {
	sqliteDatabase, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, core.E("store.New", "open", err)
	}
	// Serialise all access through a single connection. SQLite only supports
	// one writer at a time; using a pool causes SQLITE_BUSY under contention
	// because pragmas (journal_mode, busy_timeout) are per-connection and the
	// pool hands out different connections for each call.
	sqliteDatabase.SetMaxOpenConns(1)
	if _, err := sqliteDatabase.Exec("PRAGMA journal_mode=WAL"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.New", "WAL", err)
	}
	if _, err := sqliteDatabase.Exec("PRAGMA busy_timeout=5000"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.New", "busy_timeout", err)
	}
	if err := ensureSchema(sqliteDatabase); err != nil {
		sqliteDatabase.Close()
		return nil, err
	}

	purgeContext, cancel := context.WithCancel(context.Background())
	storeInstance := &Store{database: sqliteDatabase, cancelPurge: cancel, purgeInterval: 60 * time.Second}
	storeInstance.startPurge(purgeContext)
	return storeInstance, nil
}

// Usage example: `storeInstance, _ := store.New(":memory:"); defer storeInstance.Close()`
func (storeInstance *Store) Close() error {
	storeInstance.cancelPurge()
	storeInstance.purgeWaitGroup.Wait()
	return storeInstance.database.Close()
}

// Usage example: `themeValue, err := storeInstance.Get("config", "theme")`
func (storeInstance *Store) Get(group, key string) (string, error) {
	var value string
	var expiresAt sql.NullInt64
	err := storeInstance.database.QueryRow(
		"SELECT "+entryValueColumn+", expires_at FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?",
		group, key,
	).Scan(&value, &expiresAt)
	if err == sql.ErrNoRows {
		return "", core.E("store.Get", core.Concat(group, "/", key), NotFoundError)
	}
	if err != nil {
		return "", core.E("store.Get", "query", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		_, _ = storeInstance.database.Exec("DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?", group, key)
		return "", core.E("store.Get", core.Concat(group, "/", key), NotFoundError)
	}
	return value, nil
}

// Usage example: `_ = storeInstance.Set("config", "theme", "dark")`
func (storeInstance *Store) Set(group, key, value string) error {
	_, err := storeInstance.database.Exec(
		"INSERT INTO "+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, NULL) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = NULL",
		group, key, value,
	)
	if err != nil {
		return core.E("store.Set", "exec", err)
	}
	storeInstance.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Usage example: `_ = storeInstance.SetWithTTL("session", "token", "abc123", time.Minute)`
func (storeInstance *Store) SetWithTTL(group, key, value string, ttl time.Duration) error {
	expiresAt := time.Now().Add(ttl).UnixMilli()
	_, err := storeInstance.database.Exec(
		"INSERT INTO "+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, ?) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = excluded.expires_at",
		group, key, value, expiresAt,
	)
	if err != nil {
		return core.E("store.SetWithTTL", "exec", err)
	}
	storeInstance.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Usage example: `_ = storeInstance.Delete("config", "theme")`
func (storeInstance *Store) Delete(group, key string) error {
	_, err := storeInstance.database.Exec("DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?", group, key)
	if err != nil {
		return core.E("store.Delete", "exec", err)
	}
	storeInstance.notify(Event{Type: EventDelete, Group: group, Key: key, Timestamp: time.Now()})
	return nil
}

// Usage example: `keyCount, err := storeInstance.Count("config")`
func (storeInstance *Store) Count(group string) (int, error) {
	var count int
	err := storeInstance.database.QueryRow(
		"SELECT COUNT(*) FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	).Scan(&count)
	if err != nil {
		return 0, core.E("store.Count", "query", err)
	}
	return count, nil
}

// Usage example: `_ = storeInstance.DeleteGroup("cache")`
func (storeInstance *Store) DeleteGroup(group string) error {
	_, err := storeInstance.database.Exec("DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ?", group)
	if err != nil {
		return core.E("store.DeleteGroup", "exec", err)
	}
	storeInstance.notify(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	return nil
}

// Usage example: `for entry, err := range storeInstance.All("config") { if err != nil { break }; _ = entry }`
type KeyValue struct {
	Key, Value string
}

// Usage example: `configEntries, err := storeInstance.GetAll("config")`
func (storeInstance *Store) GetAll(group string) (map[string]string, error) {
	entriesByKey := make(map[string]string)
	for entry, err := range storeInstance.All(group) {
		if err != nil {
			return nil, core.E("store.GetAll", "iterate", err)
		}
		entriesByKey[entry.Key] = entry.Value
	}
	return entriesByKey, nil
}

// Usage example: `for entry, err := range storeInstance.All("config") { if err != nil { break }; _ = entry }`
func (storeInstance *Store) All(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		rows, err := storeInstance.database.Query(
			"SELECT "+entryKeyColumn+", "+entryValueColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?)",
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

// Usage example: `parts, _ := storeInstance.GetSplit("config", "hosts", ",")`
func (storeInstance *Store) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	value, err := storeInstance.Get(group, key)
	if err != nil {
		return nil, err
	}
	return splitSeq(value, separator), nil
}

// Usage example: `fields, _ := storeInstance.GetFields("config", "flags")`
func (storeInstance *Store) GetFields(group, key string) (iter.Seq[string], error) {
	value, err := storeInstance.Get(group, key)
	if err != nil {
		return nil, err
	}
	return fieldsSeq(value), nil
}

// Usage example: `renderedTemplate, err := storeInstance.Render("Hello {{ .name }}", "user")`
func (storeInstance *Store) Render(templateSource, group string) (string, error) {
	templateData := make(map[string]string)
	for entry, err := range storeInstance.All(group) {
		if err != nil {
			return "", core.E("store.Render", "iterate", err)
		}
		templateData[entry.Key] = entry.Value
	}

	renderTemplate, err := template.New("render").Parse(templateSource)
	if err != nil {
		return "", core.E("store.Render", "parse", err)
	}
	builder := core.NewBuilder()
	if err := renderTemplate.Execute(builder, templateData); err != nil {
		return "", core.E("store.Render", "exec", err)
	}
	return builder.String(), nil
}

// Usage example: `tenantKeyCount, err := storeInstance.CountAll("tenant-a:")`
func (storeInstance *Store) CountAll(groupPrefix string) (int, error) {
	var count int
	var err error
	if groupPrefix == "" {
		err = storeInstance.database.QueryRow(
			"SELECT COUNT(*) FROM "+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?)",
			time.Now().UnixMilli(),
		).Scan(&count)
	} else {
		err = storeInstance.database.QueryRow(
			"SELECT COUNT(*) FROM "+entriesTableName+" WHERE "+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
			escapeLike(groupPrefix)+"%", time.Now().UnixMilli(),
		).Scan(&count)
	}
	if err != nil {
		return 0, core.E("store.CountAll", "query", err)
	}
	return count, nil
}

// Usage example: `tenantGroupNames, err := storeInstance.Groups("tenant-a:")`
func (storeInstance *Store) Groups(groupPrefix string) ([]string, error) {
	var groupNames []string
	for groupName, err := range storeInstance.GroupsSeq(groupPrefix) {
		if err != nil {
			return nil, err
		}
		groupNames = append(groupNames, groupName)
	}
	return groupNames, nil
}

// Usage example: `for tenantGroupName, err := range storeInstance.GroupsSeq("tenant-a:") { if err != nil { break }; _ = tenantGroupName }`
func (storeInstance *Store) GroupsSeq(groupPrefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		var rows *sql.Rows
		var err error
		now := time.Now().UnixMilli()
		if groupPrefix == "" {
			rows, err = storeInstance.database.Query(
				"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?)",
				now,
			)
		} else {
			rows, err = storeInstance.database.Query(
				"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
				escapeLike(groupPrefix)+"%", now,
			)
		}
		if err != nil {
			yield("", core.E("store.GroupsSeq", "query", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var groupName string
			if err := rows.Scan(&groupName); err != nil {
				if !yield("", core.E("store.GroupsSeq", "scan", err)) {
					return
				}
				continue
			}
			if !yield(groupName, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield("", core.E("store.GroupsSeq", "rows", err))
		}
	}
}

// escapeLike escapes SQLite LIKE wildcards and the escape character itself.
func escapeLike(text string) string {
	text = core.Replace(text, "^", "^^")
	text = core.Replace(text, "%", "^%")
	text = core.Replace(text, "_", "^_")
	return text
}

// Usage example: `removed, err := storeInstance.PurgeExpired()`
func (storeInstance *Store) PurgeExpired() (int64, error) {
	deleteResult, err := storeInstance.database.Exec("DELETE FROM "+entriesTableName+" WHERE expires_at IS NOT NULL AND expires_at <= ?",
		time.Now().UnixMilli())
	if err != nil {
		return 0, core.E("store.PurgeExpired", "exec", err)
	}
	return deleteResult.RowsAffected()
}

// startPurge launches a background goroutine that purges expired entries at the
// store's configured purge interval. It stops when the context is cancelled.
func (storeInstance *Store) startPurge(purgeContext context.Context) {
	storeInstance.purgeWaitGroup.Go(func() {
		ticker := time.NewTicker(storeInstance.purgeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-purgeContext.Done():
				return
			case <-ticker.C:
				if _, err := storeInstance.PurgeExpired(); err != nil {
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
func splitSeq(value, separator string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, part := range core.Split(value, separator) {
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

type schemaDatabase interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
}

const createEntriesTableSQL = `CREATE TABLE IF NOT EXISTS entries (
	group_name  TEXT NOT NULL,
	entry_key   TEXT NOT NULL,
	entry_value TEXT NOT NULL,
	expires_at  INTEGER,
	PRIMARY KEY (group_name, entry_key)
)`

// ensureSchema creates the current entries table and migrates the legacy kv
// table when present.
func ensureSchema(database *sql.DB) error {
	entriesTableExists, err := tableExists(database, entriesTableName)
	if err != nil {
		return core.E("store.New", "schema", err)
	}

	legacyEntriesTableExists, err := tableExists(database, legacyEntriesTableName)
	if err != nil {
		return core.E("store.New", "schema", err)
	}

	if entriesTableExists {
		if err := ensureExpiryColumn(database); err != nil {
			return core.E("store.New", "migration", err)
		}
		if legacyEntriesTableExists {
			if err := migrateLegacyEntriesTable(database); err != nil {
				return core.E("store.New", "migration", err)
			}
		}
		return nil
	}

	if legacyEntriesTableExists {
		if err := migrateLegacyEntriesTable(database); err != nil {
			return core.E("store.New", "migration", err)
		}
		return nil
	}

	if _, err := database.Exec(createEntriesTableSQL); err != nil {
		return core.E("store.New", "schema", err)
	}
	return nil
}

// ensureExpiryColumn adds the expiry column to the current entries table when
// it was created before TTL support.
func ensureExpiryColumn(database schemaDatabase) error {
	hasExpiryColumn, err := tableHasColumn(database, entriesTableName, "expires_at")
	if err != nil {
		return err
	}
	if hasExpiryColumn {
		return nil
	}
	if _, err := database.Exec("ALTER TABLE " + entriesTableName + " ADD COLUMN expires_at INTEGER"); err != nil {
		if !core.Contains(err.Error(), "duplicate column name") {
			return err
		}
	}
	return nil
}

// migrateLegacyEntriesTable copies rows from the old kv table into the
// descriptive entries schema and then removes the legacy table.
func migrateLegacyEntriesTable(database *sql.DB) error {
	transaction, err := database.Begin()
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()

	entriesTableExists, err := tableExists(transaction, entriesTableName)
	if err != nil {
		return err
	}
	if !entriesTableExists {
		if _, err := transaction.Exec(createEntriesTableSQL); err != nil {
			return err
		}
	}

	legacyHasExpiryColumn, err := tableHasColumn(transaction, legacyEntriesTableName, "expires_at")
	if err != nil {
		return err
	}

	insertSQL := "INSERT OR IGNORE INTO " + entriesTableName + " (" + entryGroupColumn + ", " + entryKeyColumn + ", " + entryValueColumn + ", expires_at) SELECT grp, key, value, NULL FROM " + legacyEntriesTableName
	if legacyHasExpiryColumn {
		insertSQL = "INSERT OR IGNORE INTO " + entriesTableName + " (" + entryGroupColumn + ", " + entryKeyColumn + ", " + entryValueColumn + ", expires_at) SELECT grp, key, value, expires_at FROM " + legacyEntriesTableName
	}
	if _, err := transaction.Exec(insertSQL); err != nil {
		return err
	}
	if _, err := transaction.Exec("DROP TABLE " + legacyEntriesTableName); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// tableExists reports whether the named table is present in the SQLite schema.
func tableExists(database schemaDatabase, tableName string) (bool, error) {
	var existingTableName string
	err := database.QueryRow(
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
		tableName,
	).Scan(&existingTableName)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// tableHasColumn reports whether the named column exists on the table.
func tableHasColumn(database schemaDatabase, tableName, columnName string) (bool, error) {
	rows, err := database.Query("PRAGMA table_info(" + tableName + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			columnID     int
			name         string
			columnType   string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&columnID, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == columnName {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}
