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

// NotFoundError is returned when Get cannot find a live key.
// Usage example: `if core.Is(err, store.NotFoundError) { return }`
var NotFoundError = core.E("store", "not found", nil)

// QuotaExceededError is returned when a scoped write would exceed quota.
// Usage example: `if core.Is(err, store.QuotaExceededError) { return }`
var QuotaExceededError = core.E("store", "quota exceeded", nil)

const (
	entriesTableName        = "entries"
	legacyKeyValueTableName = "kv"
	entryGroupColumn        = "group_name"
	entryKeyColumn          = "entry_key"
	entryValueColumn        = "entry_value"
)

// StoreOption customises Store construction.
// Usage example: `storeInstance, err := store.New("/tmp/go-store.db", store.WithJournal("http://127.0.0.1:8086", "core", "events"))`
type StoreOption func(*Store)

type journalConfig struct {
	url    string
	org    string
	bucket string
}

// Store provides SQLite-backed grouped entries with TTL expiry, namespace
// isolation, reactive change notifications, and optional journal support.
// Usage example: `storeInstance, err := store.New(":memory:"); if err != nil { return }; if err := storeInstance.Set("config", "colour", "blue"); err != nil { return }`
type Store struct {
	database       *sql.DB
	cancelPurge    context.CancelFunc
	purgeWaitGroup sync.WaitGroup
	purgeInterval  time.Duration // interval between background purge cycles
	journal        journalConfig
	closeLock      sync.Mutex
	closed         bool

	// Event dispatch state.
	watchers                   map[string][]chan Event
	callbacks                  []changeCallbackRegistration
	watchersLock               sync.RWMutex // protects watcher registration and dispatch
	callbacksLock              sync.RWMutex // protects callback registration and dispatch
	nextCallbackRegistrationID uint64       // monotonic ID for callback registrations
}

// WithJournal records journal connection metadata for workspace commits,
// journal queries, and archive generation.
// Usage example: `storeInstance, err := store.New("/tmp/go-store.db", store.WithJournal("http://127.0.0.1:8086", "core", "events"))`
func WithJournal(url, org, bucket string) StoreOption {
	return func(storeInstance *Store) {
		storeInstance.journal = journalConfig{url: url, org: org, bucket: bucket}
	}
}

// Usage example: `storeInstance, err := store.New(":memory:")`
// Usage example: `storeInstance, err := store.New("/tmp/go-store.db", store.WithJournal("http://127.0.0.1:8086", "core", "events"))`
func New(databasePath string, options ...StoreOption) (*Store, error) {
	sqliteDatabase, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, core.E("store.New", "open database", err)
	}
	// Serialise all access through a single connection. SQLite only supports
	// one writer at a time; using a pool causes SQLITE_BUSY under contention
	// because pragmas (journal_mode, busy_timeout) are per-connection and the
	// pool hands out different connections for each call.
	sqliteDatabase.SetMaxOpenConns(1)
	if _, err := sqliteDatabase.Exec("PRAGMA journal_mode=WAL"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.New", "set WAL journal mode", err)
	}
	if _, err := sqliteDatabase.Exec("PRAGMA busy_timeout=5000"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.New", "set busy timeout", err)
	}
	if err := ensureSchema(sqliteDatabase); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.New", "ensure schema", err)
	}

	purgeContext, cancel := context.WithCancel(context.Background())
	storeInstance := &Store{
		database:      sqliteDatabase,
		cancelPurge:   cancel,
		purgeInterval: 60 * time.Second,
		watchers:      make(map[string][]chan Event),
	}
	for _, option := range options {
		if option != nil {
			option(storeInstance)
		}
	}
	storeInstance.startBackgroundPurge(purgeContext)
	return storeInstance, nil
}

// Usage example: `storeInstance, err := store.New(":memory:"); if err != nil { return }; defer storeInstance.Close()`
func (storeInstance *Store) Close() error {
	storeInstance.closeLock.Lock()
	if storeInstance.closed {
		storeInstance.closeLock.Unlock()
		return nil
	}
	storeInstance.closed = true
	storeInstance.closeLock.Unlock()

	storeInstance.cancelPurge()
	storeInstance.purgeWaitGroup.Wait()
	if err := storeInstance.database.Close(); err != nil {
		return core.E("store.Close", "database close", err)
	}
	return nil
}

// Usage example: `colourValue, err := storeInstance.Get("config", "colour")`
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
		return "", core.E("store.Get", "query row", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		if err := storeInstance.Delete(group, key); err != nil {
			return "", core.E("store.Get", "delete expired row", err)
		}
		return "", core.E("store.Get", core.Concat(group, "/", key), NotFoundError)
	}
	return value, nil
}

// Usage example: `if err := storeInstance.Set("config", "colour", "blue"); err != nil { return }`
func (storeInstance *Store) Set(group, key, value string) error {
	_, err := storeInstance.database.Exec(
		"INSERT INTO "+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, NULL) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = NULL",
		group, key, value,
	)
	if err != nil {
		return core.E("store.Set", "execute upsert", err)
	}
	storeInstance.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Usage example: `if err := storeInstance.SetWithTTL("session", "token", "abc123", time.Minute); err != nil { return }`
func (storeInstance *Store) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	expiresAt := time.Now().Add(timeToLive).UnixMilli()
	_, err := storeInstance.database.Exec(
		"INSERT INTO "+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, ?) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = excluded.expires_at",
		group, key, value, expiresAt,
	)
	if err != nil {
		return core.E("store.SetWithTTL", "execute upsert with expiry", err)
	}
	storeInstance.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Usage example: `if err := storeInstance.Delete("config", "colour"); err != nil { return }`
func (storeInstance *Store) Delete(group, key string) error {
	deleteResult, err := storeInstance.database.Exec("DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?", group, key)
	if err != nil {
		return core.E("store.Delete", "delete row", err)
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.E("store.Delete", "count deleted rows", rowsAffectedError)
	}
	if deletedRows > 0 {
		storeInstance.notify(Event{Type: EventDelete, Group: group, Key: key, Timestamp: time.Now()})
	}
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
		return 0, core.E("store.Count", "count rows", err)
	}
	return count, nil
}

// Usage example: `if err := storeInstance.DeleteGroup("cache"); err != nil { return }`
func (storeInstance *Store) DeleteGroup(group string) error {
	deleteResult, err := storeInstance.database.Exec("DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ?", group)
	if err != nil {
		return core.E("store.DeleteGroup", "delete group", err)
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.E("store.DeleteGroup", "count deleted rows", rowsAffectedError)
	}
	if deletedRows > 0 {
		storeInstance.notify(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	}
	return nil
}

// KeyValue is one item returned by All.
// Usage example: `for entry, err := range storeInstance.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
type KeyValue struct {
	// Usage example: `if entry.Key == "colour" { return }`
	Key string
	// Usage example: `if entry.Value == "blue" { return }`
	Value string
}

// Usage example: `colourEntries, err := storeInstance.GetAll("config")`
func (storeInstance *Store) GetAll(group string) (map[string]string, error) {
	entriesByKey := make(map[string]string)
	for entry, err := range storeInstance.All(group) {
		if err != nil {
			return nil, core.E("store.GetAll", "iterate rows", err)
		}
		entriesByKey[entry.Key] = entry.Value
	}
	return entriesByKey, nil
}

// Usage example: `for entry, err := range storeInstance.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeInstance *Store) All(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		rows, err := storeInstance.database.Query(
			"SELECT "+entryKeyColumn+", "+entryValueColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn,
			group, time.Now().UnixMilli(),
		)
		if err != nil {
			yield(KeyValue{}, core.E("store.All", "query rows", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var entry KeyValue
			if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
				if !yield(KeyValue{}, core.E("store.All", "scan row", err)) {
					return
				}
				continue
			}
			if !yield(entry, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(KeyValue{}, core.E("store.All", "rows iteration", err))
		}
	}
}

// Usage example: `parts, err := storeInstance.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (storeInstance *Store) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	value, err := storeInstance.Get(group, key)
	if err != nil {
		return nil, err
	}
	return splitValueSeq(value, separator), nil
}

// Usage example: `fields, err := storeInstance.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (storeInstance *Store) GetFields(group, key string) (iter.Seq[string], error) {
	value, err := storeInstance.Get(group, key)
	if err != nil {
		return nil, err
	}
	return fieldsValueSeq(value), nil
}

// Usage example: `renderedTemplate, err := storeInstance.Render("Hello {{ .name }}", "user")`
func (storeInstance *Store) Render(templateSource, group string) (string, error) {
	templateData := make(map[string]string)
	for entry, err := range storeInstance.All(group) {
		if err != nil {
			return "", core.E("store.Render", "iterate rows", err)
		}
		templateData[entry.Key] = entry.Value
	}

	renderTemplate, err := template.New("render").Parse(templateSource)
	if err != nil {
		return "", core.E("store.Render", "parse template", err)
	}
	builder := core.NewBuilder()
	if err := renderTemplate.Execute(builder, templateData); err != nil {
		return "", core.E("store.Render", "execute template", err)
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
		return 0, core.E("store.CountAll", "count rows", err)
	}
	return count, nil
}

// Usage example: `tenantGroupNames, err := storeInstance.Groups("tenant-a:")`
// Usage example: `allGroupNames, err := storeInstance.Groups()`
func (storeInstance *Store) Groups(groupPrefix ...string) ([]string, error) {
	var groupNames []string
	for groupName, err := range storeInstance.GroupsSeq(groupPrefix...) {
		if err != nil {
			return nil, err
		}
		groupNames = append(groupNames, groupName)
	}
	return groupNames, nil
}

// Usage example: `for tenantGroupName, err := range storeInstance.GroupsSeq("tenant-a:") { if err != nil { break }; fmt.Println(tenantGroupName) }`
// Usage example: `for groupName, err := range storeInstance.GroupsSeq() { if err != nil { break }; fmt.Println(groupName) }`
func (storeInstance *Store) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	actualGroupPrefix := firstString(groupPrefix)
	return func(yield func(string, error) bool) {
		var rows *sql.Rows
		var err error
		now := time.Now().UnixMilli()
		if actualGroupPrefix == "" {
			rows, err = storeInstance.database.Query(
				"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				now,
			)
		} else {
			rows, err = storeInstance.database.Query(
				"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				escapeLike(actualGroupPrefix)+"%", now,
			)
		}
		if err != nil {
			yield("", core.E("store.GroupsSeq", "query group names", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var groupName string
			if err := rows.Scan(&groupName); err != nil {
				if !yield("", core.E("store.GroupsSeq", "scan group name", err)) {
					return
				}
				continue
			}
			if !yield(groupName, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield("", core.E("store.GroupsSeq", "rows iteration", err))
		}
	}
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// escapeLike("tenant_%") returns "tenant^_^%" so LIKE queries treat wildcards
// literally.
func escapeLike(text string) string {
	text = core.Replace(text, "^", "^^")
	text = core.Replace(text, "%", "^%")
	text = core.Replace(text, "_", "^_")
	return text
}

// Usage example: `removed, err := storeInstance.PurgeExpired()`
func (storeInstance *Store) PurgeExpired() (int64, error) {
	removedRows, err := storeInstance.purgeExpiredMatchingGroupPrefix("")
	if err != nil {
		return 0, core.E("store.PurgeExpired", "delete expired rows", err)
	}
	return removedRows, nil
}

// New(":memory:") starts a background goroutine that calls PurgeExpired every
// 60 seconds until Close stops the store.
func (storeInstance *Store) startBackgroundPurge(purgeContext context.Context) {
	storeInstance.purgeWaitGroup.Go(func() {
		ticker := time.NewTicker(storeInstance.purgeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-purgeContext.Done():
				return
			case <-ticker.C:
				if _, err := storeInstance.PurgeExpired(); err != nil {
					// For example, a logger could record the failure here. The loop
					// keeps running so the next tick can retry.
				}
			}
		}
	})
}

// splitValueSeq("red,green,blue", ",") yields "red", "green", "blue" without
// importing strings directly.
func splitValueSeq(value, separator string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, part := range core.Split(value, separator) {
			if !yield(part) {
				return
			}
		}
	}
}

// fieldsValueSeq("alpha  beta\tgamma") yields "alpha", "beta", "gamma" without
// importing strings directly.
func fieldsValueSeq(value string) iter.Seq[string] {
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

// purgeExpiredMatchingGroupPrefix deletes expired rows globally when
// groupPrefix is empty, otherwise only rows whose group starts with the given
// prefix.
func (storeInstance *Store) purgeExpiredMatchingGroupPrefix(groupPrefix string) (int64, error) {
	var (
		deleteResult sql.Result
		err          error
	)
	now := time.Now().UnixMilli()
	if groupPrefix == "" {
		deleteResult, err = storeInstance.database.Exec(
			"DELETE FROM "+entriesTableName+" WHERE expires_at IS NOT NULL AND expires_at <= ?",
			now,
		)
	} else {
		deleteResult, err = storeInstance.database.Exec(
			"DELETE FROM "+entriesTableName+" WHERE expires_at IS NOT NULL AND expires_at <= ? AND "+entryGroupColumn+" LIKE ? ESCAPE '^'",
			now, escapeLike(groupPrefix)+"%",
		)
	}
	if err != nil {
		return 0, err
	}
	removedRows, rowsAffectedErr := deleteResult.RowsAffected()
	if rowsAffectedErr != nil {
		return 0, rowsAffectedErr
	}
	return removedRows, nil
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

// ensureSchema creates the current entries table and migrates the legacy
// key-value table when present.
func ensureSchema(database *sql.DB) error {
	entriesTableExists, err := tableExists(database, entriesTableName)
	if err != nil {
		return core.E("store.ensureSchema", "schema", err)
	}

	legacyEntriesTableExists, err := tableExists(database, legacyKeyValueTableName)
	if err != nil {
		return core.E("store.ensureSchema", "schema", err)
	}

	if entriesTableExists {
		if err := ensureExpiryColumn(database); err != nil {
			return core.E("store.ensureSchema", "migration", err)
		}
		if legacyEntriesTableExists {
			if err := migrateLegacyEntriesTable(database); err != nil {
				return core.E("store.ensureSchema", "migration", err)
			}
		}
		return nil
	}

	if legacyEntriesTableExists {
		if err := migrateLegacyEntriesTable(database); err != nil {
			return core.E("store.ensureSchema", "migration", err)
		}
		return nil
	}

	if _, err := database.Exec(createEntriesTableSQL); err != nil {
		return core.E("store.ensureSchema", "schema", err)
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

// migrateLegacyEntriesTable copies rows from the old key-value table into the
// descriptive entries schema and then removes the legacy table.
func migrateLegacyEntriesTable(database *sql.DB) error {
	transaction, err := database.Begin()
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := transaction.Rollback(); rollbackErr != nil {
				// Ignore rollback failures; the original error is already being returned.
			}
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

	legacyHasExpiryColumn, err := tableHasColumn(transaction, legacyKeyValueTableName, "expires_at")
	if err != nil {
		return err
	}

	insertSQL := "INSERT OR IGNORE INTO " + entriesTableName + " (" + entryGroupColumn + ", " + entryKeyColumn + ", " + entryValueColumn + ", expires_at) SELECT grp, key, value, NULL FROM " + legacyKeyValueTableName
	if legacyHasExpiryColumn {
		insertSQL = "INSERT OR IGNORE INTO " + entriesTableName + " (" + entryGroupColumn + ", " + entryKeyColumn + ", " + entryValueColumn + ", expires_at) SELECT grp, key, value, expires_at FROM " + legacyKeyValueTableName
	}
	if _, err := transaction.Exec(insertSQL); err != nil {
		return err
	}
	if _, err := transaction.Exec("DROP TABLE " + legacyKeyValueTableName); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

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
