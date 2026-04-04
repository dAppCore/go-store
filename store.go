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

// Usage example: `if core.Is(err, store.NotFoundError) { fmt.Println("config/colour is missing") }`
var NotFoundError = core.E("store", "not found", nil)

// Usage example: `if core.Is(err, store.QuotaExceededError) { fmt.Println("tenant-a is at quota") }`
var QuotaExceededError = core.E("store", "quota exceeded", nil)

const (
	entriesTableName        = "entries"
	legacyKeyValueTableName = "kv"
	entryGroupColumn        = "group_name"
	entryKeyColumn          = "entry_key"
	entryValueColumn        = "entry_value"
)

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: "/tmp/go-store.db", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}, PurgeInterval: 30 * time.Second})`
// Prefer `store.NewConfigured(store.StoreConfig{...})` when the configuration
// is already known as a struct literal. Use `StoreOption` only when values
// need to be assembled incrementally, such as when a caller receives them from
// different sources.
type StoreOption func(*StoreConfig)

// Usage example: `config := store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: 30 * time.Second}`
type StoreConfig struct {
	// Usage example: `config := store.StoreConfig{DatabasePath: "/tmp/go-store.db"}`
	DatabasePath string
	// Usage example: `config := store.StoreConfig{Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}}`
	Journal JournalConfiguration
	// Usage example: `config := store.StoreConfig{PurgeInterval: 30 * time.Second}`
	PurgeInterval time.Duration
}

// Usage example: `if err := (store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: 30 * time.Second}).Validate(); err != nil { return }`
func (storeConfig StoreConfig) Validate() error {
	if storeConfig.DatabasePath == "" {
		return core.E(
			"store.StoreConfig.Validate",
			"database path is empty",
			nil,
		)
	}
	if storeConfig.Journal != (JournalConfiguration{}) && !storeConfig.Journal.isConfigured() {
		return core.E(
			"store.StoreConfig.Validate",
			"journal configuration must include endpoint URL, organisation, and bucket name",
			nil,
		)
	}
	if storeConfig.PurgeInterval < 0 {
		return core.E("store.StoreConfig.Validate", "purge interval must be zero or positive", nil)
	}
	return nil
}

// Usage example: `config := storeInstance.JournalConfiguration(); fmt.Println(config.EndpointURL, config.Organisation, config.BucketName)`
// The values are copied into the store and used as journal metadata.
type JournalConfiguration struct {
	// Usage example: `config := store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086"}`
	EndpointURL string
	// Usage example: `config := store.JournalConfiguration{Organisation: "core"}`
	Organisation string
	// Usage example: `config := store.JournalConfiguration{BucketName: "events"}`
	BucketName string
}

func (journalConfig JournalConfiguration) isConfigured() bool {
	return journalConfig.EndpointURL != "" &&
		journalConfig.Organisation != "" &&
		journalConfig.BucketName != ""
}

type journalConfiguration struct {
	endpointURL  string
	organisation string
	bucketName   string
}

func (journalConfig journalConfiguration) isConfigured() bool {
	return journalConfig.endpointURL != "" &&
		journalConfig.organisation != "" &&
		journalConfig.bucketName != ""
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}, PurgeInterval: 30 * time.Second})`
// Store keeps grouped key-value entries in SQLite and can also write completed
// work summaries to the journal table.
type Store struct {
	sqliteDatabase       *sql.DB
	databasePath         string
	purgeContext         context.Context
	cancelPurge          context.CancelFunc
	purgeWaitGroup       sync.WaitGroup
	purgeInterval        time.Duration // interval between background purge cycles
	journalConfiguration journalConfiguration
	closeLock            sync.Mutex
	closed               bool

	// Event dispatch state.
	watchers                   map[string][]chan Event
	callbacks                  []changeCallbackRegistration
	watchersLock               sync.RWMutex // protects watcher registration and dispatch
	callbacksLock              sync.RWMutex // protects callback registration and dispatch
	nextCallbackRegistrationID uint64       // monotonic ID for callback registrations

	orphanWorkspacesLock sync.Mutex
	orphanWorkspaces     []*Workspace
}

func (storeInstance *Store) ensureReady(operation string) error {
	if storeInstance == nil {
		return core.E(operation, "store is nil", nil)
	}
	if storeInstance.sqliteDatabase == nil {
		return core.E(operation, "store is not initialised", nil)
	}

	storeInstance.closeLock.Lock()
	closed := storeInstance.closed
	storeInstance.closeLock.Unlock()
	if closed {
		return core.E(operation, "store is closed", nil)
	}

	return nil
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: "/tmp/go-store.db", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}})`
func WithJournal(endpointURL, organisation, bucketName string) StoreOption {
	return func(storeConfig *StoreConfig) {
		if storeConfig == nil {
			return
		}
		storeConfig.Journal = JournalConfiguration{
			EndpointURL:  endpointURL,
			Organisation: organisation,
			BucketName:   bucketName,
		}
	}
}

// Usage example: `config := storeInstance.JournalConfiguration(); fmt.Println(config.EndpointURL, config.Organisation, config.BucketName)`
func (storeInstance *Store) JournalConfiguration() JournalConfiguration {
	if storeInstance == nil {
		return JournalConfiguration{}
	}
	return JournalConfiguration{
		EndpointURL:  storeInstance.journalConfiguration.endpointURL,
		Organisation: storeInstance.journalConfiguration.organisation,
		BucketName:   storeInstance.journalConfiguration.bucketName,
	}
}

// Usage example: `if storeInstance.JournalConfigured() { fmt.Println("journal is fully configured") }`
func (storeInstance *Store) JournalConfigured() bool {
	if storeInstance == nil {
		return false
	}
	return storeInstance.journalConfiguration.isConfigured()
}

// Usage example: `config := storeInstance.Config(); fmt.Println(config.DatabasePath, config.PurgeInterval)`
func (storeInstance *Store) Config() StoreConfig {
	if storeInstance == nil {
		return StoreConfig{}
	}
	return StoreConfig{
		DatabasePath:  storeInstance.databasePath,
		Journal:       storeInstance.JournalConfiguration(),
		PurgeInterval: storeInstance.purgeInterval,
	}
}

// Usage example: `databasePath := storeInstance.DatabasePath(); fmt.Println(databasePath)`
func (storeInstance *Store) DatabasePath() string {
	if storeInstance == nil {
		return ""
	}
	return storeInstance.databasePath
}

// Usage example: `if storeInstance.IsClosed() { return }`
func (storeInstance *Store) IsClosed() bool {
	if storeInstance == nil {
		return true
	}

	storeInstance.closeLock.Lock()
	closed := storeInstance.closed
	storeInstance.closeLock.Unlock()
	return closed
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: 20 * time.Millisecond})`
func WithPurgeInterval(interval time.Duration) StoreOption {
	return func(storeConfig *StoreConfig) {
		if storeConfig == nil {
			return
		}
		if interval > 0 {
			storeConfig.PurgeInterval = interval
		}
	}
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}, PurgeInterval: 20 * time.Millisecond})`
func NewConfigured(storeConfig StoreConfig) (*Store, error) {
	return openConfiguredStore("store.NewConfigured", storeConfig)
}

func openConfiguredStore(operation string, storeConfig StoreConfig) (*Store, error) {
	if err := storeConfig.Validate(); err != nil {
		return nil, core.E(operation, "validate config", err)
	}

	storeInstance, err := openSQLiteStore(operation, storeConfig.DatabasePath)
	if err != nil {
		return nil, err
	}

	if storeConfig.Journal != (JournalConfiguration{}) {
		storeInstance.journalConfiguration = journalConfiguration{
			endpointURL:  storeConfig.Journal.EndpointURL,
			organisation: storeConfig.Journal.Organisation,
			bucketName:   storeConfig.Journal.BucketName,
		}
	}
	if storeConfig.PurgeInterval > 0 {
		storeInstance.purgeInterval = storeConfig.PurgeInterval
	}

	// New() performs a non-destructive orphan scan so callers can discover
	// leftover workspaces via RecoverOrphans().
	storeInstance.orphanWorkspaces = discoverOrphanWorkspaces(defaultWorkspaceStateDirectory, storeInstance)
	storeInstance.startBackgroundPurge()
	return storeInstance, nil
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: "/tmp/go-store.db", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}})`
func New(databasePath string, options ...StoreOption) (*Store, error) {
	storeConfig := StoreConfig{DatabasePath: databasePath}
	for _, option := range options {
		if option != nil {
			option(&storeConfig)
		}
	}
	return openConfiguredStore("store.New", storeConfig)
}

func openSQLiteStore(operation, databasePath string) (*Store, error) {
	sqliteDatabase, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, core.E(operation, "open database", err)
	}
	// Serialise all access through a single connection. SQLite only supports
	// one writer at a time; using a pool causes SQLITE_BUSY under contention
	// because pragmas (journal_mode, busy_timeout) are per-connection and the
	// pool hands out different connections for each call.
	sqliteDatabase.SetMaxOpenConns(1)
	if _, err := sqliteDatabase.Exec("PRAGMA journal_mode=WAL"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E(operation, "set WAL journal mode", err)
	}
	if _, err := sqliteDatabase.Exec("PRAGMA busy_timeout=5000"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E(operation, "set busy timeout", err)
	}
	if err := ensureSchema(sqliteDatabase); err != nil {
		sqliteDatabase.Close()
		return nil, core.E(operation, "ensure schema", err)
	}

	purgeContext, cancel := context.WithCancel(context.Background())
	return &Store{
		sqliteDatabase: sqliteDatabase,
		databasePath:   databasePath,
		purgeContext:   purgeContext,
		cancelPurge:    cancel,
		purgeInterval:  60 * time.Second,
		watchers:       make(map[string][]chan Event),
	}, nil
}

// Usage example: `storeInstance, err := store.New(":memory:"); if err != nil { return }; defer storeInstance.Close()`
func (storeInstance *Store) Close() error {
	if storeInstance == nil {
		return nil
	}

	storeInstance.closeLock.Lock()
	if storeInstance.closed {
		storeInstance.closeLock.Unlock()
		return nil
	}
	storeInstance.closed = true
	storeInstance.closeLock.Unlock()

	if storeInstance.cancelPurge != nil {
		storeInstance.cancelPurge()
	}
	storeInstance.purgeWaitGroup.Wait()

	storeInstance.watchersLock.Lock()
	for groupName, registeredEvents := range storeInstance.watchers {
		for _, registeredEventChannel := range registeredEvents {
			close(registeredEventChannel)
		}
		delete(storeInstance.watchers, groupName)
	}
	storeInstance.watchersLock.Unlock()

	storeInstance.callbacksLock.Lock()
	storeInstance.callbacks = nil
	storeInstance.callbacksLock.Unlock()

	storeInstance.orphanWorkspacesLock.Lock()
	var orphanCleanupErr error
	for _, orphanWorkspace := range storeInstance.orphanWorkspaces {
		if err := orphanWorkspace.closeWithoutRemovingFiles(); err != nil && orphanCleanupErr == nil {
			orphanCleanupErr = err
		}
	}
	storeInstance.orphanWorkspaces = nil
	storeInstance.orphanWorkspacesLock.Unlock()

	if storeInstance.sqliteDatabase == nil {
		return orphanCleanupErr
	}
	if err := storeInstance.sqliteDatabase.Close(); err != nil {
		return core.E("store.Close", "database close", err)
	}
	if orphanCleanupErr != nil {
		return core.E("store.Close", "close orphan workspaces", orphanCleanupErr)
	}
	return orphanCleanupErr
}

// Usage example: `colourValue, err := storeInstance.Get("config", "colour")`
func (storeInstance *Store) Get(group, key string) (string, error) {
	if err := storeInstance.ensureReady("store.Get"); err != nil {
		return "", err
	}

	var value string
	var expiresAt sql.NullInt64
	err := storeInstance.sqliteDatabase.QueryRow(
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
	if err := storeInstance.ensureReady("store.Set"); err != nil {
		return err
	}

	_, err := storeInstance.sqliteDatabase.Exec(
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
	if err := storeInstance.ensureReady("store.SetWithTTL"); err != nil {
		return err
	}

	expiresAt := time.Now().Add(timeToLive).UnixMilli()
	_, err := storeInstance.sqliteDatabase.Exec(
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
	if err := storeInstance.ensureReady("store.Delete"); err != nil {
		return err
	}

	deleteResult, err := storeInstance.sqliteDatabase.Exec("DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?", group, key)
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
	if err := storeInstance.ensureReady("store.Count"); err != nil {
		return 0, err
	}

	var count int
	err := storeInstance.sqliteDatabase.QueryRow(
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
	if err := storeInstance.ensureReady("store.DeleteGroup"); err != nil {
		return err
	}

	deleteResult, err := storeInstance.sqliteDatabase.Exec("DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ?", group)
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

// Usage example: `if err := storeInstance.DeletePrefix("tenant-a:"); err != nil { return }`
func (storeInstance *Store) DeletePrefix(groupPrefix string) error {
	if err := storeInstance.ensureReady("store.DeletePrefix"); err != nil {
		return err
	}

	var rows *sql.Rows
	var err error
	if groupPrefix == "" {
		rows, err = storeInstance.sqliteDatabase.Query(
			"SELECT DISTINCT " + entryGroupColumn + " FROM " + entriesTableName + " ORDER BY " + entryGroupColumn,
		)
	} else {
		rows, err = storeInstance.sqliteDatabase.Query(
			"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" LIKE ? ESCAPE '^' ORDER BY "+entryGroupColumn,
			escapeLike(groupPrefix)+"%",
		)
	}
	if err != nil {
		return core.E("store.DeletePrefix", "list groups", err)
	}
	defer rows.Close()

	var groupNames []string
	for rows.Next() {
		var groupName string
		if err := rows.Scan(&groupName); err != nil {
			return core.E("store.DeletePrefix", "scan group name", err)
		}
		groupNames = append(groupNames, groupName)
	}
	if err := rows.Err(); err != nil {
		return core.E("store.DeletePrefix", "iterate groups", err)
	}
	for _, groupName := range groupNames {
		if err := storeInstance.DeleteGroup(groupName); err != nil {
			return core.E("store.DeletePrefix", "delete group", err)
		}
	}
	return nil
}

// Usage example: `for entry, err := range storeInstance.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
type KeyValue struct {
	// Usage example: `if entry.Key == "colour" { return }`
	Key string
	// Usage example: `if entry.Value == "blue" { return }`
	Value string
}

// Usage example: `colourEntries, err := storeInstance.GetAll("config")`
func (storeInstance *Store) GetAll(group string) (map[string]string, error) {
	if err := storeInstance.ensureReady("store.GetAll"); err != nil {
		return nil, err
	}

	entriesByKey := make(map[string]string)
	for entry, err := range storeInstance.All(group) {
		if err != nil {
			return nil, core.E("store.GetAll", "iterate rows", err)
		}
		entriesByKey[entry.Key] = entry.Value
	}
	return entriesByKey, nil
}

// Usage example: `page, err := storeInstance.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (storeInstance *Store) GetPage(group string, offset, limit int) ([]KeyValue, error) {
	if err := storeInstance.ensureReady("store.GetPage"); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, core.E("store.GetPage", "offset must be zero or positive", nil)
	}
	if limit < 0 {
		return nil, core.E("store.GetPage", "limit must be zero or positive", nil)
	}

	rows, err := storeInstance.sqliteDatabase.Query(
		"SELECT "+entryKeyColumn+", "+entryValueColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn+" LIMIT ? OFFSET ?",
		group, time.Now().UnixMilli(), limit, offset,
	)
	if err != nil {
		return nil, core.E("store.GetPage", "query rows", err)
	}
	defer rows.Close()

	page := make([]KeyValue, 0, limit)
	for rows.Next() {
		var entry KeyValue
		if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
			return nil, core.E("store.GetPage", "scan row", err)
		}
		page = append(page, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, core.E("store.GetPage", "rows iteration", err)
	}
	return page, nil
}

// Usage example: `for entry, err := range storeInstance.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeInstance *Store) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		if err := storeInstance.ensureReady("store.All"); err != nil {
			yield(KeyValue{}, err)
			return
		}

		rows, err := storeInstance.sqliteDatabase.Query(
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

// Usage example: `for entry, err := range storeInstance.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeInstance *Store) All(group string) iter.Seq2[KeyValue, error] {
	return storeInstance.AllSeq(group)
}

// Usage example: `parts, err := storeInstance.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (storeInstance *Store) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	if err := storeInstance.ensureReady("store.GetSplit"); err != nil {
		return nil, err
	}

	value, err := storeInstance.Get(group, key)
	if err != nil {
		return nil, err
	}
	return splitValueSeq(value, separator), nil
}

// Usage example: `fields, err := storeInstance.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (storeInstance *Store) GetFields(group, key string) (iter.Seq[string], error) {
	if err := storeInstance.ensureReady("store.GetFields"); err != nil {
		return nil, err
	}

	value, err := storeInstance.Get(group, key)
	if err != nil {
		return nil, err
	}
	return fieldsValueSeq(value), nil
}

// Usage example: `renderedTemplate, err := storeInstance.Render("Hello {{ .name }}", "user")`
func (storeInstance *Store) Render(templateSource, group string) (string, error) {
	if err := storeInstance.ensureReady("store.Render"); err != nil {
		return "", err
	}

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
	if err := storeInstance.ensureReady("store.CountAll"); err != nil {
		return 0, err
	}

	var count int
	var err error
	if groupPrefix == "" {
		err = storeInstance.sqliteDatabase.QueryRow(
			"SELECT COUNT(*) FROM "+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?)",
			time.Now().UnixMilli(),
		).Scan(&count)
	} else {
		err = storeInstance.sqliteDatabase.QueryRow(
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
	if err := storeInstance.ensureReady("store.Groups"); err != nil {
		return nil, err
	}

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
	actualGroupPrefix := firstOrEmptyString(groupPrefix)
	return func(yield func(string, error) bool) {
		if err := storeInstance.ensureReady("store.GroupsSeq"); err != nil {
			yield("", err)
			return
		}

		var rows *sql.Rows
		var err error
		now := time.Now().UnixMilli()
		if actualGroupPrefix == "" {
			rows, err = storeInstance.sqliteDatabase.Query(
				"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				now,
			)
		} else {
			rows, err = storeInstance.sqliteDatabase.Query(
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

func firstOrEmptyString(values []string) string {
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
	if err := storeInstance.ensureReady("store.PurgeExpired"); err != nil {
		return 0, err
	}

	removedRows, err := storeInstance.purgeExpiredMatchingGroupPrefix("")
	if err != nil {
		return 0, core.E("store.PurgeExpired", "delete expired rows", err)
	}
	return removedRows, nil
}

// New(":memory:", store.WithPurgeInterval(20*time.Millisecond)) starts a
// background goroutine that calls PurgeExpired on that interval until Close
// stops the store.
func (storeInstance *Store) startBackgroundPurge() {
	if storeInstance == nil {
		return
	}
	if storeInstance.purgeContext == nil {
		return
	}
	if storeInstance.purgeInterval <= 0 {
		storeInstance.purgeInterval = 60 * time.Second
	}
	purgeInterval := storeInstance.purgeInterval

	storeInstance.purgeWaitGroup.Go(func() {
		ticker := time.NewTicker(purgeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-storeInstance.purgeContext.Done():
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
	if err := storeInstance.ensureReady("store.purgeExpiredMatchingGroupPrefix"); err != nil {
		return 0, err
	}

	var (
		deleteResult sql.Result
		err          error
	)
	now := time.Now().UnixMilli()
	if groupPrefix == "" {
		deleteResult, err = storeInstance.sqliteDatabase.Exec(
			"DELETE FROM "+entriesTableName+" WHERE expires_at IS NOT NULL AND expires_at <= ?",
			now,
		)
	} else {
		deleteResult, err = storeInstance.sqliteDatabase.Exec(
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
