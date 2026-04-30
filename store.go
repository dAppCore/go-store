package store

import (
	"context"
	"database/sql"
	"iter"
	"sync" // Note: AX-6 — internal concurrency primitive; structural for store infrastructure (RFC §4 explicitly mandates).
	"text/template"
	"time"
	"unicode"

	core "dappco.re/go"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
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
	defaultPurgeInterval    = 60 * time.Second
	memoryDatabasePath      = ":memory:"
	sqliteWALPragma         = "PRAGMA journal_mode=WAL"

	opStoreConfigValidate          = "store.StoreConfig.Validate"
	opJournalConfigurationValidate = "store.JournalConfiguration.Validate"
	opNew                          = "store.New"
	opClose                        = "store.Close"
	opGet                          = "store.Get"
	opDelete                       = "store.Delete"
	opDeleteGroup                  = "store.DeleteGroup"
	opDeletePrefix                 = "store.DeletePrefix"
	opGetPage                      = "store.GetPage"
	opAll                          = "store.All"
	opRender                       = "store.Render"
	opGroupsSeq                    = "store.GroupsSeq"
	opEnsureSchema                 = "store.ensureSchema"
	opCompact                      = "store.Compact"
	opDuckDBQueryRows              = "store.DuckDB.QueryRows"
	opDuckDBEnsureScoringTables    = "store.DuckDB.EnsureScoringTables"
	opImportAll                    = "store.ImportAll"
	opImportTrainingFile           = "store.importTrainingFile"
	opImportBenchmarkFile          = "store.importBenchmarkFile"
	opImportBenchmarkQuestions     = "store.importBenchmarkQuestions"
	opImportSeeds                  = "store.importSeeds"
	opCommitToJournal              = "store.CommitToJournal"
	opQueryJournal                 = "store.QueryJournal"
	opParseJournalInt64            = "store.parseJournalInt64"
	opParseJournalFloat64          = "store.parseJournalFloat64"
	opMarshalIndent                = "store.MarshalIndent"
	opImport                       = "store.Import"
	opExport                       = "store.Export"
	opPublish                      = "store.Publish"
	opEnsureHFDatasetRepo          = "store.ensureHFDatasetRepo"
	opHFJSONRequest                = "store.hfJSONRequest"
	opUploadFileToHF               = "store.uploadFileToHF"
	opNewScopedConfigured          = "store.NewScopedConfigured"
	opScopedStoreTransaction       = "store.ScopedStore.Transaction"
	opTransaction                  = "store.Transaction"
	opTransactionGet               = "store.Transaction.Get"
	opTransactionDelete            = "store.Transaction.Delete"
	opTransactionDeleteGroup       = "store.Transaction.DeleteGroup"
	opTransactionDeletePrefix      = "store.Transaction.DeletePrefix"
	opTransactionGetPage           = "store.Transaction.GetPage"
	opTransactionAll               = "store.Transaction.All"
	opTransactionGroupsSeq         = "store.Transaction.GroupsSeq"
	opTransactionRender            = "store.Transaction.Render"
	opNewWorkspace                 = "store.NewWorkspace"
	opWorkspacePut                 = "store.Workspace.Put"
	opWorkspaceCommit              = "store.Workspace.Commit"
	opWorkspaceQuery               = "store.Workspace.Query"
	opOpenWorkspaceDatabase        = "store.openWorkspaceDatabase"
	sqlSelect                      = "SELECT "
	sqlWhere                       = " WHERE "
	sqlFrom                        = " FROM "
	sqlDeleteFrom                  = "DELETE FROM "
	sqlSelectCountFrom             = "SELECT COUNT(*) FROM "
	sqlSelectDistinct              = "SELECT DISTINCT "
	sqlInsertIntoPrefix            = "INSERT INTO "
	sqlUpdatePrefix                = "UPDATE "
	rowsIterationMessage           = "rows iteration"
	jsonlExtension                 = ".jsonl"
	duckDBExtension                = ".duckdb"
	openPathFormat                 = "open %s"
	parsePathLineFormat            = "parse %s line %d"
	exampleTenantA                 = "tenant-a"
	exampleTenant42                = "tenant-42"
	scopedStoreNilMessage          = "scoped store is nil"
	quotaCheckContext              = "quota check"
	workspaceEntryInsertValuesSQL  = " (entry_kind, entry_data, created_at) VALUES (?, ?, ?)"
)

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: "/tmp/go-store.db", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}, PurgeInterval: 30 * time.Second})`
// Prefer `store.NewConfigured(store.StoreConfig{...})` when the full
// configuration is already known. Use `StoreOption` when values need to be
// assembled incrementally, such as when a caller receives them from
// different sources.
type StoreOption func(*Store)

// Usage example: `config := store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: 30 * time.Second}`
type StoreConfig struct {
	// Usage example: `config := store.StoreConfig{DatabasePath: "/tmp/go-store.db"}`
	DatabasePath string
	// Usage example: `config := store.StoreConfig{Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}}`
	Journal JournalConfiguration
	// Usage example: `config := store.StoreConfig{PurgeInterval: 30 * time.Second}`
	PurgeInterval time.Duration
	// Usage example: `config := store.StoreConfig{WorkspaceStateDirectory: "/tmp/core-state"}`
	WorkspaceStateDirectory string
	// Usage example: `medium, _ := local.New("/srv/core"); config := store.StoreConfig{DatabasePath: ":memory:", Medium: medium}`
	// Medium overrides the raw filesystem for Compact archives and Import /
	// Export helpers, letting tests and production swap the backing transport
	// (memory, S3, cube) without touching the store API.
	Medium Medium
}

// Usage example: `config := (store.StoreConfig{DatabasePath: ":memory:"}).Normalised(); fmt.Println(config.PurgeInterval, config.WorkspaceStateDirectory)`
func (storeConfig StoreConfig) Normalised() StoreConfig {
	if storeConfig.PurgeInterval == 0 {
		storeConfig.PurgeInterval = defaultPurgeInterval
	}
	if storeConfig.WorkspaceStateDirectory == "" {
		storeConfig.WorkspaceStateDirectory = normaliseWorkspaceStateDirectory(defaultWorkspaceStateDirectory)
	} else {
		storeConfig.WorkspaceStateDirectory = normaliseWorkspaceStateDirectory(storeConfig.WorkspaceStateDirectory)
	}
	return storeConfig
}

// Usage example: `if err := (store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: 30 * time.Second}).Validate(); err != nil { return }`
func (storeConfig StoreConfig) Validate() error {
	if storeConfig.DatabasePath == "" {
		return core.E(
			opStoreConfigValidate,
			"database path is empty",
			nil,
		)
	}
	if storeConfig.Journal != (JournalConfiguration{}) {
		if err := storeConfig.Journal.Validate(); err != nil {
			return core.E(opStoreConfigValidate, "journal config", err)
		}
	}
	if storeConfig.PurgeInterval < 0 {
		return core.E(opStoreConfigValidate, "purge interval must be zero or positive", nil)
	}
	return nil
}

// Usage example: `config := store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}`
// JournalConfiguration keeps the journal connection details in one literal so
// agents can pass a single struct to `StoreConfig.Journal` or `WithJournal`.
// Usage example: `config := storeInstance.JournalConfiguration(); fmt.Println(config.EndpointURL, config.Organisation, config.BucketName)`
type JournalConfiguration struct {
	// Usage example: `config := store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086"}`
	EndpointURL string
	// Usage example: `config := store.JournalConfiguration{Organisation: "core"}`
	Organisation string
	// Usage example: `config := store.JournalConfiguration{BucketName: "events"}`
	BucketName string
}

// Usage example: `if err := (store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}).Validate(); err != nil { return }`
func (journalConfig JournalConfiguration) Validate() error {
	switch {
	case journalConfig.EndpointURL == "":
		return core.E(
			opJournalConfigurationValidate,
			`endpoint URL is empty; use values like "http://127.0.0.1:8086"`,
			nil,
		)
	case journalConfig.Organisation == "":
		return core.E(
			opJournalConfigurationValidate,
			`organisation is empty; use values like "core"`,
			nil,
		)
	case journalConfig.BucketName == "":
		return core.E(
			opJournalConfigurationValidate,
			`bucket name is empty; use values like "events"`,
			nil,
		)
	default:
		return nil
	}
}

func (journalConfig JournalConfiguration) isConfigured() bool {
	return journalConfig.EndpointURL != "" &&
		journalConfig.Organisation != "" &&
		journalConfig.BucketName != ""
}

// Store is the SQLite key-value store with TTL expiry, namespace isolation,
// reactive events, SQLite journal writes, and orphan recovery.
//
// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}, PurgeInterval: 30 * time.Second})`
// Usage example: `value, err := storeInstance.Get("config", "colour")`
type Store struct {
	db                      *sql.DB
	sqliteDatabase          *sql.DB
	databasePath            string
	workspaceStateDirectory string
	purgeContext            context.Context
	cancelPurge             context.CancelFunc
	purgeWaitGroup          sync.WaitGroup
	purgeInterval           time.Duration // interval between background purge cycles
	sqliteStoragePath       string
	sqliteStorageDirectory  string
	mediumBacked            bool
	journal                 influxdb2.Client
	bucket                  string
	org                     string
	journalConfiguration    JournalConfiguration
	medium                  Medium
	lifecycleLock           sync.Mutex
	closeLock               sync.Mutex
	isClosed                bool
	isClosing               bool

	// Event dispatch state.
	watchers       map[string][]chan Event
	callbacks      []changeCallbackRegistration
	watcherLock    sync.RWMutex // protects watcher registration and dispatch
	callbackLock   sync.RWMutex // protects callback registration and dispatch
	nextCallbackID uint64       // monotonic ID for callback registrations

	orphanWorkspaceLock    sync.Mutex
	cachedOrphanWorkspaces []*Workspace
}

func (storeInstance *Store) ensureReady(operation string) error {
	if storeInstance == nil {
		return core.E(operation, "store is nil", nil)
	}
	if storeInstance.db == nil {
		storeInstance.db = storeInstance.sqliteDatabase
	}
	if storeInstance.sqliteDatabase == nil {
		storeInstance.sqliteDatabase = storeInstance.db
	}
	if storeInstance.db == nil || storeInstance.sqliteDatabase == nil {
		return core.E(operation, "store is not initialised", nil)
	}

	storeInstance.lifecycleLock.Lock()
	closed := storeInstance.isClosed || storeInstance.isClosing
	storeInstance.lifecycleLock.Unlock()
	if closed {
		return core.E(operation, "store is closed", nil)
	}

	return nil
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: "/tmp/go-store.db", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}})`
func WithJournal(endpointURL, organisation, bucketName string) StoreOption {
	return func(storeInstance *Store) {
		if storeInstance == nil {
			return
		}
		storeInstance.journalConfiguration = JournalConfiguration{
			EndpointURL:  endpointURL,
			Organisation: organisation,
			BucketName:   bucketName,
		}
		storeInstance.org = organisation
		storeInstance.bucket = bucketName
	}
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", WorkspaceStateDirectory: "/tmp/core-state"})`
// Use this when the workspace state directory is being assembled
// incrementally; otherwise prefer a StoreConfig literal.
func WithWorkspaceStateDirectory(directory string) StoreOption {
	return func(storeInstance *Store) {
		if storeInstance == nil {
			return
		}
		storeInstance.workspaceStateDirectory = directory
	}
}

// Usage example: `config := storeInstance.JournalConfiguration(); fmt.Println(config.EndpointURL, config.Organisation, config.BucketName)`
func (storeInstance *Store) JournalConfiguration() JournalConfiguration {
	if storeInstance == nil {
		return JournalConfiguration{}
	}
	return storeInstance.journalConfiguration
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
		DatabasePath:            storeInstance.databasePath,
		Journal:                 storeInstance.JournalConfiguration(),
		PurgeInterval:           storeInstance.purgeInterval,
		WorkspaceStateDirectory: storeInstance.WorkspaceStateDirectory(),
		Medium:                  storeInstance.medium,
	}
}

// Usage example: `databasePath := storeInstance.DatabasePath(); fmt.Println(databasePath)`
func (storeInstance *Store) DatabasePath() string {
	if storeInstance == nil {
		return ""
	}
	return storeInstance.databasePath
}

// Usage example: `stateDirectory := storeInstance.WorkspaceStateDirectory(); fmt.Println(stateDirectory)`
func (storeInstance *Store) WorkspaceStateDirectory() string {
	if storeInstance == nil {
		return normaliseWorkspaceStateDirectory(defaultWorkspaceStateDirectory)
	}
	return storeInstance.workspaceStateDirectoryPath()
}

// Usage example: `if storeInstance.IsClosed() { return }`
func (storeInstance *Store) IsClosed() bool {
	if storeInstance == nil {
		return true
	}

	storeInstance.lifecycleLock.Lock()
	closed := storeInstance.isClosed
	storeInstance.lifecycleLock.Unlock()
	return closed
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: ":memory:", PurgeInterval: 20 * time.Millisecond})`
// Use this when the purge interval is being assembled incrementally; otherwise
// prefer a StoreConfig literal.
func WithPurgeInterval(interval time.Duration) StoreOption {
	return func(storeInstance *Store) {
		if storeInstance == nil {
			return
		}
		if interval > 0 {
			storeInstance.purgeInterval = interval
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
	storeConfig = storeConfig.Normalised()

	storeInstance, err := openSQLiteStore(operation, storeConfig.DatabasePath, storeConfig.Medium)
	if err != nil {
		return nil, err
	}

	if storeConfig.Journal != (JournalConfiguration{}) {
		storeInstance.journalConfiguration = storeConfig.Journal
		storeInstance.org = storeConfig.Journal.Organisation
		storeInstance.bucket = storeConfig.Journal.BucketName
		storeInstance.journal = influxdb2.NewClient(storeConfig.Journal.EndpointURL, "")
	}
	storeInstance.purgeInterval = storeConfig.PurgeInterval
	storeInstance.workspaceStateDirectory = storeConfig.WorkspaceStateDirectory
	storeInstance.medium = storeConfig.Medium

	// New() performs a non-destructive orphan scan so callers can discover
	// leftover workspaces via RecoverOrphans().
	storeInstance.cachedOrphanWorkspaces = discoverOrphanWorkspaces(storeInstance.workspaceStateDirectoryPath(), storeInstance)
	storeInstance.startBackgroundPurge()
	return storeInstance, nil
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: "/tmp/go-store.db", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}})`
func New(databasePath string, options ...StoreOption) (*Store, error) {
	scratch := &Store{
		databasePath:            databasePath,
		workspaceStateDirectory: normaliseWorkspaceStateDirectory(defaultWorkspaceStateDirectory),
		purgeInterval:           defaultPurgeInterval,
		watchers:                make(map[string][]chan Event),
	}
	for _, option := range options {
		if option != nil {
			option(scratch)
		}
	}

	storeConfig := scratch.Config()
	storeConfig.DatabasePath = databasePath
	storeConfig.Journal = scratch.JournalConfiguration()
	storeConfig.PurgeInterval = scratch.purgeInterval
	storeConfig.WorkspaceStateDirectory = scratch.WorkspaceStateDirectory()
	storeConfig.Medium = scratch.medium

	storeInstance, err := openConfiguredStore(opNew, storeConfig)
	if err != nil {
		return nil, err
	}
	return storeInstance, nil
}

func openSQLiteStore(operation, databasePath string, medium Medium) (*Store, error) {
	storage, err := prepareSQLiteStorage(operation, databasePath, medium)
	if err != nil {
		return nil, err
	}

	sqliteDatabase, err := sql.Open("sqlite", storage.path)
	if err != nil {
		return nil, core.E(operation, "open database", err)
	}
	if err := configureSQLiteDatabase(operation, sqliteDatabase); err != nil {
		_ = sqliteDatabase.Close()
		return nil, err
	}

	purgeContext, cancel := context.WithCancel(context.Background())
	return &Store{
		db:                      sqliteDatabase,
		sqliteDatabase:          sqliteDatabase,
		databasePath:            databasePath,
		workspaceStateDirectory: normaliseWorkspaceStateDirectory(defaultWorkspaceStateDirectory),
		purgeContext:            purgeContext,
		cancelPurge:             cancel,
		purgeInterval:           defaultPurgeInterval,
		sqliteStoragePath:       storage.path,
		sqliteStorageDirectory:  storage.directory,
		mediumBacked:            storage.mediumBacked,
		medium:                  medium,
		watchers:                make(map[string][]chan Event),
	}, nil
}

type sqliteStorageConfig struct {
	path         string
	directory    string
	mediumBacked bool
}

func prepareSQLiteStorage(operation, databasePath string, medium Medium) (sqliteStorageConfig, error) {
	storage := sqliteStorageConfig{path: databasePath}
	storage.mediumBacked = medium != nil && databasePath != "" && databasePath != memoryDatabasePath
	if !storage.mediumBacked {
		return storage, nil
	}
	filesystem := (&core.Fs{}).NewUnrestricted()
	storage.directory = filesystem.TempDir("go-store")
	storage.path = core.Path(storage.directory, "store.db")
	if !medium.Exists(databasePath) {
		return storage, nil
	}
	content, err := medium.Read(databasePath)
	if err != nil {
		return sqliteStorageConfig{}, core.E(operation, "read database from medium", err)
	}
	if result := filesystem.Write(storage.path, content); !result.OK {
		return sqliteStorageConfig{}, core.E(operation, "seed sqlite file from medium", result.Value.(error))
	}
	return storage, nil
}

func configureSQLiteDatabase(operation string, sqliteDatabase *sql.DB) error {
	// Serialise all access through a single connection. SQLite only supports
	// one writer at a time; using a pool causes SQLITE_BUSY under contention
	// because pragmas (journal_mode, busy_timeout) are per-connection and the
	// pool hands out different connections for each call.
	sqliteDatabase.SetMaxOpenConns(1)
	if _, err := sqliteDatabase.Exec(sqliteWALPragma); err != nil {
		return core.E(operation, "set WAL journal mode", err)
	}
	if _, err := sqliteDatabase.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return core.E(operation, "set busy timeout", err)
	}
	if err := ensureSchema(sqliteDatabase); err != nil {
		return core.E(operation, "ensure schema", err)
	}
	return nil
}

func (storeInstance *Store) workspaceStateDirectoryPath() string {
	if storeInstance == nil || storeInstance.workspaceStateDirectory == "" {
		return normaliseWorkspaceStateDirectory(defaultWorkspaceStateDirectory)
	}
	return normaliseWorkspaceStateDirectory(storeInstance.workspaceStateDirectory)
}

// Usage example: `storeInstance, err := store.New(":memory:"); if err != nil { return }; defer func() { _ = storeInstance.Close() }()`
func (storeInstance *Store) Close() error {
	if storeInstance == nil {
		return nil
	}

	storeInstance.closeLock.Lock()
	defer storeInstance.closeLock.Unlock()

	storeInstance.lifecycleLock.Lock()
	if storeInstance.isClosed {
		storeInstance.lifecycleLock.Unlock()
		return nil
	}
	storeInstance.isClosing = true
	storeInstance.lifecycleLock.Unlock()

	if storeInstance.cancelPurge != nil {
		storeInstance.cancelPurge()
	}
	storeInstance.purgeWaitGroup.Wait()

	if storeInstance.journal != nil {
		storeInstance.journal.Close()
	}

	storeInstance.watcherLock.Lock()
	for groupName, registeredEvents := range storeInstance.watchers {
		for _, registeredEventChannel := range registeredEvents {
			close(registeredEventChannel)
		}
		delete(storeInstance.watchers, groupName)
	}
	storeInstance.watcherLock.Unlock()

	storeInstance.callbackLock.Lock()
	storeInstance.callbacks = nil
	storeInstance.callbackLock.Unlock()

	storeInstance.orphanWorkspaceLock.Lock()
	var orphanCleanupErr error
	for _, orphanWorkspace := range storeInstance.cachedOrphanWorkspaces {
		if err := orphanWorkspace.closeWithoutRemovingFiles(); err != nil && orphanCleanupErr == nil {
			orphanCleanupErr = err
		}
	}
	storeInstance.cachedOrphanWorkspaces = nil
	storeInstance.orphanWorkspaceLock.Unlock()

	if storeInstance.db == nil {
		storeInstance.db = storeInstance.sqliteDatabase
	}
	if storeInstance.sqliteDatabase == nil {
		storeInstance.sqliteDatabase = storeInstance.db
	}
	if storeInstance.sqliteDatabase == nil {
		storeInstance.markClosed()
		return orphanCleanupErr
	}
	if err := storeInstance.sqliteDatabase.Close(); err != nil {
		return core.E(opClose, "database close", err)
	}
	if err := storeInstance.syncMediumBackedDatabase(); err != nil {
		return core.E(opClose, "sync medium-backed database", err)
	}
	storeInstance.markClosed()
	if orphanCleanupErr != nil {
		return core.E(opClose, "close orphan workspaces", orphanCleanupErr)
	}
	return orphanCleanupErr
}

func (storeInstance *Store) markClosed() {
	storeInstance.lifecycleLock.Lock()
	storeInstance.isClosed = true
	storeInstance.isClosing = false
	storeInstance.lifecycleLock.Unlock()
}

func (storeInstance *Store) syncMediumBackedDatabase() error {
	if storeInstance == nil || !storeInstance.mediumBacked || storeInstance.medium == nil {
		return nil
	}
	if storeInstance.databasePath == "" || storeInstance.databasePath == memoryDatabasePath {
		return nil
	}
	if storeInstance.sqliteStoragePath == "" {
		return nil
	}

	filesystem := (&core.Fs{}).NewUnrestricted()
	readResult := filesystem.Read(storeInstance.sqliteStoragePath)
	if !readResult.OK {
		return readResult.Value.(error)
	}
	if err := storeInstance.medium.Write(storeInstance.databasePath, readResult.Value.(string)); err != nil {
		return err
	}

	if storeInstance.sqliteStorageDirectory != "" {
		_ = filesystem.DeleteAll(storeInstance.sqliteStorageDirectory)
		return nil
	}
	for _, path := range []string{storeInstance.sqliteStoragePath + "-wal", storeInstance.sqliteStoragePath + "-shm"} {
		_ = filesystem.Delete(path)
	}
	return nil
}

// Usage example: `colourValue, err := storeInstance.Get("config", "colour")`
func (storeInstance *Store) Get(group, key string) (string, error) {
	if err := storeInstance.ensureReady(opGet); err != nil {
		return "", err
	}

	var value string
	var expiresAt sql.NullInt64
	err := storeInstance.sqliteDatabase.QueryRow(
		sqlSelect+entryValueColumn+", expires_at FROM "+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?",
		group, key,
	).Scan(&value, &expiresAt)
	if err == sql.ErrNoRows {
		return "", core.E(opGet, core.Concat(group, "/", key), NotFoundError)
	}
	if err != nil {
		return "", core.E(opGet, "query row", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		if err := storeInstance.Delete(group, key); err != nil {
			return "", core.E(opGet, "delete expired row", err)
		}
		return "", core.E(opGet, core.Concat(group, "/", key), NotFoundError)
	}
	return value, nil
}

// Usage example: `if err := storeInstance.Set("config", "colour", "blue"); err != nil { return }`
func (storeInstance *Store) Set(group, key, value string) error {
	if err := storeInstance.ensureReady("store.Set"); err != nil {
		return err
	}

	_, err := storeInstance.sqliteDatabase.Exec(
		sqlInsertIntoPrefix+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, NULL) "+
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
		sqlInsertIntoPrefix+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, ?) "+
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
	if err := storeInstance.ensureReady(opDelete); err != nil {
		return err
	}

	deleteResult, err := storeInstance.sqliteDatabase.Exec(sqlDeleteFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?", group, key)
	if err != nil {
		return core.E(opDelete, "delete row", err)
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.E(opDelete, "count deleted rows", rowsAffectedError)
	}
	if deletedRows > 0 {
		storeInstance.notify(Event{Type: EventDelete, Group: group, Key: key, Timestamp: time.Now()})
	}
	return nil
}

// Usage example: `exists, err := storeInstance.Exists("config", "colour")`
// Usage example: `if exists, _ := storeInstance.Exists("session", "token"); !exists { fmt.Println("session expired") }`
func (storeInstance *Store) Exists(group, key string) (bool, error) {
	if err := storeInstance.ensureReady("store.Exists"); err != nil {
		return false, err
	}

	return liveEntryExists(storeInstance.sqliteDatabase, group, key)
}

// Usage example: `exists, err := storeInstance.GroupExists("config")`
// Usage example: `if exists, _ := storeInstance.GroupExists("tenant-a:config"); !exists { fmt.Println("group is empty") }`
func (storeInstance *Store) GroupExists(group string) (bool, error) {
	if err := storeInstance.ensureReady("store.GroupExists"); err != nil {
		return false, err
	}

	count, err := storeInstance.Count(group)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Usage example: `keyCount, err := storeInstance.Count("config")`
func (storeInstance *Store) Count(group string) (int, error) {
	if err := storeInstance.ensureReady("store.Count"); err != nil {
		return 0, err
	}

	var count int
	err := storeInstance.sqliteDatabase.QueryRow(
		sqlSelectCountFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	).Scan(&count)
	if err != nil {
		return 0, core.E("store.Count", "count rows", err)
	}
	return count, nil
}

// Usage example: `if err := storeInstance.DeleteGroup("cache"); err != nil { return }`
func (storeInstance *Store) DeleteGroup(group string) error {
	if err := storeInstance.ensureReady(opDeleteGroup); err != nil {
		return err
	}

	deleteResult, err := storeInstance.sqliteDatabase.Exec(sqlDeleteFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ?", group)
	if err != nil {
		return core.E(opDeleteGroup, "delete group", err)
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.E(opDeleteGroup, "count deleted rows", rowsAffectedError)
	}
	if deletedRows > 0 {
		storeInstance.notify(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	}
	return nil
}

// Usage example: `if err := storeInstance.DeletePrefix("tenant-a:"); err != nil { return }`
func (storeInstance *Store) DeletePrefix(groupPrefix string) error {
	if err := storeInstance.ensureReady(opDeletePrefix); err != nil {
		return err
	}

	var rows *sql.Rows
	var err error
	if groupPrefix == "" {
		rows, err = storeInstance.sqliteDatabase.Query(
			sqlSelectDistinct + entryGroupColumn + sqlFrom + entriesTableName + " ORDER BY " + entryGroupColumn,
		)
	} else {
		rows, err = storeInstance.sqliteDatabase.Query(
			sqlSelectDistinct+entryGroupColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" LIKE ? ESCAPE '^' ORDER BY "+entryGroupColumn,
			escapeLike(groupPrefix)+"%",
		)
	}
	if err != nil {
		return core.E(opDeletePrefix, "list groups", err)
	}
	defer func() { _ = rows.Close() }()

	var groupNames []string
	for rows.Next() {
		var groupName string
		if err := rows.Scan(&groupName); err != nil {
			return core.E(opDeletePrefix, "scan group name", err)
		}
		groupNames = append(groupNames, groupName)
	}
	if err := rows.Err(); err != nil {
		return core.E(opDeletePrefix, "iterate groups", err)
	}
	for _, groupName := range groupNames {
		if err := storeInstance.DeleteGroup(groupName); err != nil {
			return core.E(opDeletePrefix, "delete group", err)
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
	if err := storeInstance.ensureReady(opGetPage); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, core.E(opGetPage, "offset must be zero or positive", nil)
	}
	if limit < 0 {
		return nil, core.E(opGetPage, "limit must be zero or positive", nil)
	}

	rows, err := storeInstance.sqliteDatabase.Query(
		sqlSelect+entryKeyColumn+", "+entryValueColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn+" LIMIT ? OFFSET ?",
		group, time.Now().UnixMilli(), limit, offset,
	)
	if err != nil {
		return nil, core.E(opGetPage, "query rows", err)
	}
	defer func() { _ = rows.Close() }()

	page := make([]KeyValue, 0, limit)
	for rows.Next() {
		var entry KeyValue
		if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
			return nil, core.E(opGetPage, "scan row", err)
		}
		page = append(page, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, core.E(opGetPage, rowsIterationMessage, err)
	}
	return page, nil
}

// Usage example: `for entry, err := range storeInstance.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeInstance *Store) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		if err := storeInstance.ensureReady(opAll); err != nil {
			yield(KeyValue{}, err)
			return
		}

		rows, err := storeInstance.sqliteDatabase.Query(
			sqlSelect+entryKeyColumn+", "+entryValueColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn,
			group, time.Now().UnixMilli(),
		)
		if err != nil {
			yield(KeyValue{}, core.E(opAll, "query rows", err))
			return
		}
		defer func() { _ = rows.Close() }()

		yieldKeyValueRows(rows, opAll, yield)
	}
}

func yieldKeyValueRows(rows *sql.Rows, operation string, yield func(KeyValue, error) bool) {
	for rows.Next() {
		var entry KeyValue
		if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
			if !yield(KeyValue{}, core.E(operation, "scan row", err)) {
				return
			}
			continue
		}
		if !yield(entry, nil) {
			return
		}
	}
	if err := rows.Err(); err != nil {
		yield(KeyValue{}, core.E(operation, rowsIterationMessage, err))
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
	if err := storeInstance.ensureReady(opRender); err != nil {
		return "", err
	}

	templateData := make(map[string]string)
	for entry, err := range storeInstance.All(group) {
		if err != nil {
			return "", core.E(opRender, "iterate rows", err)
		}
		templateData[entry.Key] = entry.Value
	}

	renderTemplate, err := template.New("render").Parse(templateSource)
	if err != nil {
		return "", core.E(opRender, "parse template", err)
	}
	builder := core.NewBuilder()
	if err := renderTemplate.Execute(builder, templateData); err != nil {
		return "", core.E(opRender, "execute template", err)
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
			sqlSelectCountFrom+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?)",
			time.Now().UnixMilli(),
		).Scan(&count)
	} else {
		err = storeInstance.sqliteDatabase.QueryRow(
			sqlSelectCountFrom+entriesTableName+sqlWhere+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
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
	actualGroupPrefix := firstStringOrEmpty(groupPrefix)
	return func(yield func(string, error) bool) {
		if err := storeInstance.ensureReady(opGroupsSeq); err != nil {
			yield("", err)
			return
		}

		var rows *sql.Rows
		var err error
		now := time.Now().UnixMilli()
		if actualGroupPrefix == "" {
			rows, err = storeInstance.sqliteDatabase.Query(
				sqlSelectDistinct+entryGroupColumn+sqlFrom+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				now,
			)
		} else {
			rows, err = storeInstance.sqliteDatabase.Query(
				sqlSelectDistinct+entryGroupColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				escapeLike(actualGroupPrefix)+"%", now,
			)
		}
		if err != nil {
			yield("", core.E(opGroupsSeq, "query group names", err))
			return
		}
		defer func() { _ = rows.Close() }()

		yieldGroupRows(rows, opGroupsSeq, yield)
	}
}

func yieldGroupRows(rows *sql.Rows, operation string, yield func(string, error) bool) {
	for rows.Next() {
		var groupName string
		if err := rows.Scan(&groupName); err != nil {
			if !yield("", core.E(operation, "scan group name", err)) {
				return
			}
			continue
		}
		if !yield(groupName, nil) {
			return
		}
	}
	if err := rows.Err(); err != nil {
		yield("", core.E(operation, rowsIterationMessage, err))
	}
}

func firstStringOrEmpty(values []string) string {
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

	cutoffUnixMilli := time.Now().UnixMilli()
	expiredEntries, err := deleteExpiredEntriesMatchingGroupPrefix(storeInstance.sqliteDatabase, "", cutoffUnixMilli)
	if err != nil {
		return 0, core.E("store.PurgeExpired", "delete expired rows", err)
	}
	storeInstance.notifyExpiredEntries(expiredEntries)
	return int64(len(expiredEntries)), nil
}

func (storeInstance *Store) notifyExpiredEntries(expiredEntries []expiredEntryRef) {
	for _, expiredEntry := range expiredEntries {
		storeInstance.notify(eventFromExpiredEntry(expiredEntry))
	}
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
		storeInstance.purgeInterval = defaultPurgeInterval
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
					_ = err
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

type expiredEntryRef struct {
	group string
	key   string
}

func eventFromExpiredEntry(expiredEntry expiredEntryRef) Event {
	return Event{
		Type:      EventDelete,
		Group:     expiredEntry.group,
		Key:       expiredEntry.key,
		Timestamp: time.Now(),
	}
}

func deleteExpiredEntriesMatchingGroupPrefix(database schemaDatabase, groupPrefix string, cutoffUnixMilli int64) ([]expiredEntryRef, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if groupPrefix == "" {
		rows, err = database.Query(
			sqlDeleteFrom+entriesTableName+" WHERE expires_at IS NOT NULL AND expires_at <= ? RETURNING "+entryGroupColumn+", "+entryKeyColumn,
			cutoffUnixMilli,
		)
	} else {
		rows, err = database.Query(
			sqlDeleteFrom+entriesTableName+" WHERE expires_at IS NOT NULL AND expires_at <= ? AND "+entryGroupColumn+" LIKE ? ESCAPE '^' RETURNING "+entryGroupColumn+", "+entryKeyColumn,
			cutoffUnixMilli, escapeLike(groupPrefix)+"%",
		)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	expiredEntries := make([]expiredEntryRef, 0)
	for rows.Next() {
		var expiredEntry expiredEntryRef
		if err := rows.Scan(&expiredEntry.group, &expiredEntry.key); err != nil {
			return nil, err
		}
		expiredEntries = append(expiredEntries, expiredEntry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return expiredEntries, nil
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
		return core.E(opEnsureSchema, "schema", err)
	}

	legacyEntriesTableExists, err := tableExists(database, legacyKeyValueTableName)
	if err != nil {
		return core.E(opEnsureSchema, "schema", err)
	}

	if entriesTableExists {
		if err := ensureExpiryColumn(database); err != nil {
			return core.E(opEnsureSchema, "migration", err)
		}
		if legacyEntriesTableExists {
			if err := migrateLegacyEntriesTable(database); err != nil {
				return core.E(opEnsureSchema, "migration", err)
			}
		}
		return nil
	}

	if legacyEntriesTableExists {
		if err := migrateLegacyEntriesTable(database); err != nil {
			return core.E(opEnsureSchema, "migration", err)
		}
		return nil
	}

	if _, err := database.Exec(createEntriesTableSQL); err != nil {
		return core.E(opEnsureSchema, "schema", err)
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
				_ = rollbackErr
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
	defer func() { _ = rows.Close() }()

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
