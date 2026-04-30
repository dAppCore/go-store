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
func (storeConfig StoreConfig) Validate() core.Result {
	if storeConfig.DatabasePath == "" {
		return core.Fail(core.E(
			opStoreConfigValidate,
			"database path is empty",
			nil,
		))
	}
	if storeConfig.Journal != (JournalConfiguration{}) {
		if result := storeConfig.Journal.Validate(); !result.OK {
			err, _ := result.Value.(error)
			return core.Fail(core.E(opStoreConfigValidate, "journal config", err))
		}
	}
	if storeConfig.PurgeInterval < 0 {
		return core.Fail(core.E(opStoreConfigValidate, "purge interval must be zero or positive", nil))
	}
	return core.Ok(nil)
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
func (journalConfig JournalConfiguration) Validate() core.Result {
	switch {
	case journalConfig.EndpointURL == "":
		return core.Fail(core.E(
			opJournalConfigurationValidate,
			`endpoint URL is empty; use values like "http://127.0.0.1:8086"`,
			nil,
		))
	case journalConfig.Organisation == "":
		return core.Fail(core.E(
			opJournalConfigurationValidate,
			`organisation is empty; use values like "core"`,
			nil,
		))
	case journalConfig.BucketName == "":
		return core.Fail(core.E(
			opJournalConfigurationValidate,
			`bucket name is empty; use values like "events"`,
			nil,
		))
	default:
		return core.Ok(nil)
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

func (storeInstance *Store) ensureReady(operation string) core.Result {
	if storeInstance == nil {
		return core.Fail(core.E(operation, "store is nil", nil))
	}
	if storeInstance.db == nil {
		storeInstance.db = storeInstance.sqliteDatabase
	}
	if storeInstance.sqliteDatabase == nil {
		storeInstance.sqliteDatabase = storeInstance.db
	}
	if storeInstance.db == nil || storeInstance.sqliteDatabase == nil {
		return core.Fail(core.E(operation, "store is not initialised", nil))
	}

	storeInstance.lifecycleLock.Lock()
	closed := storeInstance.isClosed || storeInstance.isClosing
	storeInstance.lifecycleLock.Unlock()
	if closed {
		return core.Fail(core.E(operation, "store is closed", nil))
	}

	return core.Ok(nil)
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
func NewConfigured(storeConfig StoreConfig) (*Store, core.Result) {
	return openConfiguredStore("store.NewConfigured", storeConfig)
}

func openConfiguredStore(operation string, storeConfig StoreConfig) (*Store, core.Result) {
	if result := storeConfig.Validate(); !result.OK {
		err, _ := result.Value.(error)
		return nil, core.Fail(core.E(operation, "validate config", err))
	}
	storeConfig = storeConfig.Normalised()

	storeInstance, result := openSQLiteStore(operation, storeConfig.DatabasePath, storeConfig.Medium)
	if !result.OK {
		return nil, result
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
	return storeInstance, core.Ok(nil)
}

// Usage example: `storeInstance, err := store.NewConfigured(store.StoreConfig{DatabasePath: "/tmp/go-store.db", Journal: store.JournalConfiguration{EndpointURL: "http://127.0.0.1:8086", Organisation: "core", BucketName: "events"}})`
func New(databasePath string, options ...StoreOption) (*Store, core.Result) {
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

	storeInstance, result := openConfiguredStore(opNew, storeConfig)
	if !result.OK {
		return nil, result
	}
	return storeInstance, core.Ok(nil)
}

func openSQLiteStore(operation, databasePath string, medium Medium) (*Store, core.Result) {
	storage, result := prepareSQLiteStorage(operation, databasePath, medium)
	if !result.OK {
		return nil, result
	}

	sqliteDatabase, openErr := sql.Open("sqlite", storage.path)
	if openErr != nil {
		return nil, core.Fail(core.E(operation, "open database", openErr))
	}
	if result := configureSQLiteDatabase(operation, sqliteDatabase); !result.OK {
		if closeErr := sqliteDatabase.Close(); closeErr != nil {
			core.Error("sqlite close after configure failed", "err", closeErr)
		}
		return nil, result
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
	}, core.Ok(nil)
}

type sqliteStorageConfig struct {
	path         string
	directory    string
	mediumBacked bool
}

func prepareSQLiteStorage(operation, databasePath string, medium Medium) (sqliteStorageConfig, core.Result) {
	storage := sqliteStorageConfig{path: databasePath}
	storage.mediumBacked = medium != nil && databasePath != "" && databasePath != memoryDatabasePath
	if !storage.mediumBacked {
		return storage, core.Ok(nil)
	}
	filesystem := (&core.Fs{}).NewUnrestricted()
	storage.directory = filesystem.TempDir("go-store")
	storage.path = core.Path(storage.directory, "store.db")
	if !medium.Exists(databasePath) {
		return storage, core.Ok(nil)
	}
	content, result := medium.Read(databasePath)
	if !result.OK {
		err, _ := result.Value.(error)
		return sqliteStorageConfig{}, core.Fail(core.E(operation, "read database from medium", err))
	}
	if result := filesystem.Write(storage.path, content); !result.OK {
		return sqliteStorageConfig{}, core.Fail(core.E(operation, "seed sqlite file from medium", result.Value.(error)))
	}
	return storage, core.Ok(nil)
}

func configureSQLiteDatabase(operation string, sqliteDatabase *sql.DB) core.Result {
	// Serialise all access through a single connection. SQLite only supports
	// one writer at a time; using a pool causes SQLITE_BUSY under contention
	// because pragmas (journal_mode, busy_timeout) are per-connection and the
	// pool hands out different connections for each call.
	sqliteDatabase.SetMaxOpenConns(1)
	if _, err := sqliteDatabase.Exec(sqliteWALPragma); err != nil {
		return core.Fail(core.E(operation, "set WAL journal mode", err))
	}
	if _, err := sqliteDatabase.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return core.Fail(core.E(operation, "set busy timeout", err))
	}
	if result := ensureSchema(sqliteDatabase); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(operation, "ensure schema", err))
	}
	return core.Ok(nil)
}

func (storeInstance *Store) workspaceStateDirectoryPath() string {
	if storeInstance == nil || storeInstance.workspaceStateDirectory == "" {
		return normaliseWorkspaceStateDirectory(defaultWorkspaceStateDirectory)
	}
	return normaliseWorkspaceStateDirectory(storeInstance.workspaceStateDirectory)
}

// Usage example: `storeInstance, err := store.New(":memory:"); if err != nil { return }; defer func() { _ = storeInstance.Close() }()`
func (storeInstance *Store) Close() core.Result {
	if storeInstance == nil {
		return core.Ok(nil)
	}

	storeInstance.closeLock.Lock()
	defer storeInstance.closeLock.Unlock()

	storeInstance.lifecycleLock.Lock()
	if storeInstance.isClosed {
		storeInstance.lifecycleLock.Unlock()
		return core.Ok(nil)
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
	var orphanCleanupResult core.Result
	for _, orphanWorkspace := range storeInstance.cachedOrphanWorkspaces {
		if result := orphanWorkspace.closeWithoutRemovingFiles(); !result.OK && orphanCleanupResult.Value == nil {
			orphanCleanupResult = result
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
		if orphanCleanupResult.Value != nil {
			return orphanCleanupResult
		}
		return core.Ok(nil)
	}
	if err := storeInstance.sqliteDatabase.Close(); err != nil {
		return core.Fail(core.E(opClose, "database close", err))
	}
	if result := storeInstance.syncMediumBackedDatabase(); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opClose, "sync medium-backed database", err))
	}
	storeInstance.markClosed()
	if orphanCleanupResult.Value != nil {
		err, _ := orphanCleanupResult.Value.(error)
		return core.Fail(core.E(opClose, "close orphan workspaces", err))
	}
	return core.Ok(nil)
}

func (storeInstance *Store) markClosed() {
	storeInstance.lifecycleLock.Lock()
	storeInstance.isClosed = true
	storeInstance.isClosing = false
	storeInstance.lifecycleLock.Unlock()
}

func (storeInstance *Store) syncMediumBackedDatabase() core.Result {
	if storeInstance == nil || !storeInstance.mediumBacked || storeInstance.medium == nil {
		return core.Ok(nil)
	}
	if storeInstance.databasePath == "" || storeInstance.databasePath == memoryDatabasePath {
		return core.Ok(nil)
	}
	if storeInstance.sqliteStoragePath == "" {
		return core.Ok(nil)
	}

	filesystem := (&core.Fs{}).NewUnrestricted()
	readResult := filesystem.Read(storeInstance.sqliteStoragePath)
	if !readResult.OK {
		return readResult
	}
	if result := storeInstance.medium.Write(storeInstance.databasePath, readResult.Value.(string)); !result.OK {
		return result
	}

	if storeInstance.sqliteStorageDirectory != "" {
		if result := filesystem.DeleteAll(storeInstance.sqliteStorageDirectory); !result.OK {
			core.Error("sqlite storage directory cleanup failed", "err", result.Error())
		}
		return core.Ok(nil)
	}
	for _, path := range []string{storeInstance.sqliteStoragePath + "-wal", storeInstance.sqliteStoragePath + "-shm"} {
		if result := filesystem.Delete(path); !result.OK {
			core.Error("sqlite sidecar cleanup failed", "file", path, "err", result.Error())
		}
	}
	return core.Ok(nil)
}

// Usage example: `colourValue, err := storeInstance.Get("config", "colour")`
func (storeInstance *Store) Get(group, key string) (string, core.Result) {
	if result := storeInstance.ensureReady(opGet); !result.OK {
		return "", result
	}

	var value string
	var expiresAt sql.NullInt64
	err := storeInstance.sqliteDatabase.QueryRow(
		sqlSelect+entryValueColumn+", expires_at FROM "+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?",
		group, key,
	).Scan(&value, &expiresAt)
	if err == sql.ErrNoRows {
		return "", core.Fail(core.E(opGet, core.Concat(group, "/", key), NotFoundError))
	}
	if err != nil {
		return "", core.Fail(core.E(opGet, "query row", err))
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		if result := storeInstance.Delete(group, key); !result.OK {
			err, _ := result.Value.(error)
			return "", core.Fail(core.E(opGet, "delete expired row", err))
		}
		return "", core.Fail(core.E(opGet, core.Concat(group, "/", key), NotFoundError))
	}
	return value, core.Ok(nil)
}

// Usage example: `if err := storeInstance.Set("config", "colour", "blue"); err != nil { return }`
func (storeInstance *Store) Set(group, key, value string) core.Result {
	if result := storeInstance.ensureReady("store.Set"); !result.OK {
		return result
	}

	_, err := storeInstance.sqliteDatabase.Exec(
		sqlInsertIntoPrefix+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, NULL) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = NULL",
		group, key, value,
	)
	if err != nil {
		return core.Fail(core.E("store.Set", "execute upsert", err))
	}
	storeInstance.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return core.Ok(nil)
}

// Usage example: `if err := storeInstance.SetWithTTL("session", "token", "abc123", time.Minute); err != nil { return }`
func (storeInstance *Store) SetWithTTL(group, key, value string, timeToLive time.Duration) core.Result {
	if result := storeInstance.ensureReady("store.SetWithTTL"); !result.OK {
		return result
	}

	expiresAt := time.Now().Add(timeToLive).UnixMilli()
	_, err := storeInstance.sqliteDatabase.Exec(
		sqlInsertIntoPrefix+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, ?) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = excluded.expires_at",
		group, key, value, expiresAt,
	)
	if err != nil {
		return core.Fail(core.E("store.SetWithTTL", "execute upsert with expiry", err))
	}
	storeInstance.notify(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return core.Ok(nil)
}

// Usage example: `if err := storeInstance.Delete("config", "colour"); err != nil { return }`
func (storeInstance *Store) Delete(group, key string) core.Result {
	if result := storeInstance.ensureReady(opDelete); !result.OK {
		return result
	}

	deleteResult, err := storeInstance.sqliteDatabase.Exec(sqlDeleteFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?", group, key)
	if err != nil {
		return core.Fail(core.E(opDelete, "delete row", err))
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.Fail(core.E(opDelete, "count deleted rows", rowsAffectedError))
	}
	if deletedRows > 0 {
		storeInstance.notify(Event{Type: EventDelete, Group: group, Key: key, Timestamp: time.Now()})
	}
	return core.Ok(nil)
}

// Usage example: `exists, err := storeInstance.Exists("config", "colour")`
// Usage example: `if exists, _ := storeInstance.Exists("session", "token"); !exists { fmt.Println("session expired") }`
func (storeInstance *Store) Exists(group, key string) (bool, core.Result) {
	if result := storeInstance.ensureReady("store.Exists"); !result.OK {
		return false, result
	}

	return liveEntryExists(storeInstance.sqliteDatabase, group, key)
}

// Usage example: `exists, err := storeInstance.GroupExists("config")`
// Usage example: `if exists, _ := storeInstance.GroupExists("tenant-a:config"); !exists { fmt.Println("group is empty") }`
func (storeInstance *Store) GroupExists(group string) (bool, core.Result) {
	if result := storeInstance.ensureReady("store.GroupExists"); !result.OK {
		return false, result
	}

	count, result := storeInstance.Count(group)
	if !result.OK {
		return false, result
	}
	return count > 0, core.Ok(nil)
}

// Usage example: `keyCount, err := storeInstance.Count("config")`
func (storeInstance *Store) Count(group string) (int, core.Result) {
	if result := storeInstance.ensureReady("store.Count"); !result.OK {
		return 0, result
	}

	var count int
	err := storeInstance.sqliteDatabase.QueryRow(
		sqlSelectCountFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	).Scan(&count)
	if err != nil {
		return 0, core.Fail(core.E("store.Count", "count rows", err))
	}
	return count, core.Ok(nil)
}

// Usage example: `if err := storeInstance.DeleteGroup("cache"); err != nil { return }`
func (storeInstance *Store) DeleteGroup(group string) core.Result {
	if result := storeInstance.ensureReady(opDeleteGroup); !result.OK {
		return result
	}

	deleteResult, err := storeInstance.sqliteDatabase.Exec(sqlDeleteFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ?", group)
	if err != nil {
		return core.Fail(core.E(opDeleteGroup, "delete group", err))
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.Fail(core.E(opDeleteGroup, "count deleted rows", rowsAffectedError))
	}
	if deletedRows > 0 {
		storeInstance.notify(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	}
	return core.Ok(nil)
}

// Usage example: `if err := storeInstance.DeletePrefix("tenant-a:"); err != nil { return }`
func (storeInstance *Store) DeletePrefix(groupPrefix string) core.Result {
	if result := storeInstance.ensureReady(opDeletePrefix); !result.OK {
		return result
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
		return core.Fail(core.E(opDeletePrefix, "list groups", err))
	}
	defer func() { _ = rows.Close() }()

	var groupNames []string
	for rows.Next() {
		var groupName string
		if err := rows.Scan(&groupName); err != nil {
			return core.Fail(core.E(opDeletePrefix, "scan group name", err))
		}
		groupNames = append(groupNames, groupName)
	}
	if err := rows.Err(); err != nil {
		return core.Fail(core.E(opDeletePrefix, "iterate groups", err))
	}
	for _, groupName := range groupNames {
		if result := storeInstance.DeleteGroup(groupName); !result.OK {
			err, _ := result.Value.(error)
			return core.Fail(core.E(opDeletePrefix, "delete group", err))
		}
	}
	return core.Ok(nil)
}

// Usage example: `for entry, err := range storeInstance.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
type KeyValue struct {
	// Usage example: `if entry.Key == "colour" { return }`
	Key string
	// Usage example: `if entry.Value == "blue" { return }`
	Value string
}

// Usage example: `colourEntries, err := storeInstance.GetAll("config")`
func (storeInstance *Store) GetAll(group string) (map[string]string, core.Result) {
	if result := storeInstance.ensureReady("store.GetAll"); !result.OK {
		return nil, result
	}

	entriesByKey := make(map[string]string)
	for entry, err := range storeInstance.All(group) {
		if err != nil {
			return nil, core.Fail(core.E("store.GetAll", "iterate rows", err))
		}
		entriesByKey[entry.Key] = entry.Value
	}
	return entriesByKey, core.Ok(nil)
}

// Usage example: `page, err := storeInstance.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (storeInstance *Store) GetPage(group string, offset, limit int) ([]KeyValue, core.Result) {
	if result := storeInstance.ensureReady(opGetPage); !result.OK {
		return nil, result
	}
	if offset < 0 {
		return nil, core.Fail(core.E(opGetPage, "offset must be zero or positive", nil))
	}
	if limit < 0 {
		return nil, core.Fail(core.E(opGetPage, "limit must be zero or positive", nil))
	}

	rows, err := storeInstance.sqliteDatabase.Query(
		sqlSelect+entryKeyColumn+", "+entryValueColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn+" LIMIT ? OFFSET ?",
		group, time.Now().UnixMilli(), limit, offset,
	)
	if err != nil {
		return nil, core.Fail(core.E(opGetPage, "query rows", err))
	}
	defer func() { _ = rows.Close() }()

	page := make([]KeyValue, 0, limit)
	for rows.Next() {
		var entry KeyValue
		if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
			return nil, core.Fail(core.E(opGetPage, "scan row", err))
		}
		page = append(page, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, core.Fail(core.E(opGetPage, rowsIterationMessage, err))
	}
	return page, core.Ok(nil)
}

// Usage example: `for entry, err := range storeInstance.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeInstance *Store) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		if result := storeInstance.ensureReady(opAll); !result.OK {
			err, _ := result.Value.(error)
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
func (storeInstance *Store) GetSplit(group, key, separator string) (iter.Seq[string], core.Result) {
	if result := storeInstance.ensureReady("store.GetSplit"); !result.OK {
		return nil, result
	}

	value, result := storeInstance.Get(group, key)
	if !result.OK {
		return nil, result
	}
	return splitValueSeq(value, separator), core.Ok(nil)
}

// Usage example: `fields, err := storeInstance.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (storeInstance *Store) GetFields(group, key string) (iter.Seq[string], core.Result) {
	if result := storeInstance.ensureReady("store.GetFields"); !result.OK {
		return nil, result
	}

	value, result := storeInstance.Get(group, key)
	if !result.OK {
		return nil, result
	}
	return fieldsValueSeq(value), core.Ok(nil)
}

// Usage example: `renderedTemplate, err := storeInstance.Render("Hello {{ .name }}", "user")`
func (storeInstance *Store) Render(templateSource, group string) (string, core.Result) {
	if result := storeInstance.ensureReady(opRender); !result.OK {
		return "", result
	}

	templateData := make(map[string]string)
	for entry, err := range storeInstance.All(group) {
		if err != nil {
			return "", core.Fail(core.E(opRender, "iterate rows", err))
		}
		templateData[entry.Key] = entry.Value
	}

	renderTemplate, err := template.New("render").Parse(templateSource)
	if err != nil {
		return "", core.Fail(core.E(opRender, "parse template", err))
	}
	builder := core.NewBuilder()
	if err := renderTemplate.Execute(builder, templateData); err != nil {
		return "", core.Fail(core.E(opRender, "execute template", err))
	}
	return builder.String(), core.Ok(nil)
}

// Usage example: `tenantKeyCount, err := storeInstance.CountAll("tenant-a:")`
func (storeInstance *Store) CountAll(groupPrefix string) (int, core.Result) {
	if result := storeInstance.ensureReady("store.CountAll"); !result.OK {
		return 0, result
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
		return 0, core.Fail(core.E("store.CountAll", "count rows", err))
	}
	return count, core.Ok(nil)
}

// Usage example: `tenantGroupNames, err := storeInstance.Groups("tenant-a:")`
// Usage example: `allGroupNames, err := storeInstance.Groups()`
func (storeInstance *Store) Groups(groupPrefix ...string) ([]string, core.Result) {
	if result := storeInstance.ensureReady("store.Groups"); !result.OK {
		return nil, result
	}

	var groupNames []string
	for groupName, err := range storeInstance.GroupsSeq(groupPrefix...) {
		if err != nil {
			return nil, core.Fail(err)
		}
		groupNames = append(groupNames, groupName)
	}
	return groupNames, core.Ok(nil)
}

// Usage example: `for tenantGroupName, err := range storeInstance.GroupsSeq("tenant-a:") { if err != nil { break }; fmt.Println(tenantGroupName) }`
// Usage example: `for groupName, err := range storeInstance.GroupsSeq() { if err != nil { break }; fmt.Println(groupName) }`
func (storeInstance *Store) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	actualGroupPrefix := firstStringOrEmpty(groupPrefix)
	return func(yield func(string, error) bool) {
		if result := storeInstance.ensureReady(opGroupsSeq); !result.OK {
			err, _ := result.Value.(error)
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
func (storeInstance *Store) PurgeExpired() (int64, core.Result) {
	if result := storeInstance.ensureReady("store.PurgeExpired"); !result.OK {
		return 0, result
	}

	cutoffUnixMilli := time.Now().UnixMilli()
	expiredEntries, result := deleteExpiredEntriesMatchingGroupPrefix(storeInstance.sqliteDatabase, "", cutoffUnixMilli)
	if !result.OK {
		err, _ := result.Value.(error)
		return 0, core.Fail(core.E("store.PurgeExpired", "delete expired rows", err))
	}
	storeInstance.notifyExpiredEntries(expiredEntries)
	return int64(len(expiredEntries)), core.Ok(nil)
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
				if _, result := storeInstance.PurgeExpired(); !result.OK {
					// For example, a logger could record the failure here. The loop
					// keeps running so the next tick can retry.
					core.Error("background purge failed", "err", result.Error())
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

func deleteExpiredEntriesMatchingGroupPrefix(database schemaDatabase, groupPrefix string, cutoffUnixMilli int64) ([]expiredEntryRef, core.Result) {
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
		return nil, core.Fail(err)
	}
	defer func() { _ = rows.Close() }()

	expiredEntries := make([]expiredEntryRef, 0)
	for rows.Next() {
		var expiredEntry expiredEntryRef
		if err := rows.Scan(&expiredEntry.group, &expiredEntry.key); err != nil {
			return nil, core.Fail(err)
		}
		expiredEntries = append(expiredEntries, expiredEntry)
	}
	if err := rows.Err(); err != nil {
		return nil, core.Fail(err)
	}
	return expiredEntries, core.Ok(nil)
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
func ensureSchema(database *sql.DB) core.Result {
	entriesTableExists, result := tableExists(database, entriesTableName)
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opEnsureSchema, "schema", err))
	}

	legacyEntriesTableExists, result := tableExists(database, legacyKeyValueTableName)
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opEnsureSchema, "schema", err))
	}

	if entriesTableExists {
		if result := ensureExpiryColumn(database); !result.OK {
			err, _ := result.Value.(error)
			return core.Fail(core.E(opEnsureSchema, "migration", err))
		}
		if legacyEntriesTableExists {
			if result := migrateLegacyEntriesTable(database); !result.OK {
				err, _ := result.Value.(error)
				return core.Fail(core.E(opEnsureSchema, "migration", err))
			}
		}
		return core.Ok(nil)
	}

	if legacyEntriesTableExists {
		if result := migrateLegacyEntriesTable(database); !result.OK {
			err, _ := result.Value.(error)
			return core.Fail(core.E(opEnsureSchema, "migration", err))
		}
		return core.Ok(nil)
	}

	if _, err := database.Exec(createEntriesTableSQL); err != nil {
		return core.Fail(core.E(opEnsureSchema, "schema", err))
	}
	return core.Ok(nil)
}

// ensureExpiryColumn adds the expiry column to the current entries table when
// it was created before TTL support.
func ensureExpiryColumn(database schemaDatabase) core.Result {
	hasExpiryColumn, result := tableHasColumn(database, entriesTableName, "expires_at")
	if !result.OK {
		return result
	}
	if hasExpiryColumn {
		return core.Ok(nil)
	}
	if _, err := database.Exec("ALTER TABLE " + entriesTableName + " ADD COLUMN expires_at INTEGER"); err != nil {
		if !core.Contains(err.Error(), "duplicate column name") {
			return core.Fail(err)
		}
	}
	return core.Ok(nil)
}

// migrateLegacyEntriesTable copies rows from the old key-value table into the
// descriptive entries schema and then removes the legacy table.
func migrateLegacyEntriesTable(database *sql.DB) core.Result {
	transaction, err := database.Begin()
	if err != nil {
		return core.Fail(err)
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := transaction.Rollback(); rollbackErr != nil {
				core.Error("legacy entries migration rollback failed", "err", rollbackErr)
			}
		}
	}()

	entriesTableExists, result := tableExists(transaction, entriesTableName)
	if !result.OK {
		return result
	}
	if !entriesTableExists {
		if _, err := transaction.Exec(createEntriesTableSQL); err != nil {
			return core.Fail(err)
		}
	}

	legacyHasExpiryColumn, result := tableHasColumn(transaction, legacyKeyValueTableName, "expires_at")
	if !result.OK {
		return result
	}

	insertSQL := "INSERT OR IGNORE INTO " + entriesTableName + " (" + entryGroupColumn + ", " + entryKeyColumn + ", " + entryValueColumn + ", expires_at) SELECT grp, key, value, NULL FROM " + legacyKeyValueTableName
	if legacyHasExpiryColumn {
		insertSQL = "INSERT OR IGNORE INTO " + entriesTableName + " (" + entryGroupColumn + ", " + entryKeyColumn + ", " + entryValueColumn + ", expires_at) SELECT grp, key, value, expires_at FROM " + legacyKeyValueTableName
	}
	if _, err := transaction.Exec(insertSQL); err != nil {
		return core.Fail(err)
	}
	if _, err := transaction.Exec("DROP TABLE " + legacyKeyValueTableName); err != nil {
		return core.Fail(err)
	}
	if err := transaction.Commit(); err != nil {
		return core.Fail(err)
	}
	committed = true
	return core.Ok(nil)
}

func tableExists(database schemaDatabase, tableName string) (bool, core.Result) {
	var existingTableName string
	err := database.QueryRow(
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
		tableName,
	).Scan(&existingTableName)
	if err == sql.ErrNoRows {
		return false, core.Ok(nil)
	}
	if err != nil {
		return false, core.Fail(err)
	}
	return true, core.Ok(nil)
}

func tableHasColumn(database schemaDatabase, tableName, columnName string) (bool, core.Result) {
	rows, err := database.Query("PRAGMA table_info(" + tableName + ")")
	if err != nil {
		return false, core.Fail(err)
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
			return false, core.Fail(err)
		}
		if name == columnName {
			return true, core.Ok(nil)
		}
	}
	if err := rows.Err(); err != nil {
		return false, core.Fail(err)
	}
	return false, core.Ok(nil)
}
