package store

import (
	"database/sql"
	"io/fs"
	"maps"
	"slices"
	"sync" // Note: AX-6 — internal concurrency primitive; structural for store infrastructure (RFC §4 explicitly mandates).
	"time"

	core "dappco.re/go/core"
)

const (
	workspaceEntriesTableName   = "workspace_entries"
	workspaceSummaryGroupPrefix = "workspace"
	workspaceQuarantineDirName  = "quarantine"
)

const createWorkspaceEntriesTableSQL = `CREATE TABLE IF NOT EXISTS workspace_entries (
	entry_id    BIGINT PRIMARY KEY DEFAULT nextval('workspace_entries_entry_id_seq'),
	entry_kind  TEXT NOT NULL,
	entry_data  TEXT NOT NULL,
	created_at  BIGINT NOT NULL
)`

const createWorkspaceEntriesViewSQL = `CREATE VIEW IF NOT EXISTS entries AS
SELECT
	entry_id AS id,
	entry_kind AS kind,
	entry_data AS data,
	created_at
FROM workspace_entries`

var defaultWorkspaceStateDirectory = ".core/state/"

// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session"); if err != nil { return }; defer workspace.Discard()`
// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session-2026-03-30"); if err != nil { return }; defer workspace.Discard(); _ = workspace.Put("like", map[string]any{"user": "@alice"})`
// Each workspace keeps mutable work-in-progress in a DuckDB file such as
// `.core/state/scroll-session.duckdb` until `Commit()` or `Discard()` removes
// it.
type Workspace struct {
	name                  string
	store                 *Store
	db                    *sql.DB
	sqliteDatabase        *sql.DB
	databasePath          string
	filesystem            *core.Fs
	cachedOrphanAggregate map[string]any

	lifecycleLock sync.Mutex
	isClosed      bool
}

// Usage example: `workspaceName := workspace.Name(); fmt.Println(workspaceName)`
func (workspace *Workspace) Name() string {
	if workspace == nil {
		return ""
	}
	return workspace.name
}

// Usage example: `workspacePath := workspace.DatabasePath(); fmt.Println(workspacePath)`
func (workspace *Workspace) DatabasePath() string {
	if workspace == nil {
		return ""
	}
	return workspace.databasePath
}

// Usage example: `if err := workspace.Close(); err != nil { return }`
// Usage example: `if err := workspace.Close(); err != nil { return }; orphans := storeInstance.RecoverOrphans(".core/state"); _ = orphans`
// `Close()` keeps the `.duckdb` file on disk so `RecoverOrphans(".core/state")`
// can reopen it after a crash or interrupted agent run.
func (workspace *Workspace) Close() error {
	return workspace.closeWithoutRemovingFiles()
}

func (workspace *Workspace) ensureReady(operation string) error {
	if workspace == nil {
		return core.E(operation, "workspace is nil", nil)
	}
	if workspace.store == nil {
		return core.E(operation, "workspace store is nil", nil)
	}
	if workspace.db == nil {
		workspace.db = workspace.sqliteDatabase
	}
	if workspace.sqliteDatabase == nil {
		workspace.sqliteDatabase = workspace.db
	}
	if workspace.db == nil {
		return core.E(operation, "workspace database is nil", nil)
	}
	if workspace.sqliteDatabase == nil {
		return core.E(operation, "workspace database is nil", nil)
	}
	if workspace.filesystem == nil {
		return core.E(operation, "workspace filesystem is nil", nil)
	}
	if err := workspace.store.ensureReady(operation); err != nil {
		return err
	}

	workspace.lifecycleLock.Lock()
	closed := workspace.isClosed
	workspace.lifecycleLock.Unlock()
	if closed {
		return core.E(operation, "workspace is closed", nil)
	}

	return nil
}

// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session-2026-03-30"); if err != nil { return }; defer workspace.Discard()`
// This creates `.core/state/scroll-session-2026-03-30.duckdb` by default and
// removes it when the workspace is committed or discarded.
func (storeInstance *Store) NewWorkspace(name string) (*Workspace, error) {
	if err := storeInstance.ensureReady("store.NewWorkspace"); err != nil {
		return nil, err
	}

	workspaceNameValidation := core.ValidateName(name)
	if !workspaceNameValidation.OK {
		return nil, core.E("store.NewWorkspace", "validate workspace name", workspaceNameValidation.Value.(error))
	}

	filesystem := (&core.Fs{}).NewUnrestricted()
	stateDirectory := storeInstance.workspaceStateDirectoryPath()
	databasePath := workspaceFilePath(stateDirectory, name)
	if filesystem.Exists(databasePath) {
		return nil, core.E("store.NewWorkspace", core.Concat("workspace already exists: ", name), nil)
	}
	if result := filesystem.EnsureDir(stateDirectory); !result.OK {
		return nil, core.E("store.NewWorkspace", "ensure state directory", result.Value.(error))
	}

	sqliteDatabase, err := openWorkspaceDatabase(databasePath)
	if err != nil {
		return nil, core.E("store.NewWorkspace", "open workspace database", err)
	}

	return &Workspace{
		name:           name,
		store:          storeInstance,
		db:             sqliteDatabase,
		sqliteDatabase: sqliteDatabase,
		databasePath:   databasePath,
		filesystem:     filesystem,
	}, nil
}

// discoverOrphanWorkspacePaths(".core/state") returns leftover SQLite workspace
// files such as `scroll-session.duckdb` without opening them.
func discoverOrphanWorkspacePaths(stateDirectory string) []string {
	filesystem := (&core.Fs{}).NewUnrestricted()
	if stateDirectory == "" {
		stateDirectory = defaultWorkspaceStateDirectory
	}
	if !filesystem.Exists(stateDirectory) {
		return nil
	}

	listResult := filesystem.List(stateDirectory)
	if !listResult.OK {
		return nil
	}

	directoryEntries, ok := listResult.Value.([]fs.DirEntry)
	if !ok {
		return nil
	}

	slices.SortFunc(directoryEntries, func(left, right fs.DirEntry) int {
		switch {
		case left.Name() < right.Name():
			return -1
		case left.Name() > right.Name():
			return 1
		default:
			return 0
		}
	})

	orphanPaths := make([]string, 0, len(directoryEntries))
	for _, dirEntry := range directoryEntries {
		if dirEntry.IsDir() || !core.HasSuffix(dirEntry.Name(), ".duckdb") {
			continue
		}
		orphanPaths = append(orphanPaths, workspaceFilePath(stateDirectory, core.TrimSuffix(dirEntry.Name(), ".duckdb")))
	}
	return orphanPaths
}

func discoverOrphanWorkspaces(stateDirectory string, store *Store) []*Workspace {
	return loadRecoveredWorkspaces(stateDirectory, store)
}

func loadRecoveredWorkspaces(stateDirectory string, store *Store) []*Workspace {
	filesystem := (&core.Fs{}).NewUnrestricted()
	orphanWorkspaces := make([]*Workspace, 0)
	for _, databasePath := range discoverOrphanWorkspacePaths(stateDirectory) {
		sqliteDatabase, err := openWorkspaceDatabase(databasePath)
		if err != nil {
			quarantineOrphanWorkspaceFiles(filesystem, stateDirectory, databasePath)
			continue
		}
		orphanWorkspace := &Workspace{
			name:           workspaceNameFromPath(stateDirectory, databasePath),
			store:          store,
			db:             sqliteDatabase,
			sqliteDatabase: sqliteDatabase,
			databasePath:   databasePath,
			filesystem:     filesystem,
		}
		aggregate, err := orphanWorkspace.aggregateFieldsWithoutReadiness()
		if err != nil {
			_ = orphanWorkspace.closeWithoutRemovingFiles()
			quarantineOrphanWorkspaceFiles(filesystem, stateDirectory, databasePath)
			continue
		}
		orphanWorkspace.cachedOrphanAggregate = aggregate
		orphanWorkspaces = append(orphanWorkspaces, orphanWorkspace)
	}
	return orphanWorkspaces
}

func normaliseWorkspaceStateDirectory(stateDirectory string) string {
	return normaliseDirectoryPath(stateDirectory)
}

func workspaceNameFromPath(stateDirectory, databasePath string) string {
	relativePath := core.TrimPrefix(databasePath, joinPath(stateDirectory, ""))
	return core.TrimSuffix(relativePath, ".duckdb")
}

// Usage example: `orphans := storeInstance.RecoverOrphans(".core/state"); for _, orphanWorkspace := range orphans { fmt.Println(orphanWorkspace.Name(), orphanWorkspace.Aggregate()) }`
// This reopens leftover `.duckdb` files such as `scroll-session-2026-03-30`
// so callers can inspect `Aggregate()` and choose `Commit()` or `Discard()`.
// Unreadable orphan files are moved under `.core/state/quarantine/`.
func (storeInstance *Store) RecoverOrphans(stateDirectory string) []*Workspace {
	if storeInstance == nil {
		return nil
	}

	if stateDirectory == "" {
		stateDirectory = storeInstance.workspaceStateDirectoryPath()
	}
	stateDirectory = normaliseWorkspaceStateDirectory(stateDirectory)

	if stateDirectory == storeInstance.workspaceStateDirectoryPath() {
		storeInstance.orphanWorkspaceLock.Lock()
		cachedWorkspaces := slices.Clone(storeInstance.cachedOrphanWorkspaces)
		storeInstance.cachedOrphanWorkspaces = nil
		storeInstance.orphanWorkspaceLock.Unlock()
		if len(cachedWorkspaces) > 0 {
			return cachedWorkspaces
		}
	}
	return loadRecoveredWorkspaces(stateDirectory, storeInstance)
}

// Usage example: `err := workspace.Put("like", map[string]any{"user": "@alice", "post": "video_123"})`
func (workspace *Workspace) Put(kind string, data map[string]any) error {
	if err := workspace.ensureReady("store.Workspace.Put"); err != nil {
		return err
	}

	if kind == "" {
		return core.E("store.Workspace.Put", "kind is empty", nil)
	}
	if data == nil {
		data = map[string]any{}
	}

	dataJSON, err := marshalJSONText(data, "store.Workspace.Put", "marshal entry data")
	if err != nil {
		return err
	}

	_, err = workspace.sqliteDatabase.Exec(
		"INSERT INTO "+workspaceEntriesTableName+" (entry_kind, entry_data, created_at) VALUES (?, ?, ?)",
		kind,
		dataJSON,
		time.Now().UnixMilli(),
	)
	if err != nil {
		return core.E("store.Workspace.Put", "insert entry", err)
	}
	return nil
}

// Usage example: `entryCount, err := workspace.Count(); if err != nil { return }; fmt.Println(entryCount)`
func (workspace *Workspace) Count() (int, error) {
	if err := workspace.ensureReady("store.Workspace.Count"); err != nil {
		return 0, err
	}

	var count int
	err := workspace.sqliteDatabase.QueryRow(
		"SELECT COUNT(*) FROM " + workspaceEntriesTableName,
	).Scan(&count)
	if err != nil {
		return 0, core.E("store.Workspace.Count", "count entries", err)
	}
	return count, nil
}

// Usage example: `summary := workspace.Aggregate(); fmt.Println(summary["like"])`
func (workspace *Workspace) Aggregate() map[string]any {
	if workspace.shouldUseOrphanAggregate() {
		return workspace.aggregateFallback()
	}
	if err := workspace.ensureReady("store.Workspace.Aggregate"); err != nil {
		return workspace.aggregateFallback()
	}

	fields, err := workspace.aggregateFields()
	if err != nil {
		return workspace.aggregateFallback()
	}
	return fields
}

// Usage example: `result := workspace.Commit(); if !result.OK { return }; fmt.Println(result.Value)`
// `Commit()` writes one completed workspace row to the journal, upserts the
// `workspace:NAME/summary` entry, and removes the workspace file.
func (workspace *Workspace) Commit() core.Result {
	if err := workspace.ensureReady("store.Workspace.Commit"); err != nil {
		return core.Result{Value: err, OK: false}
	}

	fields, err := workspace.aggregateFields()
	if err != nil {
		return core.Result{Value: core.E("store.Workspace.Commit", "aggregate workspace", err), OK: false}
	}
	if err := workspace.store.commitWorkspaceAggregate(workspace.name, fields); err != nil {
		return core.Result{Value: err, OK: false}
	}
	if err := workspace.closeAndRemoveFiles(); err != nil {
		return core.Result{Value: err, OK: false}
	}
	return core.Result{Value: cloneAnyMap(fields), OK: true}
}

// Usage example: `workspace.Discard()`
func (workspace *Workspace) Discard() {
	if workspace == nil {
		return
	}
	_ = workspace.closeAndRemoveFiles()
}

// Usage example: `result := workspace.Query("SELECT entry_kind, COUNT(*) AS count FROM workspace_entries GROUP BY entry_kind")`
// `result.Value` contains `[]map[string]any`, which lets an agent inspect the
// current buffer state without defining extra result types.
func (workspace *Workspace) Query(query string) core.Result {
	if err := workspace.ensureReady("store.Workspace.Query"); err != nil {
		return core.Result{Value: err, OK: false}
	}

	rows, err := workspace.sqliteDatabase.Query(query)
	if err != nil {
		return core.Result{Value: core.E("store.Workspace.Query", "query workspace", err), OK: false}
	}
	defer rows.Close()

	rowMaps, err := queryRowsAsMaps(rows)
	if err != nil {
		return core.Result{Value: core.E("store.Workspace.Query", "scan rows", err), OK: false}
	}
	return core.Result{Value: rowMaps, OK: true}
}

func (workspace *Workspace) aggregateFields() (map[string]any, error) {
	if err := workspace.ensureReady("store.Workspace.aggregateFields"); err != nil {
		return nil, err
	}
	return workspace.aggregateFieldsWithoutReadiness()
}

func (workspace *Workspace) captureAggregateSnapshot() map[string]any {
	if workspace == nil || workspace.sqliteDatabase == nil {
		return nil
	}

	fields, err := workspace.aggregateFieldsWithoutReadiness()
	if err != nil {
		return nil
	}
	return fields
}

func (workspace *Workspace) aggregateFallback() map[string]any {
	if workspace == nil || workspace.cachedOrphanAggregate == nil {
		return map[string]any{}
	}
	return maps.Clone(workspace.cachedOrphanAggregate)
}

func (workspace *Workspace) shouldUseOrphanAggregate() bool {
	if workspace == nil || workspace.cachedOrphanAggregate == nil {
		return false
	}
	if workspace.filesystem == nil || workspace.databasePath == "" {
		return false
	}
	return !workspace.filesystem.Exists(workspace.databasePath)
}

func (workspace *Workspace) aggregateFieldsWithoutReadiness() (map[string]any, error) {
	rows, err := workspace.sqliteDatabase.Query(
		"SELECT entry_kind, COUNT(*) FROM " + workspaceEntriesTableName + " GROUP BY entry_kind ORDER BY entry_kind",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := make(map[string]any)
	for rows.Next() {
		var (
			kind  string
			count int
		)
		if err := rows.Scan(&kind, &count); err != nil {
			return nil, err
		}
		fields[kind] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return fields, nil
}

func (workspace *Workspace) closeAndRemoveFiles() error {
	return workspace.closeAndCleanup(true)
}

// closeWithoutRemovingFiles closes the database handle but leaves the orphan
// file on disk so a later store instance can recover it.
func (workspace *Workspace) closeWithoutRemovingFiles() error {
	return workspace.closeAndCleanup(false)
}

func (workspace *Workspace) closeAndCleanup(removeFiles bool) error {
	if workspace == nil {
		return nil
	}
	if workspace.sqliteDatabase == nil {
		return nil
	}

	workspace.lifecycleLock.Lock()
	alreadyClosed := workspace.isClosed
	if !alreadyClosed {
		workspace.isClosed = true
	}
	workspace.lifecycleLock.Unlock()

	if !alreadyClosed {
		if err := workspace.sqliteDatabase.Close(); err != nil {
			return core.E("store.Workspace.closeAndCleanup", "close workspace database", err)
		}
	}
	if !removeFiles || workspace.filesystem == nil {
		return nil
	}
	for _, path := range []string{workspace.databasePath, workspace.databasePath + "-wal", workspace.databasePath + "-shm"} {
		if result := workspace.filesystem.Delete(path); !result.OK && workspace.filesystem.Exists(path) {
			return core.E("store.Workspace.closeAndCleanup", "delete workspace file", result.Value.(error))
		}
	}
	return nil
}

func (storeInstance *Store) commitWorkspaceAggregate(workspaceName string, fields map[string]any) error {
	if err := storeInstance.ensureReady("store.Workspace.Commit"); err != nil {
		return err
	}
	if err := ensureJournalSchema(storeInstance.sqliteDatabase); err != nil {
		return core.E("store.Workspace.Commit", "ensure journal schema", err)
	}

	transaction, err := storeInstance.sqliteDatabase.Begin()
	if err != nil {
		return core.E("store.Workspace.Commit", "begin transaction", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()

	fieldsJSON, err := marshalJSONText(fields, "store.Workspace.Commit", "marshal summary")
	if err != nil {
		return err
	}
	tagsJSON, err := marshalJSONText(map[string]string{"workspace": workspaceName}, "store.Workspace.Commit", "marshal tags")
	if err != nil {
		return err
	}

	if err := commitJournalEntry(
		transaction,
		storeInstance.journalBucket(),
		workspaceName,
		fieldsJSON,
		tagsJSON,
		time.Now().UnixMilli(),
	); err != nil {
		return core.E("store.Workspace.Commit", "insert journal entry", err)
	}

	if _, err := transaction.Exec(
		"INSERT INTO "+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, NULL) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = NULL",
		workspaceSummaryGroup(workspaceName),
		"summary",
		fieldsJSON,
	); err != nil {
		return core.E("store.Workspace.Commit", "upsert workspace summary", err)
	}

	if err := transaction.Commit(); err != nil {
		return core.E("store.Workspace.Commit", "commit transaction", err)
	}
	committed = true
	storeInstance.notify(Event{
		Type:      EventSet,
		Group:     workspaceSummaryGroup(workspaceName),
		Key:       "summary",
		Value:     fieldsJSON,
		Timestamp: time.Now(),
	})
	return nil
}

func openWorkspaceDatabase(databasePath string) (*sql.DB, error) {
	sqliteDatabase, err := sql.Open("duckdb", databasePath)
	if err != nil {
		return nil, core.E("store.openWorkspaceDatabase", "open workspace database", err)
	}
	sqliteDatabase.SetMaxOpenConns(1)
	if err := sqliteDatabase.Ping(); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.openWorkspaceDatabase", "ping workspace database", err)
	}
	if _, err := sqliteDatabase.Exec("CREATE SEQUENCE IF NOT EXISTS workspace_entries_entry_id_seq START 1"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.openWorkspaceDatabase", "create workspace entry sequence", err)
	}
	if _, err := sqliteDatabase.Exec(createWorkspaceEntriesTableSQL); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.openWorkspaceDatabase", "create workspace entries table", err)
	}
	if _, err := sqliteDatabase.Exec(createWorkspaceEntriesViewSQL); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.openWorkspaceDatabase", "create workspace entries view", err)
	}
	return sqliteDatabase, nil
}

func workspaceSummaryGroup(workspaceName string) string {
	return core.Concat(workspaceSummaryGroupPrefix, ":", workspaceName)
}

func workspaceFilePath(stateDirectory, name string) string {
	return joinPath(stateDirectory, core.Concat(name, ".duckdb"))
}

func workspaceQuarantineFilePath(stateDirectory, databasePath string) string {
	return joinPath(
		joinPath(stateDirectory, workspaceQuarantineDirName),
		core.Concat(workspaceNameFromPath(stateDirectory, databasePath), ".duckdb"),
	)
}

func quarantineOrphanWorkspaceFiles(filesystem *core.Fs, stateDirectory, databasePath string) {
	if filesystem == nil || databasePath == "" {
		return
	}
	quarantineDirectory := joinPath(stateDirectory, workspaceQuarantineDirName)
	if result := filesystem.EnsureDir(quarantineDirectory); !result.OK {
		return
	}
	quarantinePath := availableQuarantineWorkspacePath(
		filesystem,
		workspaceQuarantineFilePath(stateDirectory, databasePath),
	)
	quarantineWorkspaceFile(filesystem, databasePath, quarantinePath)
	quarantineWorkspaceFile(filesystem, databasePath+"-wal", quarantinePath+"-wal")
	quarantineWorkspaceFile(filesystem, databasePath+"-shm", quarantinePath+"-shm")
}

func availableQuarantineWorkspacePath(filesystem *core.Fs, preferredPath string) string {
	if !workspaceQuarantinePathExists(filesystem, preferredPath) {
		return preferredPath
	}
	stem := core.TrimSuffix(preferredPath, ".duckdb")
	for index := 1; ; index++ {
		candidatePath := core.Concat(stem, ".", core.Itoa(index), ".duckdb")
		if !workspaceQuarantinePathExists(filesystem, candidatePath) {
			return candidatePath
		}
	}
}

func workspaceQuarantinePathExists(filesystem *core.Fs, databasePath string) bool {
	return filesystem.Exists(databasePath) || filesystem.Exists(databasePath+"-wal") || filesystem.Exists(databasePath+"-shm")
}

func quarantineWorkspaceFile(filesystem *core.Fs, sourcePath, quarantinePath string) {
	if filesystem == nil || !filesystem.Exists(sourcePath) {
		return
	}
	_ = filesystem.Rename(sourcePath, quarantinePath)
}

func joinPath(base, name string) string {
	if base == "" {
		return name
	}
	return core.Concat(normaliseDirectoryPath(base), "/", name)
}

func normaliseDirectoryPath(directory string) string {
	for directory != "" && core.HasSuffix(directory, "/") {
		directory = core.TrimSuffix(directory, "/")
	}
	return directory
}
