package store

import (
	"database/sql"
	"io/fs"
	"maps"
	"slices"
	"sync" // Note: AX-6 — internal concurrency primitive; structural for store infrastructure (RFC §4 explicitly mandates).
	"time"

	core "dappco.re/go"
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
func (workspace *Workspace) Close() core.Result {
	return workspace.closeWithoutRemovingFiles()
}

func (workspace *Workspace) ensureReady(operation string) core.Result {
	if workspace == nil {
		return core.Fail(core.E(operation, "workspace is nil", nil))
	}
	if workspace.store == nil {
		return core.Fail(core.E(operation, "workspace store is nil", nil))
	}
	if workspace.db == nil {
		return core.Fail(core.E(operation, "workspace database is nil", nil))
	}
	if workspace.filesystem == nil {
		return core.Fail(core.E(operation, "workspace filesystem is nil", nil))
	}
	if result := workspace.store.ensureReady(operation); !result.OK {
		return result
	}

	workspace.lifecycleLock.Lock()
	closed := workspace.isClosed
	workspace.lifecycleLock.Unlock()
	if closed {
		return core.Fail(core.E(operation, "workspace is closed", nil))
	}

	return core.Ok(nil)
}

// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session-2026-03-30"); if err != nil { return }; defer workspace.Discard()`
// This creates `.core/state/scroll-session-2026-03-30.duckdb` by default and
// removes it when the workspace is committed or discarded.
func (storeInstance *Store) NewWorkspace(name string) (*Workspace, core.Result) {
	if result := storeInstance.ensureReady(opNewWorkspace); !result.OK {
		return nil, result
	}

	workspaceNameValidation := core.ValidateName(name)
	if !workspaceNameValidation.OK {
		return nil, core.Fail(core.E(opNewWorkspace, "validate workspace name", workspaceNameValidation.Value.(error)))
	}

	filesystem := (&core.Fs{}).NewUnrestricted()
	stateDirectory := storeInstance.workspaceStateDirectoryPath()
	databasePath := workspaceFilePath(stateDirectory, name)
	if filesystem.Exists(databasePath) {
		return nil, core.Fail(core.E(opNewWorkspace, core.Concat("workspace already exists: ", name), nil))
	}
	if result := filesystem.EnsureDir(stateDirectory); !result.OK {
		return nil, core.Fail(core.E(opNewWorkspace, "ensure state directory", result.Value.(error)))
	}

	database, result := openWorkspaceDatabase(databasePath)
	if !result.OK {
		err, _ := result.Value.(error)
		return nil, core.Fail(core.E(opNewWorkspace, "open workspace database", err))
	}

	return &Workspace{
		name:         name,
		store:        storeInstance,
		db:           database,
		databasePath: databasePath,
		filesystem:   filesystem,
	}, core.Ok(nil)
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
		if dirEntry.IsDir() || !core.HasSuffix(dirEntry.Name(), duckDBExtension) {
			continue
		}
		orphanPaths = append(orphanPaths, workspaceFilePath(stateDirectory, core.TrimSuffix(dirEntry.Name(), duckDBExtension)))
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
		workspaceName := workspaceNameFromPath(stateDirectory, databasePath)
		if workspaceCommitMarkerExists(store, workspaceName) {
			removeWorkspaceDatabaseFiles(filesystem, databasePath)
			continue
		}
		database, result := openWorkspaceDatabase(databasePath)
		if !result.OK {
			quarantineOrphanWorkspaceFiles(filesystem, stateDirectory, databasePath)
			continue
		}
		orphanWorkspace := &Workspace{
			name:         workspaceName,
			store:        store,
			db:           database,
			databasePath: databasePath,
			filesystem:   filesystem,
		}
		aggregate, result := orphanWorkspace.aggregateFieldsWithoutReadiness()
		if !result.OK {
			if closeResult := orphanWorkspace.closeWithoutRemovingFiles(); !closeResult.OK {
				core.Error("orphan workspace close failed", "err", closeResult.Error())
			}
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
	return core.TrimSuffix(relativePath, duckDBExtension)
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
func (workspace *Workspace) Put(kind string, data map[string]any) core.Result {
	if result := workspace.ensureReady(opWorkspacePut); !result.OK {
		return result
	}

	if kind == "" {
		return core.Fail(core.E(opWorkspacePut, "kind is empty", nil))
	}
	if data == nil {
		data = map[string]any{}
	}

	dataJSON, result := marshalJSONText(data, opWorkspacePut, "marshal entry data")
	if !result.OK {
		return result
	}

	_, execErr := workspace.db.Exec(
		sqlInsertIntoPrefix+workspaceEntriesTableName+workspaceEntryInsertValuesSQL,
		kind,
		dataJSON,
		time.Now().UnixMilli(),
	)
	if execErr != nil {
		return core.Fail(core.E(opWorkspacePut, "insert entry", execErr))
	}
	return core.Ok(nil)
}

// Usage example: `entryCount, err := workspace.Count(); if err != nil { return }; fmt.Println(entryCount)`
func (workspace *Workspace) Count() (int, core.Result) {
	if result := workspace.ensureReady("store.Workspace.Count"); !result.OK {
		return 0, result
	}

	var count int
	err := workspace.db.QueryRow(
		sqlSelectCountFrom + workspaceEntriesTableName,
	).Scan(&count)
	if err != nil {
		return 0, core.Fail(core.E("store.Workspace.Count", "count entries", err))
	}
	return count, core.Ok(nil)
}

// Usage example: `summary := workspace.Aggregate(); fmt.Println(summary["like"])`
func (workspace *Workspace) Aggregate() map[string]any {
	if workspace.shouldUseOrphanAggregate() {
		return workspace.aggregateFallback()
	}
	if result := workspace.ensureReady("store.Workspace.Aggregate"); !result.OK {
		return workspace.aggregateFallback()
	}

	fields, result := workspace.aggregateFields()
	if !result.OK {
		return workspace.aggregateFallback()
	}
	return fields
}

// Usage example: `result := workspace.Commit(); if !result.OK { return }; fmt.Println(result.Value)`
// `Commit()` writes one completed workspace row to the journal, upserts the
// `workspace:NAME/summary` entry, and removes the workspace file.
func (workspace *Workspace) Commit() core.Result {
	if result := workspace.ensureReady(opWorkspaceCommit); !result.OK {
		return result
	}

	fields, result := workspace.aggregateFields()
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opWorkspaceCommit, "aggregate workspace", err))
	}
	if result := workspace.store.commitWorkspaceAggregate(workspace.name, fields); !result.OK {
		return result
	}
	if result := workspace.closeAndRemoveFiles(); !result.OK {
		return core.Ok(cloneAnyMap(fields))
	}
	return core.Ok(cloneAnyMap(fields))
}

// Usage example: `workspace.Discard()`
func (workspace *Workspace) Discard() {
	if workspace == nil {
		return
	}
	if result := workspace.closeAndRemoveFiles(); !result.OK {
		core.Error("workspace discard failed", "err", result.Error())
	}
}

// Usage example: `result := workspace.Query("SELECT entry_kind, COUNT(*) AS count FROM workspace_entries GROUP BY entry_kind")`
// `result.Value` contains `[]map[string]any`, which lets an agent inspect the
// current buffer state without defining extra result types.
func (workspace *Workspace) Query(query string) core.Result {
	if result := workspace.ensureReady(opWorkspaceQuery); !result.OK {
		return result
	}

	rows, err := workspace.db.Query(query)
	if err != nil {
		return core.Fail(core.E(opWorkspaceQuery, "query workspace", err))
	}
	defer func() { _ = rows.Close() }()

	rowMaps, result := queryRowsAsMaps(rows)
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opWorkspaceQuery, "scan rows", err))
	}
	return core.Ok(rowMaps)
}

func (workspace *Workspace) aggregateFields() (map[string]any, core.Result) {
	if result := workspace.ensureReady("store.Workspace.aggregateFields"); !result.OK {
		return nil, result
	}
	return workspace.aggregateFieldsWithoutReadiness()
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

func (workspace *Workspace) aggregateFieldsWithoutReadiness() (map[string]any, core.Result) {
	rows, err := workspace.db.Query(
		"SELECT entry_kind, COUNT(*) FROM " + workspaceEntriesTableName + " GROUP BY entry_kind ORDER BY entry_kind",
	)
	if err != nil {
		return nil, core.Fail(err)
	}
	defer func() { _ = rows.Close() }()

	fields := make(map[string]any)
	for rows.Next() {
		var (
			kind  string
			count int
		)
		if err := rows.Scan(&kind, &count); err != nil {
			return nil, core.Fail(err)
		}
		fields[kind] = count
	}
	if err := rows.Err(); err != nil {
		return nil, core.Fail(err)
	}
	return fields, core.Ok(nil)
}

func (workspace *Workspace) closeAndRemoveFiles() core.Result {
	return workspace.closeAndCleanup(true)
}

// closeWithoutRemovingFiles closes the database handle but leaves the orphan
// file on disk so a later store instance can recover it.
func (workspace *Workspace) closeWithoutRemovingFiles() core.Result {
	return workspace.closeAndCleanup(false)
}

func (workspace *Workspace) closeAndCleanup(removeFiles bool) core.Result {
	if workspace == nil {
		return core.Ok(nil)
	}
	if workspace.db == nil {
		return core.Ok(nil)
	}

	workspace.lifecycleLock.Lock()
	alreadyClosed := workspace.isClosed
	if !alreadyClosed {
		workspace.isClosed = true
	}
	workspace.lifecycleLock.Unlock()

	if !alreadyClosed {
		if err := workspace.db.Close(); err != nil {
			return core.Fail(core.E("store.Workspace.closeAndCleanup", "close workspace database", err))
		}
	}
	if !removeFiles || workspace.filesystem == nil {
		return core.Ok(nil)
	}
	for _, path := range workspaceDatabaseFilePaths(workspace.databasePath) {
		if result := workspace.filesystem.Delete(path); !result.OK && workspace.filesystem.Exists(path) {
			return core.Fail(core.E("store.Workspace.closeAndCleanup", "delete workspace file", result.Value.(error)))
		}
	}
	return core.Ok(nil)
}

func (storeInstance *Store) commitWorkspaceAggregate(workspaceName string, fields map[string]any) core.Result {
	if result := storeInstance.ensureReady(opWorkspaceCommit); !result.OK {
		return result
	}
	if result := ensureJournalSchema(storeInstance.sqliteDatabase); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opWorkspaceCommit, "ensure journal schema", err))
	}

	transaction, err := storeInstance.sqliteDatabase.Begin()
	if err != nil {
		return core.Fail(core.E(opWorkspaceCommit, "begin transaction", err))
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := transaction.Rollback(); rollbackErr != nil {
				core.Error("workspace commit rollback failed", "err", rollbackErr)
			}
		}
	}()

	fieldsJSON, result := marshalJSONText(fields, opWorkspaceCommit, "marshal summary")
	if !result.OK {
		return result
	}
	tagsJSON, result := marshalJSONText(map[string]string{"workspace": workspaceName}, opWorkspaceCommit, "marshal tags")
	if !result.OK {
		return result
	}

	if result := commitJournalEntry(
		transaction,
		storeInstance.journalBucket(),
		workspaceName,
		fieldsJSON,
		tagsJSON,
		time.Now().UnixMilli(),
	); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opWorkspaceCommit, "insert journal entry", err))
	}

	if _, err := transaction.Exec(
		sqlInsertIntoPrefix+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, NULL) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = NULL",
		workspaceSummaryGroup(workspaceName),
		"summary",
		fieldsJSON,
	); err != nil {
		return core.Fail(core.E(opWorkspaceCommit, "upsert workspace summary", err))
	}

	if err := transaction.Commit(); err != nil {
		return core.Fail(core.E(opWorkspaceCommit, "commit transaction", err))
	}
	committed = true
	storeInstance.notify(Event{
		Type:      EventSet,
		Group:     workspaceSummaryGroup(workspaceName),
		Key:       "summary",
		Value:     fieldsJSON,
		Timestamp: time.Now(),
	})
	return core.Ok(nil)
}

func openWorkspaceDatabase(databasePath string) (*sql.DB, core.Result) {
	database, err := sql.Open("duckdb", databasePath)
	if err != nil {
		return nil, core.Fail(core.E(opOpenWorkspaceDatabase, "open workspace database", err))
	}
	database.SetMaxOpenConns(1)
	if err := database.Ping(); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			core.Error("workspace database close after ping failed", "err", closeErr)
		}
		return nil, core.Fail(core.E(opOpenWorkspaceDatabase, "ping workspace database", err))
	}
	if _, err := database.Exec("CREATE SEQUENCE IF NOT EXISTS workspace_entries_entry_id_seq START 1"); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			core.Error("workspace database close after sequence failed", "err", closeErr)
		}
		return nil, core.Fail(core.E(opOpenWorkspaceDatabase, "create workspace entry sequence", err))
	}
	if _, err := database.Exec(createWorkspaceEntriesTableSQL); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			core.Error("workspace database close after table failed", "err", closeErr)
		}
		return nil, core.Fail(core.E(opOpenWorkspaceDatabase, "create workspace entries table", err))
	}
	if _, err := database.Exec(createWorkspaceEntriesViewSQL); err != nil {
		if closeErr := database.Close(); closeErr != nil {
			core.Error("workspace database close after view failed", "err", closeErr)
		}
		return nil, core.Fail(core.E(opOpenWorkspaceDatabase, "create workspace entries view", err))
	}
	return database, core.Ok(nil)
}

func workspaceSummaryGroup(workspaceName string) string {
	return core.Concat(workspaceSummaryGroupPrefix, ":", workspaceName)
}

func workspaceFilePath(stateDirectory, name string) string {
	return joinPath(stateDirectory, core.Concat(name, duckDBExtension))
}

func workspaceQuarantineFilePath(stateDirectory, databasePath string) string {
	return joinPath(
		joinPath(stateDirectory, workspaceQuarantineDirName),
		core.Concat(workspaceNameFromPath(stateDirectory, databasePath), duckDBExtension),
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
	sourcePaths := workspaceDatabaseFilePaths(databasePath)
	quarantinePaths := workspaceDatabaseFilePaths(quarantinePath)
	for index, sourcePath := range sourcePaths {
		quarantineWorkspaceFile(filesystem, sourcePath, quarantinePaths[index])
	}
}

func availableQuarantineWorkspacePath(filesystem *core.Fs, preferredPath string) string {
	if !workspaceQuarantinePathExists(filesystem, preferredPath) {
		return preferredPath
	}
	stem := core.TrimSuffix(preferredPath, duckDBExtension)
	for index := 1; ; index++ {
		candidatePath := core.Concat(stem, ".", core.Sprint(index), duckDBExtension)
		if !workspaceQuarantinePathExists(filesystem, candidatePath) {
			return candidatePath
		}
	}
}

func workspaceQuarantinePathExists(filesystem *core.Fs, databasePath string) bool {
	for _, path := range workspaceDatabaseFilePaths(databasePath) {
		if filesystem.Exists(path) {
			return true
		}
	}
	return false
}

func workspaceCommitMarkerExists(storeInstance *Store, workspaceName string) bool {
	if storeInstance == nil || workspaceName == "" {
		return false
	}
	exists, result := storeInstance.Exists(workspaceSummaryGroup(workspaceName), "summary")
	return result.OK && exists
}

func removeWorkspaceDatabaseFiles(filesystem *core.Fs, databasePath string) {
	if filesystem == nil || databasePath == "" {
		return
	}
	for _, path := range workspaceDatabaseFilePaths(databasePath) {
		if result := filesystem.Delete(path); !result.OK {
			core.Error("workspace database cleanup failed", "file", path, "err", result.Error())
		}
	}
}

func workspaceDatabaseFilePaths(databasePath string) []string {
	if core.HasSuffix(databasePath, duckDBExtension) {
		return []string{databasePath, databasePath + ".wal"}
	}
	return []string{databasePath, databasePath + "-wal", databasePath + "-shm"}
}

func quarantineWorkspaceFile(filesystem *core.Fs, sourcePath, quarantinePath string) {
	if filesystem == nil || !filesystem.Exists(sourcePath) {
		return
	}
	if result := filesystem.Rename(sourcePath, quarantinePath); !result.OK {
		core.Error("workspace quarantine rename failed", "err", result.Error())
	}
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
