package store

import (
	"database/sql"
	"io/fs"
	"maps"
	"slices"
	"sync"
	"time"

	core "dappco.re/go/core"
)

const (
	workspaceEntriesTableName   = "workspace_entries"
	workspaceSummaryGroupPrefix = "workspace"
)

const createWorkspaceEntriesTableSQL = `CREATE TABLE IF NOT EXISTS workspace_entries (
	entry_id    INTEGER PRIMARY KEY AUTOINCREMENT,
	entry_kind  TEXT NOT NULL,
	entry_data  TEXT NOT NULL,
	created_at  INTEGER NOT NULL
)`

const createWorkspaceEntriesViewSQL = `CREATE VIEW IF NOT EXISTS entries AS
SELECT
	entry_id AS id,
	entry_kind AS kind,
	entry_data AS data,
	created_at
FROM workspace_entries`

var defaultWorkspaceStateDirectory = ".core/state/"

// Workspace keeps mutable work-in-progress in a SQLite file such as
// `.core/state/scroll-session.duckdb` until Commit() or Discard() removes it.
//
// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session"); if err != nil { return }; defer workspace.Discard()`
//
// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session-2026-03-30"); if err != nil { return }; defer workspace.Discard(); _ = workspace.Put("like", map[string]any{"user": "@alice"})`
type Workspace struct {
	name            string
	parentStore     *Store
	sqliteDatabase  *sql.DB
	databasePath    string
	filesystem      *core.Fs
	orphanAggregate map[string]any

	closeLock sync.Mutex
	closed    bool
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

// Close keeps the workspace file on disk so `RecoverOrphans(".core/state/")`
// can reopen it later.
//
// Usage example: `if err := workspace.Close(); err != nil { return }`
// Usage example: `if err := workspace.Close(); err != nil { return }; orphans := storeInstance.RecoverOrphans(".core/state"); _ = orphans`
func (workspace *Workspace) Close() error {
	return workspace.closeWithoutRemovingFiles()
}

func (workspace *Workspace) ensureReady(operation string) error {
	if workspace == nil {
		return core.E(operation, "workspace is nil", nil)
	}
	if workspace.parentStore == nil {
		return core.E(operation, "workspace store is nil", nil)
	}
	if workspace.sqliteDatabase == nil {
		return core.E(operation, "workspace database is nil", nil)
	}
	if workspace.filesystem == nil {
		return core.E(operation, "workspace filesystem is nil", nil)
	}
	if err := workspace.parentStore.ensureReady(operation); err != nil {
		return err
	}

	workspace.closeLock.Lock()
	closed := workspace.closed
	workspace.closeLock.Unlock()
	if closed {
		return core.E(operation, "workspace is closed", nil)
	}

	return nil
}

// NewWorkspace opens a SQLite workspace file such as
// `.core/state/scroll-session-2026-03-30.duckdb` and removes it when the
// workspace is committed or discarded.
//
// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session-2026-03-30"); if err != nil { return }; defer workspace.Discard()`
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
		parentStore:    storeInstance,
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

func discoverOrphanWorkspaces(stateDirectory string, parentStore *Store) []*Workspace {
	return loadRecoveredWorkspaces(stateDirectory, parentStore)
}

func loadRecoveredWorkspaces(stateDirectory string, parentStore *Store) []*Workspace {
	filesystem := (&core.Fs{}).NewUnrestricted()
	orphanWorkspaces := make([]*Workspace, 0)
	for _, databasePath := range discoverOrphanWorkspacePaths(stateDirectory) {
		sqliteDatabase, err := openWorkspaceDatabase(databasePath)
		if err != nil {
			continue
		}
		orphanWorkspace := &Workspace{
			name:           workspaceNameFromPath(stateDirectory, databasePath),
			parentStore:    parentStore,
			sqliteDatabase: sqliteDatabase,
			databasePath:   databasePath,
			filesystem:     filesystem,
		}
		orphanWorkspace.orphanAggregate = orphanWorkspace.captureAggregateSnapshot()
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

// RecoverOrphans(".core/state") returns orphaned workspaces such as
// `scroll-session-2026-03-30.duckdb` so callers can inspect Aggregate() and
// choose Commit() or Discard().
//
// Usage example: `orphans := storeInstance.RecoverOrphans(".core/state"); for _, orphanWorkspace := range orphans { fmt.Println(orphanWorkspace.Name(), orphanWorkspace.Aggregate()) }`
func (storeInstance *Store) RecoverOrphans(stateDirectory string) []*Workspace {
	if storeInstance == nil {
		return nil
	}

	if stateDirectory == "" {
		stateDirectory = storeInstance.workspaceStateDirectoryPath()
	}
	stateDirectory = normaliseWorkspaceStateDirectory(stateDirectory)

	if stateDirectory == storeInstance.workspaceStateDirectoryPath() {
		storeInstance.orphanWorkspacesLock.Lock()
		cachedWorkspaces := slices.Clone(storeInstance.orphanWorkspaces)
		storeInstance.orphanWorkspaces = nil
		storeInstance.orphanWorkspacesLock.Unlock()
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

// Commit writes one completed workspace row to the journal and upserts the
// summary entry in `workspace:NAME`.
//
// Usage example: `result := workspace.Commit(); if !result.OK { return }; fmt.Println(result.Value)`
func (workspace *Workspace) Commit() core.Result {
	if err := workspace.ensureReady("store.Workspace.Commit"); err != nil {
		return core.Result{Value: err, OK: false}
	}

	fields, err := workspace.aggregateFields()
	if err != nil {
		return core.Result{Value: core.E("store.Workspace.Commit", "aggregate workspace", err), OK: false}
	}
	if err := workspace.parentStore.commitWorkspaceAggregate(workspace.name, fields); err != nil {
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

// Query runs SQL against the workspace buffer and returns rows as
// `[]map[string]any` for ad-hoc inspection.
//
// Usage example: `result := workspace.Query("SELECT entry_kind, COUNT(*) AS count FROM workspace_entries GROUP BY entry_kind")`
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
	if workspace == nil || workspace.orphanAggregate == nil {
		return map[string]any{}
	}
	return maps.Clone(workspace.orphanAggregate)
}

func (workspace *Workspace) shouldUseOrphanAggregate() bool {
	if workspace == nil || workspace.orphanAggregate == nil {
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
	if workspace.sqliteDatabase == nil || workspace.filesystem == nil {
		return nil
	}

	workspace.closeLock.Lock()
	alreadyClosed := workspace.closed
	if !alreadyClosed {
		workspace.closed = true
	}
	workspace.closeLock.Unlock()

	if !alreadyClosed {
		if err := workspace.sqliteDatabase.Close(); err != nil {
			return core.E("store.Workspace.closeAndCleanup", "close workspace database", err)
		}
	}
	if !removeFiles {
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
	sqliteDatabase, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, core.E("store.openWorkspaceDatabase", "open workspace database", err)
	}
	sqliteDatabase.SetMaxOpenConns(1)
	if _, err := sqliteDatabase.Exec("PRAGMA journal_mode=WAL"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.openWorkspaceDatabase", "set WAL journal mode", err)
	}
	if _, err := sqliteDatabase.Exec("PRAGMA busy_timeout=5000"); err != nil {
		sqliteDatabase.Close()
		return nil, core.E("store.openWorkspaceDatabase", "set busy timeout", err)
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
