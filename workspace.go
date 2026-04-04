package store

import (
	"database/sql"
	"io/fs"
	"slices"
	"sync"
	"time"

	core "dappco.re/go/core"
)

const (
	workspaceEntriesTableName  = "workspace_entries"
	workspaceIdentityGroupName = "workspace"
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

var defaultWorkspaceStateDirectory = ".core/state"

// Workspace buffers mutable work-in-progress in `.core/state/scroll-session.duckdb`
// until Commit or Discard removes the file.
//
// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session-2026-03-30"); if err != nil { return }; defer workspace.Discard()`
type Workspace struct {
	name         string
	backingStore *Store
	database     *sql.DB
	databasePath string
	filesystem   *core.Fs

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

func (workspace *Workspace) ensureReady(operation string) error {
	if workspace == nil {
		return core.E(operation, "workspace is nil", nil)
	}
	if workspace.backingStore == nil {
		return core.E(operation, "workspace store is nil", nil)
	}
	if workspace.database == nil {
		return core.E(operation, "workspace database is nil", nil)
	}
	if workspace.filesystem == nil {
		return core.E(operation, "workspace filesystem is nil", nil)
	}
	if err := workspace.backingStore.ensureReady(operation); err != nil {
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

// Usage example: `workspace, err := storeInstance.NewWorkspace("scroll-session-2026-03-30")`
// The workspace database file lives at `.core/state/scroll-session-2026-03-30.duckdb`
// and is removed when the workspace is committed or discarded.
func (storeInstance *Store) NewWorkspace(name string) (*Workspace, error) {
	if err := storeInstance.ensureReady("store.NewWorkspace"); err != nil {
		return nil, err
	}

	validation := core.ValidateName(name)
	if !validation.OK {
		return nil, core.E("store.NewWorkspace", "validate workspace name", validation.Value.(error))
	}

	filesystem := (&core.Fs{}).NewUnrestricted()
	databasePath := workspaceFilePath(defaultWorkspaceStateDirectory, name)
	if filesystem.Exists(databasePath) {
		return nil, core.E("store.NewWorkspace", core.Concat("workspace already exists: ", name), nil)
	}
	if result := filesystem.EnsureDir(defaultWorkspaceStateDirectory); !result.OK {
		return nil, core.E("store.NewWorkspace", "ensure state directory", result.Value.(error))
	}

	workspaceDatabase, err := openWorkspaceDatabase(databasePath)
	if err != nil {
		return nil, core.E("store.NewWorkspace", "open workspace database", err)
	}

	return &Workspace{
		name:         name,
		backingStore: storeInstance,
		database:     workspaceDatabase,
		databasePath: databasePath,
		filesystem:   filesystem,
	}, nil
}

// discoverOrphanWorkspacePaths(".core/state") returns leftover workspace files
// such as `scroll-session.duckdb` without opening them.
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

func discoverOrphanWorkspaces(stateDirectory string, backingStore *Store) []*Workspace {
	filesystem := (&core.Fs{}).NewUnrestricted()
	orphanWorkspaces := make([]*Workspace, 0)
	for _, databasePath := range discoverOrphanWorkspacePaths(stateDirectory) {
		workspaceDatabase, err := openWorkspaceDatabase(databasePath)
		if err != nil {
			continue
		}
		orphanWorkspaces = append(orphanWorkspaces, &Workspace{
			name:         workspaceNameFromPath(stateDirectory, databasePath),
			backingStore: backingStore,
			database:     workspaceDatabase,
			databasePath: databasePath,
			filesystem:   filesystem,
		})
	}
	return orphanWorkspaces
}

func workspaceNameFromPath(stateDirectory, databasePath string) string {
	relativePath := core.TrimPrefix(databasePath, joinPath(stateDirectory, ""))
	return core.TrimSuffix(relativePath, ".duckdb")
}

// RecoverOrphans(".core/state") returns orphaned workspaces such as
// `scroll-session.duckdb` so callers can inspect Aggregate() and then Discard().
// Usage example: `orphans := storeInstance.RecoverOrphans(".core/state")`
func (storeInstance *Store) RecoverOrphans(stateDirectory string) []*Workspace {
	if storeInstance == nil {
		return nil
	}

	if stateDirectory == "" {
		stateDirectory = defaultWorkspaceStateDirectory
	}

	if stateDirectory == defaultWorkspaceStateDirectory {
		storeInstance.orphanWorkspacesLock.Lock()
		cachedWorkspaces := storeInstance.orphanWorkspaces
		storeInstance.orphanWorkspaces = nil
		storeInstance.orphanWorkspacesLock.Unlock()
		if len(cachedWorkspaces) > 0 {
			return cachedWorkspaces
		}
	}

	filesystem := (&core.Fs{}).NewUnrestricted()
	var orphanWorkspaces []*Workspace
	for _, databasePath := range discoverOrphanWorkspacePaths(stateDirectory) {
		workspaceDatabase, err := openWorkspaceDatabase(databasePath)
		if err != nil {
			continue
		}
		orphanWorkspaces = append(orphanWorkspaces, &Workspace{
			name:         workspaceNameFromPath(stateDirectory, databasePath),
			backingStore: storeInstance,
			database:     workspaceDatabase,
			databasePath: databasePath,
			filesystem:   filesystem,
		})
	}
	return orphanWorkspaces
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

	dataJSON, err := jsonString(data, "store.Workspace.Put", "marshal entry data")
	if err != nil {
		return err
	}

	_, err = workspace.database.Exec(
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

// Usage example: `summary := workspace.Aggregate()`
func (workspace *Workspace) Aggregate() map[string]any {
	if err := workspace.ensureReady("store.Workspace.Aggregate"); err != nil {
		return map[string]any{}
	}

	fields, err := workspace.aggregateFields()
	if err != nil {
		return map[string]any{}
	}
	return fields
}

// Commit writes one journal point for the workspace and upserts the summary
// row in `workspace:NAME`.
// Usage example: `result := workspace.Commit()`
func (workspace *Workspace) Commit() core.Result {
	if err := workspace.ensureReady("store.Workspace.Commit"); err != nil {
		return core.Result{Value: err, OK: false}
	}

	fields, err := workspace.aggregateFields()
	if err != nil {
		return core.Result{Value: core.E("store.Workspace.Commit", "aggregate workspace", err), OK: false}
	}
	if err := workspace.backingStore.commitWorkspaceAggregate(workspace.name, fields); err != nil {
		return core.Result{Value: err, OK: false}
	}
	if err := workspace.closeAndRemoveFiles(); err != nil {
		return core.Result{Value: err, OK: false}
	}
	return core.Result{Value: fields, OK: true}
}

// Usage example: `workspace.Discard()`
func (workspace *Workspace) Discard() {
	if workspace == nil {
		return
	}
	_ = workspace.closeAndRemoveFiles()
}

// Usage example: `result := workspace.Query("SELECT entry_kind, COUNT(*) AS count FROM workspace_entries GROUP BY entry_kind")`
func (workspace *Workspace) Query(sqlQuery string) core.Result {
	if err := workspace.ensureReady("store.Workspace.Query"); err != nil {
		return core.Result{Value: err, OK: false}
	}

	rows, err := workspace.database.Query(sqlQuery)
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

	rows, err := workspace.database.Query(
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
	if workspace == nil {
		return nil
	}
	if workspace.database == nil || workspace.filesystem == nil {
		return nil
	}

	workspace.closeLock.Lock()
	defer workspace.closeLock.Unlock()

	if workspace.closed {
		return nil
	}
	workspace.closed = true

	if err := workspace.database.Close(); err != nil {
		return core.E("store.Workspace.closeAndRemoveFiles", "close workspace database", err)
	}
	for _, path := range []string{workspace.databasePath, workspace.databasePath + "-wal", workspace.databasePath + "-shm"} {
		if result := workspace.filesystem.Delete(path); !result.OK && workspace.filesystem.Exists(path) {
			return core.E("store.Workspace.closeAndRemoveFiles", "delete workspace file", result.Value.(error))
		}
	}
	return nil
}

func (storeInstance *Store) commitWorkspaceAggregate(workspaceName string, fields map[string]any) error {
	if err := storeInstance.ensureReady("store.Workspace.Commit"); err != nil {
		return err
	}
	if err := ensureJournalSchema(storeInstance.database); err != nil {
		return core.E("store.Workspace.Commit", "ensure journal schema", err)
	}

	transaction, err := storeInstance.database.Begin()
	if err != nil {
		return core.E("store.Workspace.Commit", "begin transaction", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()

	fieldsJSON, err := jsonString(fields, "store.Workspace.Commit", "marshal summary")
	if err != nil {
		return err
	}
	tagsJSON, err := jsonString(map[string]string{"workspace": workspaceName}, "store.Workspace.Commit", "marshal tags")
	if err != nil {
		return err
	}

	if err := insertJournalEntry(
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
	workspaceDatabase, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, err
	}
	workspaceDatabase.SetMaxOpenConns(1)
	if _, err := workspaceDatabase.Exec("PRAGMA journal_mode=WAL"); err != nil {
		workspaceDatabase.Close()
		return nil, err
	}
	if _, err := workspaceDatabase.Exec("PRAGMA busy_timeout=5000"); err != nil {
		workspaceDatabase.Close()
		return nil, err
	}
	if _, err := workspaceDatabase.Exec(createWorkspaceEntriesTableSQL); err != nil {
		workspaceDatabase.Close()
		return nil, err
	}
	if _, err := workspaceDatabase.Exec(createWorkspaceEntriesViewSQL); err != nil {
		workspaceDatabase.Close()
		return nil, err
	}
	return workspaceDatabase, nil
}

func workspaceSummaryGroup(workspaceName string) string {
	return core.Concat(workspaceIdentityGroupName, ":", workspaceName)
}

func workspaceFilePath(stateDirectory, name string) string {
	return joinPath(stateDirectory, core.Concat(name, ".duckdb"))
}

func joinPath(base, name string) string {
	if base == "" {
		return name
	}
	return core.Concat(core.TrimSuffix(base, "/"), "/", name)
}
