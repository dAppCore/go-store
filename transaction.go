package store

import (
	"database/sql"
	"iter"
	"text/template"
	"time"

	core "dappco.re/go/core"
)

// Usage example: `err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error { return transaction.Set("config", "colour", "blue") })`
// Usage example: `if err := transaction.Delete("config", "colour"); err != nil { return err }`
type StoreTransaction struct {
	storeInstance     *Store
	sqliteTransaction *sql.Tx
	pendingEvents     []Event
}

// Usage example: `err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error { if err := transaction.Set("tenant-a:config", "colour", "blue"); err != nil { return err }; return transaction.Set("tenant-b:config", "language", "en-GB") })`
func (storeInstance *Store) Transaction(operation func(*StoreTransaction) error) error {
	if err := storeInstance.ensureReady("store.Transaction"); err != nil {
		return err
	}
	if operation == nil {
		return core.E("store.Transaction", "operation is nil", nil)
	}

	transaction, err := storeInstance.sqliteDatabase.Begin()
	if err != nil {
		return core.E("store.Transaction", "begin transaction", err)
	}

	storeTransaction := &StoreTransaction{
		storeInstance:     storeInstance,
		sqliteTransaction: transaction,
	}

	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()

	if err := operation(storeTransaction); err != nil {
		return core.E("store.Transaction", "execute transaction", err)
	}
	if err := transaction.Commit(); err != nil {
		return core.E("store.Transaction", "commit transaction", err)
	}
	committed = true

	for _, event := range storeTransaction.pendingEvents {
		storeInstance.notify(event)
	}
	return nil
}

func (storeTransaction *StoreTransaction) ensureReady(operation string) error {
	if storeTransaction == nil {
		return core.E(operation, "transaction is nil", nil)
	}
	if storeTransaction.storeInstance == nil {
		return core.E(operation, "transaction store is nil", nil)
	}
	if storeTransaction.sqliteTransaction == nil {
		return core.E(operation, "transaction database is nil", nil)
	}
	if err := storeTransaction.storeInstance.ensureReady(operation); err != nil {
		return err
	}
	return nil
}

func (storeTransaction *StoreTransaction) recordEvent(event Event) {
	if storeTransaction == nil {
		return
	}
	storeTransaction.pendingEvents = append(storeTransaction.pendingEvents, event)
}

// Usage example: `exists, err := transaction.Exists("config", "colour")`
// Usage example: `if exists, _ := transaction.Exists("session", "token"); !exists { return core.E("auth", "session expired", nil) }`
func (storeTransaction *StoreTransaction) Exists(group, key string) (bool, error) {
	if err := storeTransaction.ensureReady("store.Transaction.Exists"); err != nil {
		return false, err
	}

	return liveEntryExists(storeTransaction.sqliteTransaction, group, key)
}

// Usage example: `exists, err := transaction.GroupExists("config")`
func (storeTransaction *StoreTransaction) GroupExists(group string) (bool, error) {
	if err := storeTransaction.ensureReady("store.Transaction.GroupExists"); err != nil {
		return false, err
	}

	count, err := storeTransaction.Count(group)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Usage example: `value, err := transaction.Get("config", "colour")`
func (storeTransaction *StoreTransaction) Get(group, key string) (string, error) {
	if err := storeTransaction.ensureReady("store.Transaction.Get"); err != nil {
		return "", err
	}

	var value string
	var expiresAt sql.NullInt64
	err := storeTransaction.sqliteTransaction.QueryRow(
		"SELECT "+entryValueColumn+", expires_at FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?",
		group, key,
	).Scan(&value, &expiresAt)
	if err == sql.ErrNoRows {
		return "", core.E("store.Transaction.Get", core.Concat(group, "/", key), NotFoundError)
	}
	if err != nil {
		return "", core.E("store.Transaction.Get", "query row", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		if err := storeTransaction.Delete(group, key); err != nil {
			return "", core.E("store.Transaction.Get", "delete expired row", err)
		}
		return "", core.E("store.Transaction.Get", core.Concat(group, "/", key), NotFoundError)
	}
	return value, nil
}

// Usage example: `if err := transaction.Set("config", "colour", "blue"); err != nil { return err }`
func (storeTransaction *StoreTransaction) Set(group, key, value string) error {
	if err := storeTransaction.ensureReady("store.Transaction.Set"); err != nil {
		return err
	}

	_, err := storeTransaction.sqliteTransaction.Exec(
		"INSERT INTO "+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, NULL) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = NULL",
		group, key, value,
	)
	if err != nil {
		return core.E("store.Transaction.Set", "execute upsert", err)
	}
	storeTransaction.recordEvent(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Usage example: `if err := transaction.SetWithTTL("session", "token", "abc123", time.Minute); err != nil { return err }`
func (storeTransaction *StoreTransaction) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	if err := storeTransaction.ensureReady("store.Transaction.SetWithTTL"); err != nil {
		return err
	}

	expiresAt := time.Now().Add(timeToLive).UnixMilli()
	_, err := storeTransaction.sqliteTransaction.Exec(
		"INSERT INTO "+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, ?) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = excluded.expires_at",
		group, key, value, expiresAt,
	)
	if err != nil {
		return core.E("store.Transaction.SetWithTTL", "execute upsert with expiry", err)
	}
	storeTransaction.recordEvent(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return nil
}

// Usage example: `if err := transaction.Delete("config", "colour"); err != nil { return err }`
func (storeTransaction *StoreTransaction) Delete(group, key string) error {
	if err := storeTransaction.ensureReady("store.Transaction.Delete"); err != nil {
		return err
	}

	deleteResult, err := storeTransaction.sqliteTransaction.Exec(
		"DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?",
		group, key,
	)
	if err != nil {
		return core.E("store.Transaction.Delete", "delete row", err)
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.E("store.Transaction.Delete", "count deleted rows", rowsAffectedError)
	}
	if deletedRows > 0 {
		storeTransaction.recordEvent(Event{Type: EventDelete, Group: group, Key: key, Timestamp: time.Now()})
	}
	return nil
}

// Usage example: `if err := transaction.DeleteGroup("cache"); err != nil { return err }`
func (storeTransaction *StoreTransaction) DeleteGroup(group string) error {
	if err := storeTransaction.ensureReady("store.Transaction.DeleteGroup"); err != nil {
		return err
	}

	deleteResult, err := storeTransaction.sqliteTransaction.Exec(
		"DELETE FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ?",
		group,
	)
	if err != nil {
		return core.E("store.Transaction.DeleteGroup", "delete group", err)
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.E("store.Transaction.DeleteGroup", "count deleted rows", rowsAffectedError)
	}
	if deletedRows > 0 {
		storeTransaction.recordEvent(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	}
	return nil
}

// Usage example: `if err := transaction.DeletePrefix("tenant-a:"); err != nil { return err }`
func (storeTransaction *StoreTransaction) DeletePrefix(groupPrefix string) error {
	if err := storeTransaction.ensureReady("store.Transaction.DeletePrefix"); err != nil {
		return err
	}

	var rows *sql.Rows
	var err error
	if groupPrefix == "" {
		rows, err = storeTransaction.sqliteTransaction.Query(
			"SELECT DISTINCT " + entryGroupColumn + " FROM " + entriesTableName + " ORDER BY " + entryGroupColumn,
		)
	} else {
		rows, err = storeTransaction.sqliteTransaction.Query(
			"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" LIKE ? ESCAPE '^' ORDER BY "+entryGroupColumn,
			escapeLike(groupPrefix)+"%",
		)
	}
	if err != nil {
		return core.E("store.Transaction.DeletePrefix", "list groups", err)
	}
	defer rows.Close()

	var groupNames []string
	for rows.Next() {
		var groupName string
		if err := rows.Scan(&groupName); err != nil {
			return core.E("store.Transaction.DeletePrefix", "scan group name", err)
		}
		groupNames = append(groupNames, groupName)
	}
	if err := rows.Err(); err != nil {
		return core.E("store.Transaction.DeletePrefix", "iterate groups", err)
	}
	for _, groupName := range groupNames {
		if err := storeTransaction.DeleteGroup(groupName); err != nil {
			return core.E("store.Transaction.DeletePrefix", "delete group", err)
		}
	}
	return nil
}

// Usage example: `keyCount, err := transaction.Count("config")`
func (storeTransaction *StoreTransaction) Count(group string) (int, error) {
	if err := storeTransaction.ensureReady("store.Transaction.Count"); err != nil {
		return 0, err
	}

	var count int
	err := storeTransaction.sqliteTransaction.QueryRow(
		"SELECT COUNT(*) FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	).Scan(&count)
	if err != nil {
		return 0, core.E("store.Transaction.Count", "count rows", err)
	}
	return count, nil
}

// Usage example: `colourEntries, err := transaction.GetAll("config")`
func (storeTransaction *StoreTransaction) GetAll(group string) (map[string]string, error) {
	if err := storeTransaction.ensureReady("store.Transaction.GetAll"); err != nil {
		return nil, err
	}

	entriesByKey := make(map[string]string)
	for entry, err := range storeTransaction.All(group) {
		if err != nil {
			return nil, core.E("store.Transaction.GetAll", "iterate rows", err)
		}
		entriesByKey[entry.Key] = entry.Value
	}
	return entriesByKey, nil
}

// Usage example: `page, err := transaction.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (storeTransaction *StoreTransaction) GetPage(group string, offset, limit int) ([]KeyValue, error) {
	if err := storeTransaction.ensureReady("store.Transaction.GetPage"); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, core.E("store.Transaction.GetPage", "offset must be zero or positive", nil)
	}
	if limit < 0 {
		return nil, core.E("store.Transaction.GetPage", "limit must be zero or positive", nil)
	}

	rows, err := storeTransaction.sqliteTransaction.Query(
		"SELECT "+entryKeyColumn+", "+entryValueColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn+" LIMIT ? OFFSET ?",
		group, time.Now().UnixMilli(), limit, offset,
	)
	if err != nil {
		return nil, core.E("store.Transaction.GetPage", "query rows", err)
	}
	defer rows.Close()

	page := make([]KeyValue, 0, limit)
	for rows.Next() {
		var entry KeyValue
		if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
			return nil, core.E("store.Transaction.GetPage", "scan row", err)
		}
		page = append(page, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, core.E("store.Transaction.GetPage", "rows iteration", err)
	}
	return page, nil
}

// Usage example: `for entry, err := range transaction.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeTransaction *StoreTransaction) All(group string) iter.Seq2[KeyValue, error] {
	return storeTransaction.AllSeq(group)
}

// Usage example: `for entry, err := range transaction.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeTransaction *StoreTransaction) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		if err := storeTransaction.ensureReady("store.Transaction.All"); err != nil {
			yield(KeyValue{}, err)
			return
		}

		rows, err := storeTransaction.sqliteTransaction.Query(
			"SELECT "+entryKeyColumn+", "+entryValueColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn,
			group, time.Now().UnixMilli(),
		)
		if err != nil {
			yield(KeyValue{}, core.E("store.Transaction.All", "query rows", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var entry KeyValue
			if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
				if !yield(KeyValue{}, core.E("store.Transaction.All", "scan row", err)) {
					return
				}
				continue
			}
			if !yield(entry, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(KeyValue{}, core.E("store.Transaction.All", "rows iteration", err))
		}
	}
}

// Usage example: `removedRows, err := transaction.CountAll("tenant-a:")`
func (storeTransaction *StoreTransaction) CountAll(groupPrefix string) (int, error) {
	if err := storeTransaction.ensureReady("store.Transaction.CountAll"); err != nil {
		return 0, err
	}

	var count int
	var err error
	if groupPrefix == "" {
		err = storeTransaction.sqliteTransaction.QueryRow(
			"SELECT COUNT(*) FROM "+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?)",
			time.Now().UnixMilli(),
		).Scan(&count)
	} else {
		err = storeTransaction.sqliteTransaction.QueryRow(
			"SELECT COUNT(*) FROM "+entriesTableName+" WHERE "+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
			escapeLike(groupPrefix)+"%", time.Now().UnixMilli(),
		).Scan(&count)
	}
	if err != nil {
		return 0, core.E("store.Transaction.CountAll", "count rows", err)
	}
	return count, nil
}

// Usage example: `groupNames, err := transaction.Groups("tenant-a:")`
// Usage example: `groupNames, err := transaction.Groups()`
func (storeTransaction *StoreTransaction) Groups(groupPrefix ...string) ([]string, error) {
	if err := storeTransaction.ensureReady("store.Transaction.Groups"); err != nil {
		return nil, err
	}

	var groupNames []string
	for groupName, err := range storeTransaction.GroupsSeq(groupPrefix...) {
		if err != nil {
			return nil, err
		}
		groupNames = append(groupNames, groupName)
	}
	return groupNames, nil
}

// Usage example: `for groupName, err := range transaction.GroupsSeq("tenant-a:") { if err != nil { break }; fmt.Println(groupName) }`
// Usage example: `for groupName, err := range transaction.GroupsSeq() { if err != nil { break }; fmt.Println(groupName) }`
func (storeTransaction *StoreTransaction) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	actualGroupPrefix := firstStringOrEmpty(groupPrefix)
	return func(yield func(string, error) bool) {
		if err := storeTransaction.ensureReady("store.Transaction.GroupsSeq"); err != nil {
			yield("", err)
			return
		}

		var rows *sql.Rows
		var err error
		now := time.Now().UnixMilli()
		if actualGroupPrefix == "" {
			rows, err = storeTransaction.sqliteTransaction.Query(
				"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				now,
			)
		} else {
			rows, err = storeTransaction.sqliteTransaction.Query(
				"SELECT DISTINCT "+entryGroupColumn+" FROM "+entriesTableName+" WHERE "+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				escapeLike(actualGroupPrefix)+"%", now,
			)
		}
		if err != nil {
			yield("", core.E("store.Transaction.GroupsSeq", "query group names", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var groupName string
			if err := rows.Scan(&groupName); err != nil {
				if !yield("", core.E("store.Transaction.GroupsSeq", "scan group name", err)) {
					return
				}
				continue
			}
			if !yield(groupName, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield("", core.E("store.Transaction.GroupsSeq", "rows iteration", err))
		}
	}
}

// Usage example: `renderedTemplate, err := transaction.Render("Hello {{ .name }}", "user")`
func (storeTransaction *StoreTransaction) Render(templateSource, group string) (string, error) {
	if err := storeTransaction.ensureReady("store.Transaction.Render"); err != nil {
		return "", err
	}

	templateData := make(map[string]string)
	for entry, err := range storeTransaction.All(group) {
		if err != nil {
			return "", core.E("store.Transaction.Render", "iterate rows", err)
		}
		templateData[entry.Key] = entry.Value
	}

	renderTemplate, err := template.New("render").Parse(templateSource)
	if err != nil {
		return "", core.E("store.Transaction.Render", "parse template", err)
	}
	builder := core.NewBuilder()
	if err := renderTemplate.Execute(builder, templateData); err != nil {
		return "", core.E("store.Transaction.Render", "execute template", err)
	}
	return builder.String(), nil
}

// Usage example: `parts, err := transaction.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (storeTransaction *StoreTransaction) GetSplit(group, key, separator string) (iter.Seq[string], error) {
	if err := storeTransaction.ensureReady("store.Transaction.GetSplit"); err != nil {
		return nil, err
	}

	value, err := storeTransaction.Get(group, key)
	if err != nil {
		return nil, err
	}
	return splitValueSeq(value, separator), nil
}

// Usage example: `fields, err := transaction.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (storeTransaction *StoreTransaction) GetFields(group, key string) (iter.Seq[string], error) {
	if err := storeTransaction.ensureReady("store.Transaction.GetFields"); err != nil {
		return nil, err
	}

	value, err := storeTransaction.Get(group, key)
	if err != nil {
		return nil, err
	}
	return fieldsValueSeq(value), nil
}

// Usage example: `removedRows, err := transaction.PurgeExpired(); if err != nil { return err }; fmt.Println(removedRows)`
func (storeTransaction *StoreTransaction) PurgeExpired() (int64, error) {
	if err := storeTransaction.ensureReady("store.Transaction.PurgeExpired"); err != nil {
		return 0, err
	}

	cutoffUnixMilli := time.Now().UnixMilli()
	expiredEntries, err := listExpiredEntriesMatchingGroupPrefix(storeTransaction.sqliteTransaction, "", cutoffUnixMilli)
	if err != nil {
		return 0, core.E("store.Transaction.PurgeExpired", "list expired rows", err)
	}

	removedRows, err := purgeExpiredMatchingGroupPrefix(storeTransaction.sqliteTransaction, "", cutoffUnixMilli)
	if err != nil {
		return 0, core.E("store.Transaction.PurgeExpired", "delete expired rows", err)
	}
	if removedRows > 0 {
		for _, expiredEntry := range expiredEntries {
			storeTransaction.recordEvent(Event{
				Type:      EventDelete,
				Group:     expiredEntry.group,
				Key:       expiredEntry.key,
				Timestamp: time.Now(),
			})
		}
	}
	return removedRows, nil
}
