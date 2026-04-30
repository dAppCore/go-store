package store

import (
	"database/sql"
	"iter"
	"text/template"
	"time"

	core "dappco.re/go"
)

// Usage example: `err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error { return transaction.Set("config", "colour", "blue") })`
// Usage example: `if err := transaction.Delete("config", "colour"); err != nil { return err }`
type StoreTransaction struct {
	storeInstance     *Store
	sqliteTransaction *sql.Tx
	pendingEvents     []Event
}

// Usage example: `err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error { if err := transaction.Set("tenant-a:config", "colour", "blue"); err != nil { return err }; return transaction.Set("tenant-b:config", "language", "en-GB") })`
func (storeInstance *Store) Transaction(operation func(*StoreTransaction) core.Result) core.Result {
	if result := storeInstance.ensureReady(opTransaction); !result.OK {
		return result
	}
	if operation == nil {
		return core.Fail(core.E(opTransaction, "operation is nil", nil))
	}

	transaction, err := storeInstance.sqliteDatabase.Begin()
	if err != nil {
		return core.Fail(core.E(opTransaction, "begin transaction", err))
	}

	storeTransaction := &StoreTransaction{
		storeInstance:     storeInstance,
		sqliteTransaction: transaction,
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := transaction.Rollback(); rollbackErr != nil {
				core.Error("store transaction rollback failed", "err", rollbackErr)
			}
		}
	}()

	if result := operation(storeTransaction); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opTransaction, "execute transaction", err))
	}
	if err := transaction.Commit(); err != nil {
		return core.Fail(core.E(opTransaction, "commit transaction", err))
	}
	committed = true

	for _, event := range storeTransaction.pendingEvents {
		storeInstance.notify(event)
	}
	return core.Ok(nil)
}

func (storeTransaction *StoreTransaction) ensureReady(operation string) core.Result {
	if storeTransaction == nil {
		return core.Fail(core.E(operation, "transaction is nil", nil))
	}
	if storeTransaction.storeInstance == nil {
		return core.Fail(core.E(operation, "transaction store is nil", nil))
	}
	if storeTransaction.sqliteTransaction == nil {
		return core.Fail(core.E(operation, "transaction database is nil", nil))
	}
	if result := storeTransaction.storeInstance.ensureReady(operation); !result.OK {
		return result
	}
	return core.Ok(nil)
}

func (storeTransaction *StoreTransaction) recordEvent(event Event) {
	if storeTransaction == nil {
		return
	}
	storeTransaction.pendingEvents = append(storeTransaction.pendingEvents, event)
}

// Usage example: `exists, err := transaction.Exists("config", "colour")`
// Usage example: `if exists, _ := transaction.Exists("session", "token"); !exists { return core.E("auth", "session expired", nil) }`
func (storeTransaction *StoreTransaction) Exists(group, key string) (bool, core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.Exists"); !result.OK {
		return false, result
	}

	return liveEntryExists(storeTransaction.sqliteTransaction, group, key)
}

// Usage example: `exists, err := transaction.GroupExists("config")`
func (storeTransaction *StoreTransaction) GroupExists(group string) (bool, core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.GroupExists"); !result.OK {
		return false, result
	}

	count, result := storeTransaction.Count(group)
	if !result.OK {
		return false, result
	}
	return count > 0, core.Ok(nil)
}

// Usage example: `value, err := transaction.Get("config", "colour")`
func (storeTransaction *StoreTransaction) Get(group, key string) (string, core.Result) {
	if result := storeTransaction.ensureReady(opTransactionGet); !result.OK {
		return "", result
	}

	var value string
	var expiresAt sql.NullInt64
	err := storeTransaction.sqliteTransaction.QueryRow(
		sqlSelect+entryValueColumn+", expires_at FROM "+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?",
		group, key,
	).Scan(&value, &expiresAt)
	if err == sql.ErrNoRows {
		return "", core.Fail(core.E(opTransactionGet, core.Concat(group, "/", key), NotFoundError))
	}
	if err != nil {
		return "", core.Fail(core.E(opTransactionGet, "query row", err))
	}
	if expiresAt.Valid && expiresAt.Int64 <= time.Now().UnixMilli() {
		if result := storeTransaction.Delete(group, key); !result.OK {
			err, _ := result.Value.(error)
			return "", core.Fail(core.E(opTransactionGet, "delete expired row", err))
		}
		return "", core.Fail(core.E(opTransactionGet, core.Concat(group, "/", key), NotFoundError))
	}
	return value, core.Ok(nil)
}

// Usage example: `if err := transaction.Set("config", "colour", "blue"); err != nil { return err }`
func (storeTransaction *StoreTransaction) Set(group, key, value string) core.Result {
	if result := storeTransaction.ensureReady("store.Transaction.Set"); !result.OK {
		return result
	}

	_, err := storeTransaction.sqliteTransaction.Exec(
		sqlInsertIntoPrefix+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, NULL) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = NULL",
		group, key, value,
	)
	if err != nil {
		return core.Fail(core.E("store.Transaction.Set", "execute upsert", err))
	}
	storeTransaction.recordEvent(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return core.Ok(nil)
}

// Usage example: `if err := transaction.SetWithTTL("session", "token", "abc123", time.Minute); err != nil { return err }`
func (storeTransaction *StoreTransaction) SetWithTTL(group, key, value string, timeToLive time.Duration) core.Result {
	if result := storeTransaction.ensureReady("store.Transaction.SetWithTTL"); !result.OK {
		return result
	}

	expiresAt := time.Now().Add(timeToLive).UnixMilli()
	_, err := storeTransaction.sqliteTransaction.Exec(
		sqlInsertIntoPrefix+entriesTableName+" ("+entryGroupColumn+", "+entryKeyColumn+", "+entryValueColumn+", expires_at) VALUES (?, ?, ?, ?) "+
			"ON CONFLICT("+entryGroupColumn+", "+entryKeyColumn+") DO UPDATE SET "+entryValueColumn+" = excluded."+entryValueColumn+", expires_at = excluded.expires_at",
		group, key, value, expiresAt,
	)
	if err != nil {
		return core.Fail(core.E("store.Transaction.SetWithTTL", "execute upsert with expiry", err))
	}
	storeTransaction.recordEvent(Event{Type: EventSet, Group: group, Key: key, Value: value, Timestamp: time.Now()})
	return core.Ok(nil)
}

// Usage example: `if err := transaction.Delete("config", "colour"); err != nil { return err }`
func (storeTransaction *StoreTransaction) Delete(group, key string) core.Result {
	if result := storeTransaction.ensureReady(opTransactionDelete); !result.OK {
		return result
	}

	deleteResult, err := storeTransaction.sqliteTransaction.Exec(
		sqlDeleteFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND "+entryKeyColumn+" = ?",
		group, key,
	)
	if err != nil {
		return core.Fail(core.E(opTransactionDelete, "delete row", err))
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.Fail(core.E(opTransactionDelete, "count deleted rows", rowsAffectedError))
	}
	if deletedRows > 0 {
		storeTransaction.recordEvent(Event{Type: EventDelete, Group: group, Key: key, Timestamp: time.Now()})
	}
	return core.Ok(nil)
}

// Usage example: `if err := transaction.DeleteGroup("cache"); err != nil { return err }`
func (storeTransaction *StoreTransaction) DeleteGroup(group string) core.Result {
	if result := storeTransaction.ensureReady(opTransactionDeleteGroup); !result.OK {
		return result
	}

	deleteResult, err := storeTransaction.sqliteTransaction.Exec(
		sqlDeleteFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ?",
		group,
	)
	if err != nil {
		return core.Fail(core.E(opTransactionDeleteGroup, "delete group", err))
	}
	deletedRows, rowsAffectedError := deleteResult.RowsAffected()
	if rowsAffectedError != nil {
		return core.Fail(core.E(opTransactionDeleteGroup, "count deleted rows", rowsAffectedError))
	}
	if deletedRows > 0 {
		storeTransaction.recordEvent(Event{Type: EventDeleteGroup, Group: group, Timestamp: time.Now()})
	}
	return core.Ok(nil)
}

// Usage example: `if err := transaction.DeletePrefix("tenant-a:"); err != nil { return err }`
func (storeTransaction *StoreTransaction) DeletePrefix(groupPrefix string) core.Result {
	if result := storeTransaction.ensureReady(opTransactionDeletePrefix); !result.OK {
		return result
	}

	var rows *sql.Rows
	var err error
	if groupPrefix == "" {
		rows, err = storeTransaction.sqliteTransaction.Query(
			sqlSelectDistinct + entryGroupColumn + sqlFrom + entriesTableName + " ORDER BY " + entryGroupColumn,
		)
	} else {
		rows, err = storeTransaction.sqliteTransaction.Query(
			sqlSelectDistinct+entryGroupColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" LIKE ? ESCAPE '^' ORDER BY "+entryGroupColumn,
			escapeLike(groupPrefix)+"%",
		)
	}
	if err != nil {
		return core.Fail(core.E(opTransactionDeletePrefix, "list groups", err))
	}
	defer func() { _ = rows.Close() }()

	var groupNames []string
	for rows.Next() {
		var groupName string
		if err := rows.Scan(&groupName); err != nil {
			return core.Fail(core.E(opTransactionDeletePrefix, "scan group name", err))
		}
		groupNames = append(groupNames, groupName)
	}
	if err := rows.Err(); err != nil {
		return core.Fail(core.E(opTransactionDeletePrefix, "iterate groups", err))
	}
	for _, groupName := range groupNames {
		if result := storeTransaction.DeleteGroup(groupName); !result.OK {
			err, _ := result.Value.(error)
			return core.Fail(core.E(opTransactionDeletePrefix, "delete group", err))
		}
	}
	return core.Ok(nil)
}

// Usage example: `keyCount, err := transaction.Count("config")`
func (storeTransaction *StoreTransaction) Count(group string) (int, core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.Count"); !result.OK {
		return 0, result
	}

	var count int
	err := storeTransaction.sqliteTransaction.QueryRow(
		sqlSelectCountFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	).Scan(&count)
	if err != nil {
		return 0, core.Fail(core.E("store.Transaction.Count", "count rows", err))
	}
	return count, core.Ok(nil)
}

// Usage example: `colourEntries, err := transaction.GetAll("config")`
func (storeTransaction *StoreTransaction) GetAll(group string) (map[string]string, core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.GetAll"); !result.OK {
		return nil, result
	}

	entriesByKey := make(map[string]string)
	for entry, err := range storeTransaction.All(group) {
		if err != nil {
			return nil, core.Fail(core.E("store.Transaction.GetAll", "iterate rows", err))
		}
		entriesByKey[entry.Key] = entry.Value
	}
	return entriesByKey, core.Ok(nil)
}

// Usage example: `page, err := transaction.GetPage("config", 0, 25); if err != nil { return }; for _, entry := range page { fmt.Println(entry.Key, entry.Value) }`
func (storeTransaction *StoreTransaction) GetPage(group string, offset, limit int) ([]KeyValue, core.Result) {
	if result := storeTransaction.ensureReady(opTransactionGetPage); !result.OK {
		return nil, result
	}
	if offset < 0 {
		return nil, core.Fail(core.E(opTransactionGetPage, "offset must be zero or positive", nil))
	}
	if limit < 0 {
		return nil, core.Fail(core.E(opTransactionGetPage, "limit must be zero or positive", nil))
	}

	rows, err := storeTransaction.sqliteTransaction.Query(
		sqlSelect+entryKeyColumn+", "+entryValueColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn+" LIMIT ? OFFSET ?",
		group, time.Now().UnixMilli(), limit, offset,
	)
	if err != nil {
		return nil, core.Fail(core.E(opTransactionGetPage, "query rows", err))
	}
	defer func() { _ = rows.Close() }()

	page := make([]KeyValue, 0, limit)
	for rows.Next() {
		var entry KeyValue
		if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
			return nil, core.Fail(core.E(opTransactionGetPage, "scan row", err))
		}
		page = append(page, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, core.Fail(core.E(opTransactionGetPage, rowsIterationMessage, err))
	}
	return page, core.Ok(nil)
}

// Usage example: `for entry, err := range transaction.All("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeTransaction *StoreTransaction) All(group string) iter.Seq2[KeyValue, error] {
	return storeTransaction.AllSeq(group)
}

// Usage example: `for entry, err := range transaction.AllSeq("config") { if err != nil { break }; fmt.Println(entry.Key, entry.Value) }`
func (storeTransaction *StoreTransaction) AllSeq(group string) iter.Seq2[KeyValue, error] {
	return func(yield func(KeyValue, error) bool) {
		if result := storeTransaction.ensureReady(opTransactionAll); !result.OK {
			err, _ := result.Value.(error)
			yield(KeyValue{}, err)
			return
		}

		rows, err := storeTransaction.sqliteTransaction.Query(
			sqlSelect+entryKeyColumn+", "+entryValueColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryKeyColumn,
			group, time.Now().UnixMilli(),
		)
		if err != nil {
			yield(KeyValue{}, core.E(opTransactionAll, "query rows", err))
			return
		}
		defer func() { _ = rows.Close() }()

		yieldKeyValueRows(rows, opTransactionAll, yield)
	}
}

// Usage example: `removedRows, err := transaction.CountAll("tenant-a:")`
func (storeTransaction *StoreTransaction) CountAll(groupPrefix string) (int, core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.CountAll"); !result.OK {
		return 0, result
	}

	var count int
	var err error
	if groupPrefix == "" {
		err = storeTransaction.sqliteTransaction.QueryRow(
			sqlSelectCountFrom+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?)",
			time.Now().UnixMilli(),
		).Scan(&count)
	} else {
		err = storeTransaction.sqliteTransaction.QueryRow(
			sqlSelectCountFrom+entriesTableName+sqlWhere+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?)",
			escapeLike(groupPrefix)+"%", time.Now().UnixMilli(),
		).Scan(&count)
	}
	if err != nil {
		return 0, core.Fail(core.E("store.Transaction.CountAll", "count rows", err))
	}
	return count, core.Ok(nil)
}

// Usage example: `groupNames, err := transaction.Groups("tenant-a:")`
// Usage example: `groupNames, err := transaction.Groups()`
func (storeTransaction *StoreTransaction) Groups(groupPrefix ...string) ([]string, core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.Groups"); !result.OK {
		return nil, result
	}

	var groupNames []string
	for groupName, err := range storeTransaction.GroupsSeq(groupPrefix...) {
		if err != nil {
			return nil, core.Fail(err)
		}
		groupNames = append(groupNames, groupName)
	}
	return groupNames, core.Ok(nil)
}

// Usage example: `for groupName, err := range transaction.GroupsSeq("tenant-a:") { if err != nil { break }; fmt.Println(groupName) }`
// Usage example: `for groupName, err := range transaction.GroupsSeq() { if err != nil { break }; fmt.Println(groupName) }`
func (storeTransaction *StoreTransaction) GroupsSeq(groupPrefix ...string) iter.Seq2[string, error] {
	actualGroupPrefix := firstStringOrEmpty(groupPrefix)
	return func(yield func(string, error) bool) {
		if result := storeTransaction.ensureReady(opTransactionGroupsSeq); !result.OK {
			err, _ := result.Value.(error)
			yield("", err)
			return
		}

		var rows *sql.Rows
		var err error
		now := time.Now().UnixMilli()
		if actualGroupPrefix == "" {
			rows, err = storeTransaction.sqliteTransaction.Query(
				sqlSelectDistinct+entryGroupColumn+sqlFrom+entriesTableName+" WHERE (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				now,
			)
		} else {
			rows, err = storeTransaction.sqliteTransaction.Query(
				sqlSelectDistinct+entryGroupColumn+sqlFrom+entriesTableName+sqlWhere+entryGroupColumn+" LIKE ? ESCAPE '^' AND (expires_at IS NULL OR expires_at > ?) ORDER BY "+entryGroupColumn,
				escapeLike(actualGroupPrefix)+"%", now,
			)
		}
		if err != nil {
			yield("", core.E(opTransactionGroupsSeq, "query group names", err))
			return
		}
		defer func() { _ = rows.Close() }()

		yieldGroupRows(rows, opTransactionGroupsSeq, yield)
	}
}

// Usage example: `renderedTemplate, err := transaction.Render("Hello {{ .name }}", "user")`
func (storeTransaction *StoreTransaction) Render(templateSource, group string) (string, core.Result) {
	if result := storeTransaction.ensureReady(opTransactionRender); !result.OK {
		return "", result
	}

	templateData := make(map[string]string)
	for entry, err := range storeTransaction.All(group) {
		if err != nil {
			return "", core.Fail(core.E(opTransactionRender, "iterate rows", err))
		}
		templateData[entry.Key] = entry.Value
	}

	renderTemplate, err := template.New("render").Parse(templateSource)
	if err != nil {
		return "", core.Fail(core.E(opTransactionRender, "parse template", err))
	}
	builder := core.NewBuilder()
	if err := renderTemplate.Execute(builder, templateData); err != nil {
		return "", core.Fail(core.E(opTransactionRender, "execute template", err))
	}
	return builder.String(), core.Ok(nil)
}

// Usage example: `parts, err := transaction.GetSplit("config", "hosts", ","); if err != nil { return }; for part := range parts { fmt.Println(part) }`
func (storeTransaction *StoreTransaction) GetSplit(group, key, separator string) (iter.Seq[string], core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.GetSplit"); !result.OK {
		return nil, result
	}

	value, result := storeTransaction.Get(group, key)
	if !result.OK {
		return nil, result
	}
	return splitValueSeq(value, separator), core.Ok(nil)
}

// Usage example: `fields, err := transaction.GetFields("config", "flags"); if err != nil { return }; for field := range fields { fmt.Println(field) }`
func (storeTransaction *StoreTransaction) GetFields(group, key string) (iter.Seq[string], core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.GetFields"); !result.OK {
		return nil, result
	}

	value, result := storeTransaction.Get(group, key)
	if !result.OK {
		return nil, result
	}
	return fieldsValueSeq(value), core.Ok(nil)
}

// Usage example: `removedRows, err := transaction.PurgeExpired(); if err != nil { return err }; fmt.Println(removedRows)`
func (storeTransaction *StoreTransaction) PurgeExpired() (int64, core.Result) {
	if result := storeTransaction.ensureReady("store.Transaction.PurgeExpired"); !result.OK {
		return 0, result
	}

	cutoffUnixMilli := time.Now().UnixMilli()
	expiredEntries, result := deleteExpiredEntriesMatchingGroupPrefix(storeTransaction.sqliteTransaction, "", cutoffUnixMilli)
	if !result.OK {
		err, _ := result.Value.(error)
		return 0, core.Fail(core.E("store.Transaction.PurgeExpired", "delete expired rows", err))
	}
	storeTransaction.recordExpiredEntries(expiredEntries)
	return int64(len(expiredEntries)), core.Ok(nil)
}

func (storeTransaction *StoreTransaction) recordExpiredEntries(expiredEntries []expiredEntryRef) {
	for _, expiredEntry := range expiredEntries {
		storeTransaction.recordEvent(eventFromExpiredEntry(expiredEntry))
	}
}
