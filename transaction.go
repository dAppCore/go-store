package store

import (
	"database/sql"
	"time"

	core "dappco.re/go/core"
)

// Usage example: `err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error { return transaction.Set("config", "colour", "blue") })`
type StoreTransaction struct {
	store         *Store
	transaction   *sql.Tx
	pendingEvents []Event
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
		store:       storeInstance,
		transaction: transaction,
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
	if storeTransaction.store == nil {
		return core.E(operation, "transaction store is nil", nil)
	}
	if storeTransaction.transaction == nil {
		return core.E(operation, "transaction database is nil", nil)
	}
	if err := storeTransaction.store.ensureReady(operation); err != nil {
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

// Usage example: `value, err := tx.Get("config", "colour")`
func (storeTransaction *StoreTransaction) Get(group, key string) (string, error) {
	if err := storeTransaction.ensureReady("store.Transaction.Get"); err != nil {
		return "", err
	}

	var value string
	var expiresAt sql.NullInt64
	err := storeTransaction.transaction.QueryRow(
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

// Usage example: `if err := tx.Set("config", "colour", "blue"); err != nil { return err }`
func (storeTransaction *StoreTransaction) Set(group, key, value string) error {
	if err := storeTransaction.ensureReady("store.Transaction.Set"); err != nil {
		return err
	}

	_, err := storeTransaction.transaction.Exec(
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

// Usage example: `if err := tx.SetWithTTL("session", "token", "abc123", time.Minute); err != nil { return err }`
func (storeTransaction *StoreTransaction) SetWithTTL(group, key, value string, timeToLive time.Duration) error {
	if err := storeTransaction.ensureReady("store.Transaction.SetWithTTL"); err != nil {
		return err
	}

	expiresAt := time.Now().Add(timeToLive).UnixMilli()
	_, err := storeTransaction.transaction.Exec(
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

// Usage example: `if err := tx.Delete("config", "colour"); err != nil { return err }`
func (storeTransaction *StoreTransaction) Delete(group, key string) error {
	if err := storeTransaction.ensureReady("store.Transaction.Delete"); err != nil {
		return err
	}

	deleteResult, err := storeTransaction.transaction.Exec(
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

// Usage example: `if err := tx.DeleteGroup("cache"); err != nil { return err }`
func (storeTransaction *StoreTransaction) DeleteGroup(group string) error {
	if err := storeTransaction.ensureReady("store.Transaction.DeleteGroup"); err != nil {
		return err
	}

	deleteResult, err := storeTransaction.transaction.Exec(
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

// Usage example: `if err := tx.DeletePrefix("tenant-a:"); err != nil { return err }`
func (storeTransaction *StoreTransaction) DeletePrefix(groupPrefix string) error {
	if err := storeTransaction.ensureReady("store.Transaction.DeletePrefix"); err != nil {
		return err
	}

	var rows *sql.Rows
	var err error
	if groupPrefix == "" {
		rows, err = storeTransaction.transaction.Query(
			"SELECT DISTINCT " + entryGroupColumn + " FROM " + entriesTableName + " ORDER BY " + entryGroupColumn,
		)
	} else {
		rows, err = storeTransaction.transaction.Query(
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

// Usage example: `keyCount, err := tx.Count("config")`
func (storeTransaction *StoreTransaction) Count(group string) (int, error) {
	if err := storeTransaction.ensureReady("store.Transaction.Count"); err != nil {
		return 0, err
	}

	var count int
	err := storeTransaction.transaction.QueryRow(
		"SELECT COUNT(*) FROM "+entriesTableName+" WHERE "+entryGroupColumn+" = ? AND (expires_at IS NULL OR expires_at > ?)",
		group, time.Now().UnixMilli(),
	).Scan(&count)
	if err != nil {
		return 0, core.E("store.Transaction.Count", "count rows", err)
	}
	return count, nil
}
