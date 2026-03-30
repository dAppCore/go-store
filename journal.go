package store

import (
	"database/sql"
	"regexp"
	"strconv"
	"time"

	core "dappco.re/go/core"
)

const (
	journalEntriesTableName = "journal_entries"
	defaultJournalBucket    = "store"
)

const createJournalEntriesTableSQL = `CREATE TABLE IF NOT EXISTS journal_entries (
	entry_id      INTEGER PRIMARY KEY AUTOINCREMENT,
	bucket_name   TEXT NOT NULL,
	measurement   TEXT NOT NULL,
	fields_json   TEXT NOT NULL,
	tags_json     TEXT NOT NULL,
	committed_at  INTEGER NOT NULL,
	archived_at   INTEGER
)`

var (
	journalBucketPattern      = regexp.MustCompile(`bucket:\s*"([^"]+)"`)
	journalRangePattern       = regexp.MustCompile(`range\(\s*start:\s*([^)]+)\)`)
	journalMeasurementPattern = regexp.MustCompile(`(?:_measurement|measurement)\s*==\s*"([^"]+)"`)
)

type journalExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// CommitToJournal records one completed unit of work in the store journal.
// Usage example: `result := storeInstance.CommitToJournal("scroll-session", map[string]any{"like": 4}, map[string]string{"workspace": "scroll-session"})`
func (storeInstance *Store) CommitToJournal(measurement string, fields map[string]any, tags map[string]string) core.Result {
	if measurement == "" {
		return core.Result{Value: core.E("store.CommitToJournal", "measurement is empty", nil), OK: false}
	}
	if fields == nil {
		fields = map[string]any{}
	}
	if tags == nil {
		tags = map[string]string{}
	}
	if err := ensureJournalSchema(storeInstance.database); err != nil {
		return core.Result{Value: core.E("store.CommitToJournal", "ensure journal schema", err), OK: false}
	}

	fieldsJSON, err := jsonString(fields, "store.CommitToJournal", "marshal fields")
	if err != nil {
		return core.Result{Value: err, OK: false}
	}
	tagsJSON, err := jsonString(tags, "store.CommitToJournal", "marshal tags")
	if err != nil {
		return core.Result{Value: err, OK: false}
	}

	committedAt := time.Now().UnixMilli()
	if err := insertJournalEntry(
		storeInstance.database,
		storeInstance.journalBucket(),
		measurement,
		fieldsJSON,
		tagsJSON,
		committedAt,
	); err != nil {
		return core.Result{Value: core.E("store.CommitToJournal", "insert journal entry", err), OK: false}
	}

	return core.Result{
		Value: map[string]any{
			"bucket":       storeInstance.journalBucket(),
			"measurement":  measurement,
			"fields":       fields,
			"tags":         tags,
			"committed_at": committedAt,
		},
		OK: true,
	}
}

// QueryJournal reads journal rows either through a small Flux-like filter
// surface or a direct SQL SELECT against the internal journal table.
// Usage example: `result := storeInstance.QueryJournal(\`from(bucket: "store") |> range(start: -24h)\`)`
func (storeInstance *Store) QueryJournal(flux string) core.Result {
	if err := ensureJournalSchema(storeInstance.database); err != nil {
		return core.Result{Value: core.E("store.QueryJournal", "ensure journal schema", err), OK: false}
	}

	trimmedQuery := core.Trim(flux)
	if trimmedQuery == "" {
		return storeInstance.queryJournalRows(
			"SELECT bucket_name, measurement, fields_json, tags_json, committed_at, archived_at FROM " + journalEntriesTableName + " WHERE archived_at IS NULL ORDER BY committed_at",
		)
	}
	if core.HasPrefix(trimmedQuery, "SELECT") || core.HasPrefix(trimmedQuery, "select") {
		return storeInstance.queryJournalRows(trimmedQuery)
	}

	selectSQL, arguments, err := storeInstance.queryJournalFlux(trimmedQuery)
	if err != nil {
		return core.Result{Value: err, OK: false}
	}
	return storeInstance.queryJournalRows(selectSQL, arguments...)
}

func (storeInstance *Store) queryJournalRows(query string, arguments ...any) core.Result {
	rows, err := storeInstance.database.Query(query, arguments...)
	if err != nil {
		return core.Result{Value: core.E("store.QueryJournal", "query rows", err), OK: false}
	}
	defer rows.Close()

	rowMaps, err := queryRowsAsMaps(rows)
	if err != nil {
		return core.Result{Value: core.E("store.QueryJournal", "scan rows", err), OK: false}
	}
	return core.Result{Value: inflateJournalRows(rowMaps), OK: true}
}

func (storeInstance *Store) queryJournalFlux(flux string) (string, []any, error) {
	builder := core.NewBuilder()
	builder.WriteString("SELECT bucket_name, measurement, fields_json, tags_json, committed_at, archived_at FROM ")
	builder.WriteString(journalEntriesTableName)
	builder.WriteString(" WHERE archived_at IS NULL")

	var arguments []any
	if bucket := quotedSubmatch(journalBucketPattern, flux); bucket != "" {
		builder.WriteString(" AND bucket_name = ?")
		arguments = append(arguments, bucket)
	}
	if measurement := quotedSubmatch(journalMeasurementPattern, flux); measurement != "" {
		builder.WriteString(" AND measurement = ?")
		arguments = append(arguments, measurement)
	}

	rangeMatch := quotedSubmatch(journalRangePattern, flux)
	if rangeMatch == "" {
		rangeMatch = regexpSubmatch(journalRangePattern, flux, 1)
	}
	if rangeMatch != "" {
		startTime, err := fluxStartTime(core.Trim(rangeMatch))
		if err != nil {
			return "", nil, core.E("store.QueryJournal", "parse range", err)
		}
		builder.WriteString(" AND committed_at >= ?")
		arguments = append(arguments, startTime.UnixMilli())
	}

	builder.WriteString(" ORDER BY committed_at")
	return builder.String(), arguments, nil
}

func (storeInstance *Store) journalBucket() string {
	if storeInstance.journal.bucket == "" {
		return defaultJournalBucket
	}
	return storeInstance.journal.bucket
}

func ensureJournalSchema(database schemaDatabase) error {
	if _, err := database.Exec(createJournalEntriesTableSQL); err != nil {
		return err
	}
	if _, err := database.Exec(
		"CREATE INDEX IF NOT EXISTS journal_entries_bucket_committed_at_idx ON " + journalEntriesTableName + " (bucket_name, committed_at)",
	); err != nil {
		return err
	}
	return nil
}

func insertJournalEntry(
	executor journalExecutor,
	bucket, measurement, fieldsJSON, tagsJSON string,
	committedAt int64,
) error {
	_, err := executor.Exec(
		"INSERT INTO "+journalEntriesTableName+" (bucket_name, measurement, fields_json, tags_json, committed_at, archived_at) VALUES (?, ?, ?, ?, ?, NULL)",
		bucket,
		measurement,
		fieldsJSON,
		tagsJSON,
		committedAt,
	)
	return err
}

func jsonString(value any, operation, message string) (string, error) {
	result := core.JSONMarshal(value)
	if !result.OK {
		return "", core.E(operation, message, result.Value.(error))
	}
	return string(result.Value.([]byte)), nil
}

func fluxStartTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, core.E("store.fluxStartTime", "range value is empty", nil)
	}
	if core.HasSuffix(value, "d") {
		days, err := strconv.Atoi(core.TrimSuffix(value, "d"))
		if err != nil {
			return time.Time{}, err
		}
		return time.Now().Add(time.Duration(days) * 24 * time.Hour), nil
	}
	lookback, err := time.ParseDuration(value)
	if err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(lookback), nil
}

func quotedSubmatch(pattern *regexp.Regexp, value string) string {
	match := pattern.FindStringSubmatch(value)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func regexpSubmatch(pattern *regexp.Regexp, value string, index int) string {
	match := pattern.FindStringSubmatch(value)
	if len(match) <= index {
		return ""
	}
	return match[index]
}

func queryRowsAsMaps(rows *sql.Rows) ([]map[string]any, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any
	for rows.Next() {
		rawValues := make([]any, len(columnNames))
		scanTargets := make([]any, len(columnNames))
		for i := range rawValues {
			scanTargets[i] = &rawValues[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(columnNames))
		for i, columnName := range columnNames {
			row[columnName] = normaliseRowValue(rawValues[i])
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func inflateJournalRows(rows []map[string]any) []map[string]any {
	for _, row := range rows {
		if fieldsJSON, ok := row["fields_json"].(string); ok {
			fields := make(map[string]any)
			result := core.JSONUnmarshalString(fieldsJSON, &fields)
			if result.OK {
				row["fields"] = fields
			}
		}
		if tagsJSON, ok := row["tags_json"].(string); ok {
			tags := make(map[string]string)
			result := core.JSONUnmarshalString(tagsJSON, &tags)
			if result.OK {
				row["tags"] = tags
			}
		}
	}
	return rows
}

func normaliseRowValue(value any) any {
	switch typedValue := value.(type) {
	case []byte:
		return string(typedValue)
	default:
		return typedValue
	}
}
