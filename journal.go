package store

import (
	"database/sql"
	"regexp"
	"time"

	core "dappco.re/go"
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
	journalBucketPattern       = regexp.MustCompile(`bucket:\s*"([^"]+)"`)
	journalMeasurementPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?:_measurement|measurement)\s*==\s*"([^"]+)"`),
		regexp.MustCompile(`\[\s*"(?:_measurement|measurement)"\s*\]\s*==\s*"([^"]+)"`),
	}
	journalBucketEqualityPatterns = []*regexp.Regexp{
		regexp.MustCompile(`r\.(?:_bucket|bucket|bucket_name)\s*==\s*"([^"]+)"`),
		regexp.MustCompile(`r\[\s*"(?:_bucket|bucket|bucket_name)"\s*\]\s*==\s*"([^"]+)"`),
	}
	journalStringEqualityPatterns = []*regexp.Regexp{
		regexp.MustCompile(`r\.([a-zA-Z0-9_:-]+)\s*==\s*"([^"]+)"`),
		regexp.MustCompile(`r\[\s*"([a-zA-Z0-9_:-]+)"\s*\]\s*==\s*"([^"]+)"`),
	}
	journalScalarEqualityPatterns = []*regexp.Regexp{
		regexp.MustCompile(`r\.([a-zA-Z0-9_:-]+)\s*==\s*(true|false|-?[0-9]+(?:\.[0-9]+)?)`),
		regexp.MustCompile(`r\[\s*"([a-zA-Z0-9_:-]+)"\s*\]\s*==\s*(true|false|-?[0-9]+(?:\.[0-9]+)?)`),
	}
)

type journalEqualityFilter struct {
	columnName    string
	filterValue   any
	stringCompare bool
}

type journalExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// Usage example: `result := storeInstance.CommitToJournal("scroll-session", map[string]any{"like": 4}, map[string]string{"workspace": "scroll-session"})`
// Workspace.Commit uses this same journal write path before it updates the
// summary row in `workspace:NAME`.
func (storeInstance *Store) CommitToJournal(measurement string, fields map[string]any, tags map[string]string) core.Result {
	if err := storeInstance.ensureReady(opCommitToJournal); err != nil {
		return core.Fail(err)
	}
	if measurement == "" {
		return core.Fail(core.E(opCommitToJournal, "measurement is empty", nil))
	}
	if fields == nil {
		fields = map[string]any{}
	}
	if tags == nil {
		tags = map[string]string{}
	}
	if err := ensureJournalSchema(storeInstance.sqliteDatabase); err != nil {
		return core.Fail(core.E(opCommitToJournal, "ensure journal schema", err))
	}

	fieldsJSON, err := marshalJSONText(fields, opCommitToJournal, "marshal fields")
	if err != nil {
		return core.Fail(err)
	}
	tagsJSON, err := marshalJSONText(tags, opCommitToJournal, "marshal tags")
	if err != nil {
		return core.Fail(err)
	}

	committedAt := time.Now().UnixMilli()
	if err := commitJournalEntry(
		storeInstance.sqliteDatabase,
		storeInstance.journalBucket(),
		measurement,
		fieldsJSON,
		tagsJSON,
		committedAt,
	); err != nil {
		return core.Fail(core.E(opCommitToJournal, "insert journal entry", err))
	}

	return core.Ok(
		map[string]any{
			"bucket":       storeInstance.journalBucket(),
			"measurement":  measurement,
			"fields":       cloneAnyMap(fields),
			"tags":         cloneStringMap(tags),
			"committed_at": committedAt,
		},
	)
}

// Usage example: `result := storeInstance.QueryJournal(\`from(bucket: "events") |> range(start: -24h) |> filter(fn: (r) => r.workspace == "session-a")\`)`
// Usage example: `result := storeInstance.QueryJournal("SELECT measurement, committed_at FROM journal_entries ORDER BY committed_at")`
func (storeInstance *Store) QueryJournal(flux string) core.Result {
	if err := storeInstance.ensureReady(opQueryJournal); err != nil {
		return core.Fail(err)
	}
	if err := ensureJournalSchema(storeInstance.sqliteDatabase); err != nil {
		return core.Fail(core.E(opQueryJournal, "ensure journal schema", err))
	}

	trimmedQuery := core.Trim(flux)
	if trimmedQuery == "" {
		return storeInstance.queryJournalRows(
			"SELECT bucket_name, measurement, fields_json, tags_json, committed_at, archived_at FROM " + journalEntriesTableName + " WHERE archived_at IS NULL ORDER BY committed_at, entry_id",
		)
	}
	if isRawSQLJournalQuery(trimmedQuery) {
		return storeInstance.queryJournalRows(trimmedQuery)
	}

	selectSQL, arguments, err := storeInstance.queryJournalFromFlux(trimmedQuery)
	if err != nil {
		return core.Fail(err)
	}
	return storeInstance.queryJournalRows(selectSQL, arguments...)
}

func isRawSQLJournalQuery(query string) bool {
	upperQuery := core.Upper(core.Trim(query))
	return core.HasPrefix(upperQuery, "SELECT") ||
		core.HasPrefix(upperQuery, "WITH") ||
		core.HasPrefix(upperQuery, "EXPLAIN") ||
		core.HasPrefix(upperQuery, "PRAGMA")
}

func (storeInstance *Store) queryJournalRows(query string, arguments ...any) core.Result {
	rows, err := storeInstance.sqliteDatabase.Query(query, arguments...)
	if err != nil {
		return core.Fail(core.E(opQueryJournal, "query rows", err))
	}
	defer func() { _ = rows.Close() }()

	rowMaps, err := queryRowsAsMaps(rows)
	if err != nil {
		return core.Fail(core.E(opQueryJournal, "scan rows", err))
	}
	return core.Ok(inflateJournalRows(rowMaps))
}

func (storeInstance *Store) queryJournalFromFlux(flux string) (string, []any, error) {
	queryBuilder := core.NewBuilder()
	queryBuilder.WriteString("SELECT bucket_name, measurement, fields_json, tags_json, committed_at, archived_at FROM ")
	queryBuilder.WriteString(journalEntriesTableName)
	queryBuilder.WriteString(" WHERE archived_at IS NULL")

	filters, err := journalFluxSQLFilters(flux)
	if err != nil {
		return "", nil, err
	}
	var queryArguments []any
	for _, filter := range filters {
		queryBuilder.WriteString(filter.clause)
		queryArguments = append(queryArguments, filter.args...)
	}

	queryBuilder.WriteString(" ORDER BY committed_at, entry_id")
	return queryBuilder.String(), queryArguments, nil
}

type journalSQLFilter struct {
	clause string
	args   []any
}

func journalFluxSQLFilters(flux string) ([]journalSQLFilter, error) {
	var filters []journalSQLFilter
	filters = append(filters, journalNamedFluxFilters(flux)...)
	rangeFilters, err := journalRangeSQLFilters(flux)
	if err != nil {
		return nil, err
	}
	filters = append(filters, rangeFilters...)
	filters = append(filters, journalBucketEqualitySQLFilters(flux)...)
	filters = append(filters, journalEqualitySQLFilters(flux)...)
	return filters, nil
}

func journalNamedFluxFilters(flux string) []journalSQLFilter {
	var filters []journalSQLFilter
	if bucket := quotedSubmatch(journalBucketPattern, flux); bucket != "" {
		filters = append(filters, journalSQLFilter{clause: " AND bucket_name = ?", args: []any{bucket}})
	}
	if measurement := firstQuotedSubmatch(journalMeasurementPatterns, flux); measurement != "" {
		filters = append(filters, journalSQLFilter{clause: " AND measurement = ?", args: []any{measurement}})
	}
	return filters
}

func journalRangeSQLFilters(flux string) ([]journalSQLFilter, error) {
	startRange, stopRange := journalRangeBounds(flux)
	var filters []journalSQLFilter
	if startRange != "" {
		startTime, err := parseFluxTime(core.Trim(startRange))
		if err != nil {
			return nil, core.E(opQueryJournal, "parse range", err)
		}
		filters = append(filters, journalSQLFilter{clause: " AND committed_at >= ?", args: []any{startTime.UnixMilli()}})
	}
	if stopRange != "" {
		stopTime, err := parseFluxTime(core.Trim(stopRange))
		if err != nil {
			return nil, core.E(opQueryJournal, "parse range", err)
		}
		filters = append(filters, journalSQLFilter{clause: " AND committed_at < ?", args: []any{stopTime.UnixMilli()}})
	}
	return filters, nil
}

func journalBucketEqualitySQLFilters(flux string) []journalSQLFilter {
	var filters []journalSQLFilter
	for _, pattern := range journalBucketEqualityPatterns {
		for _, match := range pattern.FindAllStringSubmatch(flux, -1) {
			if len(match) >= 2 {
				filters = append(filters, journalSQLFilter{clause: " AND bucket_name = ?", args: []any{match[1]}})
			}
		}
	}
	return filters
}

func journalEqualitySQLFilters(flux string) []journalSQLFilter {
	var filters []journalSQLFilter
	for _, filter := range journalEqualityFilters(flux) {
		filters = append(filters, journalEqualitySQLFilter(filter))
	}
	return filters
}

func journalEqualitySQLFilter(filter journalEqualityFilter) journalSQLFilter {
	if filter.stringCompare {
		return journalSQLFilter{
			clause: " AND (CAST(json_extract(tags_json, '$.\"' || ? || '\"') AS TEXT) = ? OR CAST(json_extract(fields_json, '$.\"' || ? || '\"') AS TEXT) = ?)",
			args:   []any{filter.columnName, filter.filterValue, filter.columnName, filter.filterValue},
		}
	}
	return journalSQLFilter{
		clause: " AND json_extract(fields_json, '$.\"' || ? || '\"') = ?",
		args:   []any{filter.columnName, filter.filterValue},
	}
}

func (storeInstance *Store) journalBucket() string {
	if storeInstance.journalConfiguration.BucketName == "" {
		return defaultJournalBucket
	}
	return storeInstance.journalConfiguration.BucketName
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

func commitJournalEntry(
	executor journalExecutor,
	bucket, measurement, fieldsJSON, tagsJSON string,
	committedAt int64,
) error {
	_, err := executor.Exec(
		sqlInsertIntoPrefix+journalEntriesTableName+" (bucket_name, measurement, fields_json, tags_json, committed_at, archived_at) VALUES (?, ?, ?, ?, ?, NULL)",
		bucket,
		measurement,
		fieldsJSON,
		tagsJSON,
		committedAt,
	)
	return err
}

func marshalJSONText(value any, operation, message string) (string, error) {
	result := core.JSONMarshal(value)
	if !result.OK {
		return "", core.E(operation, message, result.Value.(error))
	}
	return string(result.Value.([]byte)), nil
}

func journalRangeBounds(flux string) (string, string) {
	rangeIndex := indexOfSubstring(flux, "range(")
	if rangeIndex < 0 {
		return "", ""
	}
	contentStart := rangeIndex + len("range(")
	depth := 1
	contentEnd := -1
scanRange:
	for i := contentStart; i < len(flux); i++ {
		switch flux[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				contentEnd = i
				break scanRange
			}
		}
	}
	if contentEnd < 0 || contentEnd <= contentStart {
		return "", ""
	}

	content := flux[contentStart:contentEnd]
	startPrefix := "start:"
	startIndex := indexOfSubstring(content, startPrefix)
	if startIndex < 0 {
		return "", ""
	}
	startIndex += len(startPrefix)
	start := core.Trim(content[startIndex:])
	stop := ""
	if stopIndex := indexOfSubstring(content, ", stop:"); stopIndex >= 0 {
		start = core.Trim(content[startIndex:stopIndex])
		stop = core.Trim(content[stopIndex+len(", stop:"):])
	} else if stopIndex := indexOfSubstring(content, ",stop:"); stopIndex >= 0 {
		start = core.Trim(content[startIndex:stopIndex])
		stop = core.Trim(content[stopIndex+len(",stop:"):])
	}
	return start, stop
}

func indexOfSubstring(text, substring string) int {
	if substring == "" {
		return 0
	}
	if len(substring) > len(text) {
		return -1
	}
	for i := 0; i <= len(text)-len(substring); i++ {
		if text[i:i+len(substring)] == substring {
			return i
		}
	}
	return -1
}

func parseFluxTime(value string) (time.Time, error) {
	value = core.Trim(value)
	if value == "" {
		return time.Time{}, core.E("store.parseFluxTime", "range value is empty", nil)
	}
	value = firstStringOrEmpty(core.Split(value, ","))
	value = core.Trim(value)
	if core.HasPrefix(value, "time(v:") && core.HasSuffix(value, ")") {
		value = core.Trim(core.TrimSuffix(core.TrimPrefix(value, "time(v:"), ")"))
	}
	if core.HasPrefix(value, `"`) && core.HasSuffix(value, `"`) {
		value = core.TrimSuffix(core.TrimPrefix(value, `"`), `"`)
	}
	if value == "now()" {
		return time.Now(), nil
	}
	if core.HasSuffix(value, "d") {
		days, err := parseJournalInt64(core.TrimSuffix(value, "d"))
		if err != nil {
			return time.Time{}, err
		}
		return time.Now().Add(time.Duration(days) * 24 * time.Hour), nil
	}
	lookback, err := time.ParseDuration(value)
	if err == nil {
		return time.Now().Add(lookback), nil
	}
	parsedTime, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsedTime, nil
}

func quotedSubmatch(pattern *regexp.Regexp, value string) string {
	match := pattern.FindStringSubmatch(value)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func firstQuotedSubmatch(patterns []*regexp.Regexp, value string) string {
	for _, pattern := range patterns {
		if match := quotedSubmatch(pattern, value); match != "" {
			return match
		}
	}
	return ""
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

func journalEqualityFilters(flux string) []journalEqualityFilter {
	var filters []journalEqualityFilter
	filters = append(filters, journalStringEqualityFilters(flux)...)
	filters = append(filters, journalScalarEqualityFilters(flux)...)
	return filters
}

func journalStringEqualityFilters(flux string) []journalEqualityFilter {
	var filters []journalEqualityFilter
	for _, pattern := range journalStringEqualityPatterns {
		for _, match := range pattern.FindAllStringSubmatch(flux, -1) {
			if len(match) >= 3 {
				filters = appendJournalEqualityFilter(filters, match[1], match[2], true)
			}
		}
	}
	return filters
}

func journalScalarEqualityFilters(flux string) []journalEqualityFilter {
	var filters []journalEqualityFilter
	for _, pattern := range journalScalarEqualityPatterns {
		for _, match := range pattern.FindAllStringSubmatch(flux, -1) {
			if len(match) < 3 {
				continue
			}
			filterValue, ok := parseJournalScalarValue(match[2])
			if ok {
				filters = appendJournalEqualityFilter(filters, match[1], filterValue, false)
			}
		}
	}
	return filters
}

func appendJournalEqualityFilter(filters []journalEqualityFilter, columnName string, filterValue any, stringCompare bool) []journalEqualityFilter {
	if isReservedJournalFilterColumn(columnName) {
		return filters
	}
	return append(filters, journalEqualityFilter{
		columnName:    columnName,
		filterValue:   filterValue,
		stringCompare: stringCompare,
	})
}

func isReservedJournalFilterColumn(columnName string) bool {
	switch columnName {
	case "_measurement", "measurement", "_bucket", "bucket", "bucket_name":
		return true
	default:
		return false
	}
}

func parseJournalScalarValue(value string) (any, bool) {
	switch value {
	case "true":
		return true, true
	case "false":
		return false, true
	}

	if integerValue, err := parseJournalInt64(value); err == nil {
		return integerValue, true
	}
	if floatValue, err := parseJournalFloat64(value); err == nil {
		return floatValue, true
	}
	return nil, false
}

func parseJournalInt64(value string) (int64, error) {
	prefix, err := parseJournalNumberPrefix(value, opParseJournalInt64, "integer")
	if err != nil {
		return 0, err
	}
	limit := journalIntegerLimit(prefix.negative)
	parsed, err := parseJournalUnsignedInteger(value, prefix.index, limit)
	if err != nil {
		return 0, err
	}
	if prefix.negative {
		if parsed == uint64(1<<63) {
			return -1 << 63, nil
		}
		return -int64(parsed), nil
	}
	return int64(parsed), nil
}

func parseJournalFloat64(value string) (float64, error) {
	prefix, err := parseJournalNumberPrefix(value, opParseJournalFloat64, "float")
	if err != nil {
		return 0, err
	}
	parsed, index, digits, err := parseJournalFloatWhole(value, prefix.index)
	if err != nil {
		return 0, err
	}
	parsed, index, digits = parseJournalFloatFraction(value, index, parsed, digits)
	if digits == 0 {
		return 0, core.E(opParseJournalFloat64, "float value has no digits", nil)
	}
	if index != len(value) {
		return 0, core.E(opParseJournalFloat64, "float value contains invalid characters", nil)
	}
	if prefix.negative {
		return -parsed, nil
	}
	return parsed, nil
}

const maxJournalFloat64 = 1.79769313486231570814527423731704357e+308

type journalNumberPrefix struct {
	negative bool
	index    int
}

func parseJournalNumberPrefix(value, operation, valueName string) (journalNumberPrefix, error) {
	if value == "" {
		return journalNumberPrefix{}, core.E(operation, core.Concat(valueName, " value is empty"), nil)
	}
	prefix := journalNumberPrefix{}
	if value[0] == '-' || value[0] == '+' {
		prefix.negative = value[0] == '-'
		prefix.index = 1
		if prefix.index == len(value) {
			return journalNumberPrefix{}, core.E(operation, core.Concat(valueName, " value has no digits"), nil)
		}
	}
	return prefix, nil
}

func journalIntegerLimit(negative bool) uint64 {
	if negative {
		return uint64(1 << 63)
	}
	return uint64(1<<63 - 1)
}

func parseJournalUnsignedInteger(value string, index int, limit uint64) (uint64, error) {
	var parsed uint64
	for ; index < len(value); index++ {
		character := value[index]
		if character < '0' || character > '9' {
			return 0, core.E(opParseJournalInt64, "integer value contains non-digit characters", nil)
		}
		digit := uint64(character - '0')
		if parsed > (limit-digit)/10 {
			return 0, core.E(opParseJournalInt64, "integer value is out of range", nil)
		}
		parsed = parsed*10 + digit
	}
	return parsed, nil
}

func parseJournalFloatWhole(value string, index int) (float64, int, int, error) {
	var parsed float64
	digits := 0
	for index < len(value) && isJournalDigit(value[index]) {
		parsed = parsed*10 + float64(value[index]-'0')
		if parsed > maxJournalFloat64 {
			return 0, index, digits, core.E(opParseJournalFloat64, "float value is out of range", nil)
		}
		digits++
		index++
	}
	return parsed, index, digits, nil
}

func parseJournalFloatFraction(value string, index int, parsed float64, digits int) (float64, int, int) {
	if index >= len(value) || value[index] != '.' {
		return parsed, index, digits
	}
	index++
	scale := 0.1
	for index < len(value) && isJournalDigit(value[index]) {
		parsed += float64(value[index]-'0') * scale
		scale /= 10
		digits++
		index++
	}
	return parsed, index, digits
}

func isJournalDigit(character byte) bool {
	return character >= '0' && character <= '9'
}

func cloneAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}
