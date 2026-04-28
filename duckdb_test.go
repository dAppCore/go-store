package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestDuckdb_OpenDuckDBReadWrite_Good(t *T) {
	path := Path(t.TempDir(), "rw.duckdb")
	database, err := store.OpenDuckDBReadWrite(path)
	RequireNoError(t, err)
	defer database.Close()
	AssertEqual(t, path, database.Path())
}

func TestDuckdb_OpenDuckDBReadWrite_Bad(t *T) {
	database, err := store.OpenDuckDBReadWrite(Path(t.TempDir(), "missing", "db.duckdb"))
	AssertError(t, err)
	AssertNil(t, database)
}

func TestDuckdb_OpenDuckDBReadWrite_Ugly(t *T) {
	path := Path(t.TempDir(), "with space.duckdb")
	database, err := store.OpenDuckDBReadWrite(path)
	RequireNoError(t, err)
	defer database.Close()
	AssertContains(t, database.Path(), "space")
}

func TestDuckdb_OpenDuckDB_Good(t *T) {
	path := Path(t.TempDir(), "ro.duckdb")
	writer, err := store.OpenDuckDBReadWrite(path)
	RequireNoError(t, err)
	RequireNoError(t, writer.Close())
	reader, err := store.OpenDuckDB(path)
	RequireNoError(t, err)
	defer reader.Close()
	AssertEqual(t, path, reader.Path())
}

func TestDuckdb_OpenDuckDB_Bad(t *T) {
	reader, err := store.OpenDuckDB(Path(t.TempDir(), "missing.duckdb"))
	AssertError(t, err)
	AssertNil(t, reader)
}

func TestDuckdb_OpenDuckDB_Ugly(t *T) {
	path := Path(t.TempDir(), "ro spaced.duckdb")
	writer, err := store.OpenDuckDBReadWrite(path)
	RequireNoError(t, err)
	RequireNoError(t, writer.Close())
	reader, err := store.OpenDuckDB(path)
	RequireNoError(t, err)
	defer reader.Close()
	AssertContains(t, reader.Path(), "spaced")
}

func TestDuckdb_DuckDB_Close_Good(t *T) {
	database := ax7DuckDB(t)
	path := database.Path()
	err := database.Close()
	AssertNoError(t, err)
	AssertEqual(t, path, database.Path())
}

func TestDuckdb_DuckDB_Close_Bad(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Close())
	err := database.Exec("SELECT 1")
	AssertError(t, err)
}

func TestDuckdb_DuckDB_Close_Ugly(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Close())
	err := database.Close()
	AssertNoError(t, err)
}

func TestDuckdb_DuckDB_Path_Good(t *T) {
	database := ax7DuckDB(t)
	path := database.Path()
	AssertContains(t, path, "ax7.duckdb")
}

func TestDuckdb_DuckDB_Path_Bad(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Close())
	path := database.Path()
	AssertContains(t, path, "ax7.duckdb")
}

func TestDuckdb_DuckDB_Path_Ugly(t *T) {
	path := Path(t.TempDir(), "custom.duckdb")
	database, err := store.OpenDuckDBReadWrite(path)
	RequireNoError(t, err)
	defer database.Close()
	AssertEqual(t, path, database.Path())
}

func TestDuckdb_DuckDB_Conn_Good(t *T) {
	database := ax7DuckDB(t)
	conn := database.Conn()
	AssertNotNil(t, conn)
}

func TestDuckdb_DuckDB_Conn_Bad(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Close())
	conn := database.Conn()
	AssertNotNil(t, conn)
}

func TestDuckdb_DuckDB_Conn_Ugly(t *T) {
	database := ax7DuckDB(t)
	err := database.Conn().Ping()
	AssertNoError(t, err)
}

func TestDuckdb_DuckDB_Exec_Good(t *T) {
	database := ax7DuckDB(t)
	err := database.Exec("CREATE TABLE ax7_exec (id INTEGER)")
	AssertNoError(t, err)
}

func TestDuckdb_DuckDB_Exec_Bad(t *T) {
	database := ax7DuckDB(t)
	err := database.Exec("CREATE TABLE")
	AssertError(t, err)
}

func TestDuckdb_DuckDB_Exec_Ugly(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Exec("CREATE TABLE ax7_exec (id INTEGER)"))
	err := database.Exec("INSERT INTO ax7_exec VALUES (?)", 1)
	AssertNoError(t, err)
}

func TestDuckdb_DuckDB_QueryRowScan_Good(t *T) {
	database := ax7DuckDB(t)
	var n int
	err := database.QueryRowScan("SELECT 42", &n)
	AssertNoError(t, err)
	AssertEqual(t, 42, n)
}

func TestDuckdb_DuckDB_QueryRowScan_Bad(t *T) {
	database := ax7DuckDB(t)
	var n int
	err := database.QueryRowScan("SELECT * FROM missing", &n)
	AssertError(t, err)
}

func TestDuckdb_DuckDB_QueryRowScan_Ugly(t *T) {
	database := ax7DuckDB(t)
	var text string
	err := database.QueryRowScan("SELECT 'agent'", &text)
	AssertNoError(t, err)
	AssertEqual(t, "agent", text)
}

func TestDuckdb_DuckDB_QueryGoldenSet_Good(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	rows, err := database.QueryGoldenSet(1)
	AssertNoError(t, err)
	AssertEqual(t, "seed-1", rows[0].SeedID)
}

func TestDuckdb_DuckDB_QueryGoldenSet_Bad(t *T) {
	database := ax7DuckDB(t)
	rows, err := database.QueryGoldenSet(1)
	AssertError(t, err)
	AssertNil(t, rows)
}

func TestDuckdb_DuckDB_QueryGoldenSet_Ugly(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	rows, err := database.QueryGoldenSet(1000)
	AssertNoError(t, err)
	AssertEmpty(t, rows)
}

func TestDuckdb_DuckDB_CountGoldenSet_Good(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	count, err := database.CountGoldenSet()
	AssertNoError(t, err)
	AssertEqual(t, 1, count)
}

func TestDuckdb_DuckDB_CountGoldenSet_Bad(t *T) {
	database := ax7DuckDB(t)
	count, err := database.CountGoldenSet()
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestDuckdb_DuckDB_CountGoldenSet_Ugly(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Exec("CREATE TABLE golden_set (idx INTEGER)"))
	count, err := database.CountGoldenSet()
	AssertNoError(t, err)
	AssertEqual(t, 0, count)
}

func TestDuckdb_DuckDB_QueryExpansionPrompts_Good(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	rows, err := database.QueryExpansionPrompts("pending", 10)
	AssertNoError(t, err)
	AssertEqual(t, "seed-7", rows[0].SeedID)
}

func TestDuckdb_DuckDB_QueryExpansionPrompts_Bad(t *T) {
	database := ax7DuckDB(t)
	rows, err := database.QueryExpansionPrompts("pending", 10)
	AssertError(t, err)
	AssertNil(t, rows)
}

func TestDuckdb_DuckDB_QueryExpansionPrompts_Ugly(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	rows, err := database.QueryExpansionPrompts("done", 0)
	AssertNoError(t, err)
	AssertEmpty(t, rows)
}

func TestDuckdb_DuckDB_CountExpansionPrompts_Good(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	total, pending, err := database.CountExpansionPrompts()
	AssertNoError(t, err)
	AssertEqual(t, 1, total)
	AssertEqual(t, 1, pending)
}

func TestDuckdb_DuckDB_CountExpansionPrompts_Bad(t *T) {
	database := ax7DuckDB(t)
	total, pending, err := database.CountExpansionPrompts()
	AssertError(t, err)
	AssertEqual(t, 0, total)
	AssertEqual(t, 0, pending)
}

func TestDuckdb_DuckDB_CountExpansionPrompts_Ugly(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Exec("CREATE TABLE expansion_prompts (status VARCHAR)"))
	total, pending, err := database.CountExpansionPrompts()
	AssertNoError(t, err)
	AssertEqual(t, 0, total)
	AssertEqual(t, 0, pending)
}

func TestDuckdb_DuckDB_UpdateExpansionStatus_Good(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	err := database.UpdateExpansionStatus(7, "done")
	AssertNoError(t, err)
	rows, queryErr := database.QueryExpansionPrompts("done", 1)
	AssertNoError(t, queryErr)
	AssertLen(t, rows, 1)
}

func TestDuckdb_DuckDB_UpdateExpansionStatus_Bad(t *T) {
	database := ax7DuckDB(t)
	err := database.UpdateExpansionStatus(7, "done")
	AssertError(t, err)
}

func TestDuckdb_DuckDB_UpdateExpansionStatus_Ugly(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	err := database.UpdateExpansionStatus(999, "done")
	AssertNoError(t, err)
}

func TestDuckdb_DuckDB_QueryRows_Good(t *T) {
	database := ax7DuckDB(t)
	rows, err := database.QueryRows("SELECT 1 AS n")
	AssertNoError(t, err)
	AssertEqual(t, "1", Sprint(rows[0]["n"]))
}

func TestDuckdb_DuckDB_QueryRows_Bad(t *T) {
	database := ax7DuckDB(t)
	rows, err := database.QueryRows("SELECT * FROM missing")
	AssertError(t, err)
	AssertNil(t, rows)
}

func TestDuckdb_DuckDB_QueryRows_Ugly(t *T) {
	database := ax7DuckDB(t)
	rows, err := database.QueryRows("SELECT 'agent' AS name")
	AssertNoError(t, err)
	AssertEqual(t, "agent", rows[0]["name"])
}

func TestDuckdb_DuckDB_EnsureScoringTables_Good(t *T) {
	database := ax7DuckDB(t)
	err := database.EnsureScoringTables()
	AssertNoError(t, err)
	AssertNotEmpty(t, database.Conn())
}

func TestDuckdb_DuckDB_EnsureScoringTables_Bad(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Close())
	err := database.EnsureScoringTables()
	AssertError(t, err)
}

func TestDuckdb_DuckDB_EnsureScoringTables_Ugly(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.EnsureScoringTables())
	err := database.EnsureScoringTables()
	AssertNoError(t, err)
}

func TestDuckdb_DuckDB_WriteScoringResult_Good(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.EnsureScoringTables())
	err := database.WriteScoringResult("model", "prompt", "suite", "dimension", 0.5)
	AssertNoError(t, err)
}

func TestDuckdb_DuckDB_WriteScoringResult_Bad(t *T) {
	database := ax7DuckDB(t)
	err := database.WriteScoringResult("model", "prompt", "suite", "dimension", 0.5)
	AssertError(t, err)
}

func TestDuckdb_DuckDB_WriteScoringResult_Ugly(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.EnsureScoringTables())
	err := database.WriteScoringResult("", "", "", "", 0)
	AssertNoError(t, err)
}

func TestDuckdb_DuckDB_TableCounts_Good(t *T) {
	database := ax7DuckDB(t)
	ax7SeedDuckDB(t, database)
	counts, err := database.TableCounts()
	AssertNoError(t, err)
	AssertEqual(t, 1, counts["golden_set"])
}

func TestDuckdb_DuckDB_TableCounts_Bad(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.Close())
	counts, err := database.TableCounts()
	AssertNoError(t, err)
	AssertEmpty(t, counts)
}

func TestDuckdb_DuckDB_TableCounts_Ugly(t *T) {
	database := ax7DuckDB(t)
	RequireNoError(t, database.EnsureScoringTables())
	counts, err := database.TableCounts()
	AssertNoError(t, err)
	AssertContains(t, counts, "scoring_results")
}
