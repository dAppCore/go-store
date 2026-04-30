// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bufio"
	"database/sql"
	"io"
	"io/fs"

	core "dappco.re/go"
)

// localFs provides unrestricted filesystem access for import operations.
var localFs = (&core.Fs{}).New("/")

type duckDBImportSession interface {
	exec(query string, args ...any) core.Result
	queryRowScan(query string, dest any, args ...any) core.Result
}

type duckDBImportTransaction struct {
	transaction *sql.Tx
}

func (session duckDBImportTransaction) exec(query string, args ...any) core.Result {
	_, err := session.transaction.Exec(query, args...)
	if err != nil {
		return core.Fail(core.E("store.duckDBImportTransaction.Exec", "execute query", err))
	}
	return core.Ok(nil)
}

func (session duckDBImportTransaction) queryRowScan(query string, dest any, args ...any) core.Result {
	if err := session.transaction.QueryRow(query, args...).Scan(dest); err != nil {
		return core.Fail(core.E("store.duckDBImportTransaction.QueryRowScan", "scan row", err))
	}
	return core.Ok(nil)
}

// ScpFunc is a callback for executing SCP file transfers.
// The function receives remote source and local destination paths.
//
// Usage example:
//
//	scp := func(remote, local string) error { return exec.Command("scp", remote, local).Run() }
type ScpFunc func(remote, local string) error

// ScpDirFunc is a callback for executing recursive SCP directory transfers.
// The function receives remote source and local destination directory paths.
//
// Usage example:
//
//	scpDir := func(remote, localDir string) error { return exec.Command("scp", "-r", remote, localDir).Run() }
type ScpDirFunc func(remote, localDir string) error

// ImportConfig holds options for the import-all operation.
//
// Usage example:
//
//	cfg := store.ImportConfig{DataDir: "/Volumes/Data/lem", SkipM3: true}
type ImportConfig struct {
	// SkipM3 disables pulling files from the M3 host.
	//
	// Usage example:
	//
	//	cfg.SkipM3 // true
	SkipM3 bool

	// DataDir is the local directory containing LEM data files.
	//
	// Usage example:
	//
	//	cfg.DataDir // "/Volumes/Data/lem"
	DataDir string

	// M3Host is the SSH hostname for SCP operations. Defaults to "m3".
	//
	// Usage example:
	//
	//	cfg.M3Host // "m3"
	M3Host string

	// Scp copies a single file from the remote host. If nil, SCP is skipped.
	//
	// Usage example:
	//
	//	cfg.Scp("m3:/path/file.jsonl", "/local/file.jsonl")
	Scp ScpFunc

	// ScpDir copies a directory recursively from the remote host. If nil, SCP is skipped.
	//
	// Usage example:
	//
	//	cfg.ScpDir("m3:/path/dir/", "/local/dir/")
	ScpDir ScpDirFunc
}

// ImportAll imports all LEM data into DuckDB from M3 and local files.
//
// Usage example:
//
//	err := store.ImportAll(db, store.ImportConfig{DataDir: "/Volumes/Data/lem"}, os.Stdout)
func ImportAll(db *DuckDB, cfg ImportConfig, w io.Writer) core.Result {
	if db == nil || db.Conn() == nil {
		return core.Fail(core.E(opImportAll, "database is nil", nil))
	}

	transaction, err := db.Conn().Begin()
	if err != nil {
		return core.Fail(core.E(opImportAll, "begin import transaction", err))
	}
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := transaction.Rollback(); rollbackErr != nil {
				core.Error("import transaction rollback failed", "err", rollbackErr)
			}
		}
	}()

	run := &importAllRun{
		db:      db,
		cfg:     cfg,
		writer:  w,
		m3Host:  normalisedImportM3Host(cfg),
		totals:  make(map[string]int),
		session: duckDBImportTransaction{transaction: transaction},
	}

	for _, step := range []func() core.Result{
		run.importGoldenSet,
		run.importTrainingExamples,
		run.importBenchmarkResults,
		run.importBenchmarkQuestions,
		run.importSeeds,
	} {
		if result := step(); !result.OK {
			return result
		}
	}

	if err := transaction.Commit(); err != nil {
		return core.Fail(core.E(opImportAll, "commit import transaction", err))
	}
	committed = true

	run.printSummary()
	return core.Ok(nil)
}

type importAllRun struct {
	db      *DuckDB
	cfg     ImportConfig
	writer  io.Writer
	m3Host  string
	totals  map[string]int
	session duckDBImportSession
}

type importTrainingDirectory struct {
	name  string
	files []string
}

var importTrainingDirs = []importTrainingDirectory{
	{"training", []string{"training/train.jsonl", "training/valid.jsonl", "training/test.jsonl"}},
	{"training-2k", []string{"training-2k/train.jsonl", "training-2k/valid.jsonl", "training-2k/test.jsonl"}},
	{"training-expanded", []string{"training-expanded/train.jsonl", "training-expanded/valid.jsonl"}},
	{"training-book", []string{"training-book/train.jsonl", "training-book/valid.jsonl", "training-book/test.jsonl"}},
	{"training-conv", []string{"training-conv/train.jsonl", "training-conv/valid.jsonl", "training-conv/test.jsonl"}},
	{"gold-full", []string{"gold-full/train.jsonl", "gold-full/valid.jsonl"}},
	{"sovereignty-gold", []string{"sovereignty-gold/train.jsonl", "sovereignty-gold/valid.jsonl"}},
	{"composure-lessons", []string{"composure-lessons/train.jsonl", "composure-lessons/valid.jsonl"}},
	{"watts-full", []string{"watts-full/train.jsonl", "watts-full/valid.jsonl"}},
	{"watts-expanded", []string{"watts-expanded/train.jsonl", "watts-expanded/valid.jsonl"}},
	{"watts-composure", []string{"watts-composure-merged/train.jsonl", "watts-composure-merged/valid.jsonl"}},
	{"western-fresh", []string{"western-fresh/train.jsonl", "western-fresh/valid.jsonl"}},
	{"deepseek-soak", []string{"deepseek-western-soak/train.jsonl", "deepseek-western-soak/valid.jsonl"}},
	{"russian-bridge", []string{"russian-bridge/train.jsonl", "russian-bridge/valid.jsonl"}},
}

var (
	benchmarkQuestionNames       = []string{"truthfulqa", "gsm8k", "do_not_answer", "toxigen"}
	benchmarkResultDirectories   = []string{"results", "scale_results", "cross_arch_results", "deepseek-r1-7b"}
	standaloneBenchmarkFileNames = []string{"lem_bench", "lem_ethics", "lem_ethics_allen", "instruction_tuned", "abliterated", "base_pt"}
)

func normalisedImportM3Host(cfg ImportConfig) string {
	if cfg.M3Host != "" {
		return cfg.M3Host
	}
	return "m3"
}

func (run *importAllRun) importGoldenSet() core.Result {
	goldenPath := core.JoinPath(run.cfg.DataDir, "gold-15k.jsonl")
	if !run.cfg.SkipM3 && run.cfg.Scp != nil {
		core.Print(run.writer, "  Pulling golden set from M3...")
		remote := core.Sprintf("%s:/Volumes/Data/lem/responses/gold-15k.jsonl", run.m3Host)
		if err := run.cfg.Scp(remote, goldenPath); err != nil {
			core.Print(run.writer, "  WARNING: could not pull golden set from M3: %v", err)
		}
	}
	if !isFile(goldenPath) {
		return core.Ok(nil)
	}
	if result := run.session.exec("DROP TABLE IF EXISTS golden_set"); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "drop golden_set", err))
	}
	result := run.session.exec(core.Sprintf(`
			CREATE TABLE golden_set AS
			SELECT
				idx::INT AS idx,
			seed_id::VARCHAR AS seed_id,
			domain::VARCHAR AS domain,
			voice::VARCHAR AS voice,
			prompt::VARCHAR AS prompt,
			response::VARCHAR AS response,
			gen_time::DOUBLE AS gen_time,
			length(response)::INT AS char_count,
			length(response) - length(replace(response, ' ', '')) + 1 AS word_count
		FROM read_json_auto('%s', maximum_object_size=1048576)
	`, escapeSQLPath(goldenPath)))
	if !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "import golden_set", err))
	}
	var n int
	if result := run.session.queryRowScan("SELECT count(*) FROM golden_set", &n); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "count golden_set", err))
	}
	run.totals["golden_set"] = n
	core.Print(run.writer, "  golden_set: %d rows", n)
	return core.Ok(nil)
}

func (run *importAllRun) importTrainingExamples() core.Result {
	if result := run.pullTrainingSets(); !result.OK {
		return result
	}
	if result := run.session.exec("DROP TABLE IF EXISTS training_examples"); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "drop training_examples", err))
	}
	if result := run.session.exec(`
			CREATE TABLE training_examples (
				source VARCHAR,
				split VARCHAR,
			prompt TEXT,
			response TEXT,
			num_turns INT,
			full_messages TEXT,
			char_count INT
		)
	`); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "create training_examples", err))
	}

	trainingTotal := 0
	for _, trainingDir := range importTrainingDirs {
		n, result := run.importTrainingDirectory(trainingDir)
		if !result.OK {
			return result
		}
		trainingTotal += n
	}
	run.totals["training_examples"] = trainingTotal
	core.Print(run.writer, "  training_examples: %d rows", trainingTotal)
	return core.Ok(nil)
}

func (run *importAllRun) pullTrainingSets() core.Result {
	if run.cfg.SkipM3 || run.cfg.Scp == nil {
		return core.Ok(nil)
	}
	core.Print(run.writer, "  Pulling training sets from M3...")
	for _, trainingDir := range importTrainingDirs {
		for _, relativePath := range trainingDir.files {
			localPath := core.JoinPath(run.cfg.DataDir, relativePath)
			if result := localFs.EnsureDir(core.PathDir(localPath)); !result.OK {
				return core.Fail(core.E(opImportAll, "ensure training directory", result.Value.(error)))
			}
			remote := core.Sprintf("%s:/Volumes/Data/lem/%s", run.m3Host, relativePath)
			if err := run.cfg.Scp(remote, localPath); err != nil {
				core.Print(run.writer, "  WARNING: could not pull %s from M3: %v", relativePath, err)
			}
		}
	}
	return core.Ok(nil)
}

func (run *importAllRun) importTrainingDirectory(trainingDir importTrainingDirectory) (int, core.Result) {
	total := 0
	for _, relativePath := range trainingDir.files {
		localPath := core.JoinPath(run.cfg.DataDir, relativePath)
		if !isFile(localPath) {
			continue
		}
		n, result := importTrainingFile(run.session, localPath, trainingDir.name, trainingSplit(relativePath))
		if !result.OK {
			err, _ := result.Value.(error)
			return total, core.Fail(core.E(opImportAll, core.Sprintf("import training file %s", localPath), err))
		}
		total += n
	}
	return total, core.Ok(nil)
}

func trainingSplit(relativePath string) string {
	if core.Contains(relativePath, "valid") {
		return "valid"
	}
	if core.Contains(relativePath, "test") {
		return "test"
	}
	return "train"
}

func (run *importAllRun) importBenchmarkResults() core.Result {
	benchLocal, result := run.ensureBenchmarkDirectory()
	if !result.OK {
		return result
	}
	if result := run.pullBenchmarks(benchLocal); !result.OK {
		return result
	}
	if result := run.session.exec("DROP TABLE IF EXISTS benchmark_results"); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "drop benchmark_results", err))
	}
	if result := run.session.exec(`
			CREATE TABLE benchmark_results (
				source VARCHAR, id VARCHAR, benchmark VARCHAR, model VARCHAR,
				prompt TEXT, response TEXT, elapsed_seconds DOUBLE, domain VARCHAR
		)
	`); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "create benchmark_results", err))
	}

	benchTotal, result := run.importBenchmarkResultFiles(benchLocal)
	if !result.OK {
		return result
	}
	run.totals["benchmark_results"] = benchTotal
	core.Print(run.writer, "  benchmark_results: %d rows", benchTotal)
	return core.Ok(nil)
}

func (run *importAllRun) ensureBenchmarkDirectory() (string, core.Result) {
	benchLocal := core.JoinPath(run.cfg.DataDir, "benchmarks")
	if result := localFs.EnsureDir(benchLocal); !result.OK {
		return "", core.Fail(core.E(opImportAll, core.Sprintf("ensure benchmark directory %s", benchLocal), result.Value.(error)))
	}
	return benchLocal, core.Ok(nil)
}

func (run *importAllRun) pullBenchmarks(benchLocal string) core.Result {
	if run.cfg.SkipM3 {
		return core.Ok(nil)
	}
	core.Print(run.writer, "  Pulling benchmarks from M3...")
	if result := run.pullBenchmarkQuestionFiles(benchLocal); !result.OK {
		return result
	}
	return run.pullBenchmarkResultDirectories(benchLocal)
}

func (run *importAllRun) pullBenchmarkQuestionFiles(benchLocal string) core.Result {
	if run.cfg.Scp == nil {
		return core.Ok(nil)
	}
	for _, benchmarkName := range benchmarkQuestionNames {
		remote := core.Sprintf("%s:/Volumes/Data/lem/benchmarks/%s.jsonl", run.m3Host, benchmarkName)
		if err := run.cfg.Scp(remote, core.JoinPath(benchLocal, benchmarkName+jsonlExtension)); err != nil {
			core.Print(run.writer, "  WARNING: could not pull benchmark %s from M3: %v", benchmarkName, err)
		}
	}
	return core.Ok(nil)
}

func (run *importAllRun) pullBenchmarkResultDirectories(benchLocal string) core.Result {
	if run.cfg.ScpDir == nil {
		return core.Ok(nil)
	}
	for _, benchmarkSubdirectory := range benchmarkResultDirectories {
		localSubdirectory := core.JoinPath(benchLocal, benchmarkSubdirectory)
		if result := localFs.EnsureDir(localSubdirectory); !result.OK {
			return core.Fail(core.E(opImportAll, core.Sprintf("ensure benchmark subdirectory %s", localSubdirectory), result.Value.(error)))
		}
		remote := core.Sprintf("%s:/Volumes/Data/lem/benchmarks/%s/", run.m3Host, benchmarkSubdirectory)
		if err := run.cfg.ScpDir(remote, localSubdirectory+"/"); err != nil {
			core.Print(run.writer, "  WARNING: could not pull benchmark directory %s from M3: %v", benchmarkSubdirectory, err)
		}
	}
	return core.Ok(nil)
}

func (run *importAllRun) importBenchmarkResultFiles(benchLocal string) (int, core.Result) {
	total := 0
	for _, benchmarkSubdirectory := range benchmarkResultDirectories {
		n, result := run.importBenchmarkResultDirectory(benchLocal, benchmarkSubdirectory)
		if !result.OK {
			return total, result
		}
		total += n
	}
	n, result := run.importStandaloneBenchmarkFiles(benchLocal)
	if !result.OK {
		return total, result
	}
	return total + n, core.Ok(nil)
}

func (run *importAllRun) importBenchmarkResultDirectory(benchLocal, benchmarkSubdirectory string) (int, core.Result) {
	total := 0
	resultDir := core.JoinPath(benchLocal, benchmarkSubdirectory)
	matches := core.PathGlob(core.JoinPath(resultDir, "*"+jsonlExtension))
	for _, jsonFile := range matches {
		n, result := importBenchmarkFile(run.session, jsonFile, benchmarkSubdirectory)
		if !result.OK {
			err, _ := result.Value.(error)
			return total, core.Fail(core.E(opImportAll, core.Sprintf("import benchmark file %s", jsonFile), err))
		}
		total += n
	}
	return total, core.Ok(nil)
}

func (run *importAllRun) importStandaloneBenchmarkFiles(benchLocal string) (int, core.Result) {
	total := 0
	for _, benchmarkFile := range standaloneBenchmarkFileNames {
		localPath := core.JoinPath(benchLocal, benchmarkFile+jsonlExtension)
		if result := run.pullStandaloneBenchmarkFile(localPath, benchmarkFile); !result.OK {
			return total, result
		}
		if !isFile(localPath) {
			continue
		}
		n, result := importBenchmarkFile(run.session, localPath, "benchmark")
		if !result.OK {
			err, _ := result.Value.(error)
			return total, core.Fail(core.E(opImportAll, core.Sprintf("import benchmark file %s", localPath), err))
		}
		total += n
	}
	return total, core.Ok(nil)
}

func (run *importAllRun) pullStandaloneBenchmarkFile(localPath, benchmarkFile string) core.Result {
	if isFile(localPath) || run.cfg.SkipM3 || run.cfg.Scp == nil {
		return core.Ok(nil)
	}
	remote := core.Sprintf("%s:/Volumes/Data/lem/benchmarks/%s.jsonl", run.m3Host, benchmarkFile)
	if err := run.cfg.Scp(remote, localPath); err != nil {
		core.Print(run.writer, "  WARNING: could not pull benchmark file %s from M3: %v", benchmarkFile, err)
	}
	return core.Ok(nil)
}

func (run *importAllRun) importBenchmarkQuestions() core.Result {
	if result := run.session.exec("DROP TABLE IF EXISTS benchmark_questions"); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "drop benchmark_questions", err))
	}
	if result := run.session.exec(`
			CREATE TABLE benchmark_questions (
				benchmark VARCHAR, id VARCHAR, question TEXT,
				best_answer TEXT, correct_answers TEXT, incorrect_answers TEXT, category VARCHAR
		)
	`); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "create benchmark_questions", err))
	}

	benchLocal := core.JoinPath(run.cfg.DataDir, "benchmarks")
	benchQTotal := 0
	for _, benchmarkName := range benchmarkQuestionNames {
		localPath := core.JoinPath(benchLocal, benchmarkName+jsonlExtension)
		if !isFile(localPath) {
			continue
		}
		n, result := importBenchmarkQuestions(run.session, localPath, benchmarkName)
		if !result.OK {
			err, _ := result.Value.(error)
			return core.Fail(core.E(opImportAll, core.Sprintf("import benchmark questions %s", localPath), err))
		}
		benchQTotal += n
	}
	run.totals["benchmark_questions"] = benchQTotal
	core.Print(run.writer, "  benchmark_questions: %d rows", benchQTotal)
	return core.Ok(nil)
}

func (run *importAllRun) importSeeds() core.Result {
	if result := run.session.exec("DROP TABLE IF EXISTS seeds"); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "drop seeds", err))
	}
	if result := run.session.exec(`
			CREATE TABLE seeds (
				source_file VARCHAR, region VARCHAR, seed_id VARCHAR, domain VARCHAR, prompt TEXT
			)
	`); !result.OK {
		err, _ := result.Value.(error)
		return core.Fail(core.E(opImportAll, "create seeds", err))
	}

	seedTotal := 0
	for _, seedDir := range []string{core.JoinPath(run.cfg.DataDir, "seeds")} {
		if !isDir(seedDir) {
			continue
		}
		n, result := importSeeds(run.session, seedDir)
		if !result.OK {
			err, _ := result.Value.(error)
			return core.Fail(core.E(opImportAll, core.Sprintf("import seeds %s", seedDir), err))
		}
		seedTotal += n
	}
	run.totals["seeds"] = seedTotal
	core.Print(run.writer, "  seeds: %d rows", seedTotal)
	return core.Ok(nil)
}

func (run *importAllRun) printSummary() {
	grandTotal := 0
	core.Print(run.writer, "\n%s", repeat("=", 50))
	core.Print(run.writer, "LEM Database Import Complete")
	core.Print(run.writer, "%s", repeat("=", 50))
	for table, count := range run.totals {
		core.Print(run.writer, "  %-25s %8d", table, count)
		grandTotal += count
	}
	core.Print(run.writer, "  %s", repeat("-", 35))
	core.Print(run.writer, "  %-25s %8d", "TOTAL", grandTotal)
	core.Print(run.writer, "\nDatabase: %s", run.db.Path())
}

func importTrainingFile(db duckDBImportSession, path, source, split string) (int, core.Result) {
	r := localFs.Open(path)
	if !r.OK {
		return 0, core.Fail(core.E(opImportTrainingFile, core.Sprintf(openPathFormat, path), r.Value.(error)))
	}
	f := r.Value.(io.ReadCloser)
	defer func() { _ = f.Close() }()

	count := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		example, result := trainingExampleFromJSON(scanner.Bytes())
		if !result.OK {
			err, _ := result.Value.(error)
			return count, core.Fail(core.E(opImportTrainingFile, core.Sprintf(parsePathLineFormat, path, lineNumber), err))
		}
		if result := insertTrainingExample(db, source, split, example); !result.OK {
			err, _ := result.Value.(error)
			return count, core.Fail(core.E(opImportTrainingFile, "insert training example", err))
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, core.Fail(core.E(opImportTrainingFile, "scan training file", err))
	}
	return count, core.Ok(nil)
}

type trainingExample struct {
	messages       []ChatMessage
	prompt         string
	response       string
	assistantCount int
}

func trainingExampleFromJSON(data []byte) (trainingExample, core.Result) {
	var rec struct {
		Messages []ChatMessage `json:"messages"`
	}
	if r := core.JSONUnmarshal(data, &rec); !r.OK {
		parseErr, _ := r.Value.(error)
		return trainingExample{}, core.Fail(parseErr)
	}
	return trainingExampleFromMessages(rec.Messages), core.Ok(nil)
}

func trainingExampleFromMessages(messages []ChatMessage) trainingExample {
	example := trainingExample{messages: messages}
	for _, message := range messages {
		if message.Role == "user" && example.prompt == "" {
			example.prompt = message.Content
		}
		if message.Role == "assistant" {
			if example.response == "" {
				example.response = message.Content
			}
			example.assistantCount++
		}
	}
	return example
}

func insertTrainingExample(db duckDBImportSession, source, split string, example trainingExample) core.Result {
	return db.exec(
		`INSERT INTO training_examples VALUES (?, ?, ?, ?, ?, ?, ?)`,
		source,
		split,
		example.prompt,
		example.response,
		example.assistantCount,
		core.JSONMarshalString(example.messages),
		len(example.response),
	)
}

func importBenchmarkFile(db duckDBImportSession, path, source string) (int, core.Result) {
	r := localFs.Open(path)
	if !r.OK {
		return 0, core.Fail(core.E(opImportBenchmarkFile, core.Sprintf(openPathFormat, path), r.Value.(error)))
	}
	f := r.Value.(io.ReadCloser)
	defer func() { _ = f.Close() }()

	count := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		var rec map[string]any
		if r := core.JSONUnmarshal(scanner.Bytes(), &rec); !r.OK {
			parseErr, _ := r.Value.(error)
			return count, core.Fail(core.E(opImportBenchmarkFile, core.Sprintf(parsePathLineFormat, path, lineNumber), parseErr))
		}

		if result := db.exec(`INSERT INTO benchmark_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			source,
			core.Sprint(rec["id"]),
			strOrEmpty(rec, "benchmark"),
			strOrEmpty(rec, "model"),
			strOrEmpty(rec, "prompt"),
			strOrEmpty(rec, "response"),
			floatOrZero(rec, "elapsed_seconds"),
			strOrEmpty(rec, "domain"),
		); !result.OK {
			err, _ := result.Value.(error)
			return count, core.Fail(core.E(opImportBenchmarkFile, "insert benchmark result", err))
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, core.Fail(core.E(opImportBenchmarkFile, "scan benchmark file", err))
	}
	return count, core.Ok(nil)
}

func importBenchmarkQuestions(db duckDBImportSession, path, benchmark string) (int, core.Result) {
	r := localFs.Open(path)
	if !r.OK {
		return 0, core.Fail(core.E(opImportBenchmarkQuestions, core.Sprintf(openPathFormat, path), r.Value.(error)))
	}
	f := r.Value.(io.ReadCloser)
	defer func() { _ = f.Close() }()

	count := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		var rec map[string]any
		if r := core.JSONUnmarshal(scanner.Bytes(), &rec); !r.OK {
			parseErr, _ := r.Value.(error)
			return count, core.Fail(core.E(opImportBenchmarkQuestions, core.Sprintf(parsePathLineFormat, path, lineNumber), parseErr))
		}

		correctJSON := core.JSONMarshalString(rec["correct_answers"])
		incorrectJSON := core.JSONMarshalString(rec["incorrect_answers"])

		if result := db.exec(`INSERT INTO benchmark_questions VALUES (?, ?, ?, ?, ?, ?, ?)`,
			benchmark,
			core.Sprint(rec["id"]),
			strOrEmpty(rec, "question"),
			strOrEmpty(rec, "best_answer"),
			correctJSON,
			incorrectJSON,
			strOrEmpty(rec, "category"),
		); !result.OK {
			err, _ := result.Value.(error)
			return count, core.Fail(core.E(opImportBenchmarkQuestions, "insert benchmark question", err))
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, core.Fail(core.E(opImportBenchmarkQuestions, "scan benchmark questions", err))
	}
	return count, core.Ok(nil)
}

func importSeeds(db duckDBImportSession, seedDir string) (int, core.Result) {
	count := 0
	if result := walkDir(seedDir, func(path string) core.Result {
		imported, result := importSeedFile(db, seedDir, path)
		if !result.OK {
			return result
		}
		count += imported
		return core.Ok(nil)
	}); !result.OK {
		return count, result
	}
	return count, core.Ok(nil)
}

func importSeedFile(db duckDBImportSession, seedDir, path string) (int, core.Result) {
	if !core.HasSuffix(path, ".json") {
		return 0, core.Ok(nil)
	}
	rel := core.TrimPrefix(path, seedDir+"/")
	region := core.TrimSuffix(core.PathBase(path), ".json")
	seedsList, result := readSeedList(path, rel)
	if !result.OK {
		return 0, result
	}
	count := 0
	for _, seed := range seedsList {
		inserted, result := insertSeed(db, rel, region, seed)
		if !result.OK {
			return count, result
		}
		if inserted {
			count++
		}
	}
	return count, core.Ok(nil)
}

func readSeedList(path, rel string) ([]any, core.Result) {
	readResult := localFs.Read(path)
	if !readResult.OK {
		return nil, core.Fail(core.E(opImportSeeds, core.Sprintf("read seed file %s", rel), readResult.Value.(error)))
	}
	var raw any
	if r := core.JSONUnmarshal([]byte(readResult.Value.(string)), &raw); !r.OK {
		err, _ := r.Value.(error)
		return nil, core.Fail(core.E(opImportSeeds, core.Sprintf("parse seed file %s", rel), err))
	}
	return seedListFromRaw(raw), core.Ok(nil)
}

func seedListFromRaw(raw any) []any {
	switch v := raw.(type) {
	case []any:
		return v
	case map[string]any:
		if prompts, ok := v["prompts"].([]any); ok {
			return prompts
		}
		if seeds, ok := v["seeds"].([]any); ok {
			return seeds
		}
	}
	return nil
}

func insertSeed(db duckDBImportSession, rel, region string, seed any) (bool, core.Result) {
	switch typedSeed := seed.(type) {
	case map[string]any:
		if result := db.exec(
			`INSERT INTO seeds VALUES (?, ?, ?, ?, ?)`,
			rel,
			region,
			strOrEmpty(typedSeed, "seed_id"),
			strOrEmpty(typedSeed, "domain"),
			seedPrompt(typedSeed),
		); !result.OK {
			err, _ := result.Value.(error)
			return true, core.Fail(core.E(opImportSeeds, "insert seed prompt", err))
		}
		return true, core.Ok(nil)
	case string:
		if result := db.exec(`INSERT INTO seeds VALUES (?, ?, ?, ?, ?)`, rel, region, "", "", typedSeed); !result.OK {
			err, _ := result.Value.(error)
			return true, core.Fail(core.E(opImportSeeds, "insert seed string", err))
		}
		return true, core.Ok(nil)
	default:
		return false, core.Ok(nil)
	}
}

func seedPrompt(seed map[string]any) string {
	for _, key := range []string{"prompt", "text", "question"} {
		if prompt := strOrEmpty(seed, key); prompt != "" {
			return prompt
		}
	}
	return ""
}

// walkDir recursively visits all regular files under root, calling fn for each.
func walkDir(root string, fn func(path string) core.Result) core.Result {
	r := localFs.List(root)
	if !r.OK {
		return core.Fail(core.E("store.walkDir", core.Sprintf("list %s", root), r.Value.(error)))
	}
	entries, ok := r.Value.([]fs.DirEntry)
	if !ok {
		return core.Fail(core.E("store.walkDir", core.Sprintf("list %s returned invalid entries", root), nil))
	}
	for _, entry := range entries {
		full := core.JoinPath(root, entry.Name())
		if entry.IsDir() {
			if result := walkDir(full, fn); !result.OK {
				return result
			}
		} else {
			if result := fn(full); !result.OK {
				return result
			}
		}
	}
	return core.Ok(nil)
}

// strOrEmpty extracts a string value from a map, returning an empty string if
// the key is absent.
func strOrEmpty(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		return core.Sprint(v)
	}
	return ""
}

// floatOrZero extracts a float64 value from a map, returning zero if the key
// is absent or not a number.
func floatOrZero(m map[string]any, key string) float64 {
	if v, ok := m[key]; ok {
		if f, ok := v.(float64); ok {
			return f
		}
	}
	return 0
}

// repeat returns a string consisting of count copies of s. It avoids importing
// strings because repository conventions route string helpers through core.
func repeat(s string, count int) string {
	if count <= 0 {
		return ""
	}
	b := core.NewBuilder()
	for range count {
		b.WriteString(s)
	}
	return b.String()
}

// escapeSQLPath escapes single quotes in a file path for use in DuckDB SQL
// string literals.
func escapeSQLPath(p string) string {
	return core.Replace(p, "'", "''")
}

// isFile returns true if the path exists and is a regular file.
func isFile(path string) bool {
	return localFs.IsFile(path)
}

// isDir returns true if the path exists and is a directory.
func isDir(path string) bool {
	return localFs.IsDir(path)
}
