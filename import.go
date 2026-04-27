// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bufio"
	"io"
	"io/fs"

	core "dappco.re/go/core"
)

// localFs provides unrestricted filesystem access for import operations.
var localFs = (&core.Fs{}).New("/")

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
func ImportAll(db *DuckDB, cfg ImportConfig, w io.Writer) error {
	m3Host := cfg.M3Host
	if m3Host == "" {
		m3Host = "m3"
	}

	totals := make(map[string]int)

	// ── 1. Golden set ──
	goldenPath := core.JoinPath(cfg.DataDir, "gold-15k.jsonl")
	if !cfg.SkipM3 && cfg.Scp != nil {
		core.Print(w, "  Pulling golden set from M3...")
		remote := core.Sprintf("%s:/Volumes/Data/lem/responses/gold-15k.jsonl", m3Host)
		if err := cfg.Scp(remote, goldenPath); err != nil {
			core.Print(w, "  WARNING: could not pull golden set from M3: %v", err)
		}
	}
	if isFile(goldenPath) {
		if err := db.Exec("DROP TABLE IF EXISTS golden_set"); err != nil {
			return core.E("store.ImportAll", "drop golden_set", err)
		}
		err := db.Exec(core.Sprintf(`
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
		if err != nil {
			return core.E("store.ImportAll", "import golden_set", err)
		} else {
			var n int
			if err := db.QueryRowScan("SELECT count(*) FROM golden_set", &n); err != nil {
				return core.E("store.ImportAll", "count golden_set", err)
			}
			totals["golden_set"] = n
			core.Print(w, "  golden_set: %d rows", n)
		}
	}

	// ── 2. Training examples ──
	trainingDirs := []struct {
		name  string
		files []string
	}{
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

	trainingRoot := cfg.DataDir

	if !cfg.SkipM3 && cfg.Scp != nil {
		core.Print(w, "  Pulling training sets from M3...")
		for _, td := range trainingDirs {
			for _, rel := range td.files {
				local := core.JoinPath(trainingRoot, rel)
				if result := localFs.EnsureDir(core.PathDir(local)); !result.OK {
					return core.E("store.ImportAll", "ensure training directory", result.Value.(error))
				}
				remote := core.Sprintf("%s:/Volumes/Data/lem/%s", m3Host, rel)
				_ = cfg.Scp(remote, local) // ignore errors, file might not exist
			}
		}
	}

	if err := db.Exec("DROP TABLE IF EXISTS training_examples"); err != nil {
		return core.E("store.ImportAll", "drop training_examples", err)
	}
	if err := db.Exec(`
		CREATE TABLE training_examples (
			source VARCHAR,
			split VARCHAR,
			prompt TEXT,
			response TEXT,
			num_turns INT,
			full_messages TEXT,
			char_count INT
		)
	`); err != nil {
		return core.E("store.ImportAll", "create training_examples", err)
	}

	trainingTotal := 0
	for _, td := range trainingDirs {
		for _, rel := range td.files {
			local := core.JoinPath(trainingRoot, rel)
			if !isFile(local) {
				continue
			}

			split := "train"
			if core.Contains(rel, "valid") {
				split = "valid"
			} else if core.Contains(rel, "test") {
				split = "test"
			}

			n, err := importTrainingFile(db, local, td.name, split)
			if err != nil {
				return core.E("store.ImportAll", core.Sprintf("import training file %s", local), err)
			}
			trainingTotal += n
		}
	}
	totals["training_examples"] = trainingTotal
	core.Print(w, "  training_examples: %d rows", trainingTotal)

	// ── 3. Benchmark results ──
	benchLocal := core.JoinPath(cfg.DataDir, "benchmarks")
	localFs.EnsureDir(benchLocal)

	if !cfg.SkipM3 {
		core.Print(w, "  Pulling benchmarks from M3...")
		if cfg.Scp != nil {
			for _, bname := range []string{"truthfulqa", "gsm8k", "do_not_answer", "toxigen"} {
				remote := core.Sprintf("%s:/Volumes/Data/lem/benchmarks/%s.jsonl", m3Host, bname)
				_ = cfg.Scp(remote, core.JoinPath(benchLocal, bname+".jsonl"))
			}
		}
		if cfg.ScpDir != nil {
			for _, subdir := range []string{"results", "scale_results", "cross_arch_results", "deepseek-r1-7b"} {
				localSub := core.JoinPath(benchLocal, subdir)
				localFs.EnsureDir(localSub)
				remote := core.Sprintf("%s:/Volumes/Data/lem/benchmarks/%s/", m3Host, subdir)
				_ = cfg.ScpDir(remote, localSub+"/")
			}
		}
	}

	if err := db.Exec("DROP TABLE IF EXISTS benchmark_results"); err != nil {
		return core.E("store.ImportAll", "drop benchmark_results", err)
	}
	if err := db.Exec(`
		CREATE TABLE benchmark_results (
			source VARCHAR, id VARCHAR, benchmark VARCHAR, model VARCHAR,
			prompt TEXT, response TEXT, elapsed_seconds DOUBLE, domain VARCHAR
		)
	`); err != nil {
		return core.E("store.ImportAll", "create benchmark_results", err)
	}

	benchTotal := 0
	for _, subdir := range []string{"results", "scale_results", "cross_arch_results", "deepseek-r1-7b"} {
		resultDir := core.JoinPath(benchLocal, subdir)
		matches := core.PathGlob(core.JoinPath(resultDir, "*.jsonl"))
		for _, jf := range matches {
			n, err := importBenchmarkFile(db, jf, subdir)
			if err != nil {
				return core.E("store.ImportAll", core.Sprintf("import benchmark file %s", jf), err)
			}
			benchTotal += n
		}
	}

	// Also import standalone benchmark files.
	for _, bfile := range []string{"lem_bench", "lem_ethics", "lem_ethics_allen", "instruction_tuned", "abliterated", "base_pt"} {
		local := core.JoinPath(benchLocal, bfile+".jsonl")
		if !isFile(local) {
			if !cfg.SkipM3 && cfg.Scp != nil {
				remote := core.Sprintf("%s:/Volumes/Data/lem/benchmarks/%s.jsonl", m3Host, bfile)
				_ = cfg.Scp(remote, local)
			}
		}
		if isFile(local) {
			n, err := importBenchmarkFile(db, local, "benchmark")
			if err != nil {
				return core.E("store.ImportAll", core.Sprintf("import benchmark file %s", local), err)
			}
			benchTotal += n
		}
	}
	totals["benchmark_results"] = benchTotal
	core.Print(w, "  benchmark_results: %d rows", benchTotal)

	// ── 4. Benchmark questions ──
	if err := db.Exec("DROP TABLE IF EXISTS benchmark_questions"); err != nil {
		return core.E("store.ImportAll", "drop benchmark_questions", err)
	}
	if err := db.Exec(`
		CREATE TABLE benchmark_questions (
			benchmark VARCHAR, id VARCHAR, question TEXT,
			best_answer TEXT, correct_answers TEXT, incorrect_answers TEXT, category VARCHAR
		)
	`); err != nil {
		return core.E("store.ImportAll", "create benchmark_questions", err)
	}

	benchQTotal := 0
	for _, bname := range []string{"truthfulqa", "gsm8k", "do_not_answer", "toxigen"} {
		local := core.JoinPath(benchLocal, bname+".jsonl")
		if isFile(local) {
			n, err := importBenchmarkQuestions(db, local, bname)
			if err != nil {
				return core.E("store.ImportAll", core.Sprintf("import benchmark questions %s", local), err)
			}
			benchQTotal += n
		}
	}
	totals["benchmark_questions"] = benchQTotal
	core.Print(w, "  benchmark_questions: %d rows", benchQTotal)

	// ── 5. Seeds ──
	if err := db.Exec("DROP TABLE IF EXISTS seeds"); err != nil {
		return core.E("store.ImportAll", "drop seeds", err)
	}
	if err := db.Exec(`
		CREATE TABLE seeds (
			source_file VARCHAR, region VARCHAR, seed_id VARCHAR, domain VARCHAR, prompt TEXT
		)
	`); err != nil {
		return core.E("store.ImportAll", "create seeds", err)
	}

	seedTotal := 0
	seedDirs := []string{core.JoinPath(cfg.DataDir, "seeds"), "/tmp/lem-data/seeds", "/tmp/lem-repo/seeds"}
	for _, seedDir := range seedDirs {
		if !isDir(seedDir) {
			continue
		}
		n, err := importSeeds(db, seedDir)
		if err != nil {
			return core.E("store.ImportAll", core.Sprintf("import seeds %s", seedDir), err)
		}
		seedTotal += n
	}
	totals["seeds"] = seedTotal
	core.Print(w, "  seeds: %d rows", seedTotal)

	// ── Summary ──
	grandTotal := 0
	core.Print(w, "\n%s", repeat("=", 50))
	core.Print(w, "LEM Database Import Complete")
	core.Print(w, "%s", repeat("=", 50))
	for table, count := range totals {
		core.Print(w, "  %-25s %8d", table, count)
		grandTotal += count
	}
	core.Print(w, "  %s", repeat("-", 35))
	core.Print(w, "  %-25s %8d", "TOTAL", grandTotal)
	core.Print(w, "\nDatabase: %s", db.Path())

	return nil
}

func importTrainingFile(db *DuckDB, path, source, split string) (int, error) {
	r := localFs.Open(path)
	if !r.OK {
		return 0, core.E("store.importTrainingFile", core.Sprintf("open %s", path), r.Value.(error))
	}
	f := r.Value.(io.ReadCloser)
	defer func() { _ = f.Close() }()

	count := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		var rec struct {
			Messages []ChatMessage `json:"messages"`
		}
		if r := core.JSONUnmarshal(scanner.Bytes(), &rec); !r.OK {
			continue
		}

		prompt := ""
		response := ""
		assistantCount := 0
		for _, m := range rec.Messages {
			if m.Role == "user" && prompt == "" {
				prompt = m.Content
			}
			if m.Role == "assistant" {
				if response == "" {
					response = m.Content
				}
				assistantCount++
			}
		}

		msgsJSON := core.JSONMarshalString(rec.Messages)
		if err := db.Exec(`INSERT INTO training_examples VALUES (?, ?, ?, ?, ?, ?, ?)`,
			source, split, prompt, response, assistantCount, msgsJSON, len(response)); err != nil {
			return count, core.E("store.importTrainingFile", "insert training example", err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, core.E("store.importTrainingFile", "scan training file", err)
	}
	return count, nil
}

func importBenchmarkFile(db *DuckDB, path, source string) (int, error) {
	r := localFs.Open(path)
	if !r.OK {
		return 0, core.E("store.importBenchmarkFile", core.Sprintf("open %s", path), r.Value.(error))
	}
	f := r.Value.(io.ReadCloser)
	defer func() { _ = f.Close() }()

	count := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		var rec map[string]any
		if r := core.JSONUnmarshal(scanner.Bytes(), &rec); !r.OK {
			continue
		}

		if err := db.Exec(`INSERT INTO benchmark_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			source,
			core.Sprint(rec["id"]),
			strOrEmpty(rec, "benchmark"),
			strOrEmpty(rec, "model"),
			strOrEmpty(rec, "prompt"),
			strOrEmpty(rec, "response"),
			floatOrZero(rec, "elapsed_seconds"),
			strOrEmpty(rec, "domain"),
		); err != nil {
			return count, core.E("store.importBenchmarkFile", "insert benchmark result", err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, core.E("store.importBenchmarkFile", "scan benchmark file", err)
	}
	return count, nil
}

func importBenchmarkQuestions(db *DuckDB, path, benchmark string) (int, error) {
	r := localFs.Open(path)
	if !r.OK {
		return 0, core.E("store.importBenchmarkQuestions", core.Sprintf("open %s", path), r.Value.(error))
	}
	f := r.Value.(io.ReadCloser)
	defer func() { _ = f.Close() }()

	count := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		var rec map[string]any
		if r := core.JSONUnmarshal(scanner.Bytes(), &rec); !r.OK {
			continue
		}

		correctJSON := core.JSONMarshalString(rec["correct_answers"])
		incorrectJSON := core.JSONMarshalString(rec["incorrect_answers"])

		if err := db.Exec(`INSERT INTO benchmark_questions VALUES (?, ?, ?, ?, ?, ?, ?)`,
			benchmark,
			core.Sprint(rec["id"]),
			strOrEmpty(rec, "question"),
			strOrEmpty(rec, "best_answer"),
			correctJSON,
			incorrectJSON,
			strOrEmpty(rec, "category"),
		); err != nil {
			return count, core.E("store.importBenchmarkQuestions", "insert benchmark question", err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return count, core.E("store.importBenchmarkQuestions", "scan benchmark questions", err)
	}
	return count, nil
}

func importSeeds(db *DuckDB, seedDir string) (int, error) {
	count := 0
	var firstErr error
	walkDir(seedDir, func(path string) {
		if firstErr != nil {
			return
		}
		if !core.HasSuffix(path, ".json") {
			return
		}

		readResult := localFs.Read(path)
		if !readResult.OK {
			return
		}
		data := []byte(readResult.Value.(string))

		rel := core.TrimPrefix(path, seedDir+"/")
		region := core.TrimSuffix(core.PathBase(path), ".json")

		// Try parsing as array or object with prompts/seeds field.
		var seedsList []any
		var raw any
		if r := core.JSONUnmarshal(data, &raw); !r.OK {
			return
		}

		switch v := raw.(type) {
		case []any:
			seedsList = v
		case map[string]any:
			if prompts, ok := v["prompts"].([]any); ok {
				seedsList = prompts
			} else if seeds, ok := v["seeds"].([]any); ok {
				seedsList = seeds
			}
		}

		for _, s := range seedsList {
			switch seed := s.(type) {
			case map[string]any:
				prompt := strOrEmpty(seed, "prompt")
				if prompt == "" {
					prompt = strOrEmpty(seed, "text")
				}
				if prompt == "" {
					prompt = strOrEmpty(seed, "question")
				}
				if err := db.Exec(`INSERT INTO seeds VALUES (?, ?, ?, ?, ?)`,
					rel, region,
					strOrEmpty(seed, "seed_id"),
					strOrEmpty(seed, "domain"),
					prompt,
				); err != nil {
					firstErr = core.E("store.importSeeds", "insert seed prompt", err)
					return
				}
				count++
			case string:
				if err := db.Exec(`INSERT INTO seeds VALUES (?, ?, ?, ?, ?)`,
					rel, region, "", "", seed); err != nil {
					firstErr = core.E("store.importSeeds", "insert seed string", err)
					return
				}
				count++
			}
		}
	})
	if firstErr != nil {
		return count, firstErr
	}
	return count, nil
}

// walkDir recursively visits all regular files under root, calling fn for each.
func walkDir(root string, fn func(path string)) {
	r := localFs.List(root)
	if !r.OK {
		return
	}
	entries, ok := r.Value.([]fs.DirEntry)
	if !ok {
		return
	}
	for _, entry := range entries {
		full := core.JoinPath(root, entry.Name())
		if entry.IsDir() {
			walkDir(full, fn)
		} else {
			fn(full)
		}
	}
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

// repeat returns a string consisting of count copies of s.
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
