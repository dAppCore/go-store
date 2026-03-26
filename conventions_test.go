package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"testing"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepoConventions_Good_BannedImports(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, ".go")
	})

	var banned []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, spec := range file.Imports {
			importPath := core.TrimPrefix(core.TrimSuffix(spec.Path.Value, `"`), `"`)
			if core.HasPrefix(importPath, "forge.lthn.ai/") {
				banned = append(banned, path+": "+importPath)
			}
		}
	}

	slices.Sort(banned)
	assert.Empty(t, banned, "legacy forge.lthn.ai imports are banned")
}

func TestRepoConventions_Good_TestNaming(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, "_test.go")
	})

	var invalid []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			name := fn.Name.Name
			if !core.HasPrefix(name, "Test") || name == "TestMain" {
				continue
			}
			if core.Contains(name, "_Good") || core.Contains(name, "_Bad") || core.Contains(name, "_Ugly") {
				continue
			}
			invalid = append(invalid, path+": "+name)
		}
	}

	slices.Sort(invalid)
	assert.Empty(t, invalid, "top-level tests must include _Good, _Bad, or _Ugly in the name")
}

func repoGoFiles(t *testing.T, keep func(name string) bool) []string {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !keep(entry.Name()) {
			continue
		}
		files = append(files, core.CleanPath(entry.Name(), "/"))
	}

	slices.Sort(files)
	return files
}

func parseGoFile(t *testing.T, path string) *ast.File {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err)
	return file
}
