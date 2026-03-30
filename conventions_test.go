package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"slices"
	"testing"
	"unicode"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConventions_Imports_Good_Banned(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, ".go")
	})

	bannedImports := []string{
		"encoding/json",
		"errors",
		"fmt",
		"os",
		"os/exec",
		"path/filepath",
		"strings",
	}

	var banned []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, spec := range file.Imports {
			importPath := trimImportPath(spec.Path.Value)
			if core.HasPrefix(importPath, "forge.lthn.ai/") || slices.Contains(bannedImports, importPath) {
				banned = append(banned, core.Concat(path, ": ", importPath))
			}
		}
	}

	slices.Sort(banned)
	assert.Empty(t, banned, "banned imports should not appear in repository Go files")
}

func TestConventions_TestNaming_Good_StrictPattern(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, "_test.go")
	})

	allowedClasses := []string{"Good", "Bad", "Ugly"}
	var invalid []string
	for _, path := range files {
		expectedPrefix := testNamePrefix(path)
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
			if !core.HasPrefix(name, expectedPrefix) {
				invalid = append(invalid, core.Concat(path, ": ", name))
				continue
			}
			parts := core.Split(core.TrimPrefix(name, expectedPrefix), "_")
			if len(parts) < 2 || parts[0] == "" || !slices.Contains(allowedClasses, parts[1]) {
				invalid = append(invalid, core.Concat(path, ": ", name))
			}
		}
	}

	slices.Sort(invalid)
	assert.Empty(t, invalid, "top-level tests must follow Test<File>_<Function>_<Good|Bad|Ugly>")
}

func TestConventions_Exports_Good_UsageExamples(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, ".go") && !core.HasSuffix(name, "_test.go")
	})

	var missing []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.FuncDecl:
				if !node.Name.IsExported() {
					continue
				}
				if !core.Contains(commentText(node.Doc), "Usage example:") {
					missing = append(missing, core.Concat(path, ": ", node.Name.Name))
				}
			case *ast.GenDecl:
				for _, spec := range node.Specs {
					switch item := spec.(type) {
					case *ast.TypeSpec:
						if !item.Name.IsExported() {
							continue
						}
						if !core.Contains(commentText(item.Doc, node.Doc), "Usage example:") {
							missing = append(missing, core.Concat(path, ": ", item.Name.Name))
						}
					case *ast.ValueSpec:
						for _, name := range item.Names {
							if !name.IsExported() {
								continue
							}
							if !core.Contains(commentText(item.Doc, node.Doc), "Usage example:") {
								missing = append(missing, core.Concat(path, ": ", name.Name))
							}
						}
					}
				}
			}
		}
	}

	slices.Sort(missing)
	assert.Empty(t, missing, "exported declarations must include a usage example in their doc comment")
}

func TestConventions_Exports_Good_NoCompatibilityAliases(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, ".go") && !core.HasSuffix(name, "_test.go")
	})

	var invalid []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range node.Specs {
					switch item := spec.(type) {
					case *ast.TypeSpec:
						if item.Name.Name == "KV" {
							invalid = append(invalid, core.Concat(path, ": ", item.Name.Name))
						}
						if item.Name.Name != "Watcher" {
							continue
						}
						structType, ok := item.Type.(*ast.StructType)
						if !ok {
							continue
						}
						for _, field := range structType.Fields.List {
							for _, name := range field.Names {
								if name.Name == "Ch" {
									invalid = append(invalid, core.Concat(path, ": Watcher.Ch"))
								}
							}
						}
					case *ast.ValueSpec:
						for _, name := range item.Names {
							if name.Name == "ErrNotFound" || name.Name == "ErrQuotaExceeded" {
								invalid = append(invalid, core.Concat(path, ": ", name.Name))
							}
						}
					}
				}
			}
		}
	}

	slices.Sort(invalid)
	assert.Empty(t, invalid, "legacy compatibility aliases should not appear in the public Go API")
}

func repoGoFiles(t *testing.T, keep func(name string) bool) []string {
	t.Helper()

	result := testFS().List(".")
	requireCoreOK(t, result)

	entries, ok := result.Value.([]fs.DirEntry)
	require.True(t, ok, "unexpected directory entry type: %T", result.Value)

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !keep(entry.Name()) {
			continue
		}
		files = append(files, entry.Name())
	}

	slices.Sort(files)
	return files
}

func parseGoFile(t *testing.T, path string) *ast.File {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	require.NoError(t, err)
	return file
}

func trimImportPath(value string) string {
	return core.TrimSuffix(core.TrimPrefix(value, `"`), `"`)
}

func testNamePrefix(path string) string {
	return core.Concat("Test", camelCase(core.TrimSuffix(path, "_test.go")), "_")
}

func camelCase(value string) string {
	parts := core.Split(value, "_")
	builder := core.NewBuilder()
	for _, part := range parts {
		if part == "" {
			continue
		}
		builder.WriteString(upperFirst(part))
	}
	return builder.String()
}

func upperFirst(value string) string {
	runes := []rune(value)
	if len(runes) == 0 {
		return ""
	}
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func commentText(groups ...*ast.CommentGroup) string {
	builder := core.NewBuilder()
	for _, group := range groups {
		if group == nil {
			continue
		}
		text := core.Trim(group.Text())
		if text == "" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(text)
	}
	return builder.String()
}
