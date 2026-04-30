package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"slices"
	"testing"
	"unicode"

	core "dappco.re/go"
)

func TestConventions_Imports_Good_Banned(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, ".go")
	})

	bannedImports := []string{
		string([]rune{'e', 'n', 'c', 'o', 'd', 'i', 'n', 'g', '/', 'j', 's', 'o', 'n'}),
		string([]rune{'e', 'r', 'r', 'o', 'r', 's'}),
		string([]rune{'f', 'm', 't'}),
		string([]rune{'o', 's'}),
		string([]rune{'o', 's', '/', 'e', 'x', 'e', 'c'}),
		string([]rune{'p', 'a', 't', 'h', '/', 'f', 'i', 'l', 'e', 'p', 'a', 't', 'h'}),
		string([]rune{'s', 't', 'r', 'i', 'n', 'g', 's'}),
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
	assertEmptyf(t, banned, "banned imports should not appear in repository Go files")
}

func TestConventions_TestNaming_Good_StrictPattern(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, testGoTestFileSuffix)
	})

	invalid := invalidTestNames(t, files)
	slices.Sort(invalid)
	assertEmptyf(t, invalid, "top-level tests must follow Test<File>_<Function>_<Good|Bad|Ugly>")
}

func invalidTestNames(t *testing.T, files []string) []string {
	t.Helper()

	allowedClasses := []string{"Good", "Bad", "Ugly"}
	var invalid []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, decl := range file.Decls {
			if invalidName := invalidTestFunctionName(path, testNamePrefix(path), decl, allowedClasses); invalidName != "" {
				invalid = append(invalid, invalidName)
			}
		}
	}
	return invalid
}

func invalidTestFunctionName(path, expectedPrefix string, decl ast.Decl, allowedClasses []string) string {
	fn, ok := decl.(*ast.FuncDecl)
	if !ok || fn.Recv != nil {
		return ""
	}
	name := fn.Name.Name
	if !core.HasPrefix(name, "Test") || name == "TestMain" {
		return ""
	}
	if !core.HasPrefix(name, expectedPrefix) {
		return core.Concat(path, ": ", name)
	}
	parts := core.Split(core.TrimPrefix(name, expectedPrefix), "_")
	if len(parts) < 2 || parts[0] == "" || !containsAny(parts[1:], allowedClasses) {
		return core.Concat(path, ": ", name)
	}
	return ""
}

func containsAny(values []string, candidates []string) bool {
	for _, value := range values {
		if slices.Contains(candidates, value) {
			return true
		}
	}
	return false
}

func TestConventions_Exports_Good_UsageExamples(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, ".go") && !core.HasSuffix(name, testGoTestFileSuffix)
	})

	missing := missingUsageExamples(t, files)
	slices.Sort(missing)
	assertEmptyf(t, missing, "exported declarations must include a usage example in their doc comment")
}

func missingUsageExamples(t *testing.T, files []string) []string {
	t.Helper()

	var missing []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, decl := range file.Decls {
			missing = append(missing, missingUsageExamplesForDecl(path, decl)...)
		}
	}
	return missing
}

func missingUsageExamplesForDecl(path string, decl ast.Decl) []string {
	switch node := decl.(type) {
	case *ast.FuncDecl:
		return missingFunctionUsageExample(path, node)
	case *ast.GenDecl:
		return missingGenDeclUsageExamples(path, node)
	default:
		return nil
	}
}

func missingFunctionUsageExample(path string, node *ast.FuncDecl) []string {
	if node.Name.IsExported() && !core.Contains(commentText(node.Doc), testUsageExampleMarker) {
		return []string{core.Concat(path, ": ", node.Name.Name)}
	}
	return nil
}

func missingGenDeclUsageExamples(path string, node *ast.GenDecl) []string {
	var missing []string
	for _, spec := range node.Specs {
		missing = append(missing, missingSpecUsageExamples(path, node, spec)...)
	}
	return missing
}

func missingSpecUsageExamples(path string, node *ast.GenDecl, spec ast.Spec) []string {
	switch item := spec.(type) {
	case *ast.TypeSpec:
		if item.Name.IsExported() && !core.Contains(commentText(item.Doc, node.Doc), testUsageExampleMarker) {
			return []string{core.Concat(path, ": ", item.Name.Name)}
		}
	case *ast.ValueSpec:
		return missingValueSpecUsageExamples(path, node, item)
	}
	return nil
}

func missingValueSpecUsageExamples(path string, node *ast.GenDecl, item *ast.ValueSpec) []string {
	var missing []string
	for _, name := range item.Names {
		if name.IsExported() && !core.Contains(commentText(item.Doc, node.Doc), testUsageExampleMarker) {
			missing = append(missing, core.Concat(path, ": ", name.Name))
		}
	}
	return missing
}

func TestConventions_Exports_Good_FieldUsageExamples(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, ".go") && !core.HasSuffix(name, testGoTestFileSuffix)
	})

	missing := missingFieldUsageExamples(t, files)
	slices.Sort(missing)
	assertEmptyf(t, missing, "exported struct fields must include a usage example in their doc comment")
}

func missingFieldUsageExamples(t *testing.T, files []string) []string {
	t.Helper()

	var missing []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, decl := range file.Decls {
			if node, ok := decl.(*ast.GenDecl); ok {
				missing = append(missing, missingFieldUsageExamplesForGenDecl(path, node)...)
			}
		}
	}
	return missing
}

func missingFieldUsageExamplesForGenDecl(path string, node *ast.GenDecl) []string {
	var missing []string
	for _, spec := range node.Specs {
		typeSpec, ok := spec.(*ast.TypeSpec)
		if !ok || !typeSpec.Name.IsExported() {
			continue
		}
		missing = append(missing, missingStructFieldUsageExamples(path, typeSpec)...)
	}
	return missing
}

func missingStructFieldUsageExamples(path string, typeSpec *ast.TypeSpec) []string {
	structType, ok := typeSpec.Type.(*ast.StructType)
	if !ok {
		return nil
	}
	var missing []string
	for _, field := range structType.Fields.List {
		missing = append(missing, missingFieldUsageExample(path, typeSpec.Name.Name, field)...)
	}
	return missing
}

func missingFieldUsageExample(path, typeName string, field *ast.Field) []string {
	var missing []string
	for _, fieldName := range field.Names {
		if fieldName.IsExported() && !core.Contains(commentText(field.Doc), testUsageExampleMarker) {
			missing = append(missing, core.Concat(path, ": ", typeName, ".", fieldName.Name))
		}
	}
	return missing
}

func TestConventions_Exports_Good_NoCompatibilityAliases(t *testing.T) {
	files := repoGoFiles(t, func(name string) bool {
		return core.HasSuffix(name, ".go") && !core.HasSuffix(name, testGoTestFileSuffix)
	})

	invalid := compatibilityAliases(t, files)
	slices.Sort(invalid)
	assertEmptyf(t, invalid, "legacy compatibility aliases should not appear in the public Go API")
}

func compatibilityAliases(t *testing.T, files []string) []string {
	t.Helper()

	var invalid []string
	for _, path := range files {
		file := parseGoFile(t, path)
		for _, decl := range file.Decls {
			if node, ok := decl.(*ast.GenDecl); ok {
				invalid = append(invalid, compatibilityAliasesForGenDecl(path, node)...)
			}
		}
	}
	return invalid
}

func compatibilityAliasesForGenDecl(path string, node *ast.GenDecl) []string {
	var invalid []string
	for _, spec := range node.Specs {
		invalid = append(invalid, compatibilityAliasesForSpec(path, spec)...)
	}
	return invalid
}

func compatibilityAliasesForSpec(path string, spec ast.Spec) []string {
	switch item := spec.(type) {
	case *ast.TypeSpec:
		return compatibilityAliasesForType(path, item)
	case *ast.ValueSpec:
		return compatibilityAliasesForValues(path, item)
	default:
		return nil
	}
}

func compatibilityAliasesForType(path string, item *ast.TypeSpec) []string {
	if item.Name.Name == "KV" {
		return []string{core.Concat(path, ": ", item.Name.Name)}
	}
	if item.Name.Name != "Watcher" {
		return nil
	}
	return watcherCompatibilityAliases(path, item)
}

func watcherCompatibilityAliases(path string, item *ast.TypeSpec) []string {
	structType, ok := item.Type.(*ast.StructType)
	if !ok {
		return nil
	}
	var invalid []string
	for _, field := range structType.Fields.List {
		for _, name := range field.Names {
			if name.Name == "Ch" {
				invalid = append(invalid, core.Concat(path, ": Watcher.Ch"))
			}
		}
	}
	return invalid
}

func compatibilityAliasesForValues(path string, item *ast.ValueSpec) []string {
	var invalid []string
	for _, name := range item.Names {
		if name.Name == "ErrNotFound" || name.Name == "ErrQuotaExceeded" {
			invalid = append(invalid, core.Concat(path, ": ", name.Name))
		}
	}
	return invalid
}

func repoGoFiles(t *testing.T, keep func(name string) bool) []string {
	t.Helper()

	result := testFilesystem().List(".")
	requireCoreOK(t, result)

	entries, ok := result.Value.([]fs.DirEntry)
	assertTruef(t, ok, "unexpected directory entry type: %T", result.Value)

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
	assertNoError(t, err)
	return file
}

func trimImportPath(value string) string {
	return core.TrimSuffix(core.TrimPrefix(value, `"`), `"`)
}

func testNamePrefix(path string) string {
	return core.Concat("Test", camelCase(core.TrimSuffix(path, testGoTestFileSuffix)), "_")
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
