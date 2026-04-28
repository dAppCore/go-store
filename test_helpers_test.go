package store

import (
	"testing"

	core "dappco.re/go"
)

func testFilesystem() *core.Fs {
	return (&core.Fs{}).NewUnrestricted()
}

func testPath(tb testing.TB, name string) string {
	tb.Helper()
	return core.Path(tb.TempDir(), name)
}

func requireCoreOK(tb testing.TB, result core.Result) {
	tb.Helper()
	assertTruef(tb, result.OK, "core result failed: %v", result.Value)
}

func requireCoreReadBytes(tb testing.TB, path string) []byte {
	tb.Helper()
	result := testFilesystem().Read(path)
	requireCoreOK(tb, result)
	return []byte(result.Value.(string))
}

func requireCoreWriteBytes(tb testing.TB, path string, data []byte) {
	tb.Helper()
	requireCoreOK(tb, testFilesystem().Write(path, string(data)))
}

func repeatString(value string, count int) string {
	if count <= 0 {
		return ""
	}
	builder := core.NewBuilder()
	for range count {
		builder.WriteString(value)
	}
	return builder.String()
}

func useWorkspaceStateDirectory(tb testing.TB) string {
	tb.Helper()

	previous := defaultWorkspaceStateDirectory
	stateDirectory := testPath(tb, "state")
	defaultWorkspaceStateDirectory = stateDirectory
	tb.Cleanup(func() {
		defaultWorkspaceStateDirectory = previous
		_ = testFilesystem().DeleteAll(stateDirectory)
	})
	return stateDirectory
}

func useArchiveOutputDirectory(tb testing.TB) string {
	tb.Helper()

	previous := defaultArchiveOutputDirectory
	outputDirectory := testPath(tb, "archive")
	defaultArchiveOutputDirectory = outputDirectory
	tb.Cleanup(func() {
		defaultArchiveOutputDirectory = previous
		_ = testFilesystem().DeleteAll(outputDirectory)
	})
	return outputDirectory
}

func requireResultRows(tb testing.TB, result core.Result) []map[string]any {
	tb.Helper()

	assertTruef(tb, result.OK, "core result failed: %v", result.Value)
	rows, ok := result.Value.([]map[string]any)
	assertTruef(tb, ok, "unexpected row type: %T", result.Value)
	return rows
}
