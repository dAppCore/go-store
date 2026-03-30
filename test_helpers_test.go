package store

import (
	"testing"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/require"
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
	require.True(tb, result.OK, "core result failed: %v", result.Value)
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
