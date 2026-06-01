// SPDX-License-Identifier: EUPL-1.2

package store

import (
	goio "io"
	"testing"

	core "dappco.re/go"
)

// Real-behaviour coverage for the filesystemmedium proxy that backs
// localMedium(). Each method forwards to a *core.Fs and maps a failed
// core.Result through resultError. The store's import/export/compact paths use
// an internally-held medium, so these public-shaped proxy methods were 0%
// covered. The tests drive a real temp directory.

func newProxyMedium(t *testing.T) (Medium, string) {
	t.Helper()
	medium := localMedium()
	dir := t.TempDir()
	assertNoError(t, medium.EnsureDir(core.Path(dir, "nested")))
	return medium, dir
}

// ---------------------------------------------------------------------------
// Write / Read / WriteMode round-trip
// ---------------------------------------------------------------------------

func TestMediumProxy_WriteRead_Good_RoundTrip(t *testing.T) {
	medium, dir := newProxyMedium(t)
	path := core.Path(dir, "archive.jsonl")

	assertNoError(t, medium.Write(path, "payload"))
	content, r := medium.Read(path)
	assertNoError(t, r)
	assertEqual(t, "payload", content)
}

func TestMediumProxy_WriteMode_Good_WritesWithMode(t *testing.T) {
	medium, dir := newProxyMedium(t)
	path := core.Path(dir, "moded.jsonl")
	assertNoError(t, medium.WriteMode(path, "payload", 0o600))
	assertTrue(t, medium.IsFile(path))
}

func TestMediumProxy_Read_Bad_MissingFile(t *testing.T) {
	medium, dir := newProxyMedium(t)
	_, r := medium.Read(core.Path(dir, "absent.jsonl"))
	assertError(t, r)
}

// ---------------------------------------------------------------------------
// Existence / kind predicates
// ---------------------------------------------------------------------------

func TestMediumProxy_Predicates_Good_FileVsDir(t *testing.T) {
	medium, dir := newProxyMedium(t)
	path := core.Path(dir, "file.jsonl")
	assertNoError(t, medium.Write(path, "x"))

	assertTrue(t, medium.Exists(path))
	assertTrue(t, medium.IsFile(path))
	assertFalse(t, medium.IsDir(path))

	assertTrue(t, medium.IsDir(core.Path(dir, "nested")))
	assertFalse(t, medium.IsFile(core.Path(dir, "nested")))
	assertFalse(t, medium.Exists(core.Path(dir, "ghost")))
}

// ---------------------------------------------------------------------------
// Stat / List
// ---------------------------------------------------------------------------

func TestMediumProxy_Stat_Good_ReportsSize(t *testing.T) {
	medium, dir := newProxyMedium(t)
	path := core.Path(dir, "sized.jsonl")
	assertNoError(t, medium.Write(path, "1234567890"))

	info, r := medium.Stat(path)
	assertNoError(t, r)
	assertEqual(t, int64(10), info.Size())
}

func TestMediumProxy_Stat_Bad_MissingFile(t *testing.T) {
	medium, dir := newProxyMedium(t)
	_, r := medium.Stat(core.Path(dir, "absent"))
	assertError(t, r)
}

func TestMediumProxy_List_Good_ListsEntries(t *testing.T) {
	medium, dir := newProxyMedium(t)
	assertNoError(t, medium.Write(core.Path(dir, "a.jsonl"), "a"))
	assertNoError(t, medium.Write(core.Path(dir, "b.jsonl"), "b"))

	entries, r := medium.List(dir)
	assertNoError(t, r)
	assertTrue(t, len(entries) >= 2)
}

func TestMediumProxy_List_Bad_MissingDirectory(t *testing.T) {
	medium, dir := newProxyMedium(t)
	_, r := medium.List(core.Path(dir, "no-such-dir"))
	assertError(t, r)
}

// ---------------------------------------------------------------------------
// Stream open/create/append + Rename + Delete + DeleteAll
// ---------------------------------------------------------------------------

func TestMediumProxy_CreateOpenStreams_Good_WriteThenReadStream(t *testing.T) {
	medium, dir := newProxyMedium(t)
	path := core.Path(dir, "stream.jsonl")

	writer, r := medium.Create(path)
	assertNoError(t, r)
	_, err := writer.Write([]byte("line\n"))
	assertNoError(t, err)
	assertNoError(t, writer.Close())

	appender, r := medium.Append(path)
	assertNoError(t, r)
	_, err = appender.Write([]byte("more\n"))
	assertNoError(t, err)
	assertNoError(t, appender.Close())

	reader, r := medium.ReadStream(path)
	assertNoError(t, r)
	data, err := goio.ReadAll(reader)
	assertNoError(t, err)
	assertNoError(t, reader.Close())
	assertEqual(t, "line\nmore\n", string(data))

	file, r := medium.Open(path)
	assertNoError(t, r)
	assertNoError(t, file.Close())
}

func TestMediumProxy_WriteStream_Good_Writes(t *testing.T) {
	medium, dir := newProxyMedium(t)
	path := core.Path(dir, "ws.jsonl")
	writer, r := medium.WriteStream(path)
	assertNoError(t, r)
	_, err := writer.Write([]byte("data"))
	assertNoError(t, err)
	assertNoError(t, writer.Close())
	assertTrue(t, medium.IsFile(path))
}

func TestMediumProxy_RenameDelete_Good_Lifecycle(t *testing.T) {
	medium, dir := newProxyMedium(t)
	src := core.Path(dir, "src.jsonl")
	dst := core.Path(dir, "dst.jsonl")
	assertNoError(t, medium.Write(src, "x"))
	assertNoError(t, medium.Rename(src, dst))
	assertFalse(t, medium.Exists(src))
	assertTrue(t, medium.Exists(dst))

	assertNoError(t, medium.Delete(dst))
	assertFalse(t, medium.Exists(dst))
}

func TestMediumProxy_DeleteAll_Good_RemovesTree(t *testing.T) {
	medium, dir := newProxyMedium(t)
	sub := core.Path(dir, "tree")
	assertNoError(t, medium.EnsureDir(sub))
	assertNoError(t, medium.Write(core.Path(sub, "f.jsonl"), "x"))
	assertNoError(t, medium.DeleteAll(sub))
	assertFalse(t, medium.Exists(sub))
}

// ---------------------------------------------------------------------------
// resultError — maps a non-error Result value to a store.Medium failure
// ---------------------------------------------------------------------------

func TestMediumProxy_ResultError_Ugly_NonErrorValueWrapped(t *testing.T) {
	r := resultError(core.Result{Value: "plain string failure", OK: false})
	assertError(t, r)
	assertContainsString(t, r.Error(), "plain string failure")
}

// ---------------------------------------------------------------------------
// csvField — quoting rules for CSV export
// ---------------------------------------------------------------------------

func TestMediumProxy_CSVField_Good_PlainValueUnquoted(t *testing.T) {
	assertEqual(t, "homelab", csvField("homelab"))
	assertEqual(t, "", csvField(""))
}

func TestMediumProxy_CSVField_Ugly_QuotesSpecialCharacters(t *testing.T) {
	assertEqual(t, `"a,b"`, csvField("a,b"))
	assertEqual(t, "\"line\nbreak\"", csvField("line\nbreak"))
	assertEqual(t, "\"carriage\rreturn\"", csvField("carriage\rreturn"))
	// Embedded double-quotes are doubled inside the wrapping quotes.
	assertEqual(t, `"say ""hi"""`, csvField(`say "hi"`))
}
