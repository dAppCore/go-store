package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestCompactV090_Store_Compact_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireTrue(t, storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, nil).OK)
	result := storeInstance.Compact(store.CompactOptions{Before: Now().Add(Second), Output: t.TempDir(), Format: "gzip"})
	AssertTrue(t, result.OK)
	AssertContains(t, result.Value.(string), ".jsonl.gz")
}

func TestCompactV090_Store_Compact_Bad(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.Compact(store.CompactOptions{})
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "before")
}

func TestCompactV090_Store_Compact_Ugly(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.Compact(store.CompactOptions{Before: Now().Add(-Hour), Output: t.TempDir(), Format: "zstd"})
	AssertTrue(t, result.OK)
	AssertEqual(t, "", result.Value)
}

func TestCompactV090_CompactOptions_Normalised_Good(t *T) {
	options := store.CompactOptions{Before: Now()}
	normalised := options.Normalised()
	AssertEqual(t, "gzip", normalised.Format)
	AssertNotEmpty(t, normalised.Output)
}

func TestCompactV090_CompactOptions_Normalised_Bad(t *T) {
	options := store.CompactOptions{Format: "zstd", Output: "out"}
	normalised := options.Normalised()
	AssertEqual(t, "zstd", normalised.Format)
	AssertEqual(t, "out", normalised.Output)
}

func TestCompactV090_CompactOptions_Normalised_Ugly(t *T) {
	options := store.CompactOptions{Format: "  ", Output: ""}
	normalised := options.Normalised()
	AssertEqual(t, "gzip", normalised.Format)
	AssertNotEmpty(t, normalised.Output)
}

func TestCompactV090_CompactOptions_Validate_Good(t *T) {
	options := store.CompactOptions{Before: Now(), Format: "gzip", Output: t.TempDir()}
	err := options.Validate()
	AssertNoError(t, err)
}

func TestCompactV090_CompactOptions_Validate_Bad(t *T) {
	options := store.CompactOptions{Format: "gzip", Output: t.TempDir()}
	err := options.Validate()
	AssertError(t, err)
}

func TestCompactV090_CompactOptions_Validate_Ugly(t *T) {
	options := store.CompactOptions{Before: Now(), Format: "brotli", Output: t.TempDir()}
	err := options.Validate()
	AssertError(t, err)
}
