package store_test

import (
	store "dappco.re/go/store"
)

func TestParquet_ExportParquet_Good(t *T) {
	count, err := store.ExportParquet("training", "parquet")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestParquet_ExportParquet_Bad(t *T) {
	count, err := store.ExportParquet("", "")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestParquet_ExportParquet_Ugly(t *T) {
	count, err := store.ExportParquet("with spaces", "out")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestParquet_ExportSplitParquet_Good(t *T) {
	count, err := store.ExportSplitParquet("train.jsonl", "parquet", "train")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestParquet_ExportSplitParquet_Bad(t *T) {
	count, err := store.ExportSplitParquet("", "", "")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestParquet_ExportSplitParquet_Ugly(t *T) {
	count, err := store.ExportSplitParquet("valid.jsonl", "parquet", "valid")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}
