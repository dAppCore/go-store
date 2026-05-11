package store_test

import (
	store "dappco.re/go/store"
)

func TestParquet_ExportParquet_Good(t *T) {
	r := store.ExportParquet("training", "parquet")
	AssertError(t, r)
}

func TestParquet_ExportParquet_Bad(t *T) {
	r := store.ExportParquet("", "")
	AssertError(t, r)
}

func TestParquet_ExportParquet_Ugly(t *T) {
	r := store.ExportParquet("with spaces", "out")
	AssertError(t, r)
}

func TestParquet_ExportSplitParquet_Good(t *T) {
	r := store.ExportSplitParquet("train.jsonl", "parquet", "train")
	AssertError(t, r)
}

func TestParquet_ExportSplitParquet_Bad(t *T) {
	r := store.ExportSplitParquet("", "", "")
	AssertError(t, r)
}

func TestParquet_ExportSplitParquet_Ugly(t *T) {
	r := store.ExportSplitParquet("valid.jsonl", "parquet", "valid")
	AssertError(t, r)
}
