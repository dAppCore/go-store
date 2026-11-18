package store

import core "github.com/dappcore/go"

func ExampleExportParquet() {
	r := ExportParquet("training", "parquet")
	core.Println(r.OK)
}

func ExampleExportSplitParquet() {
	r := ExportSplitParquet("training/train.jsonl", "parquet", "train")
	core.Println(r.OK)
}
