package store

import core "dappco.re/go"

func ExampleExportParquet() {
	count, result := ExportParquet("training", "parquet")
	core.Println(count, result.OK)
}

func ExampleExportSplitParquet() {
	count, result := ExportSplitParquet("training/train.jsonl", "parquet", "train")
	core.Println(count, result.OK)
}
