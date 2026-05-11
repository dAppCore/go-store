package store

import core "dappco.re/go"

func ExampleExportParquet() {
	r := ExportParquet("training", "parquet")
	core.Println(r.OK)
}

func ExampleExportSplitParquet() {
	r := ExportSplitParquet("training/train.jsonl", "parquet", "train")
	core.Println(r.OK)
}
