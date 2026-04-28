package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestPublishV090_Publish_Good(t *T) {
	inputDir := Path(t.TempDir(), "data")
	ax7WriteFile(t, Path(inputDir, "train.parquet"), "payload")
	output := NewBuffer()
	err := store.Publish(store.PublishConfig{InputDir: inputDir, Repo: "user/dataset", DryRun: true}, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "user/dataset")
}

func TestPublishV090_Publish_Bad(t *T) {
	output := NewBuffer()
	err := store.Publish(store.PublishConfig{InputDir: "", Repo: "user/dataset", DryRun: true}, output)
	AssertError(t, err)
	AssertEqual(t, "", output.String())
}

func TestPublishV090_Publish_Ugly(t *T) {
	inputDir := Path(t.TempDir(), "data")
	ax7WriteFile(t, Path(inputDir, "valid.parquet"), "payload")
	output := NewBuffer()
	err := store.Publish(store.PublishConfig{InputDir: inputDir, Repo: "user/dataset", Public: true, DryRun: true}, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "public")
}
