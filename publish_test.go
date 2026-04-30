package store

import (
	"bytes"
	"testing"

	core "dappco.re/go"
)

func TestPublish_Publish_Bad_EmptyRepository(t *testing.T) {
	var output bytes.Buffer

	err := Publish(PublishConfig{InputDir: t.TempDir(), DryRun: true}, &output)

	assertError(t, err)
	assertContainsString(t, err.Error(), "repository is required")
}

func TestPublish_Publish_Bad_DatasetCardWithoutParquetSplit(t *testing.T) {
	inputDir := core.JoinPath(t.TempDir(), "data")
	requireCoreOK(t, testFilesystem().EnsureDir(inputDir))
	requireCoreWriteBytes(t, core.JoinPath(inputDir, "..", "dataset_card.md"), []byte("# Dataset\n"))

	var output bytes.Buffer
	err := Publish(PublishConfig{InputDir: inputDir, Repo: "snider/lem-training", DryRun: true}, &output)

	assertError(t, err)
	assertContainsString(t, err.Error(), "no Parquet files found")
}

func TestPublish_ResolveHFToken_Good_UserHomeFallback(t *testing.T) {
	homeDirectory := t.TempDir()
	t.Setenv("HF_TOKEN", "")
	t.Setenv("DIR_HOME", "")
	t.Setenv("HOME", homeDirectory)

	tokenDirectory := core.JoinPath(homeDirectory, ".huggingface")
	requireCoreOK(t, testFilesystem().EnsureDir(tokenDirectory))
	requireCoreWriteBytes(t, core.JoinPath(tokenDirectory, "token"), []byte(" hf_file_token \n"))

	assertEqual(t, "hf_file_token", resolveHFToken(""))
}

func TestPublish_Publish_Good(t *T) {
	inputDir := Path(t.TempDir(), "data")
	ax7WriteFile(t, Path(inputDir, "train.parquet"), "payload")
	output := NewBuffer()
	err := Publish(PublishConfig{InputDir: inputDir, Repo: testHFDatasetID, DryRun: true}, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), testHFDatasetID)
}

func TestPublish_Publish_Bad(t *T) {
	output := NewBuffer()
	err := Publish(PublishConfig{InputDir: "", Repo: testHFDatasetID, DryRun: true}, output)
	AssertError(t, err)
	AssertEqual(t, "", output.String())
}

func TestPublish_Publish_Ugly(t *T) {
	inputDir := Path(t.TempDir(), "data")
	ax7WriteFile(t, Path(inputDir, "valid.parquet"), "payload")
	output := NewBuffer()
	err := Publish(PublishConfig{InputDir: inputDir, Repo: testHFDatasetID, Public: true, DryRun: true}, output)
	AssertNoError(t, err)
	AssertContains(t, output.String(), "public")
}
