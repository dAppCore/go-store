package store

import (
	"bytes"
	"testing"

	core "dappco.re/go/core"
)

func TestPublish_Publish_Bad_EmptyRepository(t *testing.T) {
	var output bytes.Buffer

	err := Publish(PublishConfig{InputDir: t.TempDir(), DryRun: true}, &output)

	assertError(t, err)
	assertContainsString(t, err.Error(), "repository is required")
}

func TestPublish_ResolveHFToken_Good_UserHomeFallback(t *testing.T) {
	homeDirectory := t.TempDir()
	t.Setenv("HF_TOKEN", "")
	t.Setenv("HOME", homeDirectory)

	tokenDirectory := core.JoinPath(homeDirectory, ".huggingface")
	requireCoreOK(t, testFilesystem().EnsureDir(tokenDirectory))
	requireCoreWriteBytes(t, core.JoinPath(tokenDirectory, "token"), []byte(" hf_file_token \n"))

	assertEqual(t, "hf_file_token", resolveHFToken(""))
}
