// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bytes"
	"io"
	"io/fs"
	"net/http"
	"time"

	core "dappco.re/go/core"
)

// PublishConfig holds options for the publish operation.
//
// Usage example:
//
//	cfg := store.PublishConfig{InputDir: "/data/parquet", Repo: "snider/lem-training", Public: true}
type PublishConfig struct {
	// InputDir is the directory containing Parquet files to upload.
	//
	// Usage example:
	//
	//	cfg.InputDir // "/data/parquet"
	InputDir string

	// Repo is the HuggingFace dataset repository (e.g. "user/dataset").
	//
	// Usage example:
	//
	//	cfg.Repo // "snider/lem-training"
	Repo string

	// Public sets the dataset visibility to public when true.
	//
	// Usage example:
	//
	//	cfg.Public // true
	Public bool

	// Token is the HuggingFace API token. Falls back to HF_TOKEN env or ~/.huggingface/token.
	//
	// Usage example:
	//
	//	cfg.Token // "hf_..."
	Token string

	// DryRun lists files that would be uploaded without actually uploading.
	//
	// Usage example:
	//
	//	cfg.DryRun // true
	DryRun bool
}

// uploadEntry pairs a local file path with its remote destination.
type uploadEntry struct {
	local  string
	remote string
}

// Publish uploads Parquet files to HuggingFace Hub.
//
// It looks for train.parquet, valid.parquet, and test.parquet in InputDir,
// plus an optional dataset_card.md in the parent directory (uploaded as README.md).
// The token is resolved from PublishConfig.Token, the HF_TOKEN environment variable,
// or ~/.huggingface/token, in that order.
//
// Usage example:
//
//	err := store.Publish(store.PublishConfig{InputDir: "/data/parquet", Repo: "snider/lem-training"}, os.Stdout)
func Publish(cfg PublishConfig, w io.Writer) error {
	if cfg.InputDir == "" {
		return core.E("store.Publish", "input directory is required", nil)
	}

	token := resolveHFToken(cfg.Token)
	if token == "" && !cfg.DryRun {
		return core.E("store.Publish", "HuggingFace token required (--token, HF_TOKEN env, or ~/.huggingface/token)", nil)
	}

	files, err := collectUploadFiles(cfg.InputDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return core.E("store.Publish", core.Sprintf("no Parquet files found in %s", cfg.InputDir), nil)
	}

	if cfg.DryRun {
		core.Print(w, "Dry run: would publish to %s", cfg.Repo)
		if cfg.Public {
			core.Print(w, "  Visibility: public")
		} else {
			core.Print(w, "  Visibility: private")
		}
		for _, f := range files {
			statResult := localFs.Stat(f.local)
			if !statResult.OK {
				return core.E("store.Publish", core.Sprintf("stat %s", f.local), statResult.Value.(error))
			}
			info := statResult.Value.(fs.FileInfo)
			sizeMB := float64(info.Size()) / 1024 / 1024
			core.Print(w, "  %s -> %s (%.1f MB)", core.PathBase(f.local), f.remote, sizeMB)
		}
		return nil
	}

	core.Print(w, "Publishing to https://huggingface.co/datasets/%s", cfg.Repo)

	for _, f := range files {
		if err := uploadFileToHF(token, cfg.Repo, f.local, f.remote); err != nil {
			return core.E("store.Publish", core.Sprintf("upload %s", core.PathBase(f.local)), err)
		}
		core.Print(w, "  Uploaded %s -> %s", core.PathBase(f.local), f.remote)
	}

	core.Print(w, "\nPublished to https://huggingface.co/datasets/%s", cfg.Repo)
	return nil
}

// resolveHFToken returns a HuggingFace API token from the given value,
// HF_TOKEN env var, or ~/.huggingface/token file.
func resolveHFToken(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if env := core.Env("HF_TOKEN"); env != "" {
		return env
	}
	home := core.Env("DIR_HOME")
	if home == "" {
		return ""
	}
	r := localFs.Read(core.JoinPath(home, ".huggingface", "token"))
	if !r.OK {
		return ""
	}
	return core.Trim(r.Value.(string))
}

// collectUploadFiles finds Parquet split files and an optional dataset card.
func collectUploadFiles(inputDir string) ([]uploadEntry, error) {
	splits := []string{"train", "valid", "test"}
	var files []uploadEntry

	for _, split := range splits {
		path := core.JoinPath(inputDir, split+".parquet")
		if !isFile(path) {
			continue
		}
		files = append(files, uploadEntry{path, core.Sprintf("data/%s.parquet", split)})
	}

	// Check for dataset card in parent directory.
	cardPath := core.JoinPath(inputDir, "..", "dataset_card.md")
	if isFile(cardPath) {
		files = append(files, uploadEntry{cardPath, "README.md"})
	}

	return files, nil
}

// uploadFileToHF uploads a single file to a HuggingFace dataset repo via the
// Hub API.
func uploadFileToHF(token, repoID, localPath, remotePath string) error {
	readResult := localFs.Read(localPath)
	if !readResult.OK {
		return core.E("store.uploadFileToHF", core.Sprintf("read %s", localPath), readResult.Value.(error))
	}
	raw := []byte(readResult.Value.(string))

	url := core.Sprintf("https://huggingface.co/api/datasets/%s/upload/main/%s", repoID, remotePath)

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(raw))
	if err != nil {
		return core.E("store.uploadFileToHF", "create request", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return core.E("store.uploadFileToHF", "upload request", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return core.E("store.uploadFileToHF", core.Sprintf("upload failed: HTTP %d: %s", resp.StatusCode, string(body)), nil)
	}

	return nil
}
