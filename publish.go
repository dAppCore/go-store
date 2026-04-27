// SPDX-License-Identifier: EUPL-1.2

package store

import (
	"bytes"
	"context"
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

	// Context controls cancellation for HuggingFace API requests. When nil,
	// Publish uses context.Background().
	//
	// Usage example:
	//
	//	cfg.Context = context.Background()
	Context context.Context

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
	if cfg.Repo == "" {
		return core.E("store.Publish", "repository is required", nil)
	}

	publishContext := cfg.Context
	if publishContext == nil {
		publishContext = context.Background()
	}

	token := resolveHFToken(cfg.Token)
	if token == "" && !cfg.DryRun {
		return core.E("store.Publish", "HuggingFace token required (--token, HF_TOKEN env, or ~/.huggingface/token)", nil)
	}

	files, hasSplit, err := collectUploadFiles(cfg.InputDir)
	if err != nil {
		return err
	}
	if !hasSplit {
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

	if err := ensureHFDatasetRepo(publishContext, token, cfg.Repo, cfg.Public); err != nil {
		return core.E("store.Publish", "ensure HuggingFace dataset", err)
	}

	for _, f := range files {
		if err := uploadFileToHF(publishContext, token, cfg.Repo, f.local, f.remote); err != nil {
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
	// Core populates DIR_HOME via os.UserHomeDir while this package keeps the
	// repository-wide ban on direct os imports.
	homes := []string{core.Env("DIR_HOME")}
	if homeEnv := core.Env("HOME"); homeEnv != "" && homeEnv != homes[0] {
		homes = append(homes, homeEnv)
	}
	for _, home := range homes {
		if home == "" {
			continue
		}
		r := localFs.Read(core.JoinPath(home, ".huggingface", "token"))
		if !r.OK {
			continue
		}
		token := core.Trim(r.Value.(string))
		if token != "" {
			return token
		}
	}
	return ""
}

// collectUploadFiles finds Parquet split files and an optional dataset card.
func collectUploadFiles(inputDir string) ([]uploadEntry, bool, error) {
	splits := []string{"train", "valid", "test"}
	var files []uploadEntry
	hasSplit := false

	for _, split := range splits {
		path := core.JoinPath(inputDir, split+".parquet")
		if !isFile(path) {
			continue
		}
		files = append(files, uploadEntry{path, core.Sprintf("data/%s.parquet", split)})
		hasSplit = true
	}

	// Check for dataset card in parent directory.
	cardPath := core.JoinPath(inputDir, "..", "dataset_card.md")
	if isFile(cardPath) {
		files = append(files, uploadEntry{cardPath, "README.md"})
	}

	return files, hasSplit, nil
}

func ensureHFDatasetRepo(ctx context.Context, token, repoID string, public bool) error {
	if repoID == "" {
		return core.E("store.ensureHFDatasetRepo", "repository is required", nil)
	}

	organisation, name := splitHFRepoID(repoID)
	if name == "" {
		return core.E("store.ensureHFDatasetRepo", "repository name is required", nil)
	}

	createPayload := map[string]any{
		"name":    name,
		"type":    "dataset",
		"private": !public,
	}
	if organisation != "" {
		createPayload["organization"] = organisation
	}

	createStatus, createBody, err := hfJSONRequest(ctx, token, http.MethodPost, "https://huggingface.co/api/repos/create", createPayload)
	if err != nil {
		return core.E("store.ensureHFDatasetRepo", "create dataset repository", err)
	}
	if createStatus >= 300 && createStatus != http.StatusConflict {
		return core.E("store.ensureHFDatasetRepo", core.Sprintf("create dataset failed: HTTP %d: %s", createStatus, createBody), nil)
	}

	settingsURL := core.Sprintf("https://huggingface.co/api/repos/dataset/%s/settings", repoID)
	settingsStatus, settingsBody, err := hfJSONRequest(ctx, token, http.MethodPut, settingsURL, map[string]any{
		"private": !public,
	})
	if err != nil {
		return core.E("store.ensureHFDatasetRepo", "update dataset visibility", err)
	}
	if settingsStatus >= 300 {
		return core.E("store.ensureHFDatasetRepo", core.Sprintf("update dataset visibility failed: HTTP %d: %s", settingsStatus, settingsBody), nil)
	}
	return nil
}

func splitHFRepoID(repoID string) (organisation string, name string) {
	parts := core.Split(repoID, "/")
	if len(parts) == 1 {
		return "", repoID
	}
	return parts[0], parts[1]
}

func hfJSONRequest(ctx context.Context, token, method, url string, payload map[string]any) (int, string, error) {
	payloadJSON := core.JSONMarshalString(payload)
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBufferString(payloadJSON))
	if err != nil {
		return 0, "", core.E("store.hfJSONRequest", "create request", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", core.E("store.hfJSONRequest", "send request", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", core.E("store.hfJSONRequest", "read response body", err)
	}
	return resp.StatusCode, string(body), nil
}

// uploadFileToHF uploads a single file to a HuggingFace dataset repo via the
// Hub API.
func uploadFileToHF(ctx context.Context, token, repoID, localPath, remotePath string) error {
	openResult := localFs.Open(localPath)
	if !openResult.OK {
		return core.E("store.uploadFileToHF", core.Sprintf("open %s", localPath), openResult.Value.(error))
	}
	file := openResult.Value.(fs.File)
	defer func() { _ = file.Close() }()

	url := core.Sprintf("https://huggingface.co/api/datasets/%s/upload/main/%s", repoID, remotePath)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, file)
	if err != nil {
		return core.E("store.uploadFileToHF", "create request", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")
	if stat, err := file.Stat(); err == nil {
		req.ContentLength = stat.Size()
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return core.E("store.uploadFileToHF", "upload request", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return core.E("store.uploadFileToHF", "read error response body", readErr)
		}
		return core.E("store.uploadFileToHF", core.Sprintf("upload failed: HTTP %d: %s", resp.StatusCode, string(body)), nil)
	}

	return nil
}
