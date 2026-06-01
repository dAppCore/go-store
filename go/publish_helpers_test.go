package store

import (
	"testing"
)

// Coverage for the pure HuggingFace repo-id splitter. The networked publish
// helpers (uploadFileToHF, hfJSONRequest, ensureHFDatasetRepo) require live
// HTTP and stay uncovered by design in an offline test pass; splitHFRepoID is
// pure and was 0% covered.

func TestPublishHelpers_SplitHFRepoID_Good_OrgAndName(t *testing.T) {
	org, name := splitHFRepoID("lthn/lemma-corpus")
	assertEqual(t, "lthn", org)
	assertEqual(t, "lemma-corpus", name)
}

func TestPublishHelpers_SplitHFRepoID_Bad_NoSlashIsNameOnly(t *testing.T) {
	org, name := splitHFRepoID("lemma-corpus")
	assertEqual(t, "", org)
	assertEqual(t, "lemma-corpus", name)
}

func TestPublishHelpers_SplitHFRepoID_Ugly_ExtraSegmentsKeepFirstTwo(t *testing.T) {
	// Split returns three parts; the function only reads parts[0] and parts[1].
	org, name := splitHFRepoID("lthn/lemma/extra")
	assertEqual(t, "lthn", org)
	assertEqual(t, "lemma", name)
}
