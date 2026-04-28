package store_test

import . "dappco.re/go"

func TestJournalV090_Store_CommitToJournal_Good(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, map[string]string{"kind": "ax7"})
	AssertTrue(t, result.OK)
	AssertEqual(t, "measurement", result.Value.(map[string]any)["measurement"])
}

func TestJournalV090_Store_CommitToJournal_Bad(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.CommitToJournal("", nil, nil)
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "measurement")
}

func TestJournalV090_Store_CommitToJournal_Ugly(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.CommitToJournal("measurement", nil, nil)
	AssertTrue(t, result.OK)
	AssertNotNil(t, result.Value)
}

func TestJournalV090_Store_QueryJournal_Good(t *T) {
	storeInstance := ax7Store(t)
	RequireTrue(t, storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, nil).OK)
	result := storeInstance.QueryJournal("SELECT measurement FROM journal_entries")
	AssertTrue(t, result.OK)
	AssertNotEmpty(t, result.Value)
}

func TestJournalV090_Store_QueryJournal_Bad(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.QueryJournal("from(bucket: \"events\") |> range(start: nope)")
	AssertFalse(t, result.OK)
	AssertContains(t, result.Error(), "parse")
}

func TestJournalV090_Store_QueryJournal_Ugly(t *T) {
	storeInstance := ax7Store(t)
	result := storeInstance.QueryJournal("")
	AssertTrue(t, result.OK)
	AssertEmpty(t, result.Value)
}
