package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestTransactionV090_Store_Transaction_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error { return transaction.Set("config", "colour", "blue") })
	AssertNoError(t, err)
	AssertEqual(t, "blue", ax7MustGet(t, storeInstance, "config", "colour"))
}

func TestTransactionV090_Store_Transaction_Bad(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(nil)
	AssertError(t, err)
	AssertFalse(t, ax7MustExists(t, storeInstance, "config", "colour"))
}

func TestTransactionV090_Store_Transaction_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error { return NewError("rollback") })
	AssertError(t, err)
	AssertFalse(t, ax7MustExists(t, storeInstance, "config", "colour"))
}

func TestTransactionV090_StoreTransaction_Set_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		err := transaction.Set("config", "colour", "blue")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertEqual(t, "blue", ax7MustGet(t, storeInstance, "config", "colour"))
}

func TestTransactionV090_StoreTransaction_Set_Bad(t *T) {
	var transaction *store.StoreTransaction
	err := transaction.Set("config", "colour", "green")
	AssertError(t, err)
}

func TestTransactionV090_StoreTransaction_Set_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		err := transaction.Set("config", "colour", "green")
		AssertNoError(t, err)
		got, err := transaction.Get("config", "colour")
		AssertNoError(t, err)
		AssertEqual(t, "green", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_SetWithTTL_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		err := transaction.SetWithTTL("session", "token", "abc", Hour)
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertTrue(t, ax7MustExists(t, storeInstance, "session", "token"))
}

func TestTransactionV090_StoreTransaction_SetWithTTL_Bad(t *T) {
	var transaction *store.StoreTransaction
	err := transaction.SetWithTTL("session", "token", "abc", -Millisecond)
	AssertError(t, err)
}

func TestTransactionV090_StoreTransaction_SetWithTTL_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.SetWithTTL("session", "token", "abc", -Millisecond))
		exists, err := transaction.Exists("session", "token")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Get_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		got, err := transaction.Get("config", "colour")
		AssertNoError(t, err)
		AssertEqual(t, "blue", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Get_Bad(t *T) {
	var transaction *store.StoreTransaction
	got, err := transaction.Get("missing", "key")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestTransactionV090_StoreTransaction_Get_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		got, err := transaction.Get("", "")
		AssertError(t, err)
		AssertEqual(t, "", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Exists_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		exists, err := transaction.Exists("config", "colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Exists_Bad(t *T) {
	var transaction *store.StoreTransaction
	exists, err := transaction.Exists("missing", "key")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestTransactionV090_StoreTransaction_Exists_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		exists, err := transaction.Exists("", "")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GroupExists_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		exists, err := transaction.GroupExists("config")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GroupExists_Bad(t *T) {
	var transaction *store.StoreTransaction
	exists, err := transaction.GroupExists("missing")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestTransactionV090_StoreTransaction_GroupExists_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		exists, err := transaction.GroupExists("")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Delete_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		err := transaction.Delete("config", "colour")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertFalse(t, ax7MustExists(t, storeInstance, "config", "colour"))
}

func TestTransactionV090_StoreTransaction_Delete_Bad(t *T) {
	var transaction *store.StoreTransaction
	err := transaction.Delete("missing", "key")
	AssertError(t, err)
}

func TestTransactionV090_StoreTransaction_Delete_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		err := transaction.Delete("missing", "key")
		AssertNoError(t, err)
		exists, err := transaction.Exists("missing", "key")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_DeleteGroup_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		err := transaction.DeleteGroup("config")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertFalse(t, ax7MustGroupExists(t, storeInstance, "config"))
}

func TestTransactionV090_StoreTransaction_DeleteGroup_Bad(t *T) {
	var transaction *store.StoreTransaction
	err := transaction.DeleteGroup("missing")
	AssertError(t, err)
}

func TestTransactionV090_StoreTransaction_DeleteGroup_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		err := transaction.DeleteGroup("missing")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_DeletePrefix_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("tenant-a:config", "colour", "blue"))
		err := transaction.DeletePrefix("tenant-a:")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertFalse(t, ax7MustGroupExists(t, storeInstance, "tenant-a:config"))
}

func TestTransactionV090_StoreTransaction_DeletePrefix_Bad(t *T) {
	var transaction *store.StoreTransaction
	err := transaction.DeletePrefix("missing")
	AssertError(t, err)
}

func TestTransactionV090_StoreTransaction_DeletePrefix_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		err := transaction.DeletePrefix("missing")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Count_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		count, err := transaction.Count("config")
		AssertNoError(t, err)
		AssertEqual(t, 1, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Count_Bad(t *T) {
	var transaction *store.StoreTransaction
	count, err := transaction.Count("missing")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestTransactionV090_StoreTransaction_Count_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		count, err := transaction.Count("")
		AssertNoError(t, err)
		AssertEqual(t, 0, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GetAll_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		entries, err := transaction.GetAll("config")
		AssertNoError(t, err)
		AssertEqual(t, "1", entries["a"])
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GetAll_Bad(t *T) {
	var transaction *store.StoreTransaction
	entries, err := transaction.GetAll("missing")
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestTransactionV090_StoreTransaction_GetAll_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		entries, err := transaction.GetAll("")
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GetPage_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		page, err := transaction.GetPage("config", 0, 1)
		AssertNoError(t, err)
		AssertEqual(t, "a", page[0].Key)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GetPage_Bad(t *T) {
	var transaction *store.StoreTransaction
	page, err := transaction.GetPage("config", -1, 1)
	AssertError(t, err)
	AssertNil(t, page)
}

func TestTransactionV090_StoreTransaction_GetPage_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		page, err := transaction.GetPage("missing", 0, 1)
		AssertNoError(t, err)
		AssertEmpty(t, page)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_All_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		entries, err := ax7CollectKeyValues(transaction.All("config"))
		AssertNoError(t, err)
		AssertEqual(t, "a", entries[0].Key)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_All_Bad(t *T) {
	var transaction *store.StoreTransaction
	entries, err := ax7CollectKeyValues(transaction.All("missing"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestTransactionV090_StoreTransaction_All_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		entries, err := ax7CollectKeyValues(transaction.All(""))
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_AllSeq_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		entries, err := ax7CollectKeyValues(transaction.AllSeq("config"))
		AssertNoError(t, err)
		AssertEqual(t, "1", entries[0].Value)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_AllSeq_Bad(t *T) {
	var transaction *store.StoreTransaction
	entries, err := ax7CollectKeyValues(transaction.AllSeq("missing"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestTransactionV090_StoreTransaction_AllSeq_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		entries, err := ax7CollectKeyValues(transaction.AllSeq(""))
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_CountAll_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("tenant-a:config", "a", "1"))
		count, err := transaction.CountAll("tenant-a:")
		AssertNoError(t, err)
		AssertEqual(t, 1, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_CountAll_Bad(t *T) {
	var transaction *store.StoreTransaction
	count, err := transaction.CountAll("missing")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestTransactionV090_StoreTransaction_CountAll_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		count, err := transaction.CountAll("")
		AssertNoError(t, err)
		AssertEqual(t, 0, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Groups_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		groups, err := transaction.Groups()
		AssertNoError(t, err)
		AssertEqual(t, []string{"config"}, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Groups_Bad(t *T) {
	var transaction *store.StoreTransaction
	groups, err := transaction.Groups("missing")
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestTransactionV090_StoreTransaction_Groups_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		groups, err := transaction.Groups("")
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GroupsSeq_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		groups, err := ax7CollectGroups(transaction.GroupsSeq())
		AssertNoError(t, err)
		AssertEqual(t, []string{"config"}, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GroupsSeq_Bad(t *T) {
	var transaction *store.StoreTransaction
	groups, err := ax7CollectGroups(transaction.GroupsSeq("missing"))
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestTransactionV090_StoreTransaction_GroupsSeq_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		groups, err := ax7CollectGroups(transaction.GroupsSeq(""))
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Render_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "name", "alice"))
		rendered, err := transaction.Render("hello {{ .name }}", "config")
		AssertNoError(t, err)
		AssertEqual(t, "hello alice", rendered)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_Render_Bad(t *T) {
	var transaction *store.StoreTransaction
	rendered, err := transaction.Render("{{ .missing.field }}", "config")
	AssertError(t, err)
	AssertEqual(t, "", rendered)
}

func TestTransactionV090_StoreTransaction_Render_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		rendered, err := transaction.Render("empty", "missing")
		AssertNoError(t, err)
		AssertEqual(t, "empty", rendered)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GetSplit_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "hosts", "a,b"))
		seq, err := transaction.GetSplit("config", "hosts", ",")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GetSplit_Bad(t *T) {
	var transaction *store.StoreTransaction
	seq, err := transaction.GetSplit("missing", "hosts", ",")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestTransactionV090_StoreTransaction_GetSplit_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "hosts", "ab"))
		seq, err := transaction.GetSplit("config", "hosts", "")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GetFields_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "flags", "a b"))
		seq, err := transaction.GetFields("config", "flags")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_GetFields_Bad(t *T) {
	var transaction *store.StoreTransaction
	seq, err := transaction.GetFields("missing", "flags")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestTransactionV090_StoreTransaction_GetFields_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "flags", " a	 b  "))
		seq, err := transaction.GetFields("config", "flags")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_PurgeExpired_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		RequireNoError(t, transaction.SetWithTTL("session", "token", "abc", -Millisecond))
		removed, err := transaction.PurgeExpired()
		AssertNoError(t, err)
		AssertEqual(t, int64(1), removed)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransactionV090_StoreTransaction_PurgeExpired_Bad(t *T) {
	var transaction *store.StoreTransaction
	removed, err := transaction.PurgeExpired()
	AssertError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestTransactionV090_StoreTransaction_PurgeExpired_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *store.StoreTransaction) error {
		removed, err := transaction.PurgeExpired()
		AssertNoError(t, err)
		AssertEqual(t, int64(0), removed)
		return nil
	})
	AssertNoError(t, err)
}
