package store

import (
	"iter"
	"testing"
	"time"

	core "dappco.re/go"
)

func TestTransaction_Transaction_Good_CommitsMultipleWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	events := storeInstance.Watch("*")
	defer storeInstance.Unwatch("*", events)

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		if err := transaction.Set("alpha", "first", "1"); err != nil {
			return err
		}
		if err := transaction.Set("beta", "second", "2"); err != nil {
			return err
		}
		return nil
	})
	assertNoError(t, err)

	firstValue, err := storeInstance.Get("alpha", "first")
	assertNoError(t, err)
	assertEqual(t, "1", firstValue)

	secondValue, err := storeInstance.Get("beta", "second")
	assertNoError(t, err)
	assertEqual(t, "2", secondValue)

	received := drainEvents(events, 2, time.Second)
	assertLen(t, received, 2)
	assertEqual(t, EventSet, received[0].Type)
	assertEqual(t, "alpha", received[0].Group)
	assertEqual(t, "first", received[0].Key)
	assertEqual(t, EventSet, received[1].Type)
	assertEqual(t, "beta", received[1].Group)
	assertEqual(t, "second", received[1].Key)
}

func TestTransaction_Transaction_Good_RollbackOnError(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		if err := transaction.Set("alpha", "first", "1"); err != nil {
			return err
		}
		return core.E("test", "force rollback", nil)
	})
	assertError(t, err)

	_, err = storeInstance.Get("alpha", "first")
	assertErrorIs(t, err, NotFoundError)
}

func TestTransaction_Transaction_Good_DeletesAtomically(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("alpha", "first", "1"))
	assertNoError(t, storeInstance.Set("beta", "second", "2"))

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		if err := transaction.DeletePrefix(""); err != nil {
			return err
		}
		return nil
	})
	assertNoError(t, err)

	_, err = storeInstance.Get("alpha", "first")
	assertErrorIs(t, err, NotFoundError)
	_, err = storeInstance.Get("beta", "second")
	assertErrorIs(t, err, NotFoundError)
}

func TestTransaction_Transaction_Good_ReadHelpersSeePendingWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		if err := transaction.Set("config", "colour", "blue"); err != nil {
			return err
		}
		if err := transaction.Set("config", "hosts", "alpha beta"); err != nil {
			return err
		}
		if err := transaction.Set("audit", "enabled", "true"); err != nil {
			return err
		}

		entriesByKey, err := transaction.GetAll("config")
		assertNoError(t, err)
		assertEqual(t, map[string]string{"colour": "blue", "hosts": "alpha beta"}, entriesByKey)

		count, err := transaction.CountAll("")
		assertNoError(t, err)
		assertEqual(t, 3, count)

		groupNames, err := transaction.Groups()
		assertNoError(t, err)
		assertEqual(t, []string{"audit", "config"}, groupNames)

		renderedTemplate, err := transaction.Render("{{ .colour }} / {{ .hosts }}", "config")
		assertNoError(t, err)
		assertEqual(t, "blue / alpha beta", renderedTemplate)

		splitParts, err := transaction.GetSplit("config", "hosts", " ")
		assertNoError(t, err)
		assertEqual(t, []string{"alpha", "beta"}, collectSeq(t, splitParts))

		fieldParts, err := transaction.GetFields("config", "hosts")
		assertNoError(t, err)
		assertEqual(t, []string{"alpha", "beta"}, collectSeq(t, fieldParts))

		return nil
	})
	assertNoError(t, err)
}

func TestTransaction_Transaction_Good_PurgeExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.SetWithTTL("alpha", "ephemeral", "gone", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		removedRows, err := transaction.PurgeExpired()
		assertNoError(t, err)
		assertEqual(t, int64(1), removedRows)
		return nil
	})
	assertNoError(t, err)

	_, err = storeInstance.Get("alpha", "ephemeral")
	assertErrorIs(t, err, NotFoundError)
}

func TestTransaction_Transaction_Good_Exists(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	assertNoError(t, storeInstance.Set("config", "colour", "blue"))

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		exists, err := transaction.Exists("config", "colour")
		assertNoError(t, err)
		assertTrue(t, exists)

		exists, err = transaction.Exists("config", "missing")
		assertNoError(t, err)
		assertFalse(t, exists)

		return nil
	})
	assertNoError(t, err)
}

func TestTransaction_Transaction_Good_ExistsSeesPendingWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		exists, err := transaction.Exists("config", "colour")
		assertNoError(t, err)
		assertFalse(t, exists)

		if err := transaction.Set("config", "colour", "blue"); err != nil {
			return err
		}

		exists, err = transaction.Exists("config", "colour")
		assertNoError(t, err)
		assertTrue(t, exists)

		return nil
	})
	assertNoError(t, err)
}

func TestTransaction_Transaction_Good_GroupExists(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		exists, err := transaction.GroupExists("config")
		assertNoError(t, err)
		assertFalse(t, exists)

		if err := transaction.Set("config", "colour", "blue"); err != nil {
			return err
		}

		exists, err = transaction.GroupExists("config")
		assertNoError(t, err)
		assertTrue(t, exists)

		return nil
	})
	assertNoError(t, err)
}

func TestTransaction_ScopedStoreTransaction_Good_ExistsAndGroupExists(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")

	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		exists, err := transaction.Exists("colour")
		assertNoError(t, err)
		assertFalse(t, exists)

		if err := transaction.Set("colour", "blue"); err != nil {
			return err
		}

		exists, err = transaction.Exists("colour")
		assertNoError(t, err)
		assertTrue(t, exists)

		exists, err = transaction.ExistsIn("other", "colour")
		assertNoError(t, err)
		assertFalse(t, exists)

		if err := transaction.SetIn("config", "theme", "dark"); err != nil {
			return err
		}

		groupExists, err := transaction.GroupExists("config")
		assertNoError(t, err)
		assertTrue(t, groupExists)

		groupExists, err = transaction.GroupExists("missing-group")
		assertNoError(t, err)
		assertFalse(t, groupExists)

		return nil
	})
	assertNoError(t, err)
}

func TestTransaction_ScopedStoreTransaction_Good_GetPage(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")

	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		if err := transaction.SetIn("items", "charlie", "3"); err != nil {
			return err
		}
		if err := transaction.SetIn("items", "alpha", "1"); err != nil {
			return err
		}
		if err := transaction.SetIn("items", "bravo", "2"); err != nil {
			return err
		}

		page, err := transaction.GetPage("items", 1, 1)
		assertNoError(t, err)
		assertLen(t, page, 1)
		assertEqual(t, KeyValue{Key: "bravo", Value: "2"}, page[0])
		return nil
	})
	assertNoError(t, err)
}

func TestTransaction_ScopedStoreTransaction_Good_CommitsNamespacedWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	assertNoError(t, err)

	err = scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		if err := transaction.Set("theme", "dark"); err != nil {
			return err
		}
		if err := transaction.SetIn("preferences", "locale", "en-GB"); err != nil {
			return err
		}

		themeValue, err := transaction.Get("theme")
		assertNoError(t, err)
		assertEqual(t, "dark", themeValue)

		localeValue, err := transaction.GetFrom("preferences", "locale")
		assertNoError(t, err)
		assertEqual(t, "en-GB", localeValue)

		groupNames, err := transaction.Groups()
		assertNoError(t, err)
		assertEqual(t, []string{"default", "preferences"}, groupNames)

		return nil
	})
	assertNoError(t, err)

	themeValue, err := storeInstance.Get("tenant-a:default", "theme")
	assertNoError(t, err)
	assertEqual(t, "dark", themeValue)

	localeValue, err := storeInstance.Get("tenant-a:preferences", "locale")
	assertNoError(t, err)
	assertEqual(t, "en-GB", localeValue)
}

func TestTransaction_ScopedStoreTransaction_Good_PurgeExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")

	assertNoError(t, scopedStore.SetWithTTL("session", "token", "abc123", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		removedRows, err := transaction.PurgeExpired()
		assertNoError(t, err)
		assertEqual(t, int64(1), removedRows)
		return nil
	})
	assertNoError(t, err)

	_, err = scopedStore.GetFrom("session", "token")
	assertErrorIs(t, err, NotFoundError)
}

func TestTransaction_ScopedStoreTransaction_Good_QuotaUsesPendingWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 2, MaxGroups: 2},
	})
	assertNoError(t, err)

	err = scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		assertNoError(t, transaction.SetIn("group-1", "first", "1"))
		assertNoError(t, transaction.SetIn("group-2", "second", "2"))

		err := transaction.SetIn("group-2", "third", "3")
		assertError(t, err)
		assertTrue(t, core.Is(err, QuotaExceededError))
		return err
	})
	assertError(t, err)
	assertTrue(t, core.Is(err, QuotaExceededError))

	_, getErr := storeInstance.Get("tenant-a:group-1", "first")
	assertTrue(t, core.Is(getErr, NotFoundError))
}

func TestTransaction_ScopedStoreTransaction_Good_DeletePrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer func() { _ = storeInstance.Close() }()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	otherScopedStore := NewScoped(storeInstance, "tenant-b")

	assertNoError(t, scopedStore.SetIn("cache", "theme", "dark"))
	assertNoError(t, scopedStore.SetIn("cache-warm", "status", "ready"))
	assertNoError(t, scopedStore.SetIn("config", "colour", "blue"))
	assertNoError(t, otherScopedStore.SetIn("cache", "theme", "keep"))

	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		return transaction.DeletePrefix("cache")
	})
	assertNoError(t, err)

	_, getErr := scopedStore.GetFrom("cache", "theme")
	assertTrue(t, core.Is(getErr, NotFoundError))
	_, getErr = scopedStore.GetFrom("cache-warm", "status")
	assertTrue(t, core.Is(getErr, NotFoundError))

	colourValue, getErr := scopedStore.GetFrom("config", "colour")
	assertNoError(t, getErr)
	assertEqual(t, "blue", colourValue)

	otherValue, getErr := otherScopedStore.GetFrom("cache", "theme")
	assertNoError(t, getErr)
	assertEqual(t, "keep", otherValue)
}

func collectSeq[T any](t *testing.T, sequence iter.Seq[T]) []T {
	t.Helper()

	values := make([]T, 0)
	for value := range sequence {
		values = append(values, value)
	}
	return values
}

func TestTransaction_Store_Transaction_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error { return transaction.Set("config", "colour", "blue") })
	AssertNoError(t, err)
	AssertEqual(t, "blue", ax7MustGet(t, storeInstance, "config", "colour"))
}

func TestTransaction_Store_Transaction_Bad(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(nil)
	AssertError(t, err)
	AssertFalse(t, ax7MustExists(t, storeInstance, "config", "colour"))
}

func TestTransaction_Store_Transaction_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error { return NewError("rollback") })
	AssertError(t, err)
	AssertFalse(t, ax7MustExists(t, storeInstance, "config", "colour"))
}

func TestTransaction_StoreTransaction_Set_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		err := transaction.Set("config", "colour", "blue")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertEqual(t, "blue", ax7MustGet(t, storeInstance, "config", "colour"))
}

func TestTransaction_StoreTransaction_Set_Bad(t *T) {
	var transaction *StoreTransaction
	err := transaction.Set("config", "colour", "green")
	AssertError(t, err)
}

func TestTransaction_StoreTransaction_Set_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
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

func TestTransaction_StoreTransaction_SetWithTTL_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		err := transaction.SetWithTTL("session", "token", "abc", Hour)
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertTrue(t, ax7MustExists(t, storeInstance, "session", "token"))
}

func TestTransaction_StoreTransaction_SetWithTTL_Bad(t *T) {
	var transaction *StoreTransaction
	err := transaction.SetWithTTL("session", "token", "abc", -Millisecond)
	AssertError(t, err)
}

func TestTransaction_StoreTransaction_SetWithTTL_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.SetWithTTL("session", "token", "abc", -Millisecond))
		exists, err := transaction.Exists("session", "token")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Get_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		got, err := transaction.Get("config", "colour")
		AssertNoError(t, err)
		AssertEqual(t, "blue", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Get_Bad(t *T) {
	var transaction *StoreTransaction
	got, err := transaction.Get("missing", "key")
	AssertError(t, err)
	AssertEqual(t, "", got)
}

func TestTransaction_StoreTransaction_Get_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		got, err := transaction.Get("", "")
		AssertError(t, err)
		AssertEqual(t, "", got)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Exists_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		exists, err := transaction.Exists("config", "colour")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Exists_Bad(t *T) {
	var transaction *StoreTransaction
	exists, err := transaction.Exists("missing", "key")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestTransaction_StoreTransaction_Exists_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		exists, err := transaction.Exists("", "")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GroupExists_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		exists, err := transaction.GroupExists("config")
		AssertNoError(t, err)
		AssertTrue(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GroupExists_Bad(t *T) {
	var transaction *StoreTransaction
	exists, err := transaction.GroupExists("missing")
	AssertError(t, err)
	AssertFalse(t, exists)
}

func TestTransaction_StoreTransaction_GroupExists_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		exists, err := transaction.GroupExists("")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Delete_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		err := transaction.Delete("config", "colour")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertFalse(t, ax7MustExists(t, storeInstance, "config", "colour"))
}

func TestTransaction_StoreTransaction_Delete_Bad(t *T) {
	var transaction *StoreTransaction
	err := transaction.Delete("missing", "key")
	AssertError(t, err)
}

func TestTransaction_StoreTransaction_Delete_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		err := transaction.Delete("missing", "key")
		AssertNoError(t, err)
		exists, err := transaction.Exists("missing", "key")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_DeleteGroup_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "colour", "blue"))
		err := transaction.DeleteGroup("config")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertFalse(t, ax7MustGroupExists(t, storeInstance, "config"))
}

func TestTransaction_StoreTransaction_DeleteGroup_Bad(t *T) {
	var transaction *StoreTransaction
	err := transaction.DeleteGroup("missing")
	AssertError(t, err)
}

func TestTransaction_StoreTransaction_DeleteGroup_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		err := transaction.DeleteGroup("missing")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_DeletePrefix_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("tenant-a:config", "colour", "blue"))
		err := transaction.DeletePrefix("tenant-a:")
		AssertNoError(t, err)
		return nil
	})
	AssertNoError(t, err)
	AssertFalse(t, ax7MustGroupExists(t, storeInstance, "tenant-a:config"))
}

func TestTransaction_StoreTransaction_DeletePrefix_Bad(t *T) {
	var transaction *StoreTransaction
	err := transaction.DeletePrefix("missing")
	AssertError(t, err)
}

func TestTransaction_StoreTransaction_DeletePrefix_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		err := transaction.DeletePrefix("missing")
		AssertNoError(t, err)
		exists, err := transaction.GroupExists("missing")
		AssertNoError(t, err)
		AssertFalse(t, exists)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Count_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		count, err := transaction.Count("config")
		AssertNoError(t, err)
		AssertEqual(t, 1, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Count_Bad(t *T) {
	var transaction *StoreTransaction
	count, err := transaction.Count("missing")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestTransaction_StoreTransaction_Count_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		count, err := transaction.Count("")
		AssertNoError(t, err)
		AssertEqual(t, 0, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GetAll_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		entries, err := transaction.GetAll("config")
		AssertNoError(t, err)
		AssertEqual(t, "1", entries["a"])
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GetAll_Bad(t *T) {
	var transaction *StoreTransaction
	entries, err := transaction.GetAll("missing")
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestTransaction_StoreTransaction_GetAll_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		entries, err := transaction.GetAll("")
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GetPage_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		page, err := transaction.GetPage("config", 0, 1)
		AssertNoError(t, err)
		AssertEqual(t, "a", page[0].Key)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GetPage_Bad(t *T) {
	var transaction *StoreTransaction
	page, err := transaction.GetPage("config", -1, 1)
	AssertError(t, err)
	AssertNil(t, page)
}

func TestTransaction_StoreTransaction_GetPage_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		page, err := transaction.GetPage("missing", 0, 1)
		AssertNoError(t, err)
		AssertEmpty(t, page)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_All_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		entries, err := ax7CollectKeyValues(transaction.All("config"))
		AssertNoError(t, err)
		AssertEqual(t, "a", entries[0].Key)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_All_Bad(t *T) {
	var transaction *StoreTransaction
	entries, err := ax7CollectKeyValues(transaction.All("missing"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestTransaction_StoreTransaction_All_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		entries, err := ax7CollectKeyValues(transaction.All(""))
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_AllSeq_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		entries, err := ax7CollectKeyValues(transaction.AllSeq("config"))
		AssertNoError(t, err)
		AssertEqual(t, "1", entries[0].Value)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_AllSeq_Bad(t *T) {
	var transaction *StoreTransaction
	entries, err := ax7CollectKeyValues(transaction.AllSeq("missing"))
	AssertError(t, err)
	AssertEmpty(t, entries)
}

func TestTransaction_StoreTransaction_AllSeq_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		entries, err := ax7CollectKeyValues(transaction.AllSeq(""))
		AssertNoError(t, err)
		AssertEmpty(t, entries)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_CountAll_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("tenant-a:config", "a", "1"))
		count, err := transaction.CountAll("tenant-a:")
		AssertNoError(t, err)
		AssertEqual(t, 1, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_CountAll_Bad(t *T) {
	var transaction *StoreTransaction
	count, err := transaction.CountAll("missing")
	AssertError(t, err)
	AssertEqual(t, 0, count)
}

func TestTransaction_StoreTransaction_CountAll_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		count, err := transaction.CountAll("")
		AssertNoError(t, err)
		AssertEqual(t, 0, count)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Groups_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		groups, err := transaction.Groups()
		AssertNoError(t, err)
		AssertEqual(t, []string{"config"}, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Groups_Bad(t *T) {
	var transaction *StoreTransaction
	groups, err := transaction.Groups("missing")
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestTransaction_StoreTransaction_Groups_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		groups, err := transaction.Groups("")
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GroupsSeq_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "a", "1"))
		groups, err := ax7CollectGroups(transaction.GroupsSeq())
		AssertNoError(t, err)
		AssertEqual(t, []string{"config"}, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GroupsSeq_Bad(t *T) {
	var transaction *StoreTransaction
	groups, err := ax7CollectGroups(transaction.GroupsSeq("missing"))
	AssertError(t, err)
	AssertEmpty(t, groups)
}

func TestTransaction_StoreTransaction_GroupsSeq_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		groups, err := ax7CollectGroups(transaction.GroupsSeq(""))
		AssertNoError(t, err)
		AssertEmpty(t, groups)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Render_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "name", "alice"))
		rendered, err := transaction.Render("hello {{ .name }}", "config")
		AssertNoError(t, err)
		AssertEqual(t, "hello alice", rendered)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_Render_Bad(t *T) {
	var transaction *StoreTransaction
	rendered, err := transaction.Render("{{ .missing.field }}", "config")
	AssertError(t, err)
	AssertEqual(t, "", rendered)
}

func TestTransaction_StoreTransaction_Render_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		rendered, err := transaction.Render("empty", "missing")
		AssertNoError(t, err)
		AssertEqual(t, "empty", rendered)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GetSplit_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "hosts", "a,b"))
		seq, err := transaction.GetSplit("config", "hosts", ",")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GetSplit_Bad(t *T) {
	var transaction *StoreTransaction
	seq, err := transaction.GetSplit("missing", "hosts", ",")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestTransaction_StoreTransaction_GetSplit_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "hosts", "ab"))
		seq, err := transaction.GetSplit("config", "hosts", "")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GetFields_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "flags", "a b"))
		seq, err := transaction.GetFields("config", "flags")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_GetFields_Bad(t *T) {
	var transaction *StoreTransaction
	seq, err := transaction.GetFields("missing", "flags")
	AssertError(t, err)
	AssertNil(t, seq)
}

func TestTransaction_StoreTransaction_GetFields_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.Set("config", "flags", " a	 b  "))
		seq, err := transaction.GetFields("config", "flags")
		AssertNoError(t, err)
		AssertEqual(t, []string{"a", "b"}, ax7CollectStrings(seq))
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_PurgeExpired_Good(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		RequireNoError(t, transaction.SetWithTTL("session", "token", "abc", -Millisecond))
		removed, err := transaction.PurgeExpired()
		AssertNoError(t, err)
		AssertEqual(t, int64(1), removed)
		return nil
	})
	AssertNoError(t, err)
}

func TestTransaction_StoreTransaction_PurgeExpired_Bad(t *T) {
	var transaction *StoreTransaction
	removed, err := transaction.PurgeExpired()
	AssertError(t, err)
	AssertEqual(t, int64(0), removed)
}

func TestTransaction_StoreTransaction_PurgeExpired_Ugly(t *T) {
	storeInstance := ax7Store(t)
	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		removed, err := transaction.PurgeExpired()
		AssertNoError(t, err)
		AssertEqual(t, int64(0), removed)
		return nil
	})
	AssertNoError(t, err)
}
