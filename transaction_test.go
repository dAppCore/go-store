package store

import (
	"iter"
	"testing"
	"time"

	core "dappco.re/go/core"
)

func TestTransaction_Transaction_Good_CommitsMultipleWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
	defer storeInstance.Close()

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
