package store

import (
	"iter"
	"testing"
	"time"

	core "dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	firstValue, err := storeInstance.Get("alpha", "first")
	require.NoError(t, err)
	assert.Equal(t, "1", firstValue)

	secondValue, err := storeInstance.Get("beta", "second")
	require.NoError(t, err)
	assert.Equal(t, "2", secondValue)

	received := drainEvents(events, 2, time.Second)
	require.Len(t, received, 2)
	assert.Equal(t, EventSet, received[0].Type)
	assert.Equal(t, "alpha", received[0].Group)
	assert.Equal(t, "first", received[0].Key)
	assert.Equal(t, EventSet, received[1].Type)
	assert.Equal(t, "beta", received[1].Group)
	assert.Equal(t, "second", received[1].Key)
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
	require.Error(t, err)

	_, err = storeInstance.Get("alpha", "first")
	assert.ErrorIs(t, err, NotFoundError)
}

func TestTransaction_Transaction_Good_DeletesAtomically(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("alpha", "first", "1"))
	require.NoError(t, storeInstance.Set("beta", "second", "2"))

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		if err := transaction.DeletePrefix(""); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	_, err = storeInstance.Get("alpha", "first")
	assert.ErrorIs(t, err, NotFoundError)
	_, err = storeInstance.Get("beta", "second")
	assert.ErrorIs(t, err, NotFoundError)
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
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"colour": "blue", "hosts": "alpha beta"}, entriesByKey)

		count, err := transaction.CountAll("")
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		groupNames, err := transaction.Groups()
		require.NoError(t, err)
		assert.Equal(t, []string{"audit", "config"}, groupNames)

		renderedTemplate, err := transaction.Render("{{ .colour }} / {{ .hosts }}", "config")
		require.NoError(t, err)
		assert.Equal(t, "blue / alpha beta", renderedTemplate)

		splitParts, err := transaction.GetSplit("config", "hosts", " ")
		require.NoError(t, err)
		assert.Equal(t, []string{"alpha", "beta"}, collectSeq(t, splitParts))

		fieldParts, err := transaction.GetFields("config", "hosts")
		require.NoError(t, err)
		assert.Equal(t, []string{"alpha", "beta"}, collectSeq(t, fieldParts))

		return nil
	})
	require.NoError(t, err)
}

func TestTransaction_Transaction_Good_PurgeExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.SetWithTTL("alpha", "ephemeral", "gone", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		removedRows, err := transaction.PurgeExpired()
		require.NoError(t, err)
		assert.Equal(t, int64(1), removedRows)
		return nil
	})
	require.NoError(t, err)

	_, err = storeInstance.Get("alpha", "ephemeral")
	assert.ErrorIs(t, err, NotFoundError)
}

func TestTransaction_Transaction_Good_Exists(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	require.NoError(t, storeInstance.Set("config", "colour", "blue"))

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		exists, err := transaction.Exists("config", "colour")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = transaction.Exists("config", "missing")
		require.NoError(t, err)
		assert.False(t, exists)

		return nil
	})
	require.NoError(t, err)
}

func TestTransaction_Transaction_Good_ExistsSeesPendingWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		exists, err := transaction.Exists("config", "colour")
		require.NoError(t, err)
		assert.False(t, exists)

		if err := transaction.Set("config", "colour", "blue"); err != nil {
			return err
		}

		exists, err = transaction.Exists("config", "colour")
		require.NoError(t, err)
		assert.True(t, exists)

		return nil
	})
	require.NoError(t, err)
}

func TestTransaction_Transaction_Good_GroupExists(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	err := storeInstance.Transaction(func(transaction *StoreTransaction) error {
		exists, err := transaction.GroupExists("config")
		require.NoError(t, err)
		assert.False(t, exists)

		if err := transaction.Set("config", "colour", "blue"); err != nil {
			return err
		}

		exists, err = transaction.GroupExists("config")
		require.NoError(t, err)
		assert.True(t, exists)

		return nil
	})
	require.NoError(t, err)
}

func TestTransaction_ScopedStoreTransaction_Good_ExistsAndGroupExists(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := NewScoped(storeInstance, "tenant-a")

	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		exists, err := transaction.Exists("colour")
		require.NoError(t, err)
		assert.False(t, exists)

		if err := transaction.Set("colour", "blue"); err != nil {
			return err
		}

		exists, err = transaction.Exists("colour")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = transaction.ExistsIn("other", "colour")
		require.NoError(t, err)
		assert.False(t, exists)

		if err := transaction.SetIn("config", "theme", "dark"); err != nil {
			return err
		}

		groupExists, err := transaction.GroupExists("config")
		require.NoError(t, err)
		assert.True(t, groupExists)

		groupExists, err = transaction.GroupExists("missing-group")
		require.NoError(t, err)
		assert.False(t, groupExists)

		return nil
	})
	require.NoError(t, err)
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
		require.NoError(t, err)
		require.Len(t, page, 1)
		assert.Equal(t, KeyValue{Key: "bravo", Value: "2"}, page[0])
		return nil
	})
	require.NoError(t, err)
}

func TestTransaction_ScopedStoreTransaction_Good_CommitsNamespacedWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 4, MaxGroups: 2},
	})
	require.NoError(t, err)

	err = scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		if err := transaction.Set("theme", "dark"); err != nil {
			return err
		}
		if err := transaction.SetIn("preferences", "locale", "en-GB"); err != nil {
			return err
		}

		themeValue, err := transaction.Get("theme")
		require.NoError(t, err)
		assert.Equal(t, "dark", themeValue)

		localeValue, err := transaction.GetFrom("preferences", "locale")
		require.NoError(t, err)
		assert.Equal(t, "en-GB", localeValue)

		groupNames, err := transaction.Groups()
		require.NoError(t, err)
		assert.Equal(t, []string{"default", "preferences"}, groupNames)

		return nil
	})
	require.NoError(t, err)

	themeValue, err := storeInstance.Get("tenant-a:default", "theme")
	require.NoError(t, err)
	assert.Equal(t, "dark", themeValue)

	localeValue, err := storeInstance.Get("tenant-a:preferences", "locale")
	require.NoError(t, err)
	assert.Equal(t, "en-GB", localeValue)
}

func TestTransaction_ScopedStoreTransaction_Good_PurgeExpired(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := NewScoped(storeInstance, "tenant-a")

	require.NoError(t, scopedStore.SetWithTTL("session", "token", "abc123", 1*time.Millisecond))
	time.Sleep(5 * time.Millisecond)

	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		removedRows, err := transaction.PurgeExpired()
		require.NoError(t, err)
		assert.Equal(t, int64(1), removedRows)
		return nil
	})
	require.NoError(t, err)

	_, err = scopedStore.GetFrom("session", "token")
	assert.ErrorIs(t, err, NotFoundError)
}

func TestTransaction_ScopedStoreTransaction_Good_QuotaUsesPendingWrites(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore, err := NewScopedConfigured(storeInstance, ScopedStoreConfig{
		Namespace: "tenant-a",
		Quota:     QuotaConfig{MaxKeys: 2, MaxGroups: 2},
	})
	require.NoError(t, err)

	err = scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		require.NoError(t, transaction.SetIn("group-1", "first", "1"))
		require.NoError(t, transaction.SetIn("group-2", "second", "2"))

		err := transaction.SetIn("group-2", "third", "3")
		require.Error(t, err)
		assert.True(t, core.Is(err, QuotaExceededError))
		return err
	})
	require.Error(t, err)
	assert.True(t, core.Is(err, QuotaExceededError))

	_, getErr := storeInstance.Get("tenant-a:group-1", "first")
	assert.True(t, core.Is(getErr, NotFoundError))
}

func TestTransaction_ScopedStoreTransaction_Good_DeletePrefix(t *testing.T) {
	storeInstance, _ := New(":memory:")
	defer storeInstance.Close()

	scopedStore := NewScoped(storeInstance, "tenant-a")
	otherScopedStore := NewScoped(storeInstance, "tenant-b")

	require.NoError(t, scopedStore.SetIn("cache", "theme", "dark"))
	require.NoError(t, scopedStore.SetIn("cache-warm", "status", "ready"))
	require.NoError(t, scopedStore.SetIn("config", "colour", "blue"))
	require.NoError(t, otherScopedStore.SetIn("cache", "theme", "keep"))

	err := scopedStore.Transaction(func(transaction *ScopedStoreTransaction) error {
		return transaction.DeletePrefix("cache")
	})
	require.NoError(t, err)

	_, getErr := scopedStore.GetFrom("cache", "theme")
	assert.True(t, core.Is(getErr, NotFoundError))
	_, getErr = scopedStore.GetFrom("cache-warm", "status")
	assert.True(t, core.Is(getErr, NotFoundError))

	colourValue, getErr := scopedStore.GetFrom("config", "colour")
	require.NoError(t, getErr)
	assert.Equal(t, "blue", colourValue)

	otherValue, getErr := otherScopedStore.GetFrom("cache", "theme")
	require.NoError(t, getErr)
	assert.Equal(t, "keep", otherValue)
}

func collectSeq[T any](t *testing.T, sequence iter.Seq[T]) []T {
	t.Helper()

	values := make([]T, 0)
	for value := range sequence {
		values = append(values, value)
	}
	return values
}
