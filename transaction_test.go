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

func collectSeq[T any](t *testing.T, sequence iter.Seq[T]) []T {
	t.Helper()

	values := make([]T, 0)
	for value := range sequence {
		values = append(values, value)
	}
	return values
}
