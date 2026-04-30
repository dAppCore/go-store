package store

import (
	"time"

	core "dappco.re/go"
)

func ExampleStore_Transaction() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		return transaction.Set("config", "colour", "blue")
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_Exists() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		exists, existsResult := transaction.Exists("config", "colour")
		exampleRequireOK(existsResult)
		core.Println(exists)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_GroupExists() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		exists, existsResult := transaction.GroupExists("config")
		exampleRequireOK(existsResult)
		core.Println(exists)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_Get() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		value, getResult := transaction.Get("config", "colour")
		exampleRequireOK(getResult)
		core.Println(value)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_Set() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		return transaction.Set("config", "colour", "blue")
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_SetWithTTL() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		return transaction.SetWithTTL("cache", "token", "abc", time.Minute)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_Delete() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		return transaction.Delete("config", "colour")
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_DeleteGroup() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		return transaction.DeleteGroup("config")
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_DeletePrefix() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		return transaction.DeletePrefix("conf")
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_Count() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		count, countResult := transaction.Count("config")
		exampleRequireOK(countResult)
		core.Println(count)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_GetAll() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		values, valuesResult := transaction.GetAll("config")
		exampleRequireOK(valuesResult)
		core.Println(values["colour"])
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_GetPage() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		entries, entriesResult := transaction.GetPage("config", 0, 10)
		exampleRequireOK(entriesResult)
		core.Println(len(entries))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_All() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		entries := exampleCollectKeyValues(transaction.All("config"))
		core.Println(len(entries))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_AllSeq() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		entries := exampleCollectKeyValues(transaction.AllSeq("config"))
		core.Println(len(entries))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_CountAll() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		count, countResult := transaction.CountAll("conf")
		exampleRequireOK(countResult)
		core.Println(count)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_Groups() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		groups, groupsResult := transaction.Groups()
		exampleRequireOK(groupsResult)
		core.Println(groups)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_GroupsSeq() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		groups := exampleCollectGroups(transaction.GroupsSeq())
		core.Println(groups)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_Render() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "colour", "blue"))
		rendered, renderResult := transaction.Render("colour={{.colour}}", "config")
		exampleRequireOK(renderResult)
		core.Println(rendered)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_GetSplit() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "tags", "alpha,beta"))
		seq, splitResult := transaction.GetSplit("config", "tags", ",")
		exampleRequireOK(splitResult)
		core.Println(exampleCollectStrings(seq))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_GetFields() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.Set("config", "text", "alpha beta"))
		seq, fieldsResult := transaction.GetFields("config", "text")
		exampleRequireOK(fieldsResult)
		core.Println(exampleCollectStrings(seq))
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}

func ExampleStoreTransaction_PurgeExpired() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	result := storeInstance.Transaction(func(transaction *StoreTransaction) core.Result {
		exampleRequireOK(transaction.SetWithTTL("cache", "token", "abc", time.Nanosecond))
		removed, purgeResult := transaction.PurgeExpired()
		exampleRequireOK(purgeResult)
		core.Println(removed)
		return core.Ok(nil)
	})
	exampleRequireOK(result)
}
