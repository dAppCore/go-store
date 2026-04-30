package store

import (
	"time"

	core "dappco.re/go"
)

func ExampleCompactOptions_Normalised() {
	options := CompactOptions{Before: time.Now().Add(-24 * time.Hour)}
	normalised := options.Normalised()
	core.Println(normalised.Format)
}

func ExampleCompactOptions_Validate() {
	options := CompactOptions{Before: time.Now().Add(-24 * time.Hour), Format: "gzip"}
	result := options.Validate()
	core.Println(result.OK)
}

func ExampleStore_Compact() {
	storeInstance := exampleOpenStore()
	defer exampleCloseStore(storeInstance)
	exampleRequireOK(storeInstance.CommitToJournal("measurement", map[string]any{"value": 1}, map[string]string{"kind": "example"}))
	result := storeInstance.Compact(CompactOptions{
		Before: time.Now().Add(time.Hour),
		Medium: newFixtureMedium(),
	})
	exampleRequireOK(result)
	core.Println(result.Value)
}
