package store

import core "dappco.re/go"

func ExamplePublish() {
	buffer := core.NewBuffer()
	result := Publish(PublishConfig{
		InputDir: "parquet",
		Repo:     "snider/lem-training",
		DryRun:   true,
	}, buffer)
	core.Println(result.OK)
}
