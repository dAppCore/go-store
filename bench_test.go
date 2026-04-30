// SPDX-License-Identifier: EUPL-1.2
package store

import (
	"testing"

	core "dappco.re/go"
)

// Supplemental benchmarks beyond the core Set/Get/GetAll/FileBacked benchmarks
// in store_test.go. These add: varying group sizes, parallel throughput,
// Count on large groups, and Delete throughput.

func BenchmarkGetAll_VaryingSize(b *testing.B) {
	sizes := []int{10, 100, 1_000, 10_000}

	for _, size := range sizes {
		b.Run(core.Sprintf("size=%d", size), func(b *testing.B) {
			storeInstance, err := New(testMemoryDatabasePath)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = storeInstance.Close() }()

			for i := range size {
				_ = storeInstance.Set("bench", core.Sprintf(testKeyFormat, i), "value")
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, _ = storeInstance.GetAll("bench")
			}
		})
	}
}

func BenchmarkSetGet_Parallel(b *testing.B) {
	storeInstance, err := New(testMemoryDatabasePath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = storeInstance.Close() }()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := core.Sprintf(testKeyFormat, i)
			_ = storeInstance.Set("parallel", key, "value")
			_, _ = storeInstance.Get("parallel", key)
			i++
		}
	})
}

func BenchmarkCount_10K(b *testing.B) {
	storeInstance, err := New(testMemoryDatabasePath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = storeInstance.Close() }()

	for i := range 10_000 {
		_ = storeInstance.Set("bench", core.Sprintf(testKeyFormat, i), "value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_, _ = storeInstance.Count("bench")
	}
}

func BenchmarkDelete(b *testing.B) {
	storeInstance, err := New(testMemoryDatabasePath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = storeInstance.Close() }()

	// Pre-populate keys that will be deleted.
	for i := range b.N {
		_ = storeInstance.Set("bench", core.Sprintf(testKeyFormat, i), "value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_ = storeInstance.Delete("bench", core.Sprintf(testKeyFormat, i))
	}
}

func BenchmarkSetWithTTL(b *testing.B) {
	storeInstance, err := New(testMemoryDatabasePath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = storeInstance.Close() }()

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_ = storeInstance.SetWithTTL("bench", core.Sprintf(testKeyFormat, i), "value", 60_000_000_000) // 60s
	}
}

func BenchmarkRender(b *testing.B) {
	storeInstance, err := New(testMemoryDatabasePath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = storeInstance.Close() }()

	for i := range 50 {
		_ = storeInstance.Set("bench", core.Sprintf("key%d", i), core.Sprintf("val%d", i))
	}

	templateSource := `{{ .key0 }} {{ .key25 }} {{ .key49 }}`

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_, _ = storeInstance.Render(templateSource, "bench")
	}
}
