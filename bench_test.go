// SPDX-Licence-Identifier: EUPL-1.2
package store

import (
	"testing"

	core "dappco.re/go/core"
)

// Supplemental benchmarks beyond the core Set/Get/GetAll/FileBacked benchmarks
// in store_test.go. These add: varying group sizes, parallel throughput,
// Count on large groups, and Delete throughput.

func BenchmarkGetAll_VaryingSize(b *testing.B) {
	sizes := []int{10, 100, 1_000, 10_000}

	for _, size := range sizes {
		b.Run(core.Sprintf("size=%d", size), func(b *testing.B) {
			s, err := New(":memory:")
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()

			for i := range size {
				_ = s.Set("bench", core.Sprintf("key-%d", i), "value")
			}

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, _ = s.GetAll("bench")
			}
		})
	}
}

func BenchmarkSetGet_Parallel(b *testing.B) {
	s, err := New(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := core.Sprintf("key-%d", i)
			_ = s.Set("parallel", key, "value")
			_, _ = s.Get("parallel", key)
			i++
		}
	})
}

func BenchmarkCount_10K(b *testing.B) {
	s, err := New(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	for i := range 10_000 {
		_ = s.Set("bench", core.Sprintf("key-%d", i), "value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_, _ = s.Count("bench")
	}
}

func BenchmarkDelete(b *testing.B) {
	s, err := New(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	// Pre-populate keys that will be deleted.
	for i := range b.N {
		_ = s.Set("bench", core.Sprintf("key-%d", i), "value")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_ = s.Delete("bench", core.Sprintf("key-%d", i))
	}
}

func BenchmarkSetWithTTL(b *testing.B) {
	s, err := New(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_ = s.SetWithTTL("bench", core.Sprintf("key-%d", i), "value", 60_000_000_000) // 60s
	}
}

func BenchmarkRender(b *testing.B) {
	s, err := New(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	for i := range 50 {
		_ = s.Set("bench", core.Sprintf("key%d", i), core.Sprintf("val%d", i))
	}

	tmpl := `{{ .key0 }} {{ .key25 }} {{ .key49 }}`

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_, _ = s.Render(tmpl, "bench")
	}
}
