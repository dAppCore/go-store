package store

import (
	"reflect"
	"sort"
	"testing"

	core "dappco.re/go/core"
)

func assertNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertNoErrorf(t testing.TB, err error, format string, args ...any) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v — "+format, append([]any{err}, args...)...)
	}
}

func assertError(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func assertErrorIs(t testing.TB, err, target error) {
	t.Helper()
	if !errIs(err, target) {
		t.Fatalf("expected error matching %v, got %v", target, err)
	}
}

func assertEqual(t testing.TB, want, got any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func assertEqualf(t testing.TB, want, got any, format string, args ...any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v — "+format, append([]any{want, got}, args...)...)
	}
}

func assertTrue(t testing.TB, cond bool) {
	t.Helper()
	if !cond {
		t.Fatal("expected true")
	}
}

func assertTruef(t testing.TB, cond bool, format string, args ...any) {
	t.Helper()
	if !cond {
		t.Fatalf("expected true — "+format, args...)
	}
}

func assertFalse(t testing.TB, cond bool) {
	t.Helper()
	if cond {
		t.Fatal("expected false")
	}
}

func assertFalsef(t testing.TB, cond bool, format string, args ...any) {
	t.Helper()
	if cond {
		t.Fatalf("expected false — "+format, args...)
	}
}

func assertNil(t testing.TB, value any) {
	t.Helper()
	if !isNil(value) {
		t.Fatalf("expected nil, got %v", value)
	}
}

func assertNilf(t testing.TB, value any, format string, args ...any) {
	t.Helper()
	if !isNil(value) {
		t.Fatalf("expected nil, got %v — "+format, append([]any{value}, args...)...)
	}
}

func assertNotNil(t testing.TB, value any) {
	t.Helper()
	if isNil(value) {
		t.Fatal("expected non-nil")
	}
}

func assertEmpty(t testing.TB, value any) {
	t.Helper()
	if !isEmpty(value) {
		t.Fatalf("expected empty, got %v", value)
	}
}

func assertEmptyf(t testing.TB, value any, format string, args ...any) {
	t.Helper()
	if !isEmpty(value) {
		t.Fatalf("expected empty, got %v — "+format, append([]any{value}, args...)...)
	}
}

func assertNotEmpty(t testing.TB, value any) {
	t.Helper()
	if isEmpty(value) {
		t.Fatal("expected non-empty")
	}
}

func assertLen(t testing.TB, value any, want int) {
	t.Helper()
	got := lenOf(value)
	if got != want {
		t.Fatalf("expected len %d, got %d", want, got)
	}
}

func assertLenf(t testing.TB, value any, want int, format string, args ...any) {
	t.Helper()
	got := lenOf(value)
	if got != want {
		t.Fatalf("expected len %d, got %d — "+format, append([]any{want, got}, args...)...)
	}
}

func assertContainsString(t testing.TB, haystack, needle string) {
	t.Helper()
	if !stringContains(haystack, needle) {
		t.Fatalf("expected %q to contain %q", haystack, needle)
	}
}

func assertContainsElement(t testing.TB, collection, element any) {
	t.Helper()
	if !containsElement(collection, element) {
		t.Fatalf("expected collection to contain %v", element)
	}
}

func assertElementsMatch(t testing.TB, want, got any) {
	t.Helper()
	if !elementsMatch(want, got) {
		t.Fatalf("expected same elements: want %v, got %v", want, got)
	}
}

func assertLessOrEqual(t testing.TB, got, want int) {
	t.Helper()
	if got > want {
		t.Fatalf("expected %d <= %d", got, want)
	}
}

func assertSamef(t testing.TB, want, got any, format string, args ...any) {
	t.Helper()
	if !samePointer(want, got) {
		t.Fatalf("expected same pointer, got %v vs %v — "+format, append([]any{want, got}, args...)...)
	}
}

func assertGreaterf(t testing.TB, got, want int, format string, args ...any) {
	t.Helper()
	if got <= want {
		t.Fatalf("expected %d > %d — "+format, append([]any{got, want}, args...)...)
	}
}

func assertNotPanics(t testing.TB, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	fn()
}

func errIs(err, target error) bool {
	return core.Is(err, target)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return rv.IsNil()
	}
	return false
}

func isEmpty(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.String:
		return rv.Len() == 0
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return true
		}
		return isEmpty(rv.Elem().Interface())
	}
	return reflect.DeepEqual(value, reflect.Zero(rv.Type()).Interface())
}

func lenOf(value any) int {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.String:
		return rv.Len()
	}
	return -1
}

func stringContains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func containsElement(collection, element any) bool {
	rv := reflect.ValueOf(collection)
	switch rv.Kind() {
	case reflect.String:
		needle, ok := element.(string)
		if !ok {
			return false
		}
		return stringContains(rv.String(), needle)
	case reflect.Array, reflect.Slice:
		for i := 0; i < rv.Len(); i++ {
			if reflect.DeepEqual(rv.Index(i).Interface(), element) {
				return true
			}
		}
		return false
	case reflect.Map:
		for _, key := range rv.MapKeys() {
			if reflect.DeepEqual(key.Interface(), element) {
				return true
			}
		}
		return false
	}
	return false
}

func elementsMatch(want, got any) bool {
	wantSlice := toAnySlice(want)
	gotSlice := toAnySlice(got)
	if wantSlice == nil || gotSlice == nil {
		return false
	}
	if len(wantSlice) != len(gotSlice) {
		return false
	}
	sortAny(wantSlice)
	sortAny(gotSlice)
	for i := range wantSlice {
		if !reflect.DeepEqual(wantSlice[i], gotSlice[i]) {
			return false
		}
	}
	return true
}

func toAnySlice(value any) []any {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Array, reflect.Slice:
		result := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			result[i] = rv.Index(i).Interface()
		}
		return result
	}
	return nil
}

func sortAny(values []any) {
	sort.Slice(values, func(i, j int) bool {
		return less(values[i], values[j])
	})
}

func less(a, b any) bool {
	aValue := reflect.ValueOf(a)
	bValue := reflect.ValueOf(b)
	if aValue.Kind() != bValue.Kind() {
		return aValue.Kind() < bValue.Kind()
	}
	switch aValue.Kind() {
	case reflect.String:
		return aValue.String() < bValue.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return aValue.Int() < bValue.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return aValue.Uint() < bValue.Uint()
	case reflect.Float32, reflect.Float64:
		return aValue.Float() < bValue.Float()
	}
	return false
}

func samePointer(want, got any) bool {
	wantValue := reflect.ValueOf(want)
	gotValue := reflect.ValueOf(got)
	if !wantValue.IsValid() || !gotValue.IsValid() {
		return false
	}
	if wantValue.Kind() != reflect.Ptr || gotValue.Kind() != reflect.Ptr {
		return false
	}
	return wantValue.Pointer() == gotValue.Pointer()
}
