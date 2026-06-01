// SPDX-License-Identifier: EUPL-1.2

package store_test

import (
	store "dappco.re/go/store"
)

// Exercises the indentCompactJSON byte-encoder branches through the public
// MarshalIndent surface: nested objects/arrays, empty containers, key/value
// spacing, and the in-string escape handling (escaped quotes and commas inside
// strings must not trigger re-indentation). writeValueByte / writeStringByte
// were ~50% covered.

func TestJsonIndent_MarshalIndent_Good_NestedStructure(t *T) {
	value := map[string]any{
		"outer": map[string]any{"inner": []any{float64(1), float64(2)}},
	}
	data, err := store.MarshalIndent(value, "", "  ")
	AssertNoError(t, err)
	out := string(data)
	// Nested object + array force several newline-indent emissions.
	AssertContains(t, out, "\n")
	AssertContains(t, out, "  ")
	AssertContains(t, out, `"inner"`)
}

func TestJsonIndent_MarshalIndent_Ugly_EmptyContainersStayInline(t *T) {
	// Empty object and array hit the look-ahead branch that suppresses the
	// newline-indent for immediately-closing brackets.
	emptyObject, err := store.MarshalIndent(map[string]any{}, "", "  ")
	AssertNoError(t, err)
	AssertEqual(t, "{}", string(emptyObject))

	emptyArray, err := store.MarshalIndent([]any{}, "", "  ")
	AssertNoError(t, err)
	AssertEqual(t, "[]", string(emptyArray))
}

func TestJsonIndent_MarshalIndent_Ugly_StringWithSpecialCharacters(t *T) {
	// Commas, colons, braces and escaped quotes inside a string value must be
	// emitted verbatim — the in-string state suppresses structural formatting.
	value := map[string]string{"key": `a, b: {c} "quoted"`}
	data, err := store.MarshalIndent(value, "", "  ")
	AssertNoError(t, err)
	out := string(data)
	AssertContains(t, out, `a, b: {c} \"quoted\"`)
	// The single key/value pair means exactly one ": " separator outside the
	// string, and the literal comma inside the string did not add a newline.
	AssertContains(t, out, `"key": `)
}

func TestJsonIndent_MarshalIndent_Good_PrefixApplied(t *T) {
	data, err := store.MarshalIndent(map[string]string{"a": "1"}, ">>", "  ")
	AssertNoError(t, err)
	AssertContains(t, string(data), "\n>>")
}
