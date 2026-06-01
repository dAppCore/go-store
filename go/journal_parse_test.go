package store

import (
	"testing"
)

// Real-behaviour coverage for the journal scalar number parsers. These pure
// functions back parseJournalScalarValue, which decides whether a Flux filter
// literal is a bool, int, float, or unquoted string. They had zero direct
// coverage before this file.

// ---------------------------------------------------------------------------
// parseJournalFloat64 — happy path
// ---------------------------------------------------------------------------

func TestJournalParse_ParseJournalFloat64_Good_WholeAndFraction(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  float64
	}{
		{"whole", "42", 42},
		{"fraction", "3.5", 3.5},
		{"leadingZeroFraction", "0.25", 0.25},
		{"negativeFraction", "-1.5", -1.5},
		{"explicitPositive", "+2.5", 2.5},
		{"trailingDotConsumesNothing", "7.", 7},
	}
	for _, tc := range cases {
		got, result := parseJournalFloat64(tc.value)
		assertNoErrorf(t, result, "value %q", tc.value)
		assertEqualf(t, tc.want, got, "value %q", tc.value)
	}
}

// ---------------------------------------------------------------------------
// parseJournalFloat64 — rejected inputs (Bad)
// ---------------------------------------------------------------------------

func TestJournalParse_ParseJournalFloat64_Bad_EmptyValue(t *testing.T) {
	_, result := parseJournalFloat64("")
	assertError(t, result)
	assertContainsString(t, result.Error(), "float value is empty")
}

func TestJournalParse_ParseJournalFloat64_Bad_SignWithoutDigits(t *testing.T) {
	_, result := parseJournalFloat64("-")
	assertError(t, result)
	assertContainsString(t, result.Error(), "float value has no digits")
}

func TestJournalParse_ParseJournalFloat64_Bad_NonNumericTrailer(t *testing.T) {
	_, result := parseJournalFloat64("3.5ms")
	assertError(t, result)
	assertContainsString(t, result.Error(), "float value contains invalid characters")
}

func TestJournalParse_ParseJournalFloat64_Bad_DotOnly(t *testing.T) {
	// A lone "." has a sign-free prefix, zero whole digits, and the fraction
	// loop consumes the dot but finds no digits — total digit count is zero.
	_, result := parseJournalFloat64(".")
	assertError(t, result)
	assertContainsString(t, result.Error(), "float value has no digits")
}

// ---------------------------------------------------------------------------
// parseJournalFloat64 — boundary / range (Ugly)
// ---------------------------------------------------------------------------

func TestJournalParse_ParseJournalFloat64_Ugly_OverflowWholePart(t *testing.T) {
	// 310 nines overflows the maxJournalFloat64 guard in the whole-number loop.
	huge := ""
	for range 310 {
		huge += "9"
	}
	_, result := parseJournalFloat64(huge)
	assertError(t, result)
	assertContainsString(t, result.Error(), "float value is out of range")
}

// ---------------------------------------------------------------------------
// parseJournalInt64 — happy path + boundaries
// ---------------------------------------------------------------------------

func TestJournalParse_ParseJournalInt64_Good_SignedValues(t *testing.T) {
	cases := []struct {
		value string
		want  int64
	}{
		{"0", 0},
		{"42", 42},
		{"-42", -42},
		{"+42", 42},
		{"9223372036854775807", 1<<63 - 1},
		{"-9223372036854775808", -1 << 63},
	}
	for _, tc := range cases {
		got, result := parseJournalInt64(tc.value)
		assertNoErrorf(t, result, "value %q", tc.value)
		assertEqualf(t, tc.want, got, "value %q", tc.value)
	}
}

func TestJournalParse_ParseJournalInt64_Bad_NonDigit(t *testing.T) {
	_, result := parseJournalInt64("12x")
	assertError(t, result)
	assertContainsString(t, result.Error(), "non-digit characters")
}

func TestJournalParse_ParseJournalInt64_Bad_Empty(t *testing.T) {
	_, result := parseJournalInt64("")
	assertError(t, result)
	assertContainsString(t, result.Error(), "integer value is empty")
}

func TestJournalParse_ParseJournalInt64_Ugly_PositiveOverflow(t *testing.T) {
	_, result := parseJournalInt64("9223372036854775808")
	assertError(t, result)
	assertContainsString(t, result.Error(), "out of range")
}

func TestJournalParse_ParseJournalInt64_Ugly_NegativeOverflow(t *testing.T) {
	_, result := parseJournalInt64("-9223372036854775809")
	assertError(t, result)
	assertContainsString(t, result.Error(), "out of range")
}

// ---------------------------------------------------------------------------
// parseJournalScalarValue — the public dispatcher over the parsers above
// ---------------------------------------------------------------------------

func TestJournalParse_ParseJournalScalarValue_Good_TypeDispatch(t *testing.T) {
	if value, ok := parseJournalScalarValue("true"); !ok || value != true {
		t.Fatalf("true: got (%v, %v)", value, ok)
	}
	if value, ok := parseJournalScalarValue("false"); !ok || value != false {
		t.Fatalf("false: got (%v, %v)", value, ok)
	}
	if value, ok := parseJournalScalarValue("7"); !ok || value != int64(7) {
		t.Fatalf("int: got (%v, %v)", value, ok)
	}
	if value, ok := parseJournalScalarValue("7.5"); !ok || value != 7.5 {
		t.Fatalf("float: got (%v, %v)", value, ok)
	}
}

func TestJournalParse_ParseJournalScalarValue_Bad_UnquotedStringIsNotScalar(t *testing.T) {
	value, ok := parseJournalScalarValue("homelab")
	assertFalse(t, ok)
	assertNil(t, value)
}
