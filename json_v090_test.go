package store_test

import (
	. "dappco.re/go"
	store "dappco.re/go/store"
)

func TestJsonV090_RawMessage_MarshalJSON_Good(t *T) {
	raw := store.RawMessage([]byte(`{"name":"alice"}`))
	data, err := raw.MarshalJSON()
	AssertNoError(t, err)
	AssertEqual(t, `{"name":"alice"}`, string(data))
}

func TestJsonV090_RawMessage_MarshalJSON_Bad(t *T) {
	raw := store.RawMessage(nil)
	data, err := raw.MarshalJSON()
	AssertNoError(t, err)
	AssertEqual(t, "null", string(data))
}

func TestJsonV090_RawMessage_MarshalJSON_Ugly(t *T) {
	raw := store.RawMessage([]byte("not-json"))
	data, err := raw.MarshalJSON()
	AssertNoError(t, err)
	AssertEqual(t, "not-json", string(data))
}

func TestJsonV090_RawMessage_UnmarshalJSON_Good(t *T) {
	var raw store.RawMessage
	err := raw.UnmarshalJSON([]byte(`{"name":"alice"}`))
	AssertNoError(t, err)
	AssertEqual(t, `{"name":"alice"}`, string(raw))
}

func TestJsonV090_RawMessage_UnmarshalJSON_Bad(t *T) {
	var raw *store.RawMessage
	err := raw.UnmarshalJSON([]byte(`{"name":"alice"}`))
	AssertError(t, err)
	AssertNil(t, raw)
}

func TestJsonV090_RawMessage_UnmarshalJSON_Ugly(t *T) {
	raw := store.RawMessage([]byte("old"))
	err := raw.UnmarshalJSON([]byte("null"))
	AssertNoError(t, err)
	AssertEqual(t, "null", string(raw))
}

func TestJsonV090_MarshalIndent_Good(t *T) {
	data, err := store.MarshalIndent(map[string]string{"name": "alice"}, "", "  ")
	AssertNoError(t, err)
	AssertContains(t, string(data), "\n")
}

func TestJsonV090_MarshalIndent_Bad(t *T) {
	data, err := store.MarshalIndent(func() {}, "", "  ")
	AssertError(t, err)
	AssertNil(t, data)
}

func TestJsonV090_MarshalIndent_Ugly(t *T) {
	data, err := store.MarshalIndent([]string{"a", "b"}, "", "")
	AssertNoError(t, err)
	AssertEqual(t, `["a","b"]`, string(data))
}
