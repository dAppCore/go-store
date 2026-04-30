package store_test

import (
	store "dappco.re/go/store"
)

func TestJson_RawMessage_MarshalJSONCore_Good(t *T) {
	raw := store.RawMessage([]byte(`{"name":"alice"}`))
	data, err := raw.MarshalJSONCore()
	AssertNoError(t, err)
	AssertEqual(t, `{"name":"alice"}`, string(data))
}

func TestJson_RawMessage_MarshalJSONCore_Bad(t *T) {
	raw := store.RawMessage(nil)
	data, err := raw.MarshalJSONCore()
	AssertNoError(t, err)
	AssertEqual(t, "null", string(data))
}

func TestJson_RawMessage_MarshalJSONCore_Ugly(t *T) {
	raw := store.RawMessage([]byte("not-json"))
	data, err := raw.MarshalJSONCore()
	AssertNoError(t, err)
	AssertEqual(t, "not-json", string(data))
}

func TestJson_RawMessage_UnmarshalJSONCore_Good(t *T) {
	var raw store.RawMessage
	err := raw.UnmarshalJSONCore([]byte(`{"name":"alice"}`))
	AssertNoError(t, err)
	AssertEqual(t, `{"name":"alice"}`, string(raw))
}

func TestJson_RawMessage_UnmarshalJSONCore_Bad(t *T) {
	var raw *store.RawMessage
	err := raw.UnmarshalJSONCore([]byte(`{"name":"alice"}`))
	AssertError(t, err)
	AssertNil(t, raw)
}

func TestJson_RawMessage_UnmarshalJSONCore_Ugly(t *T) {
	raw := store.RawMessage([]byte("old"))
	err := raw.UnmarshalJSONCore([]byte("null"))
	AssertNoError(t, err)
	AssertEqual(t, "null", string(raw))
}

func TestJson_MarshalIndent_Good(t *T) {
	data, err := store.MarshalIndent(map[string]string{"name": "alice"}, "", "  ")
	AssertNoError(t, err)
	AssertContains(t, string(data), "\n")
}

func TestJson_MarshalIndent_Bad(t *T) {
	data, err := store.MarshalIndent(func() {
		// Intentionally empty: function values cannot be marshalled as JSON.
	}, "", "  ")
	AssertError(t, err)
	AssertNil(t, data)
}

func TestJson_MarshalIndent_Ugly(t *T) {
	data, err := store.MarshalIndent([]string{"a", "b"}, "", "")
	AssertNoError(t, err)
	AssertEqual(t, `["a","b"]`, string(data))
}
