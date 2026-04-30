package store

import core "dappco.re/go"

func ExampleRawMessage_MarshalJSONCore() {
	raw := RawMessage([]byte(`{"name":"Ada"}`))
	data, result := raw.MarshalJSONCore()
	exampleRequireOK(result)
	core.Println(string(data))
}

func ExampleRawMessage_UnmarshalJSONCore() {
	var raw RawMessage
	result := raw.UnmarshalJSONCore([]byte(`{"name":"Ada"}`))
	exampleRequireOK(result)
	core.Println(string(raw))
}

func ExampleMarshalIndent() {
	data, result := MarshalIndent(map[string]string{"name": "Ada"}, "", "  ")
	exampleRequireOK(result)
	core.Println(string(data))
}
