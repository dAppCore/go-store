// SPDX-License-Identifier: EUPL-1.2

// JSON helpers for storage consumers.
// Re-exports the minimum JSON surface needed by downstream users like
// go-cache and go-tenant so they don't need to import encoding/json directly.
// Internally uses core/go JSON primitives.
package store

import core "dappco.re/go"

// RawMessage is a raw encoded JSON value.
// Use in structs where the JSON should be stored as-is without re-encoding.
//
// Usage example:
//
//	type CacheEntry struct {
//	    Data store.RawMessage `json:"data"`
//	}
//	cacheEntry := CacheEntry{Data: store.RawMessage([]byte("{\"name\":\"Alice\"}"))}
type RawMessage []byte

// MarshalJSONCore returns the raw bytes as-is. If empty, returns `null`.
//
// Usage example: `bytes, err := store.RawMessage([]byte("{\"name\":\"Alice\"}")).MarshalJSONCore()`
func (raw RawMessage) MarshalJSONCore() ([]byte, core.Result) {
	if len(raw) == 0 {
		return []byte("null"), core.Ok(nil)
	}
	return raw, core.Ok(nil)
}

// UnmarshalJSONCore stores the raw JSON bytes without decoding them.
//
// Usage example: `var raw store.RawMessage; err := raw.UnmarshalJSONCore([]byte("{\"name\":\"Alice\"}"))`
func (raw *RawMessage) UnmarshalJSONCore(data []byte) core.Result {
	if raw == nil {
		return core.Fail(core.E("store.RawMessage.UnmarshalJSONCore", "nil receiver", nil))
	}
	*raw = append((*raw)[:0], data...)
	return core.Ok(nil)
}

// MarshalIndent serialises a value to pretty-printed JSON bytes.
// Uses core.JSONMarshal internally then applies prefix/indent formatting
// so consumers get readable output without importing encoding/json.
//
// Usage example: `data, err := store.MarshalIndent(map[string]string{"name": "Alice"}, "", "  ")`
func MarshalIndent(value any, prefix, indent string) ([]byte, core.Result) {
	marshalled := core.JSONMarshal(value)
	if !marshalled.OK {
		if err, ok := marshalled.Value.(error); ok {
			return nil, core.Fail(core.E(opMarshalIndent, "marshal", err))
		}
		return nil, core.Fail(core.E(opMarshalIndent, "marshal", nil))
	}
	raw, ok := marshalled.Value.([]byte)
	if !ok {
		return nil, core.Fail(core.E(opMarshalIndent, "non-bytes result", nil))
	}
	if prefix == "" && indent == "" {
		return raw, core.Ok(nil)
	}

	buf := core.NewBuilder()
	if result := indentCompactJSON(buf, raw, prefix, indent); !result.OK {
		err, _ := result.Value.(error)
		return nil, core.Fail(core.E(opMarshalIndent, "indent", err))
	}
	return []byte(buf.String()), core.Ok(nil)
}

// indentCompactJSON formats compact JSON bytes with prefix+indent.
// Mirrors json.Indent's semantics without importing encoding/json.
//
// Usage example: `builder := core.NewBuilder(); _ = indentCompactJSON(builder, []byte("{\"name\":\"Alice\"}"), "", "  ")`
func indentCompactJSON(buf interface {
	WriteByte(byte) error
	WriteString(string) (int, error)
}, src []byte, prefix, indent string) core.Result {
	state := compactJSONIndentState{}

	writeNewlineIndent := func(level int) core.Result {
		if err := buf.WriteByte('\n'); err != nil {
			return core.Fail(err)
		}
		if _, err := buf.WriteString(prefix); err != nil {
			return core.Fail(err)
		}
		for i := 0; i < level; i++ {
			if _, err := buf.WriteString(indent); err != nil {
				return core.Fail(err)
			}
		}
		return core.Ok(nil)
	}

	for i := 0; i < len(src); i++ {
		c := src[i]
		if state.inString {
			if result := state.writeStringByte(buf, c); !result.OK {
				return result
			}
			continue
		}
		if result := state.writeValueByte(buf, src, i, writeNewlineIndent); !result.OK {
			return result
		}
	}
	return core.Ok(nil)
}

type compactJSONIndentState struct {
	depth    int
	inString bool
	escaped  bool
}

func (state *compactJSONIndentState) writeStringByte(buf interface {
	WriteByte(byte) error
}, c byte) core.Result {
	if err := buf.WriteByte(c); err != nil {
		return core.Fail(err)
	}
	if state.escaped {
		state.escaped = false
		return core.Ok(nil)
	}
	if c == '\\' {
		state.escaped = true
		return core.Ok(nil)
	}
	if c == '"' {
		state.inString = false
	}
	return core.Ok(nil)
}

func (state *compactJSONIndentState) writeValueByte(buf interface {
	WriteByte(byte) error
}, src []byte, index int, writeNewlineIndent func(int) core.Result) core.Result {
	c := src[index]
	switch c {
	case '"':
		state.inString = true
		if err := buf.WriteByte(c); err != nil {
			return core.Fail(err)
		}
		return core.Ok(nil)
	case '{', '[':
		return state.writeOpeningValueByte(buf, src, index, writeNewlineIndent)
	case '}', ']':
		return state.writeClosingValueByte(buf, src, index, writeNewlineIndent)
	case ',':
		if err := buf.WriteByte(c); err != nil {
			return core.Fail(err)
		}
		return writeNewlineIndent(state.depth)
	case ':':
		if err := buf.WriteByte(c); err != nil {
			return core.Fail(err)
		}
		if err := buf.WriteByte(' '); err != nil {
			return core.Fail(err)
		}
		return core.Ok(nil)
	case ' ', '\t', '\n', '\r':
		return core.Ok(nil)
	default:
		if err := buf.WriteByte(c); err != nil {
			return core.Fail(err)
		}
		return core.Ok(nil)
	}
}

func (state *compactJSONIndentState) writeOpeningValueByte(buf interface {
	WriteByte(byte) error
}, src []byte, index int, writeNewlineIndent func(int) core.Result) core.Result {
	if err := buf.WriteByte(src[index]); err != nil {
		return core.Fail(err)
	}
	state.depth++
	if index+1 < len(src) && (src[index+1] == '}' || src[index+1] == ']') {
		return core.Ok(nil)
	}
	return writeNewlineIndent(state.depth)
}

func (state *compactJSONIndentState) writeClosingValueByte(buf interface {
	WriteByte(byte) error
}, src []byte, index int, writeNewlineIndent func(int) core.Result) core.Result {
	if index > 0 && src[index-1] != '{' && src[index-1] != '[' {
		state.depth--
		if result := writeNewlineIndent(state.depth); !result.OK {
			return result
		}
	} else {
		state.depth--
	}
	if err := buf.WriteByte(src[index]); err != nil {
		return core.Fail(err)
	}
	return core.Ok(nil)
}
