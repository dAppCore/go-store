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

// MarshalJSON returns the raw bytes as-is. If empty, returns `null`.
//
// Usage example: `bytes, err := store.RawMessage([]byte("{\"name\":\"Alice\"}")).MarshalJSON()`
func (raw RawMessage) MarshalJSON() ([]byte, error) {
	if len(raw) == 0 {
		return []byte("null"), nil
	}
	return raw, nil
}

// UnmarshalJSON stores the raw JSON bytes without decoding them.
//
// Usage example: `var raw store.RawMessage; err := raw.UnmarshalJSON([]byte("{\"name\":\"Alice\"}"))`
func (raw *RawMessage) UnmarshalJSON(data []byte) error {
	if raw == nil {
		return core.E("store.RawMessage.UnmarshalJSON", "nil receiver", nil)
	}
	*raw = append((*raw)[:0], data...)
	return nil
}

// MarshalIndent serialises a value to pretty-printed JSON bytes.
// Uses core.JSONMarshal internally then applies prefix/indent formatting
// so consumers get readable output without importing encoding/json.
//
// Usage example: `data, err := store.MarshalIndent(map[string]string{"name": "Alice"}, "", "  ")`
func MarshalIndent(value any, prefix, indent string) ([]byte, error) {
	marshalled := core.JSONMarshal(value)
	if !marshalled.OK {
		if err, ok := marshalled.Value.(error); ok {
			return nil, core.E(opMarshalIndent, "marshal", err)
		}
		return nil, core.E(opMarshalIndent, "marshal", nil)
	}
	raw, ok := marshalled.Value.([]byte)
	if !ok {
		return nil, core.E(opMarshalIndent, "non-bytes result", nil)
	}
	if prefix == "" && indent == "" {
		return raw, nil
	}

	buf := core.NewBuilder()
	if err := indentCompactJSON(buf, raw, prefix, indent); err != nil {
		return nil, core.E(opMarshalIndent, "indent", err)
	}
	return []byte(buf.String()), nil
}

// indentCompactJSON formats compact JSON bytes with prefix+indent.
// Mirrors json.Indent's semantics without importing encoding/json.
//
// Usage example: `builder := core.NewBuilder(); _ = indentCompactJSON(builder, []byte("{\"name\":\"Alice\"}"), "", "  ")`
func indentCompactJSON(buf interface {
	WriteByte(byte) error
	WriteString(string) (int, error)
}, src []byte, prefix, indent string) error {
	state := compactJSONIndentState{}

	writeNewlineIndent := func(level int) error {
		if err := buf.WriteByte('\n'); err != nil {
			return err
		}
		if _, err := buf.WriteString(prefix); err != nil {
			return err
		}
		for i := 0; i < level; i++ {
			if _, err := buf.WriteString(indent); err != nil {
				return err
			}
		}
		return nil
	}

	for i := 0; i < len(src); i++ {
		c := src[i]
		if state.inString {
			if err := state.writeStringByte(buf, c); err != nil {
				return err
			}
			continue
		}
		if err := state.writeValueByte(buf, src, i, writeNewlineIndent); err != nil {
			return err
		}
	}
	return nil
}

type compactJSONIndentState struct {
	depth    int
	inString bool
	escaped  bool
}

func (state *compactJSONIndentState) writeStringByte(buf interface {
	WriteByte(byte) error
}, c byte) error {
	if err := buf.WriteByte(c); err != nil {
		return err
	}
	if state.escaped {
		state.escaped = false
		return nil
	}
	if c == '\\' {
		state.escaped = true
		return nil
	}
	if c == '"' {
		state.inString = false
	}
	return nil
}

func (state *compactJSONIndentState) writeValueByte(buf interface {
	WriteByte(byte) error
}, src []byte, index int, writeNewlineIndent func(int) error) error {
	c := src[index]
	switch c {
	case '"':
		state.inString = true
		return buf.WriteByte(c)
	case '{', '[':
		return state.writeOpeningValueByte(buf, src, index, writeNewlineIndent)
	case '}', ']':
		return state.writeClosingValueByte(buf, src, index, writeNewlineIndent)
	case ',':
		if err := buf.WriteByte(c); err != nil {
			return err
		}
		return writeNewlineIndent(state.depth)
	case ':':
		if err := buf.WriteByte(c); err != nil {
			return err
		}
		return buf.WriteByte(' ')
	case ' ', '\t', '\n', '\r':
		return nil
	default:
		return buf.WriteByte(c)
	}
}

func (state *compactJSONIndentState) writeOpeningValueByte(buf interface {
	WriteByte(byte) error
}, src []byte, index int, writeNewlineIndent func(int) error) error {
	if err := buf.WriteByte(src[index]); err != nil {
		return err
	}
	state.depth++
	if index+1 < len(src) && (src[index+1] == '}' || src[index+1] == ']') {
		return nil
	}
	return writeNewlineIndent(state.depth)
}

func (state *compactJSONIndentState) writeClosingValueByte(buf interface {
	WriteByte(byte) error
}, src []byte, index int, writeNewlineIndent func(int) error) error {
	if index > 0 && src[index-1] != '{' && src[index-1] != '[' {
		state.depth--
		if err := writeNewlineIndent(state.depth); err != nil {
			return err
		}
	} else {
		state.depth--
	}
	return buf.WriteByte(src[index])
}
