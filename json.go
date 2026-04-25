// SPDX-License-Identifier: EUPL-1.2

// JSON helpers for storage consumers.
// Re-exports the minimum JSON surface needed by downstream users like
// go-cache and go-tenant so they don't need to import encoding/json directly.
// Internally uses core/go JSON primitives.
package store

import core "dappco.re/go/core"

// RawMessage is a raw encoded JSON value.
// Use in structs where the JSON should be stored as-is without re-encoding.
//
// Usage example:
//
//	type CacheEntry struct {
//	    Data store.RawMessage `json:"data"`
//	}
type RawMessage []byte

// MarshalJSON returns the raw bytes as-is. If empty, returns `null`.
//
// Usage example: `bytes, err := raw.MarshalJSON()`
func (raw RawMessage) MarshalJSON() ([]byte, error) {
	if len(raw) == 0 {
		return []byte("null"), nil
	}
	return raw, nil
}

// UnmarshalJSON stores the raw JSON bytes without decoding them.
//
// Usage example: `var raw store.RawMessage; err := raw.UnmarshalJSON(data)`
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
// Usage example: `data, err := store.MarshalIndent(entry, "", "  ")`
func MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	marshalled := core.JSONMarshal(v)
	if !marshalled.OK {
		if err, ok := marshalled.Value.(error); ok {
			return nil, core.E("store.MarshalIndent", "marshal", err)
		}
		return nil, core.E("store.MarshalIndent", "marshal", nil)
	}
	raw, ok := marshalled.Value.([]byte)
	if !ok {
		return nil, core.E("store.MarshalIndent", "non-bytes result", nil)
	}
	if prefix == "" && indent == "" {
		return raw, nil
	}

	buf := core.NewBuilder()
	if err := indentCompactJSON(buf, raw, prefix, indent); err != nil {
		return nil, core.E("store.MarshalIndent", "indent", err)
	}
	return []byte(buf.String()), nil
}

// indentCompactJSON formats compact JSON bytes with prefix+indent.
// Mirrors json.Indent's semantics without importing encoding/json.
//
// Usage example: `builder := core.NewBuilder(); _ = indentCompactJSON(builder, compact, "", "  ")`
func indentCompactJSON(buf interface {
	WriteByte(byte) error
	WriteString(string) (int, error)
}, src []byte, prefix, indent string) error {
	depth := 0
	inString := false
	escaped := false

	writeNewlineIndent := func(level int) {
		buf.WriteByte('\n')
		buf.WriteString(prefix)
		for i := 0; i < level; i++ {
			buf.WriteString(indent)
		}
	}

	for i := 0; i < len(src); i++ {
		c := src[i]
		if inString {
			buf.WriteByte(c)
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
			buf.WriteByte(c)
		case '{', '[':
			buf.WriteByte(c)
			depth++
			// Look ahead for empty object/array.
			if i+1 < len(src) && (src[i+1] == '}' || src[i+1] == ']') {
				continue
			}
			writeNewlineIndent(depth)
		case '}', ']':
			// Only indent if previous byte wasn't the matching opener.
			if i > 0 && src[i-1] != '{' && src[i-1] != '[' {
				depth--
				writeNewlineIndent(depth)
			} else {
				depth--
			}
			buf.WriteByte(c)
		case ',':
			buf.WriteByte(c)
			writeNewlineIndent(depth)
		case ':':
			buf.WriteByte(c)
			buf.WriteByte(' ')
		case ' ', '\t', '\n', '\r':
			// Drop whitespace from compact source.
		default:
			buf.WriteByte(c)
		}
	}
	return nil
}
