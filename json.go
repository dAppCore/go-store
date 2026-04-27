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
// Usage example: `builder := core.NewBuilder(); _ = indentCompactJSON(builder, []byte("{\"name\":\"Alice\"}"), "", "  ")`
func indentCompactJSON(buf interface {
	WriteByte(byte) error
	WriteString(string) (int, error)
}, src []byte, prefix, indent string) error {
	depth := 0
	inString := false
	escaped := false

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
		if inString {
			if err := buf.WriteByte(c); err != nil {
				return err
			}
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
			if err := buf.WriteByte(c); err != nil {
				return err
			}
		case '{', '[':
			if err := buf.WriteByte(c); err != nil {
				return err
			}
			depth++
			// Look ahead for empty object/array.
			if i+1 < len(src) && (src[i+1] == '}' || src[i+1] == ']') {
				continue
			}
			if err := writeNewlineIndent(depth); err != nil {
				return err
			}
		case '}', ']':
			// Only indent if previous byte wasn't the matching opener.
			if i > 0 && src[i-1] != '{' && src[i-1] != '[' {
				depth--
				if err := writeNewlineIndent(depth); err != nil {
					return err
				}
			} else {
				depth--
			}
			if err := buf.WriteByte(c); err != nil {
				return err
			}
		case ',':
			if err := buf.WriteByte(c); err != nil {
				return err
			}
			if err := writeNewlineIndent(depth); err != nil {
				return err
			}
		case ':':
			if err := buf.WriteByte(c); err != nil {
				return err
			}
			if err := buf.WriteByte(' '); err != nil {
				return err
			}
		case ' ', '\t', '\n', '\r':
			// Drop whitespace from compact source.
		default:
			if err := buf.WriteByte(c); err != nil {
				return err
			}
		}
	}
	return nil
}
