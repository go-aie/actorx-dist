// Package wire implements the SDK's self-contained remote v1 wire boundary.
package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"
)

const Version = "actorx.remote/v1"
const MaxBytes = 8192

// Validate bounds the entire encoded value and preserves number precision.
func Validate(raw []byte) error {
	if len(raw) > MaxBytes || !utf8.Valid(raw) || !validSurrogates(raw) {
		return fmt.Errorf("invalid JSON encoding or size")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 64 {
			return fmt.Errorf("JSON depth exceeded")
		}
		t, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for d.More() {
				t, err := d.Token()
				if err != nil {
					return err
				}
				key, ok := t.(string)
				if !ok || keys[key] {
					return fmt.Errorf("duplicate JSON key")
				}
				keys[key] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected delimiter")
		}
		_, err = d.Token()
		return err
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

// encoding/json replaces unpaired UTF-16 escapes; reject them before decoding.
// Syntax itself is checked by the token walker.
func validSurrogates(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) || raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n < 0xd800 || n > 0xdbff {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

func Encode(value any) (encoded []byte, failure error) {
	// Custom MarshalJSON/MarshalText is user code. Keep diagnostics controlled
	// even when it panics; never expose its panic value or stack.
	defer func() {
		if recover() != nil {
			encoded, failure = nil, fmt.Errorf("JSON encoding failed")
		}
	}()
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("JSON encoding failed")
	}
	if err := Validate(raw); err != nil {
		return nil, fmt.Errorf("invalid encoded JSON")
	}
	return raw, nil
}

func Decode(raw []byte) (any, error) {
	if err := Validate(raw); err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value any
	err := d.Decode(&value)
	return value, err
}

// Fields enforces exact parameter names and field presence; null stays distinct.
func Fields(raw []byte, allowed []string, required ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("expected object")
	}
	for k := range fields {
		found := false
		for _, name := range allowed {
			if k == name {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown parameter")
		}
	}
	for _, k := range required {
		if _, ok := fields[k]; !ok {
			return nil, fmt.Errorf("missing parameter")
		}
	}
	return fields, nil
}
