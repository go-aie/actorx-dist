// Package strictjson bounds JSON syntax before decoding into protocol structs.
package strictjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

// Validate requires one UTF-8 JSON value with unique object keys and bounded complexity.
// Unknown fields and protocol semantics remain the destination decoder's responsibility.
func Validate(data []byte, maxBytes, maxDepth, maxNodes int) error {
	if len(data) > maxBytes || !utf8.Valid(data) {
		return fmt.Errorf("invalid JSON size or UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	nodes := 0
	var value func(int) error
	value = func(depth int) error {
		nodes++
		if depth > maxDepth || nodes > maxNodes {
			return fmt.Errorf("JSON complexity exceeded")
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || keys[name] {
					return fmt.Errorf("duplicate or invalid JSON key")
				}
				keys[name] = true
				if err := value(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := value(depth + 1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}
