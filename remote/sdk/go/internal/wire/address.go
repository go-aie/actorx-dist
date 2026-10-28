package wire

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"unicode"
	"unicode/utf8"
)

var actorType = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)

func Identity(t, id string) error {
	if len(t) > 64 || !actorType.MatchString(t) || len(id) == 0 || len(id) > 128 || !utf8.ValidString(id) {
		return fmt.Errorf("invalid actor identity")
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return fmt.Errorf("invalid actor identity")
		}
	}
	return nil
}

type Address struct {
	Version    int    `json:"version"`
	Type       string `json:"type"`
	ID         string `json:"id"`
	Generation string `json:"generation,omitempty"`
}

// ParseAddress requires a selected generation when fixed is true.
func ParseAddress(raw []byte, fixed bool) (Address, error) {
	bad := fmt.Errorf("invalid actor address")
	f, err := Fields(raw, []string{"version", "type", "id", "generation"}, "version", "type", "id")
	if err != nil {
		return Address{}, bad
	}
	for _, v := range f {
		if string(v) == "null" {
			return Address{}, bad
		}
	}
	var a Address
	if json.Unmarshal(raw, &a) != nil || a.Version != 1 || Identity(a.Type, a.ID) != nil {
		return Address{}, bad
	}
	if a.Generation == "" {
		if fixed {
			return Address{}, bad
		}
		return a, nil
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(a.Generation)
	if err != nil || len(b) != 16 || base64.RawURLEncoding.EncodeToString(b) != a.Generation {
		return Address{}, bad
	}
	return a, nil
}
