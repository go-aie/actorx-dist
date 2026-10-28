// Package identity supplies the canonical, reversible routing encoding.
package identity

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"regexp"
	"unicode"
	"unicode/utf8"

	"github.com/go-aie/actorx"
)

const MaxTypeBytes = 64
const MaxIDBytes = 128

var typeName = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)

func Validate(actorType, id string) error {
	if len(actorType) > MaxTypeBytes || !typeName.MatchString(actorType) ||
		len(id) == 0 || len(id) > MaxIDBytes || !utf8.ValidString(id) {
		return actorx.ErrSetup
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return actorx.ErrSetup
		}
	}
	return nil
}

// Token never emits NATS wildcards, separators or base64 padding.
func Token(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }

func Untoken(token string) (string, error) {
	value, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || token == "" || Token(string(value)) != token || !utf8.Valid(value) {
		return "", actorx.ErrSetup
	}
	return string(value), nil
}

// NewOpaque produces 128 random bits, encoded as 22 canonical characters.
func NewOpaque() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("%w: random identity: %v", actorx.ErrSetup, err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
