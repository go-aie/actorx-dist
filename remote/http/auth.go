// Package remotehttp adapts a remote Gateway to authenticated HTTP and SSE.
// The caller owns Gateway and Server shutdown; do not apply a short server
// WriteTimeout to registration streams. Each frame has its own write deadline.
package remotehttp

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// Options configures the shared static Bearer credential. Generate APIKey from
// at least 32 random bytes. An empty or malformed key fails construction.
type Options struct {
	APIKey string
}

func bearerToken(s string) bool {
	if s == "" {
		return false
	}
	padding := false
	for _, c := range s {
		if c == '=' {
			padding = true
			continue
		}
		if padding || !(c >= 'a' && c <= 'z' ||
			c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' ||
			strings.ContainsRune("-._~+/", c)) {
			return false
		}
	}
	return true
}

func (h *handler) authenticate(w http.ResponseWriter, r *http.Request) bool {
	values := r.Header.Values("Authorization")
	challenge := `Bearer realm="actorx-remote"`
	if len(values) != 0 {
		challenge += `, error="invalid_token"`
	}

	// Reject ambiguous headers before comparing a single credential's digest.
	valid := false
	if len(values) == 1 {
		scheme, token, ok := strings.Cut(values[0], " ")
		// RFC 6750 permits one or more SP between scheme and credential.
		token = strings.TrimLeft(token, " ")
		if ok && strings.EqualFold(scheme, "Bearer") && bearerToken(token) {
			digest := sha256.Sum256([]byte(token))
			valid = subtle.ConstantTimeCompare(digest[:], h.key[:]) == 1
		}
	}

	if !valid {
		w.Header().Set("WWW-Authenticate", challenge)
		http.Error(w, "Authentication required", http.StatusUnauthorized)
	}
	return valid
}
