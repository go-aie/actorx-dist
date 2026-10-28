package identity

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-aie/actorx"
)

func TestRouting(t *testing.T) {
	seen := map[string]bool{}
	for _, id := range []string{"a.b", "a/b", "*", ">", "订单-42", "é", "e\u0301", strings.Repeat("x", MaxIDBytes)} {
		if err := Validate("counter.v1", id); err != nil {
			t.Fatal(err)
		}
		token := Token(id)
		if seen[token] || strings.ContainsAny(token, ".*> =") {
			t.Fatalf("unsafe/colliding token %q", token)
		}
		seen[token] = true
		got, err := Untoken(token)
		if err != nil || got != id {
			t.Fatalf("roundtrip %q: %q %v", id, got, err)
		}
	}
	if Token("counter") != "Y291bnRlcg" || Token("orders-42") != "b3JkZXJzLTQy" {
		t.Fatal("routing fixture changed")
	}
	for _, pair := range [][2]string{
		{"", "a"},
		{"Counter", "a"},
		{"a..b", "a"},
		{"a*", "a"},
		{"a", ""},
		{"a", "a\n"},
		{"a", "\xff"},
		{"a", strings.Repeat("a", 129)},
		{strings.Repeat("a", 65), "a"},
	} {
		if !errors.Is(Validate(pair[0], pair[1]), actorx.ErrSetup) {
			t.Fatalf("accepted %q", pair)
		}
	}
	for _, token := range []string{"", "YQ==", "YR", "YQ\n", "*", "_w"} {
		if _, err := Untoken(token); err == nil {
			t.Fatalf("accepted noncanonical token %q", token)
		}
	}
	for range 100 {
		token, err := NewOpaque()
		if err != nil || len(token) != 22 || seen[token] {
			t.Fatalf("opaque: %q %v", token, err)
		}
		seen[token] = true
	}
}
