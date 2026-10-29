package actorxdist

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/naming"
	"github.com/nats-io/nats.go"
)

var testDefaultNames, _ = naming.New(naming.DefaultNamespace)

func TestNamespaceConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []any
	}{
		{"default", nil},
		{"orders_prod", []any{WithNamespace("orders_prod")}},
		{"orders-prod", []any{WithNamespace("ignored"), WithNamespace("orders-prod")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := testRuntime(t, tc.options...)
			n := &network{rt: rt}
			prefix := "actorxdist." + tc.name
			for got, want := range map[string]string{
				rt.names.Namespace:                               tc.name,
				n.factorySubject("counter"):                      prefix + ".factory.Y291bnRlcg",
				n.controlSubject("counter", "token"):             prefix + ".factory.Y291bnRlcg.token.control.*",
				n.actorSubject(ActorKey{"counter", "orders-42"}): prefix + ".actor.Y291bnRlcg.b3JkZXJzLTQy",
				rt.names.TypesBucket:                             "actorxdist_" + tc.name + "_types_v1",
				rt.names.CurrentBucket:                           "actorxdist_" + tc.name + "_activations_v1",
				rt.names.ExitsBucket:                             "actorxdist_" + tc.name + "_exits_v1",
			} {
				if got != want {
					t.Fatalf("got %q, want %q", got, want)
				}
			}
		})
	}
	// Creating a later Runtime must not change an earlier Runtime's routes.
	a, b := testRuntime(t, WithNamespace("a")), testRuntime(t, WithNamespace("b"))
	if a.names.Namespace != "a" || b.names.Namespace != "b" {
		t.Fatal("namespace configuration escaped its Runtime")
	}
	_ = testRuntime(t, WithNamespace("a"+strings.Repeat("b", 63)))
}

func TestInvalidNamespaceBeforeConnection(t *testing.T) {
	for _, namespace := range []string{"", "Upper", "has.dot", "has space", "a.*", "a.>", "a/b", "a\n", "中文", "_prefix", "1prefix", strings.Repeat("a", 65)} {
		t.Run(namespace, func(t *testing.T) {
			configured := false
			rt, err := NewRuntime(context.Background(),
				WithNATS("nats://127.0.0.1:1", func(*nats.Options) error { configured = true; return nil }),
				WithNamespace(namespace),
			)
			if rt != nil || !errors.Is(err, actorx.ErrSetup) || configured {
				t.Fatal("invalid namespace reached connection setup", rt, err, configured)
			}
		})
	}
}
