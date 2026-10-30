package actorxdist

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/recovery"
	"github.com/nats-io/nats.go"
)

type namespaceSeries struct{ Count int }
type namespaceExtra struct{}

type namespaceCounter struct{ counter }

func (c *namespaceCounter) Bindings() []actorx.Binding {
	var base *counter
	if c != nil {
		base = &c.counter
	}
	return []actorx.Binding{actorx.Handle(base.add), actorx.Handle(base.read), actorx.HandleStream(c.stream)}
}

func (c *namespaceCounter) stream(ctx actorx.Context, msg namespaceSeries, reply actorx.StreamReply[int]) error {
	for i := range msg.Count {
		if err := reply.Write(ctx, c.n+i, 8); err != nil {
			return err
		}
	}
	reply.Complete(nil)
	return nil
}

// A different contract for the same Type proves that Registry isolation covers
// immutable contracts as well as activation ownership and business routing.
type namespaceOtherCounter struct{ namespaceCounter }

func (c *namespaceOtherCounter) Bindings() []actorx.Binding {
	var base *namespaceCounter
	if c != nil {
		base = &c.namespaceCounter
	}
	return append(base.Bindings(), actorx.Handle(c.extra))
}

func (*namespaceOtherCounter) extra(actorx.Context, namespaceExtra) (bool, error) { return true, nil }

// Optional real-transport gate. The isolated server must allow at least nine
// streams and reserve enough file storage for three sets of Registry buckets.
func TestNamespaceNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server with room for nine streams")
	}
	base := fmt.Sprintf("ns_%x", time.Now().UnixNano())
	newRuntime := func(namespace string) *Runtime {
		return testRuntimeWithOptions(t, []Option{WithNATS(url), WithNamespace(namespace)})
	}
	a, b := newRuntime(base+"_a"), newRuntime(base+"_b")
	clientA, clientB := newRuntime(base+"_a"), newRuntime(base+"_b")
	ctx := testContext(t)
	const typ = "namespace-counter"
	if _, err := a.Register(ctx, ActorSpec[*namespaceCounter]{Type: typ, New: func(ActorKey) *namespaceCounter { return &namespaceCounter{} }}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Register(ctx, ActorSpec[*namespaceOtherCounter]{Type: typ, New: func(ActorKey) *namespaceOtherCounter { return &namespaceOtherCounter{} }}); err != nil {
		t.Fatal("same Type in a different namespace must allow a different contract", err)
	}
	addrA, addrB := mustAddr(t, clientA, typ, "same-id"), mustAddr(t, clientB, typ, "same-id")
	for _, tc := range []struct {
		addr  Addr
		value int
	}{{addrA, 5}, {addrB, 10}} {
		if err := tc.addr.Send(ctx, add{tc.value}); err != nil {
			t.Fatal(err)
		}
		if got, err := tc.addr.Call[int](ctx, read{}); err != nil || got != tc.value {
			t.Fatal("namespace shared business state", got, err)
		}
		count := 0
		for got, err := range tc.addr.Stream[int](ctx, namespaceSeries{Count: 9}) {
			if err != nil {
				t.Fatal(err)
			}
			want := tc.value + count
			count++
			if got != want {
				t.Fatalf("chunk %d, want %d", got, want)
			}
		}
		if count != 9 {
			t.Fatal(count)
		}
		for _, err := range tc.addr.Stream[int](ctx, namespaceSeries{Count: 100}) {
			if err != nil {
				t.Fatal(err)
			}
			break
		}

	}
	if err := addrA.Stop(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := addrA.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.net.registry.exit(ctx, addrA.key, addrA.generation()); err != nil {
		t.Fatal("exit was not written to the owning namespace", err)
	}
	if _, err := b.net.registry.get(ctx, b.names.ExitsBucket, "e."+addrA.generation(), true); !errors.Is(err, nats.ErrKeyNotFound) {
		t.Fatal("exit leaked into the other namespace", err)
	}
	if got, err := addrB.Call[int](ctx, read{}); err != nil || got != 10 {
		t.Fatal("stopping one namespace affected the other", got, err)
	}
	if _, err := b.net.factoryRequest(ctx, "confirm", addrA.key, addrA.generation(), a.registrationToken(typ), "", nil); !errors.Is(err, ErrOwnerUnavailable) {
		t.Fatal("control route crossed namespaces", err)
	}

	t.Run("recovery and maintenance", func(t *testing.T) {
		// These records have no process or business subscriptions. Both stores
		// intentionally contain the same key and tokens, as a namespace fence test.
		orphan := testOwnership(t)
		orphan.Key = ActorKey{typ, "orphan"}
		for _, rt := range []*Runtime{a, b} {
			if _, err := rt.net.registry.claim(ctx, orphan, nil); err != nil {
				t.Fatal(err)
			}
		}
		key := recovery.Key{Type: typ, ID: "orphan"}
		snapshot, err := recovery.Inspect(ctx, a.net.nc, a.names.Namespace, key)
		if err != nil || snapshot.Namespace != a.names.Namespace || snapshot.Generation != orphan.Generation {
			t.Fatal(snapshot, err)
		}
		binary := filepath.Join(t.TempDir(), "dist-recover")
		if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/dist-recover").CombinedOutput(); err != nil {
			t.Fatalf("build recovery command: %v\n%s", err, output)
		}
		output, err := exec.CommandContext(ctx, binary, "-nats", url, "-namespace", a.names.Namespace, "-type", typ, "-id", "orphan").CombinedOutput()
		var observed recovery.Snapshot
		if err != nil || json.Unmarshal(output, &observed) != nil || observed.Namespace != snapshot.Namespace || observed.Generation != snapshot.Generation {
			t.Fatalf("recovery command selected wrong namespace: %v\n%s", err, output)
		}
		var audit bytes.Buffer
		if err := recovery.Apply(ctx, a.net.nc, b.names.Namespace, snapshot, true, "test-only orphan", &audit); err == nil || audit.Len() != 0 {
			t.Fatal("cross-namespace recovery was not rejected", err)
		}
		if err := recovery.Apply(ctx, a.net.nc, a.names.Namespace, snapshot, true, "test-only orphan without process or subscriptions", &audit); err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(audit.Bytes(), []byte(`"namespace":"`+a.names.Namespace+`"`)) {
			t.Fatal("recovery audit omitted namespace", audit.String())
		}
		if _, err := a.net.registry.owner(ctx, orphan.Key); !errors.Is(err, nats.ErrKeyNotFound) {
			t.Fatal("recovery did not release the selected namespace", err)
		}
		raw, err := a.net.registry.rawLast(ctx, a.names.CurrentBucket, currentKey(orphan.Key))
		if err != nil || raw.Header.Get("KV-Operation") != "DEL" {
			t.Fatal("missing recovery deletion marker", raw, err)
		}
		if err := a.net.registry.purgeMarker(ctx, a.names.CurrentBucket, currentKey(orphan.Key), raw.Sequence); err != nil {
			t.Fatal(err)
		}
		if _, err := a.net.registry.rawLast(ctx, a.names.CurrentBucket, currentKey(orphan.Key)); !errors.Is(err, nats.ErrMsgNotFound) {
			t.Fatal("maintenance did not purge the selected marker", err)
		}
		if got, err := b.net.registry.owner(ctx, orphan.Key); err != nil || got.value.Generation != orphan.Generation {
			t.Fatal("recovery or maintenance modified the other namespace", got, err)
		}
	})
}
