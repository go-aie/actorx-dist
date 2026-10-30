package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/go-aie/actorx"
)

func TestAddrValueSnapshotsAndStrictDecoding(t *testing.T) {
	rt := testRuntime(t)
	ctx := testContext(t)

	var zero Addr
	if !zero.IsZero() || zero.Key() != (ActorKey{}) {
		t.Fatal(zero)
	}
	if _, err := json.Marshal(zero); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
	if err := zero.Send(ctx, read{}); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
	if _, err := zero.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
	if err := streamError(zero.Stream[int](ctx, series{})); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}

	for _, ids := range [][]string{{""}, {"1", "2"}, {"\n"}} {
		if addr, err := rt.Addr("counter", ids...); !addr.IsZero() || !errors.Is(err, actorx.ErrSetup) {
			t.Fatal(addr, err)
		}
	}

	a, _ := rt.Addr("counter")
	b, _ := rt.Addr("counter")
	if a.Key() == b.Key() || a.Key().ID == "" {
		t.Fatal("generated ID not independent")
	}

	// Value copies share selection; lookup and decode allocate independent state.
	addr := mustAddr(t, rt, "counter", "订单/*")
	copy := addr
	separate := mustAddr(t, rt, "counter", "订单/*")
	raw, err := json.Marshal(addr)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := rt.DecodeAddr(raw)
	if err != nil || decoded.Key() != addr.Key() || decoded.state == addr.state {
		t.Fatal(decoded, err)
	}
	gen := token(t)
	if err := addr.bind(gen); err != nil {
		t.Fatal(err)
	}
	if copy.generation() != gen || separate.generation() != "" || decoded.generation() != "" {
		t.Fatal("copy/independent state")
	}
	bound, err := json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := rt.DecodeAddr(bound)
	if err != nil || imported.generation() != gen || imported.state == addr.state {
		t.Fatal(imported, err)
	}
	if err := addr.bind(token(t)); !errors.Is(err, ErrActivationUnknown) || addr.generation() != gen {
		t.Fatal(err)
	}

	// Import requires an explicit Runtime and rejects ambiguous wire identities.
	var unmarshal Addr
	if err := json.Unmarshal(bound, &unmarshal); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
	for _, data := range []string{
		`null`,
		`[]`,
		`{}`,
		`{"version":null,"type":"counter","id":"1"}`,
		`{"Version":1,"type":"counter","id":"1"}`,
		`{"version":1,"type":"counter","id":"1","extra":1}`,
		`{"version":1,"type":"counter","id":"1","generation":""}`,
		`{"version":1,"type":"counter","id":"1","generation":null}`,
		`{"version":1,"type":"counter","id":"1","generation":"bad"}`,
		`{"version":1,"type":"counter","id":"1","id":"2"}`,
		`{"version":1,"type":"counter","id":"1"} {}`,
		`{"version":1,"type":"counter","id":null}`,
		`{"version":1.0,"type":"counter","id":"1"}`,
	} {
		if got, err := rt.DecodeAddr([]byte(data)); !got.IsZero() || !errors.Is(err, actorx.ErrSetup) {
			t.Errorf("%s: %v %v", data, got, err)
		}
	}
	if _, err := rt.DecodeAddr([]byte(`{"version":2,"type":"counter","id":"1"}`)); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}

	if err := addr.Send(ctx, read{}); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal("unexpectedly installed a public business transport", err)
	}
	if len(rt.ledger) != 0 {
		t.Fatal("Addr/Decode performed activation")
	}

	// Closure rejects new handles but preserves export of the existing identity.
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Addr("counter", "1"); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := rt.DecodeAddr(bound); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal(err)
	}
	if got, err := json.Marshal(addr); err != nil || string(got) != string(bound) {
		t.Fatal(string(got), err)
	}

	var zeroRT Runtime
	if _, err := zeroRT.Addr("counter"); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
	if err := zeroRT.Close(context.Background()); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
}

func TestConcurrentBindingAndEncoding(t *testing.T) {
	rt := testRuntime(t)
	addr := mustAddr(t, rt, "counter", "1")
	gen := token(t)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				if err := addr.bind(gen); err != nil {
					t.Error(err)
				}
			}
			for range 20 {
				raw, err := json.Marshal(addr)
				if err != nil {
					t.Error(err)
					return
				}
				got, err := rt.DecodeAddr(raw)
				if err != nil || (got.generation() != "" && got.generation() != gen) {
					t.Error(got, err)
				}
			}
		}()
	}
	wg.Wait()
}
