package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/nats-io/nats.go"
)

// Number deliberately matches encoding/json.Number in name and underlying type.
type Number string

func TestCallerIdentityIncludesPackageAndResultName(t *testing.T) {
	c, err := buildContract([]actorx.CallDescriptor{actorx.Request[json.Number, json.Number]()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkInvocation(c, Number("1"), actorx.CallKind, reflect.TypeFor[json.Number]()); !errors.Is(err, actorx.ErrUnhandled) {
		t.Fatal(err)
	}
	if _, err := checkInvocation(c, json.Number("1"), actorx.CallKind, reflect.TypeFor[Number]()); !errors.Is(err, actorx.ErrReplyType) {
		t.Fatal(err)
	}
	if _, err := checkInvocation(c, json.Number("1"), actorx.CallKind, reflect.TypeFor[json.Number]()); err != nil {
		t.Fatal(err)
	}
}

// Use the production invocation path, with Registry failures injected at the
// existing transport boundary. Successful Registry reads are covered by NATS.
func testContractNetwork(t *testing.T, options ...any) *network {
	t.Helper()
	rt := testRuntime(t, options...)
	n := &network{
		rt:           rt,
		registry:     newRegistry(nil, rt.names),
		contracts:    newContractCache(rt.limits.transport.ContractCacheTypes),
		stop:         make(chan struct{}),
		codecSlots:   make(chan struct{}, 8),
		payloadSlots: make(chan struct{}, rt.limits.payloads()),
	}
	rt.net, rt.invoke = n, n.invokeBusiness
	return n
}

func TestTypeContractHitsAndReadFailures(t *testing.T) {
	n := testContractNetwork(t, WithTransportLimits(TransportLimits{ContractCacheTypes: 1}))
	contract := cacheTestContract([]byte("cached"))
	cachePut(t, n.contracts, "cached", contract)
	reads := 0
	var readErr error = nats.ErrKeyNotFound
	n.registry.readFault = func(bucket, key string) error {
		reads++
		if bucket != testDefaultNames.TypesBucket || key != "t."+identity.Token("missing") {
			t.Error("unexpected Registry read", bucket, key)
		}
		return readErr
	}
	if got, err := n.typeContract(testContext(t), "cached"); err != nil || string(got.data) != "cached" || reads != 0 {
		t.Fatal("hit performed Registry I/O", got, err, reads)
	}
	for _, want := range []error{ErrTypeUnavailable, context.DeadlineExceeded} {
		if want != ErrTypeUnavailable {
			readErr = want
		}
		for range 2 {
			if _, err := n.typeContract(testContext(t), "missing"); !errors.Is(err, want) {
				t.Fatal("failed read was hidden by full cache", err)
			}
			checkContractCache(t, n.contracts, 1, 1, len(contract.data))
		}
	}
	if reads != 4 {
		t.Fatal("failed reads established a negative cache", reads)
	}
}

func TestContractMissRetainsInvocationAdmission(t *testing.T) {
	for _, budget := range []string{"requests", "codec", "payload"} {
		t.Run(budget, func(t *testing.T) {
			n := testContractNetwork(t, WithTransportLimits(TransportLimits{Requests: 1}))
			n.registry.readFault = func(string, string) error {
				t.Error("miss bypassed invocation admission")
				return ErrTypeUnavailable
			}
			switch budget {
			case "requests":
				n.outgoing = 1
			case "codec":
				for range cap(n.codecSlots) {
					n.codecSlots <- struct{}{}
				}
			case "payload":
				for range cap(n.payloadSlots) {
					n.payloadSlots <- struct{}{}
				}
			}
			if _, err := mustAddr(t, n.rt, "missing", "one").Call[int](testContext(t), read{}); !errors.Is(err, actorx.ErrFull) {
				t.Fatal("miss bypassed capacity", err)
			}
			checkContractCache(t, n.contracts, 0, 0, 0)
			if budget != "requests" && n.outgoing != 0 || budget != "payload" && len(n.payloadSlots) != 0 {
				t.Fatal("failed admission leaked slots")
			}
		})
	}
}

func TestContractMissRetainsRegistryAdmission(t *testing.T) {
	for _, budget := range []string{"ordinary", "all"} {
		t.Run(budget, func(t *testing.T) {
			n := testContractNetwork(t)
			slots := n.registry.ordinary
			if budget == "all" {
				slots = n.registry.all
			}
			for range cap(slots) {
				slots <- struct{}{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := n.typeContract(ctx, "missing"); !errors.Is(err, context.Canceled) {
				t.Fatal("Registry queue ignored caller cancellation", err)
			}
			if len(slots) != cap(slots) || budget == "all" && len(n.registry.ordinary) != 0 {
				t.Fatal("Registry admission leaked slots")
			}
			checkContractCache(t, n.contracts, 0, 0, 0)
		})
	}
}

func TestRuntimeCloseJoinsContractMiss(t *testing.T) {
	n := testContractNetwork(t)
	contract := cacheTestContract([]byte("retained"))
	snapshot := cachePut(t, n.contracts, "warm", contract)
	entered, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	n.registry.readFault = func(string, string) error {
		close(entered)
		<-release
		return context.Canceled
	}
	addr := mustAddr(t, n.rt, "missing", "one")
	result := make(chan error, 1)
	go func() { _, err := addr.Call[int](testContext(t), read{}); result <- err }()
	await(t, entered)
	// A blocked Registry miss must not hold the cache lock.
	if _, ok := n.contracts.get("warm"); !ok {
		t.Fatal("lost warm contract")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := n.rt.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("Close did not wait for admitted miss", err)
	}
	select {
	case <-n.rt.done:
		t.Fatal("Close passed an unfinished Registry read")
	default:
	}
	checkContractCache(t, n.contracts, 1, 1, len(contract.data))
	release <- struct{}{}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := n.rt.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	checkContractCache(t, n.contracts, 0, 0, 0)
	if _, err := n.contracts.put("late", contract); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("closed cache was rebuilt", err)
	}
	if _, err := addr.Call[int](testContext(t), read{}); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("closed Runtime admitted another miss", err)
	}
	if string(snapshot.data) != "retained" {
		t.Fatal("Close invalidated a snapshot")
	}
}

func TestInvocationContractRejections(t *testing.T) {
	c, err := buildContract([]actorx.CallDescriptor{
		actorx.Request[add, int](), actorx.Streaming[series, string](), actorx.Message[read](),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		message any
		kind    actorx.Kind
		result  reflect.Type
		want    error
	}{
		{"call", add{}, actorx.CallKind, reflect.TypeFor[int](), nil},
		{"send", add{}, actorx.SendKind, nil, nil},
		{"wrong result", add{}, actorx.CallKind, reflect.TypeFor[string](), actorx.ErrReplyType},
		{"wrong mode", series{}, actorx.SendKind, nil, actorx.ErrMode},
		{"unknown message", "wrong", actorx.CallKind, reflect.TypeFor[int](), actorx.ErrUnhandled},
		{"nil message", nil, actorx.CallKind, reflect.TypeFor[int](), actorx.ErrUnhandled},
		{"message-only result", read{}, actorx.CallKind, reflect.TypeFor[int](), actorx.ErrReplyType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := checkInvocation(c, tc.message, tc.kind, tc.result); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}
