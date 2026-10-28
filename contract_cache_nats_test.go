package actorxdist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
)

// Optional real-transport gate; use an isolated examples/nats.conf server.
func TestContractCacheNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	newRuntime := func(t *testing.T, options ...any) *Runtime {
		t.Helper()
		return testRuntimeWithOptions(t, []Option{WithNATS(url)}, options...)
	}
	t.Run("traversal sharing eviction and reread", func(t *testing.T) {
		prefix := fmt.Sprintf("cache-%x", time.Now().UnixNano())
		const count = 70
		worker := newRuntime(t, WithExecutionLimits(ExecutionLimits{Instances: 96}))
		for i := range count {
			if _, err := counterRegistration(fmt.Sprintf("%s-%d", prefix, i))(testContext(t), worker); err != nil {
				t.Fatal(err)
			}
		}
		different := prefix + "-different"
		if _, err := registerSpec(ActorSpec[*mismatch]{Type: different, New: func(ActorKey) *mismatch { return &mismatch{} }})(testContext(t), worker); err != nil {
			t.Fatal(err)
		}
		shared := worker.specs[prefix+"-0"].contract
		other := worker.specs[different].contract
		// Activation independently confirms the Type declaration. Warm these
		// generations first so counters below measure caller-cache reads alone.
		for i := range count {
			if _, err := mustAddr(t, worker, fmt.Sprintf("%s-%d", prefix, i), "one").Call[int](testContext(t), read{}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := mustAddr(t, worker, prefix+"-0", "second").Call[int](testContext(t), read{}); err != nil {
			t.Fatal(err)
		}
		for _, capacity := range []int{0, 1, 2, 128} {
			t.Run(fmt.Sprint(capacity), func(t *testing.T) {
				client := newRuntime(t, WithTransportLimits(TransportLimits{ContractCacheTypes: capacity}))
				reads := make(map[string]*atomic.Int32)
				for i := range count {
					reads["t."+identity.Token(fmt.Sprintf("%s-%d", prefix, i))] = new(atomic.Int32)
				}
				client.net.registry.readFault = func(bucket, key string) error {
					if bucket == typesBucket && reads[key] != nil {
						reads[key].Add(1)
					}
					return nil
				}
				var first Addr
				for i := range count {
					addr := mustAddr(t, client, fmt.Sprintf("%s-%d", prefix, i), "one")
					if got, err := addr.Call[int](testContext(t), read{}); err != nil || got != 0 {
						t.Fatal("Type traversal failed", i, got, err)
					}
					if i == 0 {
						first = addr
					}
				}
				limit := client.limits.transport.ContractCacheTypes
				checkContractCache(t, client.net.contracts, min(limit, count), 1, len(shared.data))
				for key, reads := range reads {
					if reads.Load() != 1 {
						t.Fatal("new Type did not confirm its own Registry declaration", key, reads.Load())
					}
				}
				generation := first.generation()
				if got, err := first.Call[int](testContext(t), read{}); err != nil || got != 0 || first.generation() != generation {
					t.Fatal("eviction affected the fixed generation", got, err)
				}
				wantReads := int32(1)
				if limit < count {
					wantReads = 2
				}
				firstReads := reads["t."+identity.Token(first.Key().Type)]
				if firstReads.Load() != wantReads {
					t.Fatal("evicted Type was not reread", firstReads.Load(), wantReads)
				}
				// Instance identity does not consume another Type slot or Registry read.
				if got, err := mustAddr(t, client, first.Key().Type, "second").Call[int](testContext(t), read{}); err != nil || got != 0 || firstReads.Load() != wantReads {
					t.Fatal("same Type did not reuse its cache entry", got, err)
				}
				// A hit on the first Type protects it while the next insert evicts the
				// oldest Type, even though every counter Type shares canonical bytes.
				if got, err := mustAddr(t, client, different, "one").Call[int](testContext(t), read{}); err != nil || got != 0 {
					t.Fatal(got, err)
				}
				if limit == 1 {
					checkContractCache(t, client.net.contracts, 1, 1, len(other.data))
				} else {
					checkContractCache(t, client.net.contracts, min(limit, count+1), 2, len(shared.data)+len(other.data))
					if _, ok := client.net.contracts.get(first.Key().Type); !ok {
						t.Fatal("hit did not refresh LRU")
					}
					if limit < count {
						client.net.contracts.mu.Lock()
						_, retained := client.net.contracts.types[fmt.Sprintf("%s-%d", prefix, count-limit+1)]
						client.net.contracts.mu.Unlock()
						if retained {
							t.Fatal("oldest Type was not evicted")
						}
					}
				}
				if err := client.Close(testContext(t)); err != nil {
					t.Fatal(err)
				}
				checkContractCache(t, client.net.contracts, 0, 0, 0)
			})
		}
	})
	t.Run("read failure retries after registration", func(t *testing.T) {
		worker, client := newRuntime(t), newRuntime(t)
		typ := fmt.Sprintf("cache-retry-%x", time.Now().UnixNano())
		addr := mustAddr(t, client, typ, "one")
		for range 2 {
			if _, err := addr.Call[int](testContext(t), read{}); !errors.Is(err, ErrTypeUnavailable) {
				t.Fatal(err)
			}
			checkContractCache(t, client.net.contracts, 0, 0, 0)
		}
		if _, err := counterRegistration(typ)(testContext(t), worker); err != nil {
			t.Fatal(err)
		}
		if got, err := addr.Call[int](testContext(t), add{7}); err != nil || got != 7 {
			t.Fatal("failed read poisoned later invocation", got, err)
		}
	})
	t.Run("inflight call survives eviction", func(t *testing.T) {
		worker := newRuntime(t)
		client := newRuntime(t, WithTransportLimits(TransportLimits{ContractCacheTypes: 1}))
		typ := fmt.Sprintf("cache-held-%x", time.Now().UnixNano())
		entered, release := make(chan struct{}), make(chan struct{}, 1)
		defer close(release)
		if _, err := registerSpec(ActorSpec[*limitProbe]{Type: typ, New: func(ActorKey) *limitProbe {
			return &limitProbe{running: entered, unblock: release}
		}})(testContext(t), worker); err != nil {
			t.Fatal(err)
		}
		if _, err := counterRegistration(typ+"-other")(testContext(t), worker); err != nil {
			t.Fatal(err)
		}
		addr := mustAddr(t, client, typ, "one")
		done := make(chan error, 1)
		go func() {
			got, err := addr.Call[int](testContext(t), limitHold{})
			if err == nil && got != 1 {
				err = fmt.Errorf("held call returned %d", got)
			}
			done <- err
		}()
		await(t, entered)
		generation := addr.generation()
		if _, err := mustAddr(t, client, typ+"-other", "one").Call[int](testContext(t), read{}); err != nil {
			t.Fatal(err)
		}
		if _, ok := client.net.contracts.get(typ); ok {
			t.Fatal("in-flight call pinned the LRU entry")
		}
		release <- struct{}{}
		if err := <-done; err != nil {
			t.Fatal("eviction broke the in-flight call", err)
		}
		if got, err := addr.Call[int](testContext(t), read{}); err != nil || got != 1 || addr.generation() != generation {
			t.Fatal("reread changed the address generation", got, err)
		}
	})
	t.Run("Close joins successful late Registry read", func(t *testing.T) {
		worker, client := newRuntime(t), newRuntime(t)
		typ := fmt.Sprintf("cache-close-%x", time.Now().UnixNano())
		if _, err := counterRegistration(typ)(testContext(t), worker); err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		var reads atomic.Int32
		client.net.registry.readFault = func(bucket, key string) error {
			if bucket == typesBucket && key == "t."+identity.Token(typ) {
				reads.Add(1)
				close(entered)
				<-release
			}
			return nil
		}
		addr := mustAddr(t, client, typ, "one")
		done := make(chan error, 1)
		go func() { _, err := addr.Call[int](testContext(t), read{}); done <- err }()
		await(t, entered)
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := client.Close(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal("Close lost the pending Registry read", err)
		}
		select {
		case <-client.done:
			t.Fatal("Close completed before the Registry read")
		default:
		}
		unblock()
		if err := <-done; !errors.Is(err, actorx.ErrClosed) {
			t.Fatal(err)
		}
		if err := client.Close(testContext(t)); err != nil {
			t.Fatal(err)
		}
		checkContractCache(t, client.net.contracts, 0, 0, 0)
		if _, err := addr.Call[int](testContext(t), read{}); !errors.Is(err, actorx.ErrClosed) || reads.Load() != 1 {
			t.Fatal("closed Runtime rebuilt its cache", err, reads.Load())
		}
	})
}
