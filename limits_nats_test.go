package actorxdist

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

// Run against an isolated server configured like examples/nats.conf:
// ACTORX_NATS_URL=nats://127.0.0.1:4222 go test -race -run TestConfiguredLimitsNATS
func TestConfiguredLimitsNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	newRuntime := func(t *testing.T, fixtures ...any) *Runtime {
		t.Helper()
		return testRuntimeWithOptions(t, []Option{WithNATS(url)}, fixtures...)
	}

	t.Run("types above the previous fixed limit", func(t *testing.T) {
		prefix := fmt.Sprintf("types-%x", time.Now().UnixNano())
		var options []any
		for i := range 12 {
			options = append(options, counterRegistration(fmt.Sprintf("%s-%d", prefix, i)))
		}
		newRuntime(t, options...)
		client := newRuntime(t)
		for i := range 12 {
			if _, err := mustAddr(t, client, fmt.Sprintf("%s-%d", prefix, i), "one").Call[int](testContext(t), read{}); err != nil {
				t.Fatal("registered Type unavailable", i, err)
			}
		}
	})

	t.Run("configured activation creation budgets", func(t *testing.T) {
		for _, limits := range []ExecutionLimits{
			{Creating: 2, CreatingPerType: 1},
			{Creating: 20, CreatingPerType: 12},
		} {
			t.Run(fmt.Sprintf("%d-%d", limits.Creating, limits.CreatingPerType), func(t *testing.T) {
				entered := make(chan struct{}, limits.Creating)
				gates := make(map[string]chan struct{}, limits.Creating)
				for i := range limits.Creating {
					gates[fmt.Sprint(i)] = make(chan struct{})
				}
				var once sync.Once
				release := func() {
					once.Do(func() {
						for _, gate := range gates {
							close(gate)
						}
					})
				}
				defer release()
				prefix := fmt.Sprintf("creating-%x", time.Now().UnixNano())
				options := []any{WithExecutionLimits(limits)}
				for i := range 3 {
					options = append(options, registerSpec(ActorSpec[*counter]{Type: fmt.Sprintf("%s-%d", prefix, i), New: func(key ActorKey) *counter {
						if gate := gates[key.ID]; gate != nil {
							entered <- struct{}{}
							<-gate
						}
						return &counter{}
					}}))
				}
				worker := newRuntime(t, options...)
				var entries []*creation
				activate := func(typ, id string) (*creation, error) {
					return worker.factories[typ].Call[*creation](testContext(t), activateRequest{ActorKey{typ, id}})
				}
				for i := range limits.Creating {
					typ := fmt.Sprintf("%s-%d", prefix, i/limits.CreatingPerType)
					entry, err := activate(typ, fmt.Sprint(i))
					if err != nil {
						t.Fatal("activation refused below configured capacity", i, err)
					}
					entries = append(entries, entry)
					await(t, entered)
					if (i+1)%limits.CreatingPerType == 0 {
						if _, err := activate(typ, "overflow"); !errors.Is(err, actorx.ErrFull) {
							t.Fatal("per-Type activation bound", err)
						}
					}
				}
				typ := fmt.Sprintf("%s-2", prefix)
				if _, err := activate(typ, "overflow"); !errors.Is(err, actorx.ErrFull) {
					t.Fatal("global activation bound", err)
				}
				// Settle one activation at a time so this test isolates creation
				// capacity from the independent Registry I/O concurrency budget.
				for _, entry := range entries {
					gates[entry.key.ID] <- struct{}{}
					await(t, entry.done)
					if entry.err != nil {
						t.Fatal(entry.err)
					}
				}
				client := newRuntime(t)
				if _, err := mustAddr(t, client, typ, "after-startup").Call[int](testContext(t), read{}); err != nil {
					t.Fatal("startup completion did not restore creation capacity", err)
				}
			})
		}
	})

	t.Run("request admission and recovery", func(t *testing.T) {
		for _, capacity := range []int{1, 129} {
			t.Run(fmt.Sprint(capacity), func(t *testing.T) {
				typ := fmt.Sprintf("limits-%x", time.Now().UnixNano())
				replies := make(chan actorx.Reply[int], capacity)
				newRuntime(t, WithExecutionLimits(ExecutionLimits{Pending: capacity}),
					WithTransportLimits(TransportLimits{Requests: capacity}), registerSpec(ActorSpec[*limitProbe]{
						Type: typ, New: func(ActorKey) *limitProbe { return &limitProbe{replies: replies} },
					}))
				client := newRuntime(t, WithTransportLimits(TransportLimits{Requests: capacity}))
				other := newRuntime(t)
				addr, otherAddr := mustAddr(t, client, typ, "one"), mustAddr(t, other, typ, "one")
				ctx := testContext(t)
				done := make(chan error, capacity)
				held := make([]actorx.Reply[int], 0, capacity)
				for range capacity {
					go func() { _, err := addr.Call[int](ctx, limitDeferred{}); done <- err }()
					select {
					case reply := <-replies:
						held = append(held, reply)
					case err := <-done:
						t.Fatal("request refused below configured capacity", len(held), err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if _, err := addr.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrFull) {
					t.Fatal("outgoing Requests exceeded", err)
				}
				if _, err := otherAddr.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrFull) || errors.Is(err, ErrOutcomeUnknown) {
					t.Fatal("incoming Requests did not explicitly reject before admission", err)
				}
				for _, reply := range held {
					reply.Complete(1, nil)
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				}
				if value, err := addr.Call[int](ctx, read{}); value != 1 || err != nil {
					t.Fatal("settlement did not restore request capacity", value, err)
				}
			})
		}
	})

	t.Run("stream admission and callback lifetime", func(t *testing.T) {
		typ := fmt.Sprintf("limits-%x", time.Now().UnixNano())
		producer, consumer := make(chan struct{}), make(chan struct{})
		var pOnce, cOnce sync.Once
		releaseProducer := func() { pOnce.Do(func() { close(producer) }) }
		releaseConsumer := func() { cOnce.Do(func() { close(consumer) }) }
		defer releaseProducer()
		defer releaseConsumer()
		newRuntime(t, WithTransportLimits(TransportLimits{Streams: 1}), registerSpec(ActorSpec[*limitProbe]{
			Type: typ, New: func(ActorKey) *limitProbe { return &limitProbe{unblock: producer} },
		}))
		client := newRuntime(t, WithTransportLimits(TransportLimits{Streams: 1}))
		other := newRuntime(t)
		addr, otherAddr := mustAddr(t, client, typ, "one"), mustAddr(t, other, typ, "one")
		ctx := testContext(t)
		entered, done := make(chan struct{}), make(chan error, 1)
		go func() {
			var result error
			for _, err := range addr.Stream[int](ctx, limitStream{}) {
				if err != nil {
					result = err
					break
				}
				close(entered)
				<-consumer
			}
			done <- result
		}()
		await(t, entered)
		if err := streamError(addr.Stream[int](ctx, limitStream{})); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("outgoing Streams exceeded", err)
		}
		if err := streamError(otherAddr.Stream[int](ctx, limitStream{})); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("incoming Streams exceeded", err)
		}
		releaseProducer()
		// Even after producer termination, the active consumer retains capacity.
		if err := streamError(addr.Stream[int](ctx, limitStream{})); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("active callback released stream capacity", err)
		}
		releaseConsumer()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := streamError(addr.Stream[int](ctx, limitStream{})); err != nil {
			t.Fatal("settlement did not restore stream capacity", err)
		}
	})

	t.Run("instances above the previous fixed limit", func(t *testing.T) {
		typ := fmt.Sprintf("limits-%x", time.Now().UnixNano())
		newRuntime(t, WithExecutionLimits(ExecutionLimits{Instances: 65}), counterRegistration(typ))
		client := newRuntime(t)
		for i := range 65 {
			if _, err := mustAddr(t, client, typ, fmt.Sprint(i)).Call[int](testContext(t), read{}); err != nil {
				t.Fatal(i, err)
			}
		}
		if _, err := mustAddr(t, client, typ, "overflow").Call[int](testContext(t), read{}); !errors.Is(err, actorx.ErrFull) {
			t.Fatal("distributed activation exceeded Instances", err)
		}
	})
}
