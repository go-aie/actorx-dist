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

// This optional gate uses an isolated server configured as examples/nats.conf.
func TestRegistrationNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	newRuntime := func(t *testing.T) *Runtime {
		t.Helper()
		return testRuntimeWithOptions(t, []Option{WithNATS(url)})
	}
	t.Run("withdrawal retains failed activation until terminal release commits", func(t *testing.T) {
		worker := newRuntime(t)
		ctx := testContext(t)
		typ := fmt.Sprintf("held-%x", time.Now().UnixNano())
		var hold atomic.Bool
		hold.Store(true)
		defer hold.Store(false)
		blocked := make(chan struct{}, 1)
		worker.net.registry.writeFault = func(stage, bucket, _ string) error {
			if stage == "before" && bucket == testDefaultNames.ExitsBucket && hold.Load() {
				select {
				case blocked <- struct{}{}:
				default:
				}
				return context.DeadlineExceeded
			}
			return nil
		}
		r, err := worker.Register(ctx, ActorSpec[*startupProbe]{Type: typ, New: func(ActorKey) *startupProbe {
			return &startupProbe{fail: errors.New("startup failed")}
		}})
		if err != nil {
			t.Fatal(err)
		}
		e, err := r.factory.Call[*creation](ctx, activateRequest{ActorKey{typ, "one"}})
		if err != nil {
			t.Fatal(err)
		}
		await(t, e.done)
		if e.err == nil {
			t.Fatal("expected failed activation")
		}
		await(t, blocked)
		bounded, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		if err := r.Unregister(bounded); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("withdrawal dropped uncommitted terminal work", err)
		}
		if !r.control.IsValid() || worker.registrationToken(typ) != r.token {
			t.Fatal("withdrawal removed controls before settlement")
		}
		if _, err := counterRegistration(typ)(ctx, worker); !errors.Is(err, actorx.ErrSetup) {
			t.Fatal("unsettled Type was replaced", err)
		}
		hold.Store(false)
		if err := r.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
		exit, err := worker.net.registry.exit(ctx, e.key, e.generation)
		if err != nil || exit.value.Status != "COMMITTED" || exit.value.Disposition != "RELEASED" {
			t.Fatal("missing released terminal evidence", exit, err)
		}
	})

	t.Run("Close joins registration being installed", func(t *testing.T) {
		worker := newRuntime(t)
		ctx := testContext(t)
		typ := fmt.Sprintf("install-%x", time.Now().UnixNano())
		entered, release := make(chan struct{}), make(chan struct{})
		var once, enteredOnce sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		worker.net.registry.readFault = func(bucket, key string) error {
			if bucket == testDefaultNames.TypesBucket && key == "t."+identity.Token(typ) {
				enteredOnce.Do(func() { close(entered) })
				<-release
			}
			return nil
		}
		result := make(chan error, 1)
		go func() {
			_, err := counterRegistration(typ)(ctx, worker)
			result <- err
		}()
		await(t, entered)
		bounded, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		if err := worker.Close(bounded); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("Close lost installation work", err)
		}
		unblock()
		if err := <-result; !errors.Is(err, context.Canceled) && !errors.Is(err, actorx.ErrClosed) {
			t.Fatal(err)
		}
		if err := worker.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("withdrawal commits exits and preserves other registrations", func(t *testing.T) {
		worker, client := newRuntime(t), newRuntime(t)
		ctx := testContext(t)
		typ := fmt.Sprintf("register-%x", time.Now().UnixNano())
		r, err := counterRegistration(typ)(ctx, worker)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := counterRegistration(typ+"-other")(ctx, worker); err != nil {
			t.Fatal(err)
		}
		old := mustAddr(t, client, typ, "one")
		if got, err := old.Call[int](ctx, add{5}); err != nil || got != 5 {
			t.Fatal(got, err)
		}
		if err := r.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := old.Wait(ctx); err != nil {
			t.Fatal("withdrawal did not commit exit", err)
		}
		if _, err := old.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrStopped) {
			t.Fatal("retired address changed generations", err)
		}
		if _, err := worker.net.registry.get(ctx, testDefaultNames.TypesBucket, "t."+identity.Token(typ), false); err != nil {
			t.Fatal("withdrawal removed immutable contract", err)
		}
		replacement, err := counterRegistration(typ)(ctx, worker)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
		current := mustAddr(t, client, typ, "one")
		if got, err := current.Call[int](ctx, add{2}); err != nil || got != 2 || current.generation() == old.generation() {
			t.Fatal("replacement or stale withdrawal", got, err)
		}
		other := mustAddr(t, client, typ+"-other", "one")
		if got, err := other.Call[int](ctx, add{9}); err != nil || got != 9 {
			t.Fatal("other Type affected", got, err)
		}
		if err := replacement.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
		conflict, err := worker.Register(ctx, ActorSpec[*mismatch]{Type: typ, New: func(ActorKey) *mismatch { return &mismatch{} }})
		if !errors.Is(err, ErrContract) || conflict == nil {
			t.Fatal("re-registration changed the immutable contract", err)
		}
		if err := conflict.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("same Type on two workers has one activation owner", func(t *testing.T) {
		first, second, client := newRuntime(t), newRuntime(t), newRuntime(t)
		ctx := testContext(t)
		typ := fmt.Sprintf("shared-%x", time.Now().UnixNano())
		var news atomic.Int32
		spec := ActorSpec[*counter]{Type: typ, New: func(ActorKey) *counter { news.Add(1); return &counter{} }}
		r1, err := first.Register(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		r2, err := second.Register(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		results := make(chan error, 4)
		for range cap(results) {
			wg.Go(func() {
				a, err := client.Addr(typ, "same")
				if err == nil {
					_, err = a.Call[int](ctx, add{1})
				}
				results <- err
			})
		}
		wg.Wait()
		for range cap(results) {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if news.Load() != 1 {
			t.Fatal("constructed multiple owners", news.Load())
		}
		a := mustAddr(t, client, typ, "same")
		if got, err := a.Call[int](ctx, read{}); err != nil || got != 4 {
			t.Fatal(got, err)
		}
		owner, err := client.net.registry.owner(ctx, ActorKey{typ, "same"})
		if err != nil {
			t.Fatal(err)
		}
		inactive := r1
		if owner.value.Factory == r1.token {
			inactive = r2
		}
		if err := inactive.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
		if got, err := a.Call[int](ctx, read{}); err != nil || got != 4 {
			t.Fatal("withdrawing another worker affected owner", got, err)
		}
	})

	t.Run("published contract survives canceled registration and lost acknowledgement", func(t *testing.T) {
		worker := newRuntime(t)
		ctx := testContext(t)
		typ := fmt.Sprintf("rollback-%x", time.Now().UnixNano())
		canceled, cancel := context.WithCancel(ctx)
		defer cancel()
		var injected atomic.Bool
		worker.net.registry.writeFault = func(stage, bucket, key string) error {
			if stage == "after" && bucket == testDefaultNames.TypesBucket && key == "t."+identity.Token(typ) && injected.CompareAndSwap(false, true) {
				cancel()
				return context.Canceled
			}
			return nil
		}
		r, err := counterRegistration(typ)(canceled, worker)
		if err == nil || r == nil || !injected.Load() {
			t.Fatal("registration cancellation was not observed", err)
		}
		if err := r.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
		r, err = counterRegistration(typ)(ctx, worker)
		if err != nil {
			t.Fatal("failed registration retained a local slot", err)
		}
		if err := r.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
		// The same verified write path reconciles a durable write with a lost ACK.
		lost := typ + "-ack"
		worker.net.registry.writeFault = func(stage, bucket, key string) error {
			if stage == "after" && bucket == testDefaultNames.TypesBucket && key == "t."+identity.Token(lost) {
				return context.DeadlineExceeded
			}
			return nil
		}
		if _, err := counterRegistration(lost)(ctx, worker); err != nil {
			t.Fatal("did not reconcile published contract", err)
		}
	})
}
