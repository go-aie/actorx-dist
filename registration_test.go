package actorxdist

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

func TestRegistrationWithdrawalIsolationAndReplacement(t *testing.T) {
	rt := testRuntime(t, WithExecutionLimits(ExecutionLimits{Types: 2}))
	ctx := testContext(t)
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	defer release()
	spec := ActorSpec[*counter]{Type: "changing", New: func(ActorKey) *counter {
		close(entered)
		<-unblock
		return &counter{}
	}}
	registration, err := rt.Register(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := counterRegistration("other")(ctx, rt); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := rt.createLocal(ctx, ActorKey{"changing", "one"}, newOperation())
		result <- err
	}()
	await(t, entered)
	bounded, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := registration.Unregister(bounded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("withdrawal lost in-flight constructor", err)
	}
	if _, err := rt.createLocal(ctx, ActorKey{"changing", "two"}, newOperation()); !errors.Is(err, ErrTypeUnavailable) {
		t.Fatal("withdrawn Type accepted new creation", err)
	}
	if _, err := rt.Register(ctx, spec); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal("replaced an unsettled registration", err)
	}
	if _, err := counterRegistration("overflow")(ctx, rt); !errors.Is(err, actorx.ErrFull) {
		t.Fatal("draining registration released Type capacity early", err)
	}
	other := mustCreate(t, rt, ActorKey{"other", "one"}, token(t))
	if n, err := other.local.Call[int](ctx, add{3}); err != nil || n != 3 {
		t.Fatal("withdrawal affected another Type", n, err)
	}
	release()
	if err := <-result; !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("late constructor started", err)
	}
	if err := registration.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	replacement, err := counterRegistration("changing")(ctx, rt)
	if err != nil || replacement.token == registration.token {
		t.Fatal("replacement did not get a fresh identity", err)
	}
	if err := registration.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	current := mustCreate(t, rt, ActorKey{"changing", "two"}, token(t))
	if n, err := current.local.Call[int](ctx, add{7}); err != nil || n != 7 {
		t.Fatal("stale withdrawal affected successor", n, err)
	}
	if err := replacement.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := current.local.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrStopped) {
		t.Fatal("withdrawal returned before instance exit", err)
	}
	if n, err := other.local.Call[int](ctx, read{}); err != nil || n != 3 {
		t.Fatal("unrelated instance stopped", n, err)
	}
}

func TestRegistrationWaitsForStoppedAndConcurrentWaiters(t *testing.T) {
	rt := testRuntime(t)
	ctx := testContext(t)
	stopped, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	defer release()
	r, err := rt.Register(ctx, ActorSpec[*discardProbe]{Type: "probe", New: func(ActorKey) *discardProbe {
		return &discardProbe{stopped: stopped, unblock: unblock}
	}})
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, rt, ActorKey{"probe", "one"}, token(t))
	results := make(chan error, 4)
	for range cap(results) {
		go func() { results <- r.Unregister(ctx) }()
	}
	await(t, stopped)
	select {
	case err := <-results:
		t.Fatal("withdrawal completed during Stopped", err)
	default:
	}
	release()
	for range cap(results) {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Unregister(ctx); err != nil {
		t.Fatal("completed handle should remain joinable after Close", err)
	}
}

func TestRegistrationConcurrentDuplicatesAndClose(t *testing.T) {
	ctx := testContext(t)
	rt := testRuntime(t)
	start := make(chan struct{})
	type result struct {
		r   *Registration
		err error
	}
	results := make(chan result, 16)
	for range cap(results) {
		go func() {
			<-start
			r, err := counterRegistration("same")(ctx, rt)
			results <- result{r, err}
		}()
	}
	close(start)
	var winner *Registration
	for range cap(results) {
		got := <-results
		if got.err == nil {
			if winner != nil {
				t.Fatal("duplicate registration succeeded")
			}
			winner = got.r
		} else if !errors.Is(got.err, actorx.ErrSetup) {
			t.Fatal(got.err)
		}
	}
	if winner == nil {
		t.Fatal("no registration succeeded")
	}
	start = make(chan struct{})
	for i := range cap(results) {
		go func() {
			<-start
			r, err := counterRegistration(fmt.Sprintf("race%d", i))(ctx, rt)
			results <- result{r, err}
		}()
	}
	closed := make(chan error, 1)
	go func() { <-start; closed <- rt.Close(ctx) }()
	close(start)
	for range cap(results) {
		got := <-results
		if got.err != nil && !errors.Is(got.err, actorx.ErrClosed) && !errors.Is(got.err, context.Canceled) {
			t.Fatal(got.err)
		}
		if got.r != nil {
			if err := got.r.Unregister(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := winner.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	if len(rt.registrations) != 0 || len(rt.factories) != 0 || len(rt.specs) != 0 {
		t.Fatal("Close retained registrations")
	}
}

func TestRegistrationInvalidAndCanceledHandles(t *testing.T) {
	rt := testRuntime(t)
	ctx := testContext(t)
	spec := ActorSpec[*counter]{Type: "counter", New: func(ActorKey) *counter { return &counter{} }}
	for _, invalid := range []*Runtime{nil, {}} {
		if _, err := invalid.Register(ctx, spec); !errors.Is(err, actorx.ErrSetup) {
			t.Fatal(err)
		}
	}
	if _, err := rt.Register(nil, spec); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}
	for _, invalid := range []*Registration{nil, {}} {
		if err := invalid.Unregister(ctx); !errors.Is(err, actorx.ErrSetup) {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if r, err := rt.Register(canceled, spec); r != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(r, err)
	}
	if len(rt.registrations) != 0 {
		t.Fatal("canceled call reserved registration capacity")
	}
}
