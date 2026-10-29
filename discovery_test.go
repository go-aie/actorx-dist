package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

func discoverLocal(t *testing.T, rt *Runtime) DiscoveryReport {
	t.Helper()
	report, err := rt.Discover(testContext(t), DiscoveryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func TestDiscoveryLocalRegistrations(t *testing.T) {
	rt := testRuntime(t)
	empty := discoverLocal(t, rt)
	if empty.Scope != DiscoveryLocal || empty.EndReason != DiscoveryEndSnapshot || empty.Truncated ||
		len(empty.Actors) != 0 || !reflect.DeepEqual(empty.Responders, []string{rt.runtimeID}) || !opaque(rt.runtimeID) {
		t.Fatal(empty)
	}

	var constructions atomic.Int32
	for _, name := range []string{"zulu", "alpha", "middle"} {
		spec := ActorSpec[*counter]{
			Type:        name,
			Description: "计数器 " + name,
			New: func(ActorKey) *counter {
				constructions.Add(1)
				return &counter{}
			},
		}
		if _, err := rt.Register(testContext(t), spec); err != nil {
			t.Fatal(err)
		}
		spec.Description = "changed after registration"
	}

	report := discoverLocal(t, rt)
	if len(report.Actors) != 3 {
		t.Fatal(report)
	}
	for i, name := range []string{"alpha", "middle", "zulu"} {
		got := report.Actors[i]
		if got.Type != name || got.Fingerprint != rt.specs[name].contract.fingerprint ||
			!reflect.DeepEqual(got.Providers, []DiscoveryProvider{{rt.runtimeID, "计数器 " + name}}) {
			t.Fatal(got)
		}
	}
	if constructions.Load() != 0 || len(rt.ledger) != 0 {
		t.Fatal("discovery activated a business Actor")
	}

	// All returned slices are independent, including nested provider slices.
	report.Actors[0].Type = "mutated"
	report.Actors[0].Providers[0] = DiscoveryProvider{RuntimeID: "mutated", Description: "mutated"}
	report.Responders[0] = "mutated"
	again := discoverLocal(t, rt)
	if again.Actors[0].Type != "alpha" || again.Actors[0].Providers[0].Description != "计数器 alpha" ||
		again.Actors[0].Providers[0].RuntimeID != rt.runtimeID || again.Responders[0] != rt.runtimeID {
		t.Fatal("caller mutated the directory", again)
	}

	r := rt.registrations["alpha"]
	// Exercise the same synchronized installation boundary Register uses.
	rt.mu.Lock()
	r.ready = false
	rt.mu.Unlock()
	if got := discoverLocal(t, rt); len(got.Actors) != 2 || got.Actors[0].Type != "middle" {
		t.Fatal("unfinished registration was visible", got)
	}
	rt.mu.Lock()
	r.ready = true
	rt.mu.Unlock()
	if err := r.Unregister(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := discoverLocal(t, rt); len(got.Actors) != 2 || got.Actors[0].Type != "middle" {
		t.Fatal("withdrawn registration was visible", got)
	}

	// Re-registering a Type can supply a different per-registration description.
	if _, err := counterRegistration("alpha")(testContext(t), rt); err != nil {
		t.Fatal(err)
	}
	if got := discoverLocal(t, rt).Actors[0]; got.Type != "alpha" || got.Providers[0].Description != "" {
		t.Fatal(got)
	}
}

func TestDiscoveryExcludesDrainingRegistration(t *testing.T) {
	rt := testRuntime(t)
	ctx := testContext(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	r, err := rt.Register(ctx, ActorSpec[*counter]{
		Type: "draining",
		New: func(ActorKey) *counter {
			close(entered)
			<-release
			return &counter{}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	created := make(chan error, 1)
	generation := token(t)
	go func() {
		_, err := rt.createLocal(ctx, ActorKey{"draining", "one"}, generation)
		created <- err
	}()
	await(t, entered)

	r.beginUnregister() // closes readiness synchronously; New is still in flight
	if got := discoverLocal(t, rt); len(got.Actors) != 0 || len(got.Responders) != 1 {
		t.Fatal("draining Type appeared in the snapshot", got)
	}

	unblock()
	if err := <-created; !errors.Is(err, actorx.ErrClosed) {
		t.Fatal(err)
	}
	if err := r.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryRuntimeIdentity(t *testing.T) {
	a, b := testRuntime(t), testRuntime(t)
	first, second := discoverLocal(t, a), discoverLocal(t, b)
	if first.Responders[0] == second.Responders[0] {
		t.Fatal("Runtime lifetimes shared an identity")
	}
	if err := a.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	rebuilt := discoverLocal(t, testRuntime(t))
	if rebuilt.Responders[0] == first.Responders[0] || rebuilt.Responders[0] == second.Responders[0] {
		t.Fatal("a rebuilt Runtime reused an identity")
	}
}

func TestDiscoveryConcurrentRegistrationAndWithdrawal(t *testing.T) {
	rt := testRuntime(t)
	ctx := testContext(t)
	changed := make(chan error, 1)
	go func() {
		for i := range 64 {
			r, err := rt.Register(ctx, ActorSpec[*counter]{
				Type:        "changing",
				Description: fmt.Sprint(i),
				New:         func(ActorKey) *counter { return &counter{} },
			})
			if err == nil {
				err = r.Unregister(ctx)
			}
			if err != nil {
				changed <- err
				return
			}
		}
		changed <- nil
	}()

	for range 64 {
		report := discoverLocal(t, rt)
		if len(report.Actors) > 1 {
			t.Fatal("snapshot duplicated a registration", report)
		}
		for _, a := range report.Actors {
			if a.Type != "changing" || a.Fingerprint == "" || len(a.Providers) != 1 ||
				a.Providers[0].RuntimeID != rt.runtimeID || a.Providers[0].Description == "" {
				t.Fatal("snapshot mixed registration state", a)
			}
		}
	}
	if err := <-changed; err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryTruncation(t *testing.T) {
	for _, tc := range []struct {
		name, description string
		count             int
	}{
		{name: "entry count", count: maxDiscoveryEntries + 17},
		{name: "encoded bytes", count: 40, description: strings.Repeat("\x00", 512)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := testRuntime(t)
			ctx := testContext(t)
			for i := tc.count - 1; i >= 0; i-- {
				if _, err := rt.Register(ctx, ActorSpec[*counter]{
					Type:        fmt.Sprintf("type%04d", i),
					Description: tc.description,
					New:         func(ActorKey) *counter { return &counter{} },
				}); err != nil {
					t.Fatal(err)
				}
				if tc.count-i == maxDiscoveryEntries {
					if got := discoverLocal(t, rt); got.Truncated || got.EndReason != DiscoveryEndSnapshot ||
						len(got.Actors) != maxDiscoveryEntries {
						t.Fatal("exact entry boundary was truncated", got)
					}
				}
			}
			report := discoverLocal(t, rt)
			if !report.Truncated || report.EndReason != DiscoveryEndLimit ||
				len(report.Actors) == 0 || len(report.Actors) >= tc.count {
				t.Fatal(report)
			}
			if tc.name == "entry count" && len(report.Actors) != maxDiscoveryEntries {
				t.Fatal("unexpected entry limit", len(report.Actors))
			}
			for i, a := range report.Actors {
				if a.Type != fmt.Sprintf("type%04d", i) {
					t.Fatal("truncation did not keep the sorted prefix", a)
				}
			}

			snapshot, err := rt.snapshotLocal(ctx)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(snapshot)
			if err != nil || len(encoded) > maxDiscoverySnapshotBytes {
				t.Fatal("encoded snapshot exceeded its byte budget", len(encoded), err)
			}
			if tc.name == "encoded bytes" {
				next := fmt.Sprintf("type%04d", len(snapshot.Entries))
				snapshot.Entries = append(snapshot.Entries, discoveryEntry{next, rt.specs[next].contract.fingerprint, tc.description})
				encoded, _ = json.Marshal(snapshot)
				if len(encoded) <= maxDiscoverySnapshotBytes {
					t.Fatal("snapshot was truncated before reaching its byte budget")
				}
			}
		})
	}
}

func TestDiscoveryInvalidCanceledAndClosed(t *testing.T) {
	rt := testRuntime(t)
	ctx := testContext(t)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	expired, expire := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer expire()
	for _, tc := range []struct {
		name  string
		rt    *Runtime
		ctx   context.Context
		query DiscoveryQuery
		want  error
	}{
		{name: "nil Runtime", ctx: ctx, want: actorx.ErrSetup},
		{name: "zero Runtime", rt: &Runtime{}, ctx: ctx, want: actorx.ErrSetup},
		{name: "nil context", rt: rt, want: actorx.ErrSetup},
		{name: "unknown scope", rt: rt, ctx: ctx, query: DiscoveryQuery{Scope: 2}, want: actorx.ErrSetup},
		{name: "local window", rt: rt, ctx: ctx, query: DiscoveryQuery{Window: time.Millisecond}, want: actorx.ErrSetup},
		{name: "negative window", rt: rt, ctx: ctx, query: DiscoveryQuery{Window: -1}, want: actorx.ErrSetup},
		{
			name:  "excessive window",
			rt:    rt,
			ctx:   ctx,
			query: DiscoveryQuery{Scope: DiscoveryCluster, Window: 2*time.Second + 1},
			want:  actorx.ErrSetup,
		},
		{
			name:  "cluster without transport",
			rt:    rt,
			ctx:   ctx,
			query: DiscoveryQuery{Scope: DiscoveryCluster},
			want:  ErrOwnerUnavailable,
		},
		{name: "canceled", rt: rt, ctx: canceled, want: context.Canceled},
		{name: "deadline", rt: rt, ctx: expired, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report, err := tc.rt.Discover(tc.ctx, tc.query)
			if !errors.Is(err, tc.want) || !reflect.DeepEqual(report, DiscoveryReport{}) {
				t.Fatal("want zero report and", tc.want, "got", report, err)
			}
		})
	}
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if report, err := rt.Discover(ctx, DiscoveryQuery{}); !errors.Is(err, actorx.ErrClosed) ||
		!reflect.DeepEqual(report, DiscoveryReport{}) {
		t.Fatal(report, err)
	}
}

type discoveryScanCancellation struct {
	context.Context
	cancel           context.CancelFunc
	checks, cancelAt int
}

func (c *discoveryScanCancellation) Err() error {
	c.checks++
	if c.checks == c.cancelAt {
		c.cancel()
	}
	return c.Context.Err()
}

func TestDiscoveryScanCancellationDiscardsPartialSnapshot(t *testing.T) {
	rt := testRuntime(t)
	for i := range 16 {
		if _, err := counterRegistration(fmt.Sprintf("type%d", i))(testContext(t), rt); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		name     string
		cancelAt int
	}{
		{"during selection", 3},
		{"during encoding", len(rt.registrations) + 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			probe := &discoveryScanCancellation{Context: ctx, cancel: cancel, cancelAt: tc.cancelAt}
			snapshot, err := rt.snapshotLocal(probe)
			if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(snapshot, discoverySnapshot{}) {
				t.Fatal("canceled scan returned a partial snapshot", snapshot, err)
			}
		})
	}
	if got := discoverLocal(t, rt); got.Truncated || len(got.Actors) != 16 {
		t.Fatal("canceled scan affected the next observation", got)
	}
}

// Install a controlled snapshot worker on the real management engine. Its
// lifetime, mailbox, pending and task budgets are the same as production's.
func controlledDiscovery(t *testing.T, rt *Runtime, snapshot func(context.Context) (discoverySnapshot, error)) {
	t.Helper()
	addr, err := rt.management.Spawn(&discoveryActor{rt: rt, snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	rt.discovery = addr // installed before concurrent use
}

func TestDiscoveryCancellationAndCloseJoinTasks(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "close"} {
		t.Run(mode, func(t *testing.T) {
			rt := testRuntime(t)
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			controlledDiscovery(t, rt, func(ctx context.Context) (discoverySnapshot, error) {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release // cancellation does not mean actual worker exit
				return discoverySnapshot{RuntimeID: rt.runtimeID}, nil
			})

			ctx, cancel := context.WithCancel(testContext(t))
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(testContext(t), 100*time.Millisecond)
			}
			defer cancel()
			type result struct {
				report DiscoveryReport
				err    error
			}
			resultCh := make(chan result, 1)
			go func() {
				r, err := rt.Discover(ctx, DiscoveryQuery{})
				resultCh <- result{r, err}
			}()
			await(t, entered)

			want := context.Canceled
			if mode == "cancel" {
				cancel()
			} else if mode == "deadline" {
				want = context.DeadlineExceeded
			} else {
				want = actorx.ErrClosed
				rt.beginClose()
			}
			await(t, canceled)
			got := <-resultCh
			if !errors.Is(got.err, want) || !reflect.DeepEqual(got.report, DiscoveryReport{}) {
				t.Fatal("late snapshot succeeded after cancellation", got, want)
			}

			// A returned cancellation must not let Close skip the still-running worker.
			closed := make(chan error, 1)
			closeCtx := testContext(t)
			go func() { closed <- rt.Close(closeCtx) }()
			await(t, rt.life.Done())
			select {
			case err := <-closed:
				t.Fatal("Close returned before the snapshot worker exited", err)
			default:
			}
			unblock()
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDiscoveryTaskOverloadRetainsCanceledWorkers(t *testing.T) {
	for _, mode := range []string{"local", "node snapshot"} {
		t.Run(mode, func(t *testing.T) {
			rt := testRuntime(t)
			call := func(ctx context.Context) error {
				if mode == "node snapshot" {
					_, err := rt.discovery.Call[discoverySnapshot](ctx, snapshotRequest{ctx: ctx})
					return err
				}
				report, err := rt.Discover(ctx, DiscoveryQuery{})
				if err != nil && !reflect.DeepEqual(report, DiscoveryReport{}) {
					t.Error("failed query returned a partial report", report, err)
				}
				return err
			}

			capacity := rt.limits.managementTasks()
			entered, release := make(chan struct{}, capacity), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			controlledDiscovery(t, rt, func(ctx context.Context) (discoverySnapshot, error) {
				entered <- struct{}{}
				<-release
				return rt.snapshotLocal(ctx)
			})
			results := make(chan error, capacity)
			var cancelFirst context.CancelFunc
			for i := range capacity {
				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()
				if i == 0 {
					cancelFirst = cancel
				}
				go func() { results <- call(ctx) }()
				await(t, entered)
			}

			assertFull := func() {
				t.Helper()
				if err := call(testContext(t)); !errors.Is(err, actorx.ErrFull) {
					t.Fatal("task overload was not rejected", err)
				}
			}
			assertFull()
			cancelFirst()
			if err := <-results; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			assertFull() // the canceled worker still holds its task and completion slots

			unblock()
			for range capacity - 1 {
				if err := <-results; err != nil {
					t.Fatal(err)
				}
			}
			discoverLocal(t, rt)
		})
	}
}

func TestDiscoverySnapshotFailure(t *testing.T) {
	rt := testRuntime(t)
	want := errors.New("snapshot failed")
	var calls atomic.Int32
	controlledDiscovery(t, rt, func(context.Context) (discoverySnapshot, error) {
		calls.Add(1)
		return discoverySnapshot{RuntimeID: rt.runtimeID}, want
	})
	if r, err := rt.Discover(testContext(t), DiscoveryQuery{}); !errors.Is(err, want) ||
		!reflect.DeepEqual(r, DiscoveryReport{}) {
		t.Fatal("failed snapshot returned a partial report", r, err)
	}
	if calls.Load() != 1 {
		t.Fatal("failed snapshot was retried")
	}
}

type discoveryMailboxRequest struct{ Hold bool }

type discoveryMailboxProbe struct {
	*discoveryActor
	entered, release, drained chan struct{}
	remaining                 int
}

func (p *discoveryMailboxProbe) Bindings() []actorx.Binding {
	return append(p.discoveryActor.Bindings(), actorx.HandleMessage(p.hold))
}

func (p *discoveryMailboxProbe) hold(_ actorx.Context, msg discoveryMailboxRequest) error {
	if msg.Hold {
		close(p.entered)
		<-p.release
	} else {
		p.remaining--
		if p.remaining == 0 {
			close(p.drained)
		}
	}
	return nil
}

func TestDiscoveryMailboxOverload(t *testing.T) {
	rt := testRuntime(t)
	p := &discoveryMailboxProbe{
		discoveryActor: &discoveryActor{rt: rt, snapshot: rt.snapshotLocal},
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
		drained:        make(chan struct{}),
		remaining:      rt.limits.managementMailbox(),
	}
	var once sync.Once
	unblock := func() { once.Do(func() { close(p.release) }) }
	defer unblock()
	addr, err := rt.management.Spawn(p)
	if err != nil {
		t.Fatal(err)
	}
	rt.discovery = addr
	ctx := testContext(t)
	if err := addr.Send(ctx, discoveryMailboxRequest{Hold: true}); err != nil {
		t.Fatal(err)
	}
	await(t, p.entered)

	// Fill ordinary mailbox capacity while the handler is held.
	for range rt.limits.managementMailbox() {
		if err := addr.Send(ctx, discoveryMailboxRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := rt.Discover(ctx, DiscoveryQuery{}); !errors.Is(err, actorx.ErrFull) ||
		!reflect.DeepEqual(r, DiscoveryReport{}) {
		t.Fatal("mailbox overload was not rejected", r, err)
	}
	unblock()
	await(t, p.drained)
	discoverLocal(t, rt)
}

func TestDiscoveryUnexpectedStopClosesRuntime(t *testing.T) {
	rt := testRuntime(t)
	if err := rt.discovery.Stop(nil); err != nil {
		t.Fatal(err)
	}
	await(t, rt.life.Done())
	if r, err := rt.Discover(testContext(t), DiscoveryQuery{}); !errors.Is(err, actorx.ErrClosed) ||
		!reflect.DeepEqual(r, DiscoveryReport{}) {
		t.Fatal(r, err)
	}
	if err := rt.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
}
