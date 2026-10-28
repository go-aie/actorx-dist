package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

type observe struct{}

type probeResult struct {
	self      Addr
	completed chan struct{}
}

type observation struct {
	key               ActorKey
	generation, stage string
	rt                *Runtime
	ctx               actorx.Context
	err               error
}

type contextProbe struct {
	key       ActorKey
	reports   chan observation
	completed chan struct{}
	local     bool
	depth     int
	owner     *Runtime
}

func (p *contextProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{
		actorx.HandleMessage(p.start),
		actorx.HandleMessage(p.stopping),
		actorx.HandleMessage(p.stopped),
		actorx.Handle(p.observe),
	}
}

func (p *contextProbe) check(ctx actorx.Context, stage string) error {
	view := MustFrom(ctx)
	if optional, ok := From(ctx); !ok || optional != view {
		return fmt.Errorf("capability entrances disagree")
	}
	if p.local && view.(*distributedContext).rt != p.owner {
		return fmt.Errorf("local descendant changed Runtime")
	}

	type parentKey struct{}
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), parentKey{}, "value"), time.Second)
	defer cancel()
	derived := view.WithContext(parent)
	wantDeadline, _ := parent.Deadline()
	gotDeadline, hasDeadline := derived.Deadline()
	if !hasDeadline || gotDeadline != wantDeadline || derived.Value(parentKey{}) != "value" || derived.Done() != parent.Done() {
		return fmt.Errorf("derived parent semantics changed")
	}
	cancel()
	if !errors.Is(derived.Err(), context.Canceled) {
		return fmt.Errorf("derived cancellation lost")
	}

	self, err := view.Self()
	if !p.local {
		first, firstErr := MustFrom(ctx).Addr("probe", "lookup")
		second, secondErr := view.Addr("probe", "lookup")
		if firstErr != nil || secondErr != nil {
			// Shutdown closes new handles but preserves Self.
			if (firstErr != nil && !errors.Is(firstErr, actorx.ErrClosed)) || (secondErr != nil && !errors.Is(secondErr, actorx.ErrClosed)) {
				return errors.Join(firstErr, secondErr)
			}
		} else if first.Key() != second.Key() || first.state == second.state {
			return fmt.Errorf("view lookup state is shared")
		}
	}
	if p.local {
		if !self.IsZero() || !errors.Is(err, actorx.ErrSetup) {
			return fmt.Errorf("local child inherited identity: %v", err)
		}
		if _, err := view.Addr("probe", "child-target"); err != nil {
			return err
		}
		local := MustFrom(derived)
		if _, err := local.Self(); !errors.Is(err, actorx.ErrSetup) || local.(*distributedContext).rt != p.owner {
			return fmt.Errorf("derived local descendant changed environment: %v", err)
		}
		return nil
	}
	if err != nil || self.Key() != p.key || self.generation() == "" {
		return fmt.Errorf("Self: %v %v", self, err)
	}

	for _, derived := range []actorx.Context{
		ctx.WithContext(context.Background()).WithContext(context.Background()),
		view.WithContext(context.Background()).WithContext(context.Background()),
	} {
		got, err := MustFrom(derived).Self()
		if err != nil || got.Key() != p.key || got.generation() != self.generation() {
			return fmt.Errorf("derived identity changed")
		}
	}

	copied := self
	raw, err := json.Marshal(copied)
	if err != nil {
		return err
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil || fields["generation"] != self.generation() {
		return fmt.Errorf("Self snapshot missing generation")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := MustFrom(view.WithContext(canceled)).Self(); err != nil || got.generation() != self.generation() {
		return fmt.Errorf("cancellation erased Self")
	}
	p.reports <- observation{key: p.key, generation: self.generation(), rt: view.(*distributedContext).rt, stage: stage, ctx: ctx}
	return nil
}

func (p *contextProbe) start(ctx actorx.Context, _ actorx.Started) error {
	if err := p.check(ctx, "started"); err != nil {
		return err
	}
	if p.local && p.depth == 2 {
		return nil
	}
	child, err := ctx.Spawn(&contextProbe{local: true, depth: p.depth + 1, owner: MustFrom(ctx).(*distributedContext).rt})
	if err != nil {
		return err
	}
	_ = child.Stop(nil)
	if exit, err := child.Wait(ctx); err != nil || exit.Reason != nil {
		return errors.Join(err, exit.Reason)
	}
	if p.local {
		return nil
	}

	_, err = actorx.StartTask(ctx, func(context.Context) (int, error) { return 0, nil }, func(int, error) bool {
		if err := p.check(ctx, "complete"); err != nil {
			p.reports <- observation{err: err}
			ctx.Stop(err)
		}
		close(p.completed)
		return true
	}, nil)
	return err
}

func (p *contextProbe) stopping(ctx actorx.Context, _ actorx.Stopping) error {
	if p.local {
		return nil
	}
	return p.check(ctx, "stopping")
}

func (p *contextProbe) stopped(ctx actorx.Context, _ actorx.Stopped) error {
	if p.local {
		return nil
	}
	return p.check(ctx, "stopped")
}

func (p *contextProbe) observe(ctx actorx.Context, _ observe) (probeResult, error) {
	if err := p.check(ctx, "handler"); err != nil {
		return probeResult{}, err
	}
	self, err := MustFrom(ctx).Self()
	return probeResult{self, p.completed}, err
}

func TestContextViewsAndConcurrentGenerations(t *testing.T) {
	reports := make(chan observation, 256)
	completed := make(chan chan struct{}, 32)

	// No constructor captures a Runtime variable assigned after NewRuntime returns.
	spec := ActorSpec[*contextProbe]{Type: "probe", New: func(key ActorKey) *contextProbe {
		done := make(chan struct{})
		completed <- done
		return &contextProbe{key: key, reports: reports, completed: done}
	}}
	runtimes := []*Runtime{testRuntime(t, registerSpec(spec)), testRuntime(t, registerSpec(spec))}
	expected := map[*Runtime]map[string]ActorKey{}
	var saved []actorx.Context
	for _, rt := range runtimes {
		expected[rt] = map[string]ActorKey{}
	}

	// Reuse logical keys in a second generation on both independent Runtimes.
	for round := range 2 {
		var wg sync.WaitGroup
		for _, rt := range runtimes {
			for i := range 4 {
				key := ActorKey{"probe", fmt.Sprintf("key-%d", i)}
				gen := token(t)
				expected[rt][gen] = key
				wg.Add(1)
				go func() {
					defer wg.Done()
					entry := mustCreate(t, rt, key, gen)
					got, err := entry.local.Call[probeResult](testContext(t), observe{})
					if err != nil || got.self.generation() != gen || got.self.Key() != key {
						t.Error(got, err)
					}
					await(t, got.completed)
					if round == 0 {
						if err := rt.releaseLocal(testContext(t), key, gen); err != nil {
							t.Error(err)
						}
					}
				}()
			}
		}
		wg.Wait()
		for range 8 {
			await(t, <-completed)
		}
	}

	for _, rt := range runtimes {
		if err := rt.Close(testContext(t)); err != nil {
			t.Fatal(err)
		}
	}
	close(reports)

	// Every lifecycle stage must retain its own Runtime, key and generation.
	counts := map[string]int{}
	for report := range reports {
		if report.err != nil {
			t.Error(report.err)
			continue
		}
		if expected[report.rt][report.generation] != report.key {
			t.Error("mixed Runtime/key/generation", report)
		}
		counts[report.stage]++
		if report.stage == "handler" {
			saved = append(saved, report.ctx)
		}
	}
	for _, stage := range []string{"started", "handler", "complete", "stopping", "stopped"} {
		if counts[stage] != 16 {
			t.Error(stage, counts[stage])
		}
	}

	for _, original := range saved {
		view := MustFrom(original)
		if _, err := view.Self(); err != nil {
			t.Fatal("closed Self", err)
		}
		if _, err := view.Addr("probe", "other"); !errors.Is(err, actorx.ErrClosed) {
			t.Fatal(err)
		}
	}
}

type pureViewProbe struct{}

func (*pureViewProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.HandleMessage(func(ctx actorx.Context, _ actorx.Started) error {
		if view, ok := From(ctx); view != nil || ok {
			return fmt.Errorf("pure core has distributed capability")
		}
		return checkMissingContext(ctx)
	})}
}

func TestPureCoreAndManagementContext(t *testing.T) {
	rt, err := actorx.NewRuntime(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close(context.Background())
	if _, err := rt.Spawn(&pureViewProbe{}); err != nil {
		t.Fatal(err)
	}

	owner := testRuntime(t, registerSpec(ActorSpec[*contextProbe]{Type: "probe", New: func(key ActorKey) *contextProbe {
		return &contextProbe{key: key}
	}}))

	// The same management Runtime that owns Factories stays pure core.
	if _, err := owner.management.Spawn(&pureViewProbe{}); err != nil {
		t.Fatal(err)
	}
}

func checkMissingContext(ctx actorx.Context) (err error) {
	defer func() {
		value := recover()
		if value == nil || !strings.Contains(fmt.Sprint(value), "missing distributed context") {
			err = fmt.Errorf("MustFrom diagnostic: %v", value)
		}
	}()
	MustFrom(ctx)
	return fmt.Errorf("MustFrom did not panic")
}

func TestContextCapabilityDetection(t *testing.T) {
	var typedNil *distributedContext
	var coreNil *pureCoreContext
	for _, ctx := range []actorx.Context{nil, typedNil, coreNil} {
		if got, ok := From(ctx); got != nil || ok {
			t.Fatalf("From(%T): %v %v", ctx, got, ok)
		}
		if err := checkMissingContext(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Capability detection is intentionally independent of execution ownership.
	capability := &distributedContext{}
	if got, ok := From(capability); !ok || got != capability || MustFrom(capability) != got {
		t.Fatal("capability probe inspected core ownership")
	}
}

type pureCoreContext struct{ actorx.Context }
