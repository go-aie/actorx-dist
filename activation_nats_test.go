package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/nats-io/nats.go"
)

// Optional real-transport gate; use an isolated examples/nats.conf server.
func TestActivationAdmissionNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	newRuntime := func(t *testing.T, fixtures ...any) *Runtime {
		t.Helper()
		return testRuntimeWithOptions(t, []Option{WithNATS(url)}, fixtures...)
	}
	for _, tc := range []struct {
		limit  string
		limits ExecutionLimits
	}{
		{"Instances", ExecutionLimits{Instances: 1}},
		{"Creating", ExecutionLimits{Creating: 1, CreatingPerType: 2}},
		{"CreatingPerType", ExecutionLimits{Creating: 2, CreatingPerType: 1}},
	} {
		t.Run(tc.limit, func(t *testing.T) {
			worker, client := newRuntime(t, WithExecutionLimits(tc.limits)), newRuntime(t)
			ctx := testContext(t)
			typ := fmt.Sprintf("admit-%x", time.Now().UnixNano())
			entered, release := make(chan struct{}, 1), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var news atomic.Int32
			spec := ActorSpec[*counter]{Type: typ, New: func(key ActorKey) *counter {
				news.Add(1)
				if key.ID == "held" && tc.limit != "Instances" {
					entered <- struct{}{}
					<-release
				}
				return &counter{}
			}}
			if _, err := worker.Register(ctx, spec); err != nil {
				t.Fatal(err)
			}
			targetType := typ
			if tc.limit == "Creating" {
				targetType += "-other"
				spec.Type = targetType
				if _, err := worker.Register(ctx, spec); err != nil {
					t.Fatal(err)
				}
			}
			held := mustAddr(t, client, typ, "held")
			done := make(chan error, 1)
			go func() { _, err := held.Call[int](ctx, read{}); done <- err }()
			if tc.limit == "Instances" {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			} else {
				await(t, entered)
			}
			denied := mustAddr(t, client, targetType, "denied")
			if _, err := denied.Call[int](ctx, read{}); !errors.Is(err, actorx.ErrFull) || errors.Is(err, ErrActivationUnknown) || errors.Is(err, ErrOutcomeUnknown) {
				t.Fatal("expected definite capacity refusal", err)
			}
			if news.Load() != 1 || denied.generation() != "" {
				t.Fatal("refused activation constructed or selected a generation", news.Load(), denied.generation())
			}
			if _, err := worker.net.registry.owner(ctx, denied.key); !errors.Is(err, nats.ErrKeyNotFound) {
				t.Fatal("refused activation wrote ownership", err)
			}
			worker.mu.Lock()
			count, creating := len(worker.ledger), worker.creating
			worker.mu.Unlock()
			wantCreating := 1
			if tc.limit == "Instances" {
				wantCreating = 0
			}
			if count != 1 || creating != wantCreating {
				t.Fatal("refused activation leaked a reservation", count, creating)
			}
			events, dropped := worker.Diagnostics()
			found := false
			for _, event := range events {
				if event.Operation == "activate-rejected" && strings.Contains(event.Message, "activation "+tc.limit+" limit") {
					found = true
				}
			}
			if !found || dropped != 0 {
				t.Fatal("missing capacity diagnostic", events, dropped)
			}
			if tc.limit == "Instances" {
				// A full activation budget must not reject an existing generation.
				if got, err := held.Call[int](ctx, add{3}); err != nil || got != 3 {
					t.Fatal("existing generation unavailable at capacity", got, err)
				}
				generation := held.generation()
				worker.mu.Lock()
				entry := worker.ledger[generation]
				worker.mu.Unlock()
				if err := held.Stop(nil); err != nil {
					t.Fatal(err)
				}
				if _, err := held.Wait(ctx); err != nil {
					t.Fatal(err)
				}
				await(t, entry.activation.released)
			} else {
				unblock()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			if _, err := denied.Call[int](ctx, read{}); err != nil || news.Load() != 2 {
				t.Fatal("settlement did not restore capacity", err, news.Load())
			}
		})
	}

	t.Run("selected full Runtime does not forward to an idle Runtime", func(t *testing.T) {
		full, idle, client := newRuntime(t, WithExecutionLimits(ExecutionLimits{Instances: 1})), newRuntime(t), newRuntime(t)
		ctx := testContext(t)
		typ := fmt.Sprintf("local-full-%x", time.Now().UnixNano())
		var news atomic.Int32
		spec := ActorSpec[*counter]{Type: typ, New: func(ActorKey) *counter { news.Add(1); return &counter{} }}
		r, err := full.Register(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := mustAddr(t, client, typ, "held").Call[int](ctx, read{}); err != nil {
			t.Fatal(err)
		}
		if _, err := idle.Register(ctx, spec); err != nil {
			t.Fatal(err)
		}
		// Deliver to the chosen candidate explicitly; do not depend on random
		// queue-group selection to hit the full Runtime during this test.
		res := selectedActivationReply(t, full.net, r, "denied", time.Second)
		err, decodeErr := errwire.Decode(res.Error)
		if decodeErr != nil || !errors.Is(err, actorx.ErrFull) || news.Load() != 1 {
			t.Fatal("full candidate did not refuse locally", err, decodeErr, news.Load())
		}
		idle.mu.Lock()
		count := len(idle.ledger)
		idle.mu.Unlock()
		if count != 0 {
			t.Fatal("refusal forwarded activation to another Runtime")
		}
	})

	t.Run("Factory response timeout is activation unknown", func(t *testing.T) {
		worker := newRuntime(t)
		ctx := testContext(t)
		typ := fmt.Sprintf("factory-timeout-%x", time.Now().UnixNano())
		spec, err := describeActor(ActorSpec[*counter]{Type: typ, New: func(ActorKey) *counter { return &counter{} }})
		if err != nil {
			t.Fatal(err)
		}
		probe := &activationCallProbe{entered: make(chan struct{}), release: make(chan struct{})}
		gate, err := worker.management.Spawn(probe)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			close(probe.release)
			_ = gate.Stop(nil)
			_, _ = gate.Wait(ctx)
		}()
		// A standalone candidate lets the real handler call a blocked Factory
		// without mutating a live registration's immutable Factory identity.
		r := &Registration{spec: spec, token: token(t), factory: gate, ready: true}
		n := &network{rt: &Runtime{registrations: map[string]*Registration{typ: r}}, nc: worker.net.nc}
		n.ready.resume(n.ready.epoch())
		res := selectedActivationReply(t, n, r, "one", 100*time.Millisecond)
		await(t, probe.entered)
		err, decodeErr := errwire.Decode(res.Error)
		if decodeErr != nil || !errors.Is(err, ErrActivationUnknown) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, actorx.ErrFull) {
			t.Fatal("Factory timeout became a definite refusal", err, decodeErr)
		}
	})

	t.Run("uncertain claim retains its generation", func(t *testing.T) {
		worker, client := newRuntime(t), newRuntime(t)
		ctx := testContext(t)
		typ := fmt.Sprintf("claim-unknown-%x", time.Now().UnixNano())
		var written, hold atomic.Bool
		hold.Store(true)
		defer hold.Store(false)
		lost := make(chan struct{}, 1)
		worker.net.registry.writeFault = func(stage, bucket, _ string) error {
			if stage == "after" && bucket == currentBucket && written.CompareAndSwap(false, true) {
				lost <- struct{}{}
				return context.DeadlineExceeded
			}
			return nil
		}
		worker.net.registry.readFault = func(bucket, _ string) error {
			if bucket == currentBucket && written.Load() && hold.Load() {
				return context.DeadlineExceeded
			}
			return nil
		}
		var news atomic.Int32
		if _, err := worker.Register(ctx, ActorSpec[*counter]{Type: typ, New: func(ActorKey) *counter { news.Add(1); return &counter{} }}); err != nil {
			t.Fatal(err)
		}
		addr := mustAddr(t, client, typ, "one")
		waiting, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() { _, err := addr.Call[int](waiting, read{}); done <- err }()
		await(t, lost)
		cancel()
		if err := <-done; !errors.Is(err, ErrActivationUnknown) || !errors.Is(err, context.Canceled) {
			t.Fatal("uncertain claim lost its classification", err)
		}
		worker.mu.Lock()
		var generation string
		for gen := range worker.ledger {
			generation = gen
		}
		count := len(worker.ledger)
		worker.mu.Unlock()
		if count != 1 || news.Load() != 0 {
			t.Fatal("uncertain claim released capacity or constructed early", count, news.Load())
		}
		hold.Store(false)
		if _, err := addr.Call[int](ctx, read{}); err != nil || addr.generation() != generation || news.Load() != 1 {
			t.Fatal("claim reconciliation replaced or duplicated construction", err, addr.generation(), generation, news.Load())
		}
	})
}

// Exercise the actual candidate handler and wire reply while controlling which
// Runtime was selected and how long the Factory caller may wait.
func selectedActivationReply(t *testing.T, n *network, r *Registration, id string, budget time.Duration) factoryFrame {
	t.Helper()
	reply := nats.NewInbox()
	sub, err := n.nc.SubscribeSync(reply)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := n.nc.FlushWithContext(testContext(t)); err != nil {
		t.Fatal(err)
	}
	req := factoryFrame{Version: 1, Kind: "request", Operation: "activate", Type: r.spec.name, ID: id,
		Fingerprint: r.spec.contract.fingerprint, Correlation: token(t), Deadline: time.Now().Add(budget).UnixNano(),
		Reply: reply, Payload: json.RawMessage(`{"create":true}`)}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	n.handle(incoming{msg: &nats.Msg{Subject: factorySubject(req.Type), Reply: reply, Data: data}, typ: req.Type, token: r.token, registration: r})
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var res factoryFrame
	if err := decodeFactoryFrame(msg.Data, &res); err != nil || res.Status != "error" || res.Correlation != req.Correlation {
		t.Fatal("invalid activation refusal", res, err)
	}
	return res
}

type activationCallProbe struct{ entered, release chan struct{} }

func (p *activationCallProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.Handle(p.activate)}
}

func (p *activationCallProbe) activate(actorx.Context, activateRequest) (*creation, error) {
	close(p.entered)
	<-p.release
	return nil, actorx.ErrFull
}
