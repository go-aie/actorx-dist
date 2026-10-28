package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
)

func testContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type testRegistration func(context.Context, *Runtime) (*Registration, error)

func registerSpec[T actorx.Actor](spec ActorSpec[T]) testRegistration {
	return func(ctx context.Context, rt *Runtime) (*Registration, error) { return rt.Register(ctx, spec) }
}

func testRuntime(t *testing.T, options ...any) *Runtime {
	t.Helper()
	return testRuntimeWithOptions(t, []Option{func(cfg *runtimeOptions) { cfg.localTest = true }}, options...)
}

func testRuntimeWithOptions(t *testing.T, config []Option, fixtures ...any) *Runtime {
	t.Helper()
	for _, fixture := range fixtures {
		if option, ok := fixture.(Option); ok {
			config = append(config, option)
		}
	}
	rt, err := NewRuntime(testContext(t), config...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := rt.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	for _, fixture := range fixtures {
		if register, ok := fixture.(testRegistration); ok {
			if _, err := register(testContext(t), rt); err != nil {
				t.Fatal(err)
			}
		}
	}
	return rt
}

func token(t *testing.T) string {
	t.Helper()

	value, err := identity.NewOpaque()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("event timed out")
	}
}

func mustAddr(t *testing.T, rt *Runtime, typ, id string) Addr {
	t.Helper()

	addr, err := rt.Addr(typ, id)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func mustCreate(t *testing.T, rt *Runtime, key ActorKey, gen string) *creation {
	t.Helper()

	entry, err := rt.createLocal(testContext(t), key, gen)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

// Only tests install a local invocation adapter. This is neither a supported
// backend nor evidence of NATS activation/ownership/codec/terminal commits.
func installLocalAdapter(rt *Runtime) {
	var mu sync.Mutex
	generations := map[ActorKey]string{}
	rt.invoke = func(ctx context.Context, addr Addr, kind actorx.Kind, msg any, result reflect.Type, yield func(any) error) (any, error) {
		generation := addr.generation()
		if generation == "" {
			mu.Lock()
			generation = generations[addr.key]
			if generation == "" {
				generation, _ = identity.NewOpaque()
				generations[addr.key] = generation
			}
			mu.Unlock()
			if err := addr.bind(generation); err != nil {
				return nil, err
			}
		}
		entry, err := rt.createLocal(ctx, addr.key, generation)
		if err != nil {
			return nil, err
		}
		return invokeLocal(ctx, entry.local, rt.specs[addr.key.Type].descriptions, kind, msg, result, yield)
	}
}

type add struct{ N int }

type read struct{}

type series struct{}

type counter struct {
	n       int
	started *atomic.Int32
	calls   *atomic.Int32
}

func (c *counter) Bindings() []actorx.Binding {
	return []actorx.Binding{
		actorx.HandleMessage(c.start),
		actorx.Handle(c.add),
		actorx.Handle(c.read),
		actorx.HandleStream(c.series),
	}
}

func (c *counter) start(actorx.Context, actorx.Started) error {
	if c.started != nil {
		c.started.Add(1)
	}
	return nil
}

func (c *counter) add(_ actorx.Context, msg add) (int, error) {
	c.n += msg.N
	if c.calls != nil {
		c.calls.Add(1)
	}
	return c.n, nil
}

func (c *counter) read(actorx.Context, read) (int, error) { return c.n, nil }

func (c *counter) series(ctx actorx.Context, _ series, reply actorx.StreamReply[int]) error {
	for i := range 3 {
		if err := reply.Write(ctx, c.n+i, 8); err != nil {
			return err
		}
	}
	reply.Complete(nil)
	return nil
}

func counterRegistration(name string) testRegistration {
	return registerSpec(ActorSpec[*counter]{Type: name, New: func(ActorKey) *counter { return &counter{} }})
}

type erased interface{ actorx.Actor }

type valueActor struct{}

func (valueActor) Bindings() []actorx.Binding { return nil }

type metadataPanic struct{}

func (*metadataPanic) Bindings() []actorx.Binding { panic("metadata") }

type invalidBindings struct{}

func (*invalidBindings) Bindings() []actorx.Binding { return []actorx.Binding{{}} }

type mismatch struct{ live bool }

func (m *mismatch) Bindings() []actorx.Binding {
	if m != nil && m.live {
		return []actorx.Binding{actorx.Handle(func(actorx.Context, read) (string, error) { return "", nil })}
	}
	return []actorx.Binding{actorx.Handle(func(actorx.Context, read) (int, error) { return 0, nil })}
}

func TestRegistrationValidationAndSnapshots(t *testing.T) {
	cases := map[string][]testRegistration{
		"nil New":          {registerSpec(ActorSpec[*counter]{Type: "counter"})},
		"erased":           {registerSpec(ActorSpec[erased]{Type: "counter", New: func(ActorKey) erased { panic("New executed") }})},
		"value":            {registerSpec(ActorSpec[valueActor]{Type: "counter", New: func(ActorKey) valueActor { return valueActor{} }})},
		"invalid type":     {counterRegistration("Bad")},
		"duplicate":        {counterRegistration("counter"), counterRegistration("counter")},
		"metadata panic":   {registerSpec(ActorSpec[*metadataPanic]{Type: "counter", New: func(ActorKey) *metadataPanic { panic("New executed") }})},
		"invalid bindings": {registerSpec(ActorSpec[*invalidBindings]{Type: "counter", New: func(ActorKey) *invalidBindings { panic("New executed") }})},
	}
	for name, registrations := range cases {
		t.Run(name, func(t *testing.T) {
			rt := testRuntime(t)
			var err error
			for _, register := range registrations {
				_, err = register(testContext(t), rt)
				if err != nil {
					break
				}
			}
			if !errors.Is(err, actorx.ErrSetup) {
				t.Fatal(err)
			}
		})
	}
	if rt, err := NewRuntime(testContext(t), nil); rt != nil || !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(rt, err)
	}

	// Mutating the declaration later must not change its captured metadata or New.
	var newCalls, started, handler atomic.Int32
	spec := ActorSpec[*counter]{Type: "counter", New: func(ActorKey) *counter {
		newCalls.Add(1)
		return &counter{started: &started, calls: &handler}
	}}
	option := registerSpec(spec)
	spec.Type = "changed"
	spec.New = nil
	for range 2 {
		rt := testRuntime(t, option)
		if newCalls.Load() != 0 || started.Load() != 0 || handler.Load() != 0 {
			t.Fatal("registration executed business code")
		}
		if len(rt.factories) != 1 || rt.management == nil || rt.business == rt.management {
			t.Fatal("engine separation")
		}
	}

	// Live bindings must still match the registered contract at construction time.
	rt := testRuntime(t, registerSpec(ActorSpec[*mismatch]{Type: "mismatch", New: func(ActorKey) *mismatch { return &mismatch{live: true} }}))
	if _, err := rt.createLocal(testContext(t), ActorKey{"mismatch", "1"}, token(t)); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}

	client := testRuntime(t)
	if len(client.factories) != 0 {
		t.Fatal("pure caller registered types")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if rt, err := NewRuntime(canceled); rt != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(rt, err)
	}
}

func TestLocalDynamicBridgeAndButtonFlow(t *testing.T) {
	var handled atomic.Int32
	rt := testRuntime(t,
		registerSpec(ActorSpec[*counter]{Type: "counter", New: func(ActorKey) *counter {
			return &counter{calls: &handled}
		}}),
		registerSpec(ActorSpec[*button]{Type: "button", New: func(ActorKey) *button {
			return &button{}
		}}),
	)
	installLocalAdapter(rt)
	ctx := testContext(t)
	addr := mustAddr(t, rt, "counter", "clicks")
	if err := addr.Send(ctx, add{2}); err != nil {
		t.Fatal(err)
	}
	if got, err := addr.Call[int](ctx, read{}); err != nil || got != 2 {
		t.Fatal(got, err)
	}

	// Rejected calls must not execute the handler or change its state.
	if _, err := addr.Call[string](ctx, add{100}); !errors.Is(err, actorx.ErrReplyType) || handled.Load() != 1 {
		t.Fatal("bad result executed", err, handled.Load())
	}
	if _, err := addr.Call[int](ctx, series{}); !errors.Is(err, actorx.ErrMode) {
		t.Fatal(err)
	}
	if _, err := addr.Call[int](ctx, struct{}{}); !errors.Is(err, actorx.ErrUnhandled) {
		t.Fatal(err)
	}

	var chunks []int
	if err := addr.Stream[int](ctx, series{}, func(n int) error {
		chunks = append(chunks, n)
		return nil
	}); err != nil || !reflect.DeepEqual(chunks, []int{2, 3, 4}) {
		t.Fatal(chunks, err)
	}

	b := mustAddr(t, rt, "button", "buy")
	if got, err := b.Call[int](ctx, click{}); err != nil || got != 3 {
		t.Fatal(got, err)
	}
	if got, err := b.Call[int](ctx, clickTask{}); err != nil || got != 4 {
		t.Fatal(got, err)
	}

	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, entry := range rt.ledger {
		if entry.local.IsZero() {
			continue
		}
		if _, err := entry.local.Wait(ctx); err != nil {
			t.Fatal("incomplete exit", err)
		}
	}
}

type click struct{}

type clickTask struct{}

type button struct{}

func (b *button) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.Handle(b.click), actorx.HandleDeferred(b.task)}
}

func (*button) click(ctx actorx.Context, _ click) (int, error) {
	target, err := MustFrom(ctx).Addr("counter", "clicks")
	if err != nil {
		return 0, err
	}
	return target.Call[int](MustFrom(ctx), add{1})
}

func (*button) task(ctx actorx.Context, _ clickTask, reply actorx.Reply[int]) error {
	view := MustFrom(ctx)
	self, err := view.Self()
	if err != nil || self.Key().Type != "button" {
		return actorx.ErrSetup
	}
	target, err := view.Addr("counter", "clicks")
	if err != nil {
		return err
	}
	_, err = actorx.StartTask(ctx, func(taskCtx context.Context) (int, error) {
		// Communication uses the task's context, not the captured handler view.
		return target.Call[int](taskCtx, add{1})
	}, func(n int, err error) bool {
		reply.Complete(n, err)
		return true
	}, nil)
	return err
}

// Addr supports explicit JSON export, and the distributed view is a Go context.
var _ json.Marshaler = Addr{}
var _ context.Context = Context(nil)

type bindingReadProbe struct {
	reads  *atomic.Int32
	called *atomic.Int32
}

func (p *bindingReadProbe) Bindings() []actorx.Binding {
	if p != nil && p.reads != nil && p.reads.Add(1) > 1 {
		panic("live Bindings read twice")
	}
	return []actorx.Binding{actorx.HandleMessage(p.message)}
}

func (p *bindingReadProbe) message(actorx.Context, read) error {
	p.called.Add(1)
	return nil
}

func TestDeduplicationAndSingleLiveBindingRead(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(unblock) }) }
	defer release()
	var news, reads, called atomic.Int32
	rt := testRuntime(t, registerSpec(ActorSpec[*bindingReadProbe]{Type: "probe", New: func(ActorKey) *bindingReadProbe {
		news.Add(1)
		close(entered)
		<-unblock
		return &bindingReadProbe{reads: &reads, called: &called}
	}}))

	// Ensure an early failure still unblocks New before testRuntime cleanup.
	t.Cleanup(release)
	ctx := testContext(t)
	msg := createRequest{ActorKey{"probe", "1"}, token(t)}
	first, err := rt.factories["probe"].Call[*creation](ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	await(t, entered)

	// Keep New blocked while duplicate callers join the same ledger entry.
	for range 8 {
		got, err := rt.factories["probe"].Call[*creation](ctx, msg)
		if err != nil || got != first {
			t.Fatal("duplicate created another work item", got, err)
		}
	}

	release()
	await(t, first.done)
	if first.err != nil || news.Load() != 1 || reads.Load() != 1 {
		t.Fatal(first.err, news.Load(), reads.Load())
	}

	descriptions := rt.specs["probe"].descriptions
	if _, err := invokeLocal(ctx, first.local, descriptions, actorx.CallKind, read{}, reflect.TypeFor[int](), nil); !errors.Is(err, actorx.ErrReplyType) || called.Load() != 0 {
		t.Fatal(err, called.Load())
	}
	if got, err := invokeLocal(ctx, first.local, descriptions, actorx.CallKind, read{}, reflect.TypeFor[any](), nil); err != nil || got != nil || called.Load() != 1 {
		t.Fatal(got, err, called.Load())
	}
}
