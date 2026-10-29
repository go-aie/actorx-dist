package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

func TestProviderTimeoutRetainsLockAndAllEightSlots(t *testing.T) {
	r, g := newProviderGateway(t)
	entered := make(chan any, 16)
	canceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var running, maximum atomic.Int32
	if err := r.Register(context.Background(), selected.Type, func() (any, error) {
		return callActorFunc(func(ctx context.Context, v any) (any, error) {
			if v == "other" {
				return v, nil
			}
			n := running.Add(1)
			defer running.Add(-1)
			for old := maximum.Load(); n > old; old = maximum.Load() {
				if maximum.CompareAndSwap(old, n) {
					break
				}
			}
			entered <- v
			if v == "hold" {
				<-ctx.Done()
				close(canceled)
				<-release // deliberately ignores cancellation
			}
			return v, nil
		}), nil
	}); err != nil {
		t.Fatal(err)
	}
	g.create(selected)
	other := selected
	other.ID = "other"
	g.create(other)

	first := g.send("actor/call", selected, "hold", time.Now().Add(100*time.Millisecond))
	if waitValue(t, entered) != "hold" {
		t.Fatal("handler did not start")
	}
	w := g.work(first, selected.Type)
	if w == nil {
		t.Fatal("running operation unowned")
	}
	waitValue(t, canceled)
	waitValue(t, w.settled)
	w.mu.Lock()
	assertProviderCode(t, *w.terminal, 1004)
	w.mu.Unlock()
	select {
	case <-w.done:
		t.Fatal("expired handler released slot")
	default:
	}

	// Seven queued jobs plus the timed-out handler consume all eight slots.
	queued := make([]json.RawMessage, 7)
	for i := range queued {
		queued[i] = g.send("actor/call", selected, "queued", time.Now().Add(5*time.Second))
	}
	overflow := g.send("actor/call", other, "overflow", time.Now().Add(5*time.Second))
	assertProviderCode(t, g.response(overflow), 1001)
	p := g.registration(selected.Type)
	p.mu.Lock()
	slots := len(p.active)
	p.mu.Unlock()
	if slots != 8 || running.Load() != 1 {
		t.Fatal("expired or queued work lost ownership", slots, running.Load())
	}
	select {
	case v := <-entered:
		t.Fatal("queued handler bypassed expired lock", v)
	default:
	}

	// Ownership ends only when the blocked handler actually returns.
	releaseOnce.Do(func() { close(release) })
	waitValue(t, w.done)
	for _, id := range queued {
		g.response(id)
		g.join(id, selected.Type)
	}
	if maximum.Load() != 1 {
		t.Fatal("same instance ran concurrently", maximum.Load())
	}
	for range queued {
		if waitValue(t, entered) != "queued" {
			t.Fatal("unexpected entry")
		}
	}
	select {
	case m := <-g.responses:
		t.Fatal("late or duplicate terminal POST", m)
	default:
	}
}

func TestProviderQueuedExpiryAndIndependentInstance(t *testing.T) {
	r, g := newProviderGateway(t)
	entered := make(chan any, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	if err := r.Register(context.Background(), selected.Type, func() (any, error) {
		return callActorFunc(func(_ context.Context, v any) (any, error) {
			entered <- v
			if v == "hold" {
				<-release
			}
			return v, nil
		}), nil
	}); err != nil {
		t.Fatal(err)
	}
	g.create(selected)
	other := selected
	other.ID = "other"
	g.create(other)
	first := g.send("actor/call", selected, "hold", time.Now().Add(5*time.Second))
	waitValue(t, entered)
	expired := g.send("actor/call", selected, "expired", time.Now().Add(100*time.Millisecond))
	// A response from another instance is also a barrier for SSE admission.
	barrier := g.send("actor/call", other, "other", time.Now().Add(5*time.Second))
	if providerValue(t, g.response(barrier)) != "other" || waitValue(t, entered) != "other" {
		t.Fatal("other instance blocked")
	}
	g.join(barrier, selected.Type)
	w := g.work(expired, selected.Type)
	if w == nil {
		t.Fatal("queued operation not admitted")
	}
	waitValue(t, w.done)
	assertProviderCode(t, *w.terminal, 1005)
	if w.started {
		t.Fatal("expired queue entered user code")
	}

	// Already-expired requests also cannot construct a new instance.
	before := g.registration(selected.Type)
	a := selected
	a.ID = "expired-create"
	g.send("actor/create", a, nil, time.Now().Add(-time.Second))
	barrier = g.send("actor/call", other, "barrier", time.Now().Add(5*time.Second))
	g.response(barrier)
	waitValue(t, entered)
	g.join(barrier, selected.Type)
	before.mu.Lock()
	instances := len(before.instances)
	before.mu.Unlock()
	if instances != 2 {
		t.Fatal("expired create reserved instance")
	}
	releaseOnce.Do(func() { close(release) })
	g.response(first)
	g.join(first, selected.Type)
	select {
	case v := <-entered:
		t.Fatal("expired work executed", v)
	default:
	}
	select {
	case m := <-g.responses:
		t.Fatal("expired RPC opened a new reply budget", m)
	default:
	}
}

func TestProviderCloseWaitCancellationKeepsCleanupResponsibility(t *testing.T) {
	r, g := newProviderGateway(t)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls atomic.Int32
	if err := r.Register(context.Background(), selected.Type, func() (any, error) {
		return callActorFunc(func(ctx context.Context, _ any) (any, error) {
			calls.Add(1)
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return "late", nil
		}), nil
	}); err != nil {
		t.Fatal(err)
	}
	g.create(selected)
	id := g.send("actor/call", selected, nil, time.Now().Add(5*time.Second))
	waitValue(t, entered)
	w := g.work(id, selected.Type)
	queued := g.send("actor/call", selected, nil, time.Now().Add(5*time.Second))

	// Unknown method response proves the queued job was admitted first.
	g.stream(selected.Type).send(wire.Message{JSONRPC: "2.0", ID: mustJSON("barrier"), Method: "unknown"})
	g.response(mustJSON("barrier"))
	q := g.work(queued, selected.Type)

	// Canceling a wait must leave the runtime responsible for joining user code.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.WaitClosed(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	waitValue(t, canceled)
	waitValue(t, w.settled)
	waitValue(t, q.done)
	assertProviderCode(t, *q.terminal, 1009)
	if q.started || calls.Load() != 1 {
		t.Fatal("queued work started during shutdown")
	}
	waitValue(t, g.stream(selected.Type).closed)
	select {
	case <-r.closed:
		t.Fatal("shutdown abandoned running handler")
	default:
	}
	if err := r.Register(context.Background(), "demo.new", echoFactory); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if err := r.WaitClosed(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	releaseOnce.Do(func() { close(release) })
	var waits sync.WaitGroup
	for range 8 {
		waits.Go(func() {
			if err := r.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	waits.Wait()
	waitValue(t, w.done)
	assertProviderCode(t, *w.terminal, 1004)
	if len(r.registrations) != 0 {
		t.Fatal("cleanup retained registrations")
	}
}

func TestProviderBlockedFactoryRetainsExecutionOwnership(t *testing.T) {
	r, g := newProviderGateway(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var factories atomic.Int32
	if err := r.Register(context.Background(), selected.Type, func() (any, error) {
		factories.Add(1)
		close(entered)
		<-release
		return echoFactory()
	}); err != nil {
		t.Fatal(err)
	}
	id := g.send("actor/create", selected, nil, time.Now().Add(100*time.Millisecond))
	waitValue(t, entered)
	w := g.work(id, selected.Type)
	waitValue(t, w.settled)
	assertProviderCode(t, *w.terminal, 1004)
	next := g.send("actor/create", selected, nil, time.Now().Add(5*time.Second))
	g.stream(selected.Type).send(wire.Message{JSONRPC: "2.0", ID: mustJSON("barrier"), Method: "unknown"})
	g.response(mustJSON("barrier"))
	if factories.Load() != 1 {
		t.Fatal("duplicate create bypassed lock")
	}
	releaseOnce.Do(func() { close(release) })
	assertProviderCode(t, g.response(next), 0)
	g.join(next, selected.Type)
	waitValue(t, w.done)
	if factories.Load() != 1 {
		t.Fatal("late factory result was constructed again")
	}
}

func TestProviderInstanceCapacity(t *testing.T) {
	r, g := newProviderGateway(t)
	if err := r.Register(context.Background(), selected.Type, echoFactory); err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		a := selected
		a.ID = string(rune('a' + i))
		g.create(a)
	}
	id := g.send("actor/create", selected, nil, time.Now().Add(5*time.Second))
	assertProviderCode(t, g.response(id), 1001)
	p := g.registration(selected.Type)
	p.mu.Lock()
	count := len(p.instances)
	p.mu.Unlock()
	if count != 8 {
		t.Fatal(count)
	}
}

func TestProviderResponsePOSTHasOriginalDeadlineAndCannotBlockSSE(t *testing.T) {
	r, g := newProviderGateway(t)
	if err := r.Register(context.Background(), selected.Type, echoFactory); err != nil {
		t.Fatal(err)
	}
	g.create(selected)
	posted := make(chan time.Time, 1)
	var attempts atomic.Int32
	g.mu.Lock()
	g.replyHook = func(req *http.Request, m wire.Message) (*http.Response, error) {
		if string(m.ID) != `"barrier"` {
			attempts.Add(1)
			deadline, _ := req.Context().Deadline()
			posted <- deadline
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		return emptyResponse(204), nil
	}
	g.mu.Unlock()
	deadline := time.Now().Add(100 * time.Millisecond)
	id := g.send("actor/call", selected, "success", deadline)
	if waitValue(t, posted).UnixMilli() != deadline.UnixMilli() {
		t.Fatal("POST renewed deadline")
	}
	w := g.work(id, selected.Type)
	g.stream(selected.Type).send(wire.Message{JSONRPC: "2.0", ID: mustJSON("barrier"), Method: "unknown"})
	assertProviderCode(t, g.response(mustJSON("barrier")), -32601)
	waitValue(t, w.done)
	assertProviderCode(t, *w.terminal, 0)
	if attempts.Load() != 1 {
		t.Fatal("POST retried")
	}
	if err := r.Close(context.Background()); err != nil {
		t.Fatal("POST expiry closed runtime", err)
	}
}

func TestProviderRejectionQueueIsBounded(t *testing.T) {
	r, g := newProviderGateway(t)
	if err := r.Register(context.Background(), selected.Type, echoFactory); err != nil {
		t.Fatal(err)
	}
	posted := make(chan struct{})
	g.mu.Lock()
	g.replyHook = func(req *http.Request, _ wire.Message) (*http.Response, error) {
		close(posted)
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	g.mu.Unlock()
	b := g.stream(selected.Type)
	b.send(wire.Message{JSONRPC: "2.0", ID: mustJSON("blocked"), Method: "unknown"})
	waitValue(t, posted)
	for i := range 9 {
		b.send(wire.Message{JSONRPC: "2.0", ID: mustJSON(i), Method: "unknown"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var remote *RemoteError
	if err := r.WaitClosed(ctx); !errors.As(err, &remote) || remote.RPCCode != 1001 {
		t.Fatal("unbounded or blocked SSE rejection dispatch", err)
	}
}
