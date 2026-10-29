package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

type streamActorFunc func(context.Context, any, StreamWriter) error

func (f streamActorFunc) HandleStream(ctx context.Context, input any, w StreamWriter) error {
	return f(ctx, input, w)
}

type dualActor struct{ turns int }

func (a *dualActor) Handle(context.Context, any) (any, error) {
	a.turns++
	return a.turns, nil
}

func (a *dualActor) HandleStream(_ context.Context, _ any, w StreamWriter) error {
	a.turns++
	return w.Write(struct {
		Turn int `json:"turn"`
	}{a.turns})
}

func (g *providerGateway) sendStream(a Address, input any, deadline time.Time) json.RawMessage {
	id := g.nextID()
	g.stream(a.Type).send(wire.Message{
		JSONRPC: "2.0",
		ID:      id,
		Method:  "actor/stream",
		Params: mustJSON(map[string]any{
			"address":          a,
			"input":            input,
			"deadline_unix_ms": deadline.UnixMilli(),
			"stream_token":     testToken,
			"window":           wire.StreamWindow(),
		}),
	})
	return id
}

func (g *providerGateway) credit(id json.RawMessage, seq uint64) {
	g.stream(selected.Type).send(streamNotification("stream/ack", map[string]any{
		"request_id":   id,
		"stream_token": testToken,
		"ack_seq":      seq,
	}))
}

func (g *providerGateway) cancelStream(id json.RawMessage) {
	g.stream(selected.Type).send(streamNotification("stream/cancel", map[string]any{"request_id": id, "stream_token": testToken}))
}

func chunkFields(t *testing.T, m wire.Message) map[string]json.RawMessage {
	t.Helper()
	if m.Method != "stream/chunk" || m.ID != nil {
		t.Fatal("expected chunk", m)
	}
	names := []string{"request_id", "stream_token", "address", "seq", "value"}
	f, err := wire.Fields(m.Params, names, names...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func finalSequence(t *testing.T, m wire.Message) uint64 {
	t.Helper()
	raw := m.Result
	if m.Error != nil {
		raw = m.Error.Data
	}
	var f map[string]json.RawMessage
	_ = json.Unmarshal(raw, &f)
	seq, err := wire.Sequence(f["final_seq"])
	if err != nil {
		t.Fatal(err, m)
	}
	if string(f["address"]) != string(mustJSON(selected)) {
		t.Fatal("terminal address", m)
	}
	return seq
}

func registerStream(t *testing.T, r *Runtime, g *providerGateway, actor any) {
	t.Helper()
	if err := r.Register(context.Background(), selected.Type, func() (any, error) { return actor, nil }); err != nil {
		t.Fatal(err)
	}
	g.create(selected)
}

func TestProviderStreamSharesCallStateAndRetainsCredits(t *testing.T) {
	r, g := newProviderGateway(t)
	registerStream(t, r, g, &dualActor{})
	call := g.send("actor/call", selected, nil, time.Now().Add(time.Second))
	if providerValue(t, g.response(call)) != json.Number("1") {
		t.Fatal("call state")
	}
	g.join(call, selected.Type)

	id := g.sendStream(selected, nil, time.Now().Add(time.Second))
	m := waitValue(t, g.responses)
	f := chunkFields(t, m)
	if !wire.SameID(f["request_id"], id) || string(f["value"]) != `{"turn":2}` {
		t.Fatal(m)
	}
	m = g.response(id)
	assertProviderCode(t, m, 0)
	if finalSequence(t, m) != 1 {
		t.Fatal(m)
	}
	g.join(id, selected.Type)

	// A terminal POST does not release unacknowledged chunks.
	p := g.registration(selected.Type)
	key, _ := wire.IDKey(id)
	p.mu.Lock()
	ledger := p.streams[key]
	p.mu.Unlock()
	if ledger == nil {
		t.Fatal("terminal POST discarded credit ledger")
	}

	g.cancelStream(id) // late cancellation cannot rewrite success
	g.credit(id, 1)
	waitValue(t, ledger.removed)
	if err := ledger.Write("after-return"); err == nil {
		t.Fatal("writer survived return")
	}
	call = g.send("actor/call", selected, nil, time.Now().Add(time.Second))
	if providerValue(t, g.response(call)) != json.Number("3") {
		t.Fatal("stream poisoned shared object")
	}
}

func TestProviderStreamWindowUsesCompleteEnvelope(t *testing.T) {
	r, g := newProviderGateway(t)
	entered := make(chan int, 8)
	registerStream(t, r, g, streamActorFunc(func(ctx context.Context, _ any, w StreamWriter) error {
		for i := 1; i <= 6; i++ {
			entered <- i
			if err := w.Write(strings.Repeat("界", 2400)); err != nil {
				return err
			}
		}
		return nil
	}))
	id := g.sendStream(selected, customJSON{}, time.Now().Add(2*time.Second))
	total := 0
	for i := 1; i <= 4; i++ {
		if waitValue(t, entered) != i {
			t.Fatal("write order")
		}
		m := waitValue(t, g.responses)
		f := chunkFields(t, m)
		seq, _ := wire.Sequence(f["seq"])
		if seq != uint64(i) {
			t.Fatal(seq)
		}
		raw, err := wire.Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		total += len(raw)
	}
	if total > wire.WindowBytes || total <= 4*7200 {
		t.Fatal("not full envelope byte accounting", total)
	}
	if waitValue(t, entered) != 5 {
		t.Fatal("fifth write not attempted")
	}

	// A create barrier proves the SSE reader advances while Write is blocked.
	g.createOtherBarrier()
	select {
	case m := <-g.responses:
		t.Fatal("fifth chunk escaped window", m)
	default:
	}

	g.credit(id, 2)
	for i := 5; i <= 6; i++ {
		f := chunkFields(t, waitValue(t, g.responses))
		seq, _ := wire.Sequence(f["seq"])
		if seq != uint64(i) {
			t.Fatal(seq)
		}
	}
	assertProviderCode(t, g.response(id), 0)
	g.join(id, selected.Type)
	g.credit(id, 6)
}

func TestProviderStreamErrorsAndWriterLifetime(t *testing.T) {
	for _, mode := range []string{
		"empty",
		"error",
		"panic",
		"nan",
		"duplicate",
		"large",
		"deep",
		"marshal-panic",
		"partial",
		"ignore-write-error",
		"late-write",
	} {
		t.Run(mode, func(t *testing.T) {
			r, g := newProviderGateway(t)
			var saved StreamWriter
			registerStream(t, r, g, streamActorFunc(func(_ context.Context, input any, w StreamWriter) error {
				saved = w
				switch mode {
				case "empty", "late-write":
					return nil
				case "error":
					return errors.New("secret handler")
				case "panic":
					panic("secret panic")
				case "partial":
					if err := w.Write(customJSON{}); err != nil {
						return err
					}
					return errors.New("secret partial")
				case "nan":
					return w.Write(math.NaN())
				case "duplicate":
					return w.Write(json.RawMessage(`{"x":1,"x":2}`))
				case "large":
					return w.Write(strings.Repeat("x", 8100))
				case "deep":
					return w.Write(json.RawMessage(strings.Repeat("[", 63) + "0" + strings.Repeat("]", 63)))
				case "marshal-panic":
					return w.Write(panicJSON{})
				case "ignore-write-error":
					_ = w.Write(math.NaN())
					return nil
				}
				return nil
			}))
			id := g.sendStream(selected, customJSON{}, time.Now().Add(time.Second))
			if mode == "partial" {
				f := chunkFields(t, waitValue(t, g.responses))
				v, _ := wire.Decode(f["value"])
				if v.(map[string]any)["n"] != json.Number("900719925474099312345") {
					t.Fatal(v)
				}
			}
			m := g.response(id)
			code := 1000
			if mode == "empty" || mode == "late-write" {
				code = 0
			}
			assertProviderCode(t, m, code)
			seq := uint64(0)
			if mode == "partial" {
				seq = 1
			}
			if finalSequence(t, m) != seq {
				t.Fatal(m)
			}
			g.join(id, selected.Type)
			if err := saved.Write("late"); err == nil {
				t.Fatal("write after handler return")
			}
			select {
			case m := <-g.responses:
				t.Fatal("late output", m)
			default:
			}
		})
	}
}

func TestProviderStreamUnsupportedHasZeroSequence(t *testing.T) {
	r, g := newProviderGateway(t)
	registerStream(t, r, g, callActorFunc(func(context.Context, any) (any, error) { return "call", nil }))
	id := g.sendStream(selected, nil, time.Now().Add(time.Second))
	m := g.response(id)
	assertProviderCode(t, m, 1013)
	if finalSequence(t, m) != 0 {
		t.Fatal(m)
	}
	g.join(id, selected.Type)
	call := g.send("actor/call", selected, nil, time.Now().Add(time.Second))
	if providerValue(t, g.response(call)) != "call" {
		t.Fatal("fallback")
	}
}

func TestProviderStreamCancelRetainsLockAndSlot(t *testing.T) {
	r, g := newProviderGateway(t)
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	defer close(release)
	registerStream(t, r, g, streamActorFunc(func(ctx context.Context, _ any, w StreamWriter) error {
		entered <- ctx
		<-release
		_ = w.Write("ignored-after-cancel")
		return nil
	}))
	id := g.sendStream(selected, nil, time.Now().Add(2*time.Second))
	ctx := waitValue(t, entered)
	w := g.work(id, selected.Type)
	if w == nil {
		t.Fatal("missing slot")
	}
	g.cancelStream(id)
	waitValue(t, ctx.Done())
	m := g.response(id)
	assertProviderCode(t, m, 1004)
	if finalSequence(t, m) != 0 {
		t.Fatal(m)
	}
	select {
	case <-w.done:
		t.Fatal("handler released slot early")
	default:
	}

	queued := g.sendStream(selected, nil, time.Now().Add(30*time.Millisecond))
	qw := func() *providerWork { // barrier ensures queued work has been admitted
		g.createOtherBarrier()
		return g.work(queued, selected.Type)
	}()
	if qw != nil {
		waitValue(t, qw.settled)
		qw.mu.Lock()
		terminal := *qw.terminal
		qw.mu.Unlock()
		assertProviderCode(t, terminal, 1005)
		if finalSequence(t, terminal) != 0 {
			t.Fatal(terminal)
		}
	}
	select {
	case <-entered:
		t.Fatal("queued expired handler ran")
	default:
	}

	cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := r.Close(cleanup); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-r.closed:
		t.Fatal("close abandoned user handler")
	default:
	}
}

// An independent create must not wait for the blocked instance or response POST.
func (g *providerGateway) createOtherBarrier() {
	a := selected
	a.ID = "barrier"
	g.create(a)
}

func TestProviderStreamCreditValidationAndExpiry(t *testing.T) {
	for _, mode := range []string{"future", "token", "request-id", "late-valid", "expiry"} {
		t.Run(mode, func(t *testing.T) {
			r, g := newProviderGateway(t)
			registerStream(t, r, g, streamActorFunc(func(_ context.Context, _ any, w StreamWriter) error { return w.Write(nil) }))
			deadline := time.Now().Add(time.Second)
			if mode == "expiry" {
				deadline = time.Now().Add(70 * time.Millisecond)
			}
			id := g.sendStream(selected, nil, deadline)
			chunkFields(t, waitValue(t, g.responses))
			assertProviderCode(t, g.response(id), 0)
			g.join(id, selected.Type)
			p := g.registration(selected.Type)
			key, _ := wire.IDKey(id)
			p.mu.Lock()
			ledger := p.streams[key]
			p.mu.Unlock()
			if ledger == nil {
				t.Fatal("missing ledger")
			}
			if mode == "expiry" {
				waitValue(t, ledger.removed)
				return
			}
			params := map[string]any{"request_id": id, "stream_token": testToken, "ack_seq": 1}
			switch mode {
			case "future":
				params["ack_seq"] = 2
			case "token":
				params["stream_token"] = otherToken
			case "request-id":
				params["request_id"] = string(id)
			}
			g.stream(selected.Type).send(streamNotification("stream/ack", params))
			if mode == "future" || mode == "token" {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := r.WaitClosed(ctx); !errors.Is(err, ErrProtocol) {
					t.Fatal(err)
				}
			} else {
				if mode == "late-valid" {
					waitValue(t, ledger.removed)
					g.credit(id, 1)
				} else {
					g.credit(id, 1)
					waitValue(t, ledger.removed)
				}
				g.createOtherBarrier()
			}
		})
	}
}

func TestProviderStreamHandoffBeforeNextChunkAndTerminal(t *testing.T) {
	r, g := newProviderGateway(t)
	held := make(chan struct{})
	release := make(chan struct{})
	var posts atomic.Int32
	g.replyHook = func(req *http.Request, m wire.Message) (*http.Response, error) {
		if m.Method == "stream/chunk" && posts.Add(1) == 1 {
			close(held)
			select {
			case <-release:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return emptyResponse(204), nil
	}
	registerStream(t, r, g, streamActorFunc(func(_ context.Context, _ any, w StreamWriter) error {
		for i := 0; i < 2; i++ {
			if err := w.Write(i); err != nil {
				return err
			}
		}
		return nil
	}))
	id := g.sendStream(selected, nil, time.Now().Add(time.Second))
	waitValue(t, held)
	chunkFields(t, waitValue(t, g.responses))
	g.credit(id, 1)
	g.createOtherBarrier()
	select {
	case m := <-g.responses:
		t.Fatal("POST handoff bypassed", m)
	default:
	}

	close(release)
	chunkFields(t, waitValue(t, g.responses))
	assertProviderCode(t, g.response(id), 0)
}

type actorMethods struct {
	call   callActorFunc
	stream streamActorFunc
}

func (a *actorMethods) Handle(ctx context.Context, input any) (any, error) { return a.call(ctx, input) }

func (a *actorMethods) HandleStream(ctx context.Context, input any, w StreamWriter) error {
	return a.stream(ctx, input, w)
}

func TestProviderStreamAndCallShareExecutionOwnership(t *testing.T) {
	for _, mode := range []string{"call-first", "stream-first", "stream-timeout"} {
		t.Run(mode, func(t *testing.T) {
			r, g := newProviderGateway(t)
			entered := make(chan string, 4)
			release := make(chan struct{})
			released := false
			defer func() {
				if !released {
					close(release)
				}
			}()
			actor := &actorMethods{
				call: func(_ context.Context, input any) (any, error) {
					entered <- "call"
					if mode == "call-first" {
						<-release
					}
					return "ok", nil
				},
				stream: func(_ context.Context, input any, w StreamWriter) error {
					entered <- "stream"
					if mode != "call-first" {
						<-release
					}
					return w.Write(nil)
				},
			}
			registerStream(t, r, g, actor)
			deadline := time.Now().Add(time.Second)
			if mode == "stream-timeout" {
				deadline = time.Now().Add(35 * time.Millisecond)
			}
			var first, second json.RawMessage
			if mode == "call-first" {
				first = g.send("actor/call", selected, nil, deadline)
			} else {
				first = g.sendStream(selected, nil, deadline)
			}
			want := "stream"
			if mode == "call-first" {
				want = "call"
			}
			if waitValue(t, entered) != want {
				t.Fatal("first operation")
			}
			w := g.work(first, selected.Type)
			if mode == "stream-timeout" {
				waitValue(t, w.settled)
				w.mu.Lock()
				terminal := *w.terminal
				w.mu.Unlock()
				assertProviderCode(t, terminal, 1004)
			}
			if mode == "call-first" {
				second = g.sendStream(selected, customJSON{}, time.Now().Add(time.Second))
			} else {
				second = g.send("actor/call", selected, nil, time.Now().Add(time.Second))
			}
			g.createOtherBarrier()
			select {
			case name := <-entered:
				t.Fatal("overlapping methods", name)
			default:
			}
			select {
			case <-w.done:
				t.Fatal("ownership released early")
			default:
			}
			close(release)
			released = true
			want = "call"
			if mode == "call-first" {
				want = "stream"
			}
			if waitValue(t, entered) != want {
				t.Fatal("second operation")
			}
			if mode == "stream-timeout" {
				assertProviderCode(t, g.response(second), 0)
				g.join(first, selected.Type)
				return
			}
			// The response helper can collect a chunk while awaiting the other ID.
			assertProviderCode(t, g.response(first), 0)
			assertProviderCode(t, g.response(second), 0)
		})
	}
}

func TestProviderStreamLateHandoff409AndOriginalBudget(t *testing.T) {
	for _, mode := range []string{"409", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			r, g := newProviderGateway(t)
			var attempts atomic.Int32
			requestContext := make(chan context.Context, 1)
			g.replyHook = func(req *http.Request, m wire.Message) (*http.Response, error) {
				if m.Method == "stream/chunk" {
					attempts.Add(1)
					requestContext <- req.Context()
					if mode == "409" {
						return emptyResponse(409), nil
					}
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				return emptyResponse(204), nil
			}
			registerStream(t, r, g, streamActorFunc(func(_ context.Context, _ any, w StreamWriter) error {
				_ = w.Write("one")
				_ = w.Write("two")
				return nil
			}))
			deadline := time.Now().Add(60 * time.Millisecond)
			id := g.sendStream(selected, nil, deadline)
			chunkFields(t, waitValue(t, g.responses))
			ctx := waitValue(t, requestContext)
			d, _ := ctx.Deadline()
			if d.Sub(deadline) > time.Millisecond || deadline.Sub(d) > time.Millisecond {
				t.Fatal("renewed budget", d, deadline)
			}
			if mode == "409" {
				assertProviderCode(t, g.response(id), 1004)
			}
			g.join(id, selected.Type)
			if attempts.Load() != 1 {
				t.Fatal("retried Write", attempts.Load())
			}
			if err := r.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProviderStreamFinishedLedgersAreBounded(t *testing.T) {
	r, g := newProviderGateway(t)
	registerStream(t, r, g, &dualActor{})
	ids := make([]json.RawMessage, providerCapacity)
	for i := range ids {
		ids[i] = g.sendStream(selected, nil, time.Now().Add(2*time.Second))
		chunkFields(t, waitValue(t, g.responses))
		assertProviderCode(t, g.response(ids[i]), 0)
		g.join(ids[i], selected.Type)
	}
	p := g.registration(selected.Type)
	p.mu.Lock()
	count := len(p.streams)
	active := len(p.active)
	first, _ := wire.IDKey(ids[0])
	ledger := p.streams[first]
	p.mu.Unlock()
	if count != providerCapacity || active != 0 {
		t.Fatal(count, active)
	}
	rejected := g.sendStream(selected, nil, time.Now().Add(time.Second))
	m := g.response(rejected)
	assertProviderCode(t, m, 1001)
	if finalSequence(t, m) != 0 {
		t.Fatal(m)
	}

	// Retained stream credits do not reserve the instance's Call lock or slot.
	call := g.send("actor/call", selected, nil, time.Now().Add(time.Second))
	assertProviderCode(t, g.response(call), 0)
	g.join(call, selected.Type)
	g.credit(ids[0], 1)
	waitValue(t, ledger.removed)
	accepted := g.sendStream(selected, nil, time.Now().Add(time.Second))
	chunkFields(t, waitValue(t, g.responses))
	assertProviderCode(t, g.response(accepted), 0)
}

func TestProviderStreamCancelWakesWindowWait(t *testing.T) {
	r, g := newProviderGateway(t)
	attempted := make(chan int, 8)
	returned := make(chan error, 1)
	registerStream(t, r, g, streamActorFunc(func(_ context.Context, _ any, w StreamWriter) error {
		for i := 1; i <= 5; i++ {
			attempted <- i
			if err := w.Write(i); err != nil {
				returned <- err
				return nil
			} // ignored Write failure
		}
		returned <- nil
		return nil
	}))
	id := g.sendStream(selected, nil, time.Now().Add(time.Second))
	for i := 1; i <= 4; i++ {
		if waitValue(t, attempted) != i {
			t.Fatal("order")
		}
		chunkFields(t, waitValue(t, g.responses))
	}
	if waitValue(t, attempted) != 5 {
		t.Fatal("window wait")
	}
	g.createOtherBarrier()
	g.cancelStream(id)
	if err := waitValue(t, returned); err == nil {
		t.Fatal("cancellation did not fail blocked Write")
	}
	m := g.response(id)
	assertProviderCode(t, m, 1004)
	if finalSequence(t, m) != 4 {
		t.Fatal(m)
	}
	g.join(id, selected.Type)
}
