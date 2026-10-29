package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

// No listener or service packages: transport and byte-fragmented bodies drive
// registration, execution, POST handoff and cancellation independently.
type providerBody struct {
	chunks  chan []byte
	closed  chan struct{}
	once    sync.Once
	pending []byte
}

func newProviderBody() *providerBody {
	return &providerBody{chunks: make(chan []byte, 128), closed: make(chan struct{})}
}

func (b *providerBody) Read(out []byte) (int, error) {
	if len(b.pending) == 0 {
		select {
		case <-b.closed:
			return 0, io.EOF
		case b.pending = <-b.chunks:
		}
	}
	n := copy(out, b.pending)
	b.pending = b.pending[n:]
	return n, nil
}

func (b *providerBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func (b *providerBody) send(m any) {
	raw := append(append([]byte("data: "), mustJSON(m)...), '\n', '\n')
	b.chunks <- raw
}

type providerGateway struct {
	t            *testing.T
	runtime      *Runtime
	mu           sync.Mutex
	streams      map[string]*providerBody
	requests     []wire.Message
	registerHook func(*http.Request, wire.Message, *providerBody) (*http.Response, error)
	replyHook    func(*http.Request, wire.Message) (*http.Response, error)
	responses    chan wire.Message
	backlog      map[string]wire.Message
	serial       int
}

func newProviderGateway(t *testing.T) (*Runtime, *providerGateway) {
	t.Helper()
	r, err := NewRuntime("http://gateway.test", Options{APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	g := &providerGateway{t: t, runtime: r, streams: make(map[string]*providerBody),
		responses: make(chan wire.Message, 128), backlog: make(map[string]wire.Message)}
	r.http.Transport = roundTripFunc(g.roundTrip)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.Close(ctx) // failures are asserted by the individual tests
		select {
		case <-r.closed:
		default:
			t.Error("provider cleanup did not finish")
		}
	})
	return r, g
}

func (g *providerGateway) roundTrip(req *http.Request) (*http.Response, error) {
	m := rpcRequest(g.t, req)
	if req.Method != "POST" || req.URL.Path != "/runtime/rpc" ||
		req.Header.Get("Authorization") != "Bearer test-key" ||
		req.Header.Get("X-Actorx-Remote-Protocol") != wire.Version ||
		req.Header.Get("Content-Type") != "application/json" || req.GetBody != nil {
		g.t.Error("invalid provider HTTP request")
	}
	g.mu.Lock()
	g.requests = append(g.requests, m)
	registerHook, replyHook := g.registerHook, g.replyHook
	g.mu.Unlock()
	if m.Method == "runtime/register" {
		if req.Header.Get("Accept") != "text/event-stream, application/json" || req.Header.Get("X-Actorx-Remote-Session") != "" {
			g.t.Error("invalid registration headers")
		}
		if _, ok := req.Context().Deadline(); ok {
			g.t.Error("SSE inherited installation deadline")
		}
		var params struct {
			Type        string
			Description string
		}
		_ = json.Unmarshal(m.Params, &params)
		body := newProviderBody()
		g.mu.Lock()
		g.streams[params.Type] = body
		g.mu.Unlock()
		if registerHook != nil {
			return registerHook(req, m, body)
		}
		body.send(wire.Message{JSONRPC: "2.0", ID: m.ID, Result: mustJSON(map[string]any{
			"protocol": wire.Version, "type": params.Type, "session_token": "session-" + params.Type,
		})})
		return sseResponse(body), nil
	}
	if (m.Request && (m.Method != "stream/chunk" || m.ID != nil)) ||
		!strings.HasPrefix(req.Header.Get("X-Actorx-Remote-Session"), "session-") ||
		req.Header.Get("Accept") != "application/json" {
		g.t.Error("invalid provider response or session header")
	}
	g.responses <- m
	if replyHook != nil {
		return replyHook(req, m)
	}
	return emptyResponse(204), nil
}

func sseResponse(body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: body}
}

func emptyResponse(status int) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}
}

func (g *providerGateway) stream(actorType string) *providerBody {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.streams[actorType]
}

func (g *providerGateway) send(method string, a Address, input any, deadline time.Time) json.RawMessage {
	g.serial++
	id := mustJSON(g.serial) // numeric IDs must remain numeric in replies
	params := map[string]any{"address": a, "deadline_unix_ms": deadline.UnixMilli()}
	if method == "actor/call" {
		params["input"] = input
	}
	g.stream(a.Type).send(wire.Message{JSONRPC: "2.0", ID: id, Method: method, Params: mustJSON(params)})
	return id
}

func (g *providerGateway) response(id json.RawMessage) wire.Message {
	g.t.Helper()
	if m, ok := g.backlog[string(id)]; ok {
		delete(g.backlog, string(id))
		return m
	}
	for {
		m := waitValue(g.t, g.responses)
		if string(m.ID) == string(id) {
			return m
		}
		g.backlog[string(m.ID)] = m
	}
}

func (g *providerGateway) create(a Address) {
	g.t.Helper()
	id := g.send("actor/create", a, nil, time.Now().Add(5*time.Second))
	assertProviderCode(g.t, g.response(id), 0)
	g.join(id, a.Type)
}

func (g *providerGateway) registration(actorType string) *registration {
	g.runtime.mu.Lock()
	defer g.runtime.mu.Unlock()
	return g.runtime.registrations[actorType]
}

func (g *providerGateway) work(id json.RawMessage, actorType string) *providerWork {
	p := g.registration(actorType)
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for w := range p.active {
		if string(w.request.ID) == string(id) {
			return w
		}
	}
	return nil
}

func (g *providerGateway) join(id json.RawMessage, actorType string) {
	if w := g.work(id, actorType); w != nil {
		waitValue(g.t, w.done)
	}
}

func assertProviderCode(t *testing.T, m wire.Message, code int) {
	t.Helper()
	if code == 0 {
		if m.Error != nil || m.Result == nil {
			t.Fatalf("expected success: %#v", m)
		}
		return
	}
	if m.Error == nil || m.Error.Code != code {
		t.Fatalf("expected %d: %#v", code, m)
	}
	if wire.ValidateError(m.Error) != nil || strings.Contains(string(mustJSON(m)), "secret") {
		t.Fatal("unsafe provider error", m)
	}
}

func providerValue(t *testing.T, m wire.Message) any {
	t.Helper()
	assertProviderCode(t, m, 0)
	var f map[string]json.RawMessage
	_ = json.Unmarshal(m.Result, &f)
	v, err := wire.Decode(f["value"])
	if err != nil {
		t.Fatal(err)
	}
	return v
}

type callActorFunc func(context.Context, any) (any, error)

func (f callActorFunc) Handle(ctx context.Context, message any) (any, error) { return f(ctx, message) }

type streamOnlyActor struct{}

func (*streamOnlyActor) HandleStream(context.Context, any, StreamWriter) error { return nil }

func TestProviderLazyFactoryReuseAndCompleteIdentity(t *testing.T) {
	r, g := newProviderGateway(t)
	var made atomic.Int32
	factory := func() (any, error) {
		made.Add(1)
		count := 0
		return callActorFunc(func(_ context.Context, input any) (any, error) {
			count++
			return struct {
				Count  int    `json:"count"`
				Input  any    `json:"input"`
				Hidden string `json:"-"`
			}{count, input, "secret"}, nil
		}), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := r.Register(ctx, selected.Type, factory, WithDescription("counter")); err != nil {
		t.Fatal(err)
	}
	cancel() // successful installation no longer belongs to caller context
	if made.Load() != 0 {
		t.Fatal("registration constructed an instance")
	}

	// Duplicate create must reuse the first object rather than call the factory.
	g.create(selected)
	g.create(selected)
	if made.Load() != 1 {
		t.Fatal("duplicate create constructed twice")
	}
	for n := 1; n <= 2; n++ {
		id := g.send("actor/call", selected, customJSON{}, time.Now().Add(5*time.Second))
		got := providerValue(t, g.response(id)).(map[string]any)
		if got["count"] != json.Number(strconv.Itoa(n)) || got["input"].(map[string]any)["n"] != json.Number("900719925474099312345") {
			t.Fatal(got)
		}
		g.join(id, selected.Type)
	}

	// Each component of the complete address contributes to instance identity.
	for _, a := range []Address{
		{1, selected.Type, "other", selected.Generation},
		{1, selected.Type, selected.ID, "AQEBAQEBAQEBAQEBAQEBAQ"},
		{1, "demo.other", selected.ID, selected.Generation},
	} {
		if a.Type != selected.Type {
			if err := r.Register(context.Background(), a.Type, factory); err != nil {
				t.Fatal(err)
			}
		}
		g.create(a)
		id := g.send("actor/call", a, nil, time.Now().Add(5*time.Second))
		if providerValue(t, g.response(id)).(map[string]any)["count"] != json.Number("1") {
			t.Fatal("instance identity shared")
		}
		g.join(id, a.Type)
	}
	if made.Load() != 4 {
		t.Fatal(made.Load())
	}
	if err := r.Register(context.Background(), selected.Type, factory); err == nil {
		t.Fatal("duplicate registration")
	}

	p := g.registration(selected.Type)
	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.instances) != 0 || p.factory != nil || len(r.registrations) != 0 {
		t.Fatal("cleanup retained objects or factory")
	}
}

func TestProviderFactoryAndHandlerFailuresAreControlled(t *testing.T) {
	for name, factory := range map[string]ActorFactory{
		"factory-error": func() (any, error) { return nil, errors.New("secret factory error") },
		"factory-panic": func() (any, error) { panic("secret factory panic") },
		"nil":           func() (any, error) { return nil, nil },
		"typed-nil":     func() (any, error) { return (*streamOnlyActor)(nil), nil },
		"no-interface":  func() (any, error) { return &struct{}{}, nil },
	} {
		t.Run(name, func(t *testing.T) {
			r, g := newProviderGateway(t)
			if err := r.Register(context.Background(), selected.Type, factory); err != nil {
				t.Fatal(err)
			}
			id := g.send("actor/create", selected, nil, time.Now().Add(5*time.Second))
			assertProviderCode(t, g.response(id), 1000)
		})
	}
	t.Run("handler-and-json", func(t *testing.T) {
		r, g := newProviderGateway(t)
		if err := r.Register(context.Background(), selected.Type, func() (any, error) {
			return callActorFunc(func(_ context.Context, input any) (any, error) {
				switch input {
				case "error":
					return nil, errors.New("secret handler error")
				case "panic":
					panic("secret handler panic")
				case "nan":
					return math.NaN(), nil
				case "marshaler":
					return panicJSON{}, nil
				case "duplicate":
					return json.RawMessage(`{"x":1,"x":2}`), nil
				case "large":
					return strings.Repeat("x", 8100), nil
				case "deep":
					return json.RawMessage(strings.Repeat("[", 63) + "0" + strings.Repeat("]", 63)), nil
				default:
					return input, nil
				}
			}), nil
		}); err != nil {
			t.Fatal(err)
		}
		g.create(selected)
		for _, v := range []string{"error", "panic", "nan", "marshaler", "duplicate", "large", "deep"} {
			id := g.send("actor/call", selected, v, time.Now().Add(5*time.Second))
			m := g.response(id)
			assertProviderCode(t, m, 1000)
			var data struct{ Address Address }
			_ = json.Unmarshal(m.Error.Data, &data)
			if data.Address != selected {
				t.Fatal("missing failure address")
			}
			g.join(id, selected.Type)
		}
		id := g.send("actor/call", selected, "survived", time.Now().Add(5*time.Second))
		if providerValue(t, g.response(id)) != "survived" {
			t.Fatal("error poisoned instance")
		}
	})
	t.Run("stream-only-call", func(t *testing.T) {
		r, g := newProviderGateway(t)
		if err := r.Register(context.Background(), selected.Type, func() (any, error) { return &streamOnlyActor{}, nil }); err != nil {
			t.Fatal(err)
		}
		g.create(selected)
		id := g.send("actor/call", selected, nil, time.Now().Add(5*time.Second))
		m := g.response(id)
		assertProviderCode(t, m, 1013)
		var data struct{ Address Address }
		_ = json.Unmarshal(m.Error.Data, &data)
		if data.Address != selected {
			t.Fatal("1013 lost complete address")
		}
	})
}
