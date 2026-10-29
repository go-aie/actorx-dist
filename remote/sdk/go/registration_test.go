package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

func echoFactory() (any, error) {
	return callActorFunc(func(_ context.Context, v any) (any, error) { return v, nil }), nil
}

func TestRegistrationValidationAndRollback(t *testing.T) {
	t.Run("local-validation", func(t *testing.T) {
		r, g := newProviderGateway(t)
		for _, tc := range []struct {
			actorType string
			factory   ActorFactory
			opts      []RegisterOption
		}{
			{"Invalid", echoFactory, nil},
			{selected.Type, nil, nil},
			{selected.Type, echoFactory, []RegisterOption{nil}},
			{selected.Type, echoFactory, []RegisterOption{WithDescription(strings.Repeat("界", 171))}},
			{selected.Type, echoFactory, []RegisterOption{WithDescription(string([]byte{0xff}))}},
		} {
			if err := r.Register(context.Background(), tc.actorType, tc.factory, tc.opts...); err == nil {
				t.Fatal("accepted invalid registration")
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := r.Register(ctx, selected.Type, echoFactory); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if len(g.requests) != 0 {
			t.Fatal("invalid registration touched network")
		}
	})
	for _, change := range []string{
		"protocol",
		"type",
		"empty-token",
		"comma-token",
		"unicode-token",
		"long-token",
		"capabilities",
		"null-token",
		"wrong-id",
		"null-id",
		"numeric-id",
		"missing",
		"duplicate",
		"oversize",
	} {
		t.Run(change, func(t *testing.T) {
			r, g := newProviderGateway(t)
			g.registerHook = func(_ *http.Request, m wire.Message, b *providerBody) (*http.Response, error) {
				info := map[string]any{"protocol": wire.Version, "type": selected.Type, "session_token": "session-token"}
				id := m.ID
				switch change {
				case "protocol":
					info["protocol"] = "future"
				case "type":
					info["type"] = "demo.other"
				case "empty-token":
					info["session_token"] = ""
				case "comma-token":
					info["session_token"] = "a,b"
				case "unicode-token":
					info["session_token"] = "界"
				case "long-token":
					info["session_token"] = strings.Repeat("x", 257)
				case "capabilities":
					info["capabilities"] = nil
				case "null-token":
					info["session_token"] = nil
				case "wrong-id":
					id = mustJSON("other")
				case "null-id":
					id = mustJSON(nil)
				case "numeric-id":
					id = mustJSON(1)
				case "missing":
					delete(info, "protocol")
				case "duplicate":
					b.chunks <- []byte(`data: {"jsonrpc":"2.0","jsonrpc":"2.0"}` + "\n\n")
					return sseResponse(b), nil
				case "oversize":
					b.chunks <- []byte("data: " + strings.Repeat("x", wire.MaxBytes+1))
					return sseResponse(b), nil
				}
				b.send(wire.Message{JSONRPC: "2.0", ID: id, Result: mustJSON(info)})
				return sseResponse(b), nil
			}
			if err := r.Register(context.Background(), selected.Type, echoFactory); !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
			waitValue(t, g.stream(selected.Type).closed)
			if g.registration(selected.Type) != nil {
				t.Fatal("failed registration retained")
			}
			g.mu.Lock()
			g.registerHook = nil
			g.mu.Unlock()
			if err := r.Register(context.Background(), selected.Type, echoFactory); err != nil {
				t.Fatal("retry failed", err)
			}
		})
	}
}

func TestRegistrationRejectionsAndInstallCancellation(t *testing.T) {
	for _, status := range []int{401, 403, 307} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			r, g := newProviderGateway(t)
			g.registerHook = func(*http.Request, wire.Message, *providerBody) (*http.Response, error) {
				return emptyResponse(status), nil
			}
			err := r.Register(context.Background(), selected.Type, echoFactory)
			var te *TransportError
			if !errors.As(err, &te) || te.Status != status || g.registration(selected.Type) != nil {
				t.Fatal(err)
			}
			g.mu.Lock()
			defer g.mu.Unlock()
			if len(g.requests) != 1 {
				t.Fatal("registration retried")
			}
		})
	}
	for _, sse := range []bool{false, true} {
		t.Run(map[bool]string{true: "sse-error", false: "json-error"}[sse], func(t *testing.T) {
			r, g := newProviderGateway(t)
			g.registerHook = func(_ *http.Request, m wire.Message, b *providerBody) (*http.Response, error) {
				failure := providerFailure(m.ID, 1007, nil)
				if sse {
					b.send(failure)
					return sseResponse(b), nil
				}
				return jsonResponse(m.ID, nil, failure.Error), nil
			}
			var remote *RemoteError
			if err := r.Register(context.Background(), selected.Type, echoFactory); !errors.As(err, &remote) || remote.RPCCode != 1007 {
				t.Fatal(err)
			}
			if g.registration(selected.Type) != nil {
				t.Fatal("registration not rolled back")
			}
		})
	}
	for _, cancellation := range []bool{false, true} {
		t.Run(map[bool]string{true: "cancel", false: "deadline"}[cancellation], func(t *testing.T) {
			r, g := newProviderGateway(t)
			opened := make(chan struct{})
			g.registerHook = func(_ *http.Request, _ wire.Message, b *providerBody) (*http.Response, error) {
				close(opened)
				return sseResponse(b), nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.Register(ctx, selected.Type, echoFactory) }()
			waitValue(t, opened)
			if cancellation {
				cancel()
			}
			err := waitValue(t, done)
			if cancellation && !errors.Is(err, context.Canceled) ||
				!cancellation && (!errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrDeadlineExceeded)) {
				t.Fatal(err)
			}
			waitValue(t, g.stream(selected.Type).closed)
			if g.registration(selected.Type) != nil {
				t.Fatal("install cancellation retained registration")
			}
		})
	}
}

func TestProviderParamsIDsNotificationsAndFragmentation(t *testing.T) {
	r, g := newProviderGateway(t)
	if err := r.Register(context.Background(), selected.Type, echoFactory); err != nil {
		t.Fatal(err)
	}
	g.create(selected)
	b := g.stream(selected.Type)
	for _, params := range []any{
		[]any{},
		map[string]any{"address": selected},
		map[string]any{"address": selected, "input": nil, "deadline_unix_ms": nil},
		map[string]any{"address": selected, "input": nil, "deadline_unix_ms": 1.2},
		map[string]any{"address": selected, "input": nil, "deadline_unix_ms": json.Number("9223372036854775808")},
		map[string]any{"address": selected, "input": nil, "deadline_unix_ms": 1, "extra": true},
		map[string]any{"address": Address{1, selected.Type, selected.ID, ""}, "input": nil, "deadline_unix_ms": 1},
		map[string]any{
			"address":          Address{1, "demo.wrong", selected.ID, selected.Generation},
			"input":            nil,
			"deadline_unix_ms": 1,
		},
	} {
		id := g.nextID()
		b.send(wire.Message{JSONRPC: "2.0", ID: id, Method: "actor/call", Params: mustJSON(params)})
		assertProviderCode(t, g.response(id), -32602)
	}
	b.send(wire.Message{JSONRPC: "2.0", Method: "unknown"}) // notification: no reply
	id := g.send("actor/call", selected, nil, time.Now().Add(5*time.Second))
	if providerValue(t, g.response(id)) != nil {
		t.Fatal("null input changed")
	}
	g.join(id, selected.Type)
	id = g.nextID()
	raw := append(append([]byte("data: "), mustJSON(wire.Message{JSONRPC: "2.0", ID: id, Method: "unknown"})...), '\n', '\n')
	for _, c := range raw {
		b.chunks <- []byte{c}
	}
	assertProviderCode(t, g.response(id), -32601)
	select {
	case m := <-g.responses:
		t.Fatal("notification produced response", m)
	default:
	}
}

func TestProviderRequestSequenceIsScopedToRegistration(t *testing.T) {
	r, g := newProviderGateway(t)
	for _, actorType := range []string{selected.Type, "demo.other"} {
		if err := r.Register(context.Background(), actorType, echoFactory); err != nil {
			t.Fatal(err)
		}
		// Each registration starts independently; skipped IDs are also legal.
		for _, id := range []string{"r:1", "r:9007199254740991"} {
			g.stream(actorType).send(wire.Message{JSONRPC: "2.0", ID: mustJSON(id), Method: "unknown"})
			assertProviderCode(t, g.response(mustJSON(id)), -32601)
		}
	}
	// A lower, previously unseen ID is rejected as well as an exact duplicate.
	g.stream(selected.Type).send(wire.Message{JSONRPC: "2.0", ID: mustJSON("r:2"), Method: "unknown"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.WaitClosed(ctx); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestProviderSessionFailuresCloseWholeRuntime(t *testing.T) {
	for _, mode := range []string{"eof", "unsolicited", "duplicate-id", "session", "transport", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			r, g := newProviderGateway(t)
			for _, actorType := range []string{selected.Type, "demo.other"} {
				if err := r.Register(context.Background(), actorType, echoFactory); err != nil {
					t.Fatal(err)
				}
			}
			g.create(selected)
			want := ErrProtocol
			switch mode {
			case "eof":
				want = ErrTransport
				_ = g.stream(selected.Type).Close()
			case "unsolicited":
				g.stream(selected.Type).send(wire.Message{JSONRPC: "2.0", ID: mustJSON("unknown"), Result: mustJSON(nil)})
			case "duplicate-id":
				g.stream(selected.Type).send(wire.Message{JSONRPC: "2.0", ID: mustJSON("r:1"), Method: "unknown"})
			case "session", "transport":
				want = ErrSession
				g.mu.Lock()
				g.replyHook = func(*http.Request, wire.Message) (*http.Response, error) {
					if mode == "transport" {
						return nil, errors.New("secret transport error")
					}
					return emptyResponse(403), nil
				}
				g.mu.Unlock()
				if mode == "transport" {
					want = ErrTransport
				}
				id := g.send("actor/call", selected, nil, time.Now().Add(5*time.Second))
				g.response(id)
			case "oversize":
				g.stream(selected.Type).chunks <- []byte("data: " + strings.Repeat("x", 8193))
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := r.WaitClosed(ctx); !errors.Is(err, want) || strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
			waitValue(t, g.stream("demo.other").closed)
			if _, err := r.Addr(selected.Type, selected.ID); !errors.Is(err, ErrStopped) {
				t.Fatal(err)
			}
			if err := r.Close(ctx); !errors.Is(err, want) {
				t.Fatal("shutdown lost first cause", err)
			}
		})
	}
}

func TestProviderLateHandoffDoesNotRetry(t *testing.T) {
	r, g := newProviderGateway(t)
	g.replyHook = func(*http.Request, wire.Message) (*http.Response, error) { return emptyResponse(409), nil }
	if err := r.Register(context.Background(), selected.Type, echoFactory); err != nil {
		t.Fatal(err)
	}
	g.create(selected)
	id := g.send("actor/call", selected, "one", time.Now().Add(5*time.Second))
	if providerValue(t, g.response(id)) != "one" {
		t.Fatal("missing response")
	}
	g.join(id, selected.Type)
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.requests) != 3 {
		t.Fatal("late handoff retried", len(g.requests))
	}
}

func TestCloseDuringRegistrationInstallation(t *testing.T) {
	r, g := newProviderGateway(t)
	opened := make(chan struct{})
	g.registerHook = func(_ *http.Request, _ wire.Message, b *providerBody) (*http.Response, error) {
		close(opened)
		return sseResponse(b), nil // installation remains blocked on its first frame
	}
	registered := make(chan error, 1)
	go func() { registered <- r.Register(context.Background(), selected.Type, echoFactory) }()
	waitValue(t, opened)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := waitValue(t, registered); err == nil {
		t.Fatal("installation succeeded after close")
	}
	waitValue(t, g.stream(selected.Type).closed)
	if len(r.registrations) != 0 {
		t.Fatal("shutdown retained installing registration")
	}
}
