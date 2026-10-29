package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

const testToken = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
const otherToken = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"

type consumerGateway struct {
	t        *testing.T
	body     *providerBody
	request  wire.Message
	controls chan wire.Message
	hook     func(*http.Request, wire.Message) (*http.Response, error)
}

func newConsumerGateway(t *testing.T) (*Runtime, *Addr, *consumerGateway) {
	g := &consumerGateway{t: t, body: newProviderBody(), controls: make(chan wire.Message, 64)}
	r, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		m := rpcRequest(t, req)
		if req.Header.Get("X-Actorx-Remote-Session") != "" {
			t.Error("consumer sent session")
		}
		if m.Method == "actor/stream" {
			if req.Header.Get("Accept") != "text/event-stream, application/json" {
				t.Error("missing SSE Accept")
			}
			g.request = m
			g.body.send(streamNotification("stream/opened", map[string]any{"request_id": m.ID, "stream_token": testToken, "window": wire.StreamWindow()}))
			return sseResponse(g.body), nil
		}
		if m.Method == "actor/call" {
			return success(m.ID, selected, "call"), nil
		}
		g.controls <- m
		if g.hook != nil {
			return g.hook(req, m)
		}
		return emptyResponse(204), nil
	})
	return r, a, g
}

func streamNotification(method string, params any) wire.Message {
	return wire.Message{JSONRPC: "2.0", Method: method, Params: mustJSON(params)}
}

func (g *consumerGateway) chunk(seq uint64, value any) {
	g.body.send(streamNotification("stream/chunk", map[string]any{
		"request_id":   g.request.ID,
		"stream_token": testToken,
		"address":      selected,
		"seq":          seq,
		"value":        value,
	}))
}

func (g *consumerGateway) final(seq uint64) {
	g.body.send(wire.Message{
		JSONRPC: "2.0",
		ID:      g.request.ID,
		Result:  mustJSON(map[string]any{"address": selected, "final_seq": seq}),
	})
}

func (g *consumerGateway) failure(code int, seq uint64) {
	g.body.send(streamFailure(providerFailure(g.request.ID, code, &selected), seq))
}

func nextAsync(s *Stream) <-chan bool {
	ch := make(chan bool, 1)
	go func() { ch <- s.Next() }()
	return ch
}

func waitTerminal(t *testing.T, s *Stream) {
	t.Helper()
	waitValue(t, s.readerDone)
}

func TestConsumerStreamEmptyAndCreditAfterProcessing(t *testing.T) {
	for _, n := range []int{0, 6} {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			_, a, g := newConsumerGateway(t)
			s, err := a.Stream(context.Background(), taggedRequest{Text: "hello", Ignored: "secret"})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if a.Address().Generation != "" {
				t.Fatal("open selected generation")
			}
			var request map[string]json.RawMessage
			_ = json.Unmarshal(g.request.Params, &request)
			if string(request["input"]) != `{"text":"hello"}` {
				t.Fatal(string(request["input"]))
			}
			if n == 0 {
				g.final(0)
				if s.Next() || s.Err() != nil || a.Address() != selected {
					t.Fatal(s.Err(), a.Address())
				}
				return
			}

			// Filling the initial window must not acknowledge an unprocessed item.
			for i := 1; i <= 4; i++ {
				g.chunk(uint64(i), customJSON{})
			}
			if !s.Next() || s.Value().(map[string]any)["n"] != json.Number("900719925474099312345") {
				t.Fatal(s.Err())
			}
			select {
			case <-g.controls:
				t.Fatal("ack before processing")
			default:
			}

			// Each next iteration releases one item and permits another chunk.
			for i := 2; i <= 6; i++ {
				ch := nextAsync(s)
				m := waitValue(t, g.controls)
				_, token, seq, err := wire.Control(m)
				if err != nil || token != testToken || seq != uint64(i-1) {
					t.Fatal(m, err)
				}
				if i <= 3 {
					g.chunk(uint64(i+3), nil)
				}
				if !waitValue(t, ch) {
					t.Fatal(s.Err())
				}
			}

			g.final(6)

			waitTerminal(t, s)
			if s.Next() || s.Err() != nil {
				t.Fatal(s.Err())
			}
			_, _, seq, err := wire.Control(waitValue(t, g.controls))
			if err != nil || seq != 6 {
				t.Fatal(seq, err)
			}
			select {
			case <-g.controls:
				t.Fatal("cancel after success")
			default:
			}
		})
	}
}

func TestConsumerStreamPartialFailureAndFinalCreditFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{true: "remote-error", false: "credit-error"}[fail], func(t *testing.T) {
			_, a, g := newConsumerGateway(t)
			if !fail {
				g.hook = func(*http.Request, wire.Message) (*http.Response, error) { return nil, io.ErrUnexpectedEOF }
			}
			s, err := a.Stream(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			g.chunk(1, "prefix")
			if !s.Next() || s.Value() != "prefix" {
				t.Fatal(s.Err())
			}
			if fail {
				g.failure(1000, 1)
			} else {
				g.final(1)
			}
			waitTerminal(t, s)
			if s.Next() {
				t.Fatal("extra item")
			}
			var remote *RemoteError
			if fail {
				if !errors.As(s.Err(), &remote) || remote.RPCCode != 1000 {
					t.Fatal(s.Err())
				}
			} else if s.Err() != nil {
				t.Fatal(s.Err())
			}
		})
	}
}

func TestConsumerStreamRefusalEvidence(t *testing.T) {
	for _, code := range []int{1002, 1008} {
		for _, mode := range []string{"empty", "after-chunk", "bad-final"} {
			t.Run(wire.Kind(code)+"/"+mode, func(t *testing.T) {
				_, a, g := newConsumerGateway(t)
				s, err := a.Stream(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				seq := uint64(0)
				if mode != "empty" {
					seq = 1
				}
				if mode == "after-chunk" {
					g.chunk(1, "prefix")
					if !s.Next() || s.Value() != "prefix" {
						t.Fatal(s.Err())
					}
				}
				g.failure(code, seq)
				waitTerminal(t, s)
				if s.Next() {
					t.Fatal("unexpected item")
				}
				want := code
				if mode != "empty" {
					want = 1004
				}
				var remote *RemoteError
				if !errors.As(s.Err(), &remote) || remote.RPCCode != want {
					t.Fatal(s.Err())
				}
			})
		}
	}
}

func TestConsumerStreamProtocolFailuresAreUnknown(t *testing.T) {
	for _, name := range []string{
		"eof",
		"sequence",
		"token",
		"id",
		"address",
		"final-seq",
		"error-seq",
		"fifth",
		"unknown-field",
		"legacy-chunk",
		"oversize",
	} {
		t.Run(name, func(t *testing.T) {
			_, a, g := newConsumerGateway(t)
			s, err := a.Stream(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			switch name {
			case "eof":
				g.body.Close()
			case "sequence":
				g.chunk(2, nil)
			case "final-seq":
				g.final(1)
			case "error-seq":
				g.body.send(providerFailure(g.request.ID, 1000, &selected))
			case "fifth":
				for i := 1; i <= 5; i++ {
					g.chunk(uint64(i), nil)
				}
			case "oversize":
				g.chunk(1, strings.Repeat("x", 8192))
			default:
				p := map[string]any{"request_id": g.request.ID, "stream_token": testToken, "address": selected, "seq": 1, "value": nil}
				switch name {
				case "token":
					p["stream_token"] = otherToken
				case "id":
					p["request_id"] = 1
				case "address":
					b := selected
					b.ID = "other"
					p["address"] = b
				case "unknown-field":
					p["x"] = true
				}
				method := "stream/chunk"
				if name == "legacy-chunk" {
					method = "actor/chunk"
				}
				g.body.send(streamNotification(method, p))
			}
			waitTerminal(t, s)
			if s.Next() || !errors.Is(s.Err(), ErrOutcomeUnknown) {
				t.Fatal(s.Err())
			}
			if name == "eof" || name == "sequence" {
				if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrAddressUncertain) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestConsumerStreamCloseUnblocksNextAndReleasesAddr(t *testing.T) {
	for _, mode := range []string{"cold", "selected"} {
		t.Run(mode, func(t *testing.T) {
			_, a, g := newConsumerGateway(t)
			s, err := a.Stream(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if mode == "selected" {
				g.chunk(1, "prefix")
				if !s.Next() || s.Value() != "prefix" || a.Address() != selected {
					t.Fatal(s.Err(), a.Address())
				}
			}

			next := nextAsync(s)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if waitValue(t, next) {
				t.Fatal("item after close")
			}
			// Next may acknowledge the consumed prefix before Close sends cancel.
			m := waitValue(t, g.controls)
			if mode == "selected" && m.Method == "stream/ack" {
				m = waitValue(t, g.controls)
			}
			if m.Method != "stream/cancel" {
				t.Fatal(m)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			value, err := a.Call(context.Background(), nil)
			if mode == "cold" {
				if !errors.Is(err, ErrAddressUncertain) {
					t.Fatal(err)
				}
			} else if err != nil || value != "call" || a.Address() != selected {
				t.Fatal("early close lost the selected address or operation lock", value, err, a.Address())
			}
		})
	}
}

func TestConsumerStreamCanceledBeforeDispatch(t *testing.T) {
	_, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) {
		t.Error("canceled Stream reached the transport")
		return nil, errConnection
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s, err := a.Stream(ctx, nil); s != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(s, err)
	}
	if a.uncertain || a.Address().Generation != "" {
		t.Fatal("canceled Stream changed the address")
	}
}

func TestConsumerStreamDeadlinesAndAddrQueue(t *testing.T) {
	t.Run("queue-and-deadline", func(t *testing.T) {
		_, a, _ := newConsumerGateway(t)
		s, err := a.Stream(context.Background(), nil, WithStreamTimeout(80*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if _, err := a.Call(ctx, nil); !errors.Is(err, ErrDeadlineExceeded) {
			t.Fatal(err)
		}
		if s.Next() || !errors.Is(s.Err(), ErrOutcomeUnknown) {
			t.Fatal(s.Err())
		}
	})
	t.Run("caller-cancel", func(t *testing.T) {
		_, a, _ := newConsumerGateway(t)
		ctx, cancel := context.WithCancel(context.Background())
		s, err := a.Stream(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if s.Next() || !errors.Is(s.Err(), context.Canceled) {
			t.Fatal(s.Err())
		}
		s.Close()
	})
	t.Run("success-awaiting-consumption", func(t *testing.T) {
		_, a, g := newConsumerGateway(t)
		s, err := a.Stream(context.Background(), nil, WithStreamTimeout(30*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		g.chunk(1, nil)
		g.final(1)
		waitTerminal(t, s)
		waitValue(t, s.closeDone)
		if s.Next() || !errors.Is(s.Err(), ErrDeadlineExceeded) {
			t.Fatal(s.Err())
		}
		if a.Address() != selected {
			t.Fatal(a.Address())
		}
	})
	for _, d := range []time.Duration{0, -1, 61 * time.Second} {
		_, a, _ := newConsumerGateway(t)
		if _, err := a.Stream(context.Background(), nil, WithStreamTimeout(d)); err == nil {
			t.Fatal("invalid timeout")
		}
	}
}

func TestConsumerStream1013AndSetupRejections(t *testing.T) {
	for _, name := range []string{"1002", "1008", "1002-bad-final", "1008-bad-final", "1013", "missing-address", "bad-final", "401", "500", "bad-open", "legacy-open", "json-success", "connect"} {
		t.Run(name, func(t *testing.T) {
			_, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
				m := rpcRequest(t, req)
				if m.Method == "actor/call" {
					return success(m.ID, selected, "ok"), nil
				}
				if name == "401" {
					return emptyResponse(401), nil
				}
				if name == "500" {
					return emptyResponse(500), nil
				}
				if name == "connect" {
					return nil, errConnection
				}
				if name == "json-success" {
					return success(m.ID, selected, nil), nil
				}
				if name == "bad-open" || name == "legacy-open" {
					b := newProviderBody()
					method := "stream/opened"
					window := map[string]any{"items": 4, "bytes": 32768, "x": 1}
					if name == "legacy-open" {
						method = "actor/stream_open"
						delete(window, "x")
					}
					b.send(streamNotification(method, map[string]any{
						"request_id":   m.ID,
						"stream_token": testToken,
						"window":       window,
					}))
					return sseResponse(b), nil
				}
				failure := streamFailure(providerFailure(m.ID, 1013, &selected), 0)
				if strings.HasPrefix(name, "1002") || strings.HasPrefix(name, "1008") {
					code := 1002
					if strings.HasPrefix(name, "1008") {
						code = 1008
					}
					failure = providerFailure(m.ID, code, nil) // rejection before stream/opened
					if strings.HasSuffix(name, "bad-final") {
						failure = streamFailure(failure, 1)
					}
				}
				if name == "missing-address" {
					failure = streamFailure(providerFailure(m.ID, 1013, nil), 0)
				}
				if name == "bad-final" {
					failure = streamFailure(providerFailure(m.ID, 1013, &selected), 1)
				}
				return jsonResponse(m.ID, nil, failure.Error), nil
			})
			s, err := a.Stream(context.Background(), nil)
			if s != nil || err == nil {
				t.Fatal(s, err)
			}
			switch name {
			case "1013":
				if !errors.Is(err, ErrUnsupportedOperation) || a.Address() != selected {
					t.Fatal(err, a.Address())
				}
				if _, err := a.Call(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
			case "1002", "1008", "401", "connect":
				a.mu.Lock()
				uncertain := a.uncertain
				a.mu.Unlock()
				if uncertain {
					t.Fatal("definite rejection contaminated address")
				}
			default:
				if !errors.Is(err, ErrOutcomeUnknown) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestConsumerStreamRuntimeCloseAndBlockedCredit(t *testing.T) {
	for _, mode := range []string{"runtime-close", "blocked-credit"} {
		t.Run(mode, func(t *testing.T) {
			r, a, g := newConsumerGateway(t)
			held := make(chan struct{})
			if mode == "blocked-credit" {
				g.hook = func(req *http.Request, m wire.Message) (*http.Response, error) {
					if m.Method == "stream/ack" {
						close(held)
						<-req.Context().Done()
						return nil, req.Context().Err()
					}
					return emptyResponse(204), nil
				}
			}
			s, err := a.Stream(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			var next <-chan bool
			if mode == "blocked-credit" {
				g.chunk(1, nil)
				if !s.Next() {
					t.Fatal(s.Err())
				}
				next = nextAsync(s)
				waitValue(t, held)
			} else {
				next = nextAsync(s)
			}
			if mode == "runtime-close" {
				if err := r.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				s.Close()
			}
			if waitValue(t, next) {
				t.Fatal("Close did not wake Next")
			}
			for {
				m := waitValue(t, g.controls)
				if m.Method == "stream/cancel" {
					break
				}
			}
			waitValue(t, s.readerDone)
			waitValue(t, s.closeDone)
		})
	}
}

func TestConsumerStreamSetupDeadlineClosesBody(t *testing.T) {
	body := newProviderBody()
	_, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) { return sseResponse(body), nil })
	s, err := a.Stream(context.Background(), nil, WithStreamTimeout(20*time.Millisecond))
	if s != nil || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(s, err)
	}
	waitValue(t, body.closed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Stream(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestConsumerStreamPinsFirstChunkAcrossMissingTerminal(t *testing.T) {
	_, a, g := newConsumerGateway(t)
	s, err := a.Stream(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	g.chunk(1, nil)
	if !s.Next() {
		t.Fatal(s.Err())
	}
	g.body.Close()
	waitTerminal(t, s)
	if s.Next() || !errors.Is(s.Err(), ErrOutcomeUnknown) || a.Address() != selected {
		t.Fatal(s.Err(), a.Address())
	}
	if _, err := a.Call(context.Background(), nil); err != nil {
		t.Fatal("fixed generation became uncertain", err)
	}
}

func TestConsumerStreamLastCreditExpiryPreservesConsumedSuccess(t *testing.T) {
	_, a, g := newConsumerGateway(t)
	g.hook = func(req *http.Request, _ wire.Message) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	s, err := a.Stream(context.Background(), nil, WithStreamTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	g.chunk(1, nil)
	if !s.Next() {
		t.Fatal(s.Err())
	}
	g.final(1)
	waitTerminal(t, s)
	if s.Next() || s.Err() != nil {
		t.Fatal("last credit rewrote consumed success", s.Err())
	}
}
