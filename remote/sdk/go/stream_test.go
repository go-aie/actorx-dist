package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"net/http"
	"strings"
	"sync/atomic"
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
	started  chan struct{}
	hook     func(*http.Request, wire.Message) (*http.Response, error)
}

func newConsumerGateway(t *testing.T) (*Runtime, *Addr, *consumerGateway) {
	g := &consumerGateway{
		t:        t,
		body:     newProviderBody(),
		controls: make(chan wire.Message, 64),
		started:  make(chan struct{}),
	}
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
			g.body.send(streamNotification("stream/opened", map[string]any{
				"request_id":   m.ID,
				"stream_token": testToken,
				"window":       wire.StreamWindow(),
			}))
			close(g.started)
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

// Helpers consume the public sequence directly; no pull/Next adapter is used.
func collectStream(seq iter.Seq2[any, error]) ([]any, error) {
	var values []any
	for value, err := range seq {
		if err != nil {
			return values, err
		}
		values = append(values, value)
	}
	return values, nil
}

func feedStream(t *testing.T, g *consumerGateway, feed func()) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		waitValue(t, g.started)
		feed()
	}()
	return done
}

func assertCredit(t *testing.T, g *consumerGateway, want uint64) {
	t.Helper()
	m := waitValue(t, g.controls)
	_, token, seq, err := wire.Control(m)
	if err != nil || m.Method != "stream/ack" || token != testToken || seq != want {
		t.Fatal(m, err)
	}
}

func TestConsumerStreamLazySingleUseAndSnapshot(t *testing.T) {
	_, a, g := newConsumerGateway(t)
	message := &taggedRequest{Text: "before", Ignored: "secret"}
	seq := a.Stream(context.Background(), message)
	select {
	case <-g.started:
		t.Fatal("construction dispatched")
	default:
	}

	// It occupies neither an Addr slot nor Runtime work before consumption.
	if value, err := a.Call(context.Background(), nil); err != nil || value != "call" {
		t.Fatal(value, err)
	}

	message.Text = "at iteration"
	fed := feedStream(t, g, func() {
		g.chunk(1, nil)
		g.final(1)
	})
	for value, err := range seq {
		if err != nil || value != nil {
			t.Fatal(value, err)
		}
		var params map[string]json.RawMessage
		_ = json.Unmarshal(g.request.Params, &params)
		if string(params["input"]) != `{"text":"at iteration"}` {
			t.Fatal(string(params["input"]))
		}

		// Concurrent execution of the same sequence is a local usage error
		// and does not cancel or redispatch the first execution.
		second := make(chan error, 1)
		go func() {
			_, err := collectStream(seq)
			second <- err
		}()
		if err := waitValue(t, second); !errors.Is(err, ErrStreamUsed) {
			t.Fatal(err)
		}
	}

	waitValue(t, fed)
	assertCredit(t, g, 1)
	if _, err := collectStream(seq); !errors.Is(err, ErrStreamUsed) {
		t.Fatal(err)
	}
}

func TestConsumerStreamUnusedSequenceDoesNotHoldRuntime(t *testing.T) {
	r, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) {
		t.Error("unused stream dispatched")
		return nil, errConnection
	})
	seq := a.Stream(context.Background(), nil)
	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := collectStream(seq); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func TestConsumerStreamEmptyAndCreditAfterBody(t *testing.T) {
	for _, n := range []int{0, 6} {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			_, a, g := newConsumerGateway(t)
			fed := feedStream(t, g, func() {
				if n == 0 {
					g.final(0)
					return
				}
				for i := 1; i <= 4; i++ {
					g.chunk(uint64(i), customJSON{})
				}
				for i := 1; i <= n; i++ {
					assertCredit(t, g, uint64(i))
					if i <= 2 {
						g.chunk(uint64(i+4), nil)
					}
					if i == 5 {
						g.final(6)
					}
				}
			})
			count := 0
			for value, err := range a.Stream(context.Background(), nil) {
				if err != nil {
					t.Fatal(err)
				}
				count++
				if count <= 4 && value.(map[string]any)["n"] != json.Number("900719925474099312345") {
					t.Fatal(value)
				}
				// ACK for this item is emitted only after this body returns.
				if count == 1 {
					select {
					case <-g.controls:
						t.Fatal("credit before body returned")
					default:
					}
				}
				continue
			}

			waitValue(t, fed)
			if count != n || a.Address() != selected {
				t.Fatal(count, a.Address())
			}
			select {
			case m := <-g.controls:
				t.Fatal("control after exhaustion", m)
			default:
			}
		})
	}
}

func TestConsumerStreamTerminalDrainsAndFinalACK(t *testing.T) {
	for _, failure := range []bool{false, true} {
		for _, ackFailure := range []string{"none", "transport", "expiry"} {
			t.Run(fmtCase(failure, ackFailure), func(t *testing.T) {
				_, a, g := newConsumerGateway(t)
				if ackFailure != "none" {
					g.hook = func(req *http.Request, m wire.Message) (*http.Response, error) {
						_, _, seq, _ := wire.Control(m)
						if seq == 3 {
							if ackFailure == "expiry" {
								<-req.Context().Done()
								return nil, req.Context().Err()
							}
							return nil, io.ErrUnexpectedEOF
						}
						return emptyResponse(204), nil
					}
				}

				fed := feedStream(t, g, func() {
					g.chunk(1, "one")
					g.chunk(2, nil)
					g.chunk(3, "three")
					if failure {
						g.failure(1000, 3)
					} else {
						g.final(3)
					}
				})
				values, err := collectStream(a.Stream(context.Background(), nil, WithStreamTimeout(time.Second)))
				waitValue(t, fed)
				if len(values) != 3 || values[0] != "one" || values[1] != nil || values[2] != "three" {
					t.Fatal(values, err)
				}
				var remote *RemoteError
				if failure {
					if !errors.As(err, &remote) || remote.RPCCode != 1000 {
						t.Fatal(err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				for i := uint64(1); i <= 3; i++ {
					assertCredit(t, g, i)
				}
				waitValue(t, g.body.closed)
			})
		}
	}
}

func fmtCase(failure bool, ack string) string {
	if failure {
		return "failure/" + ack
	}
	return "success/" + ack
}

func TestConsumerStreamEarlyExit(t *testing.T) {
	for _, mode := range []string{"break", "return", "panic", "false"} {
		t.Run(mode, func(t *testing.T) {
			_, a, g := newConsumerGateway(t)
			fed := feedStream(t, g, func() {
				g.chunk(1, "prefix")
				g.chunk(2, "queued")
				g.final(2)
			})
			businessErr := errors.New("application failed")
			panicValue := &struct{}{}
			seq := a.Stream(context.Background(), nil)
			run := func() error {
				for value, err := range seq {
					if err != nil || value != "prefix" {
						t.Fatal(value, err)
					}
					switch mode {
					case "return":
						return businessErr
					case "panic":
						panic(panicValue)
					}
					break
				}
				return nil
			}
			if mode == "panic" {
				func() {
					defer func() {
						if got := recover(); got != panicValue {
							t.Fatal("panic changed", got)
						}
					}()
					_ = run()
				}()
			} else if mode == "false" {
				calls := 0
				seq(func(any, error) bool {
					calls++
					return false
				})
				if calls != 1 {
					t.Fatal("yield after false", calls)
				}
			} else if err := run(); mode == "return" && err != businessErr {
				t.Fatal(err)
			}

			waitValue(t, fed)
			if m := waitValue(t, g.controls); m.Method != "stream/cancel" {
				t.Fatal("early exit acknowledged current item", m)
			}
			waitValue(t, g.body.closed)
			if value, err := a.Call(context.Background(), nil); err != nil || value != "call" || a.Address() != selected {
				t.Fatal("cleanup did not preserve selected Addr", value, err)
			}
		})
	}
}

func TestConsumerStreamRefusalEvidence(t *testing.T) {
	for _, code := range []int{1002, 1008, 1013} {
		for _, mode := range []string{"empty", "after-chunk", "bad-final"} {
			t.Run(wire.Kind(code)+"/"+mode, func(t *testing.T) {
				_, a, g := newConsumerGateway(t)
				fed := feedStream(t, g, func() {
					if mode == "after-chunk" {
						g.chunk(1, "prefix")
					}
					seq := uint64(0)
					if mode != "empty" {
						seq = 1
					}
					g.failure(code, seq)
				})
				_, err := collectStream(a.Stream(context.Background(), nil))
				waitValue(t, fed)
				want := code
				if mode != "empty" {
					want = 1004
				}
				var remote *RemoteError
				if !errors.As(err, &remote) || remote.RPCCode != want {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestConsumerStreamProtocolFailuresAreUnknown(t *testing.T) {
	for _, name := range []string{
		"eof", "sequence", "token", "id", "address",
		"final-seq", "error-seq", "fifth", "unknown-field", "legacy-chunk", "oversize",
	} {
		t.Run(name, func(t *testing.T) {
			_, a, g := newConsumerGateway(t)
			fed := feedStream(t, g, func() {
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
					p := map[string]any{
						"request_id":   g.request.ID,
						"stream_token": testToken,
						"address":      selected,
						"seq":          1,
						"value":        nil,
					}
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
			})
			var got error
			for _, err := range a.Stream(context.Background(), nil) {
				if err != nil {
					got = err
					break
				}
				// Keep the first item charged until the invalid fifth frame is rejected.
				waitValue(t, g.body.closed)
			}

			waitValue(t, fed)
			if !errors.Is(got, ErrOutcomeUnknown) {
				t.Fatal(got)
			}
			if name == "eof" || name == "sequence" {
				if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrAddressUncertain) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestConsumerStreamSlowBodyCancellationOwnership(t *testing.T) {
	for _, mode := range []string{"caller", "deadline", "runtime", "producer-success", "producer-failure"} {
		t.Run(mode, func(t *testing.T) {
			r, a, g := newConsumerGateway(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			timeout := time.Second
			if mode == "deadline" {
				timeout = 80 * time.Millisecond
			}
			go func() {
				var got error
				for _, err := range a.Stream(ctx, nil, WithStreamTimeout(timeout)) {
					if err != nil {
						got = err
						break
					}
					close(entered)
					<-release
				}
				done <- got
			}()

			waitValue(t, g.started)
			g.chunk(1, "held")
			waitValue(t, entered)
			switch mode {
			case "caller":
				cancel()
			case "producer-success":
				g.final(1)
			case "producer-failure":
				g.failure(1000, 1)
			case "runtime":
				waitCtx, stop := context.WithCancel(context.Background())
				stop()
				if err := r.Close(waitCtx); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			if mode == "caller" || mode == "deadline" || mode == "runtime" {
				waitValue(t, g.body.closed)
			}

			// Neither producer completion nor cancellation may release an active body.
			if len(a.operation) != 1 {
				t.Fatal("released active Addr lock")
			}
			select {
			case <-r.closed:
				t.Fatal("Runtime closed before loop body returned")
			default:
			}
			select {
			case m := <-g.controls:
				if m.Method == "stream/ack" {
					t.Fatal("ACK while body held")
				}
			default:
			}
			if mode != "runtime" {
				queued, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
				if _, err := a.Call(queued, nil); !errors.Is(err, ErrDeadlineExceeded) {
					t.Fatal("Addr not held", err)
				}
				stop()
			}

			close(release)
			err := waitValue(t, done)
			if mode == "producer-success" {
				if err != nil {
					t.Fatal(err)
				}
			} else if mode == "producer-failure" {
				var remote *RemoteError
				if !errors.As(err, &remote) || remote.RPCCode != 1000 {
					t.Fatal(err)
				}
			} else if mode == "deadline" {
				if !errors.Is(err, ErrOutcomeUnknown) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if mode == "runtime" {
				if err := r.WaitClosed(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestConsumerStreamAbortDrainAndACKFailure(t *testing.T) {
	for _, mode := range []string{"cancel-drain", "ack-before-terminal", "ack-with-queued-terminal"} {
		t.Run(mode, func(t *testing.T) {
			_, a, g := newConsumerGateway(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode != "cancel-drain" {
				g.hook = func(*http.Request, wire.Message) (*http.Response, error) { return nil, io.ErrUnexpectedEOF }
			}
			fed := feedStream(t, g, func() {
				g.chunk(1, "first")
				if mode != "ack-before-terminal" {
					g.chunk(2, "queued")
					g.failure(1000, 2)
				}
			})
			count := 0
			var got error
			for _, err := range a.Stream(ctx, nil) {
				if err != nil {
					got = err
					// Error delivery follows cleanup and permits reusing the address.
					if _, err := a.Call(context.Background(), nil); err != nil {
						t.Fatal(err)
					}
					break
				}
				count++
				waitValue(t, fed)
				if mode == "cancel-drain" {
					cancel()
				}
			}
			if count != 1 {
				t.Fatal("continued drain after local failure", count)
			}
			if mode == "cancel-drain" {
				if !errors.Is(got, context.Canceled) {
					t.Fatal(got)
				}
			} else if !errors.Is(got, ErrOutcomeUnknown) {
				t.Fatal(got)
			}
		})
	}
}

func TestConsumerStreamSetupAndOptionErrors(t *testing.T) {
	var dispatch atomic.Int32
	_, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) {
		dispatch.Add(1)
		return nil, errConnection
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := collectStream(a.Stream(ctx, nil)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, opts := range [][]StreamOption{{nil}, {WithStreamTimeout(0)}, {WithStreamTimeout(-1)}, {WithStreamTimeout(61 * time.Second)}} {
		if _, err := collectStream(a.Stream(context.Background(), nil, opts...)); err == nil {
			t.Fatal("invalid option")
		}
	}
	for _, msg := range []any{make(chan int), panicJSON{}} {
		if _, err := collectStream(a.Stream(context.Background(), msg)); err == nil {
			t.Fatal("invalid input")
		}
	}
	if dispatch.Load() != 0 || a.uncertain {
		t.Fatal("setup error dispatched or contaminated Addr")
	}
}

func TestConsumerStreamSetupDeadlineClosesBody(t *testing.T) {
	body := newProviderBody()
	_, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) { return sseResponse(body), nil })
	if _, err := collectStream(a.Stream(context.Background(), nil, WithStreamTimeout(20*time.Millisecond))); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	waitValue(t, body.closed)
}

func TestConsumerStreamRuntimeCloseWakesReaderAndACK(t *testing.T) {
	for _, ack := range []bool{false, true} {
		t.Run(fmtCase(ack, "close"), func(t *testing.T) {
			r, a, g := newConsumerGateway(t)
			held := make(chan struct{})
			if ack {
				g.hook = func(req *http.Request, m wire.Message) (*http.Response, error) {
					if m.Method == "stream/ack" {
						close(held)
						<-req.Context().Done()
						return nil, req.Context().Err()
					}
					return emptyResponse(204), nil
				}
			}
			done := make(chan error, 1)
			go func() {
				_, err := collectStream(a.Stream(context.Background(), nil))
				done <- err
			}()

			waitValue(t, g.started)
			if ack {
				g.chunk(1, nil)
				waitValue(t, held)
			}
			if err := r.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := waitValue(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			waitValue(t, g.body.closed)
			for m := waitValue(t, g.controls); m.Method != "stream/cancel"; m = waitValue(t, g.controls) {
			}
		})
	}
}

func TestConsumerStream1013AndSetupRejections(t *testing.T) {
	for _, name := range []string{
		"1002", "1008", "1002-bad-final", "1008-bad-final", "1013", "missing-address", "bad-final",
		"401", "500", "bad-open", "legacy-open", "json-success", "connect",
	} {
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
			values, err := collectStream(a.Stream(context.Background(), nil))
			if len(values) != 0 || err == nil {
				t.Fatal(values, err)
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

func TestConsumerStreamQueueBudgetAndPinnedEOF(t *testing.T) {
	_, a, g := newConsumerGateway(t)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		var got error
		for _, err := range a.Stream(context.Background(), nil) {
			if err != nil {
				got = err
				break
			}
			close(entered)
			<-release
		}
		done <- got
	}()
	waitValue(t, g.started)
	g.chunk(1, (*int)(nil))
	waitValue(t, entered)
	if _, err := collectStream(a.Stream(context.Background(), nil, WithStreamTimeout(20*time.Millisecond))); !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatal("queue did not use Stream budget", err)
	}
	g.body.Close() // a pinned address survives missing terminal evidence
	close(release)
	if err := waitValue(t, done); !errors.Is(err, ErrOutcomeUnknown) || a.Address() != selected {
		t.Fatal(err, a.Address())
	}
	if value, err := a.Call(context.Background(), nil); err != nil || value != "call" {
		t.Fatal(value, err)
	}
}
