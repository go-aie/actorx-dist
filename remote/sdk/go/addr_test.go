package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var selected = Address{Version: 1, Type: "demo.ping", ID: "session-42", Generation: "AAAAAAAAAAAAAAAAAAAAAA"}

func newTestRuntime(t *testing.T, f roundTripFunc) (*Runtime, *Addr) {
	t.Helper()
	r, err := NewRuntime("http://gateway.test", Options{APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	r.http.Transport = f
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	a, err := r.Addr(selected.Type, selected.ID)
	if err != nil {
		t.Fatal(err)
	}
	return r, a
}

func rpcRequest(t *testing.T, req *http.Request) wire.Message {
	t.Helper()
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		t.Error(err)
		return wire.Message{}
	}
	m, err := wire.Parse(raw)
	if err != nil {
		t.Error(err)
	}
	return m
}

func jsonResponse(id json.RawMessage, result any, rpcErr *wire.RPCError) *http.Response {
	raw, _ := json.Marshal(wire.Message{JSONRPC: "2.0", ID: id, Result: mustJSON(result), Error: rpcErr})
	if rpcErr != nil {
		raw, _ = json.Marshal(wire.Message{JSONRPC: "2.0", ID: id, Error: rpcErr})
	}
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
		Body:       io.NopCloser(strings.NewReader(string(raw))),
	}
}

func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

func success(id json.RawMessage, a Address, value any) *http.Response {
	return jsonResponse(id, map[string]any{"address": a, "value": value}, nil)
}

func waitValue[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for owned operation")
		var zero T
		return zero
	}
}

type taggedRequest struct {
	Text    string `json:"text"`
	Ignored string `json:"-"`
}

type customJSON struct{}

func (customJSON) MarshalJSON() ([]byte, error) {
	return []byte(`{"n":900719925474099312345,"f":1.234567890123456789}`), nil
}

type brokenJSON struct{}

func (brokenJSON) MarshalJSON() ([]byte, error) {
	return nil, errors.New("secret marshaler diagnostic")
}

type panicJSON struct{}

func (panicJSON) MarshalJSON() ([]byte, error) { panic("secret marshaler panic") }

func TestCallStandardJSONAndPinning(t *testing.T) {
	var sent int
	_, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		sent++
		if req.Method != "POST" || req.URL.String() != "http://gateway.test/runtime/rpc" ||
			req.Header.Get("Authorization") != "Bearer test-key" ||
			req.Header.Get("Content-Type") != "application/json" ||
			req.Header.Get("Accept") != "application/json" ||
			req.Header.Get("X-Actorx-Remote-Protocol") != wire.Version ||
			req.Header.Get("X-Actorx-Remote-Session") != "" {
			t.Error("invalid consumer HTTP request")
		}
		if req.GetBody != nil {
			t.Error("replayable POST")
		}
		m := rpcRequest(t, req)
		if m.Method != "actor/call" {
			t.Error(m.Method)
		}
		var params struct {
			Address  Address
			Input    json.RawMessage
			Deadline int64 `json:"deadline_unix_ms"`
		}
		if err := json.Unmarshal(m.Params, &params); err != nil {
			t.Error(err)
		}
		if sent == 1 && params.Address.Generation != "" || sent > 1 && params.Address != selected {
			t.Error(params.Address)
		}
		deadline, _ := req.Context().Deadline()
		if deadline.UnixMilli() != params.Deadline || time.Until(deadline) > callTimeout || time.Until(deadline) < 4*time.Second {
			t.Error("incorrect total budget")
		}
		return success(m.ID, selected, params.Input), nil
	})
	if a.Address().Generation != "" || sent != 0 {
		t.Fatal("Addr did network I/O")
	}

	var pointer *taggedRequest
	for _, value := range []any{
		taggedRequest{Text: "hello", Ignored: "secret"},
		&taggedRequest{Text: "pointer"},
		customJSON{},
		json.RawMessage(`{"value":null}`),
		json.Number("900719925474099312345"),
		nil,
		pointer,
		[]any{true, "text"},
	} {
		got, err := a.Call(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := snapshot(value)
		if err != nil {
			t.Fatal(err)
		}
		want, err := wire.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if string(mustJSON(got)) != string(mustJSON(want)) {
			t.Fatalf("got %#v want %#v", got, want)
		}
		if _, ok := value.(customJSON); ok && got.(map[string]any)["n"] != json.Number("900719925474099312345") {
			t.Fatal("number precision lost")
		}
	}

	snap := a.Address()
	snap.Generation = "changed"
	snap.ID = "changed"
	if a.Address() != selected {
		t.Fatal("snapshot modified handle")
	}
}

func TestBadJSONNeverDispatchesOrPoisonsAddress(t *testing.T) {
	var sent int
	_, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		sent++
		m := rpcRequest(t, req)
		return success(m.ID, selected, nil), nil
	})
	for _, value := range []any{
		make(chan int),
		math.NaN(),
		math.Inf(1),
		brokenJSON{},
		panicJSON{},
		json.RawMessage(`{"x":1,"x":2}`),
		json.RawMessage(`"\ud800"`),
		json.RawMessage([]byte{0xff}),
		strings.Repeat("<", 2000),
		strings.Repeat("x", 8100),
		json.RawMessage(strings.Repeat("[", 63) + "0" + strings.Repeat("]", 63)),
	} {
		_, err := a.Call(context.Background(), value)
		if err == nil || strings.Contains(err.Error(), "secret") || sent != 0 {
			t.Fatalf("bad JSON dispatch: %T %v %d", value, err, sent)
		}
	}
	if _, err := a.Call(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteErrorsAndSelectedEvidence(t *testing.T) {
	for _, tc := range []struct {
		code      int
		address   bool
		want      error
		uncertain bool
	}{
		{1013, true, ErrUnsupportedOperation, false},
		{1013, false, ErrOutcomeUnknown, true},
		{1000, false, ErrOutcomeUnknown, true},
		{-32603, false, ErrOutcomeUnknown, true},
		{1012, false, ErrOutcomeUnknown, true},
		{1003, false, ErrActivationUnknown, true},
		{1004, false, ErrOutcomeUnknown, true},
		{1004, true, ErrOutcomeUnknown, false},
		{1005, false, ErrDeadlineExceeded, false},
		{1001, false, nil, false},
		{-32602, false, nil, false},
		{123456, false, nil, false},
	} {
		t.Run(wire.Kind(tc.code)+map[bool]string{true: "/address", false: "/cold"}[tc.address], func(t *testing.T) {
			var sent int
			_, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
				sent++
				m := rpcRequest(t, req)
				data := map[string]any{"kind": wire.Kind(tc.code), "extension": true}
				if tc.address {
					data["address"] = selected
				}
				return jsonResponse(m.ID, nil, &wire.RPCError{Code: tc.code, Message: "secret peer diagnostic", Data: mustJSON(data)}), nil
			})
			_, err := a.Call(context.Background(), nil)
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("error: %v", err)
			}
			var remote *RemoteError
			if !errors.As(err, &remote) {
				t.Fatalf("missing RemoteError: %v", err)
			}
			if tc.code == 123456 && (remote.Code != "unknown" || remote.RPCCode != 123456 || len(remote.Data) == 0) {
				t.Fatal(remote)
			}
			if tc.address && a.Address() != selected {
				t.Fatal("lost error address")
			}
			_, next := a.Call(context.Background(), nil)
			if errors.Is(next, ErrAddressUncertain) != tc.uncertain {
				t.Fatalf("next call: %v", next)
			}
			if tc.uncertain && sent != 1 || !tc.uncertain && sent != 2 {
				t.Fatal("incorrect dispatch count", sent)
			}
		})
	}
}

func TestMalformedResponsesBecomeUnknown(t *testing.T) {
	for _, raw := range []string{
		`{"jsonrpc":"2.0","id":"wrong","result":null}`,
		`{"jsonrpc":"2.0","id":ID,"result":null,"error":null}`,
		`{"jsonrpc":"2.0","id":ID,"result":{"address":ADDRESS}}`,
		`{"jsonrpc":"2.0","id":ID,"result":{"address":ADDRESS,"value":null,"extra":true}}`,
		`{"jsonrpc":"2.0","id":ID,"result":{"address":null,"value":null}}`,
		`{"jsonrpc":"2.0","id":ID,"error":{"code":1013,"message":"x","data":{"address":null}}}`,
		`{"jsonrpc":"2.0","id":ID,"error":{"code":1013,"message":"x","data":{"kind":"remote_error","address":ADDRESS}}}`,
		`{"jsonrpc":"2.0","id":ID,"result":{"address":ADDRESS,"value":{"x":1,"x":2}}}`,
		`{"jsonrpc":"2.0","id":ID,"result":{"address":{"version":1,"type":"demo.ping","id":"other","generation":"AAAAAAAAAAAAAAAAAAAAAA"},"value":null}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var sent int
			_, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
				sent++
				m := rpcRequest(t, req)
				body := strings.ReplaceAll(strings.ReplaceAll(raw, "ID", string(m.ID)), "ADDRESS", string(mustJSON(selected)))
				resp := success(m.ID, selected, nil)
				resp.Body = io.NopCloser(strings.NewReader(body))
				return resp, nil
			})
			_, err := a.Call(context.Background(), nil)
			if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, ErrProtocol) {
				t.Fatal(err)
			}
			if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrAddressUncertain) || sent != 1 {
				t.Fatalf("next call: %v, sent %d", err, sent)
			}
		})
	}
}

func TestFixedAddressNeverReselects(t *testing.T) {
	var sent int
	_, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		sent++
		m := rpcRequest(t, req)
		other := selected
		if sent == 2 {
			other.Generation = "AQEBAQEBAQEBAQEBAQEBAQ"
		}
		return success(m.ID, other, nil), nil
	})
	if _, err := a.Call(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	if a.Address() != selected {
		t.Fatal("generation changed")
	}
	if _, err := a.Call(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPRejectionsAndConnectionFailure(t *testing.T) {
	for _, status := range []int{307, 400, 401, 403, 413, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var sent int
			_, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
				sent++
				return &http.Response{
					StatusCode: status,
					Header:     http.Header{"Location": []string{"http://other.test"}},
					Body:       io.NopCloser(strings.NewReader("secret")),
				}, nil
			})
			_, err := a.Call(context.Background(), nil)
			var te *TransportError
			if !errors.As(err, &te) || te.Status != status || strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
			if status == 401 && !errors.Is(err, ErrAuthentication) ||
				status == 403 && !errors.Is(err, ErrSession) ||
				status == 500 && !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatal(err)
			}
			_, err = a.Call(context.Background(), nil)
			if errors.Is(err, ErrAddressUncertain) != (status >= 500) || sent != map[bool]int{true: 1, false: 2}[status >= 500] {
				t.Fatalf("%v sent %d", err, sent)
			}
		})
	}
	var sent int
	_, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) {
		sent++
		return nil, &net.OpError{Op: "dial", Err: errors.New("secret")}
	})
	for range 2 {
		if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrTransport) ||
			errors.Is(err, ErrOutcomeUnknown) || strings.Contains(err.Error(), "secret") {
			t.Fatal(err)
		}
	}
	if sent != 2 {
		t.Fatal(sent)
	}
}

func TestQueueBudgetCancellationAndIndependentHandles(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var sent atomic.Int32
	r, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		n := sent.Add(1)
		m := rpcRequest(t, req)
		if n == 1 {
			entered <- struct{}{}
			select {
			case <-release:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return success(m.ID, selected, nil), nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := a.Call(context.Background(), nil)
		done <- err
	}()
	waitValue(t, entered)

	// A queued or already-canceled Call must not reach the transport.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := a.Call(ctx, nil); !errors.Is(err, ErrDeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if sent.Load() != 1 {
		t.Fatal("queued call dispatched")
	}
	ctx, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if _, err := a.Call(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// Separate handles do not share the occupied operation lock.
	b, err := r.Addr(selected.Type, selected.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Call(context.Background(), nil); err != nil {
		t.Fatal("independent handle blocked", err)
	}

	close(release)
	if err := waitValue(t, done); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Call(context.Background(), nil); err != nil {
		t.Fatal("queue timeout poisoned handle", err)
	}
}

func TestDispatchedCancellationAndDeadline(t *testing.T) {
	for _, canceled := range []bool{true, false} {
		t.Run(map[bool]string{true: "canceled", false: "deadline"}[canceled], func(t *testing.T) {
			entered := make(chan struct{}, 1)
			var sent atomic.Int32
			_, a := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
				sent.Add(1)
				entered <- struct{}{}
				<-req.Context().Done()
				return nil, errors.New("secret transport detail")
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := a.Call(ctx, nil)
				done <- err
			}()
			waitValue(t, entered)
			if canceled {
				cancel()
			}
			err := waitValue(t, done)
			if canceled && !errors.Is(err, context.Canceled) || !canceled && !errors.Is(err, ErrOutcomeUnknown) || strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
			if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrAddressUncertain) || sent.Load() != 1 {
				t.Fatal(err)
			}
		})
	}
}
