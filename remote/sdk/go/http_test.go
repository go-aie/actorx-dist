package actorxremote

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

type oneByteReader struct{ io.Reader }

func (r oneByteReader) Read(p []byte) (int, error) { return r.Reader.Read(p[:1]) }

func TestSSEFragmentationBoundsAndEOF(t *testing.T) {
	first := `{"jsonrpc":"2.0","id":null,"result":null}`
	second := `{"jsonrpc":"2.0","id":900719925474099312345,"result":null}`
	s := newSSEReader(oneByteReader{strings.NewReader("data: " + first + "\n\ndata: " + second + "\n\n")})
	for _, want := range []string{first, second} {
		raw, err := s.next()
		if err != nil || string(raw) != want {
			t.Fatalf("%s %v", raw, err)
		}
	}
	if _, err := s.next(); !errors.Is(err, ErrTransport) {
		t.Fatal("EOF succeeded", err)
	}
	for _, raw := range []string{
		"data: {}\n",
		"data: {}",
		"data: {}\r\n\r\n",
		"event: rpc\n\n",
		"data: {}\ndata: {}\n\n",
		"data: " + strings.Repeat("x", wire.MaxBytes+1),
		"data: " + strings.Repeat("x", wire.MaxBytes+1) + "\n\n",
	} {
		if _, err := newSSEReader(oneByteReader{strings.NewReader(raw)}).next(); err == nil {
			t.Fatalf("accepted %.50s", raw)
		}
	}
	want := strings.Repeat("x", wire.MaxBytes)
	if got, err := newSSEReader(strings.NewReader("data: " + want + "\n\n")).next(); err != nil || string(got) != want {
		t.Fatal("exact frame bound", err)
	}
}

type trackedBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *trackedBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestCallResponseLimitsAndBodyOwnership(t *testing.T) {
	for _, tc := range []struct{ media, body string }{
		{"text/event-stream", "data: {}\n\n"},
		{"application/json", strings.Repeat(" ", wire.MaxBytes+1)},
		{"application/json", `{"jsonrpc":"2.0","id":null,"result":null}`},
	} {
		body := &trackedBody{Reader: strings.NewReader(tc.body)}
		_, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{tc.media}}, Body: body}, nil
		})
		if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
		if !body.closed.Load() {
			t.Fatal("response body leaked")
		}
	}
	// readBody must stop at MAX+1 instead of draining an unbounded peer.
	reader := strings.NewReader(strings.Repeat("x", wire.MaxBytes*4))
	if _, err := readBody(reader); !errors.Is(err, ErrProtocol) || reader.Len() != wire.MaxBytes*3-1 {
		t.Fatal("unbounded response read", err)
	}
}

func TestActualHTTPTransportDoesNotReplayDeliveredPOST(t *testing.T) {
	r, a := newTestRuntime(t, nil)
	r.http.Transport = r.transport
	var dials atomic.Int32
	serverDone := make(chan error, 1)
	r.transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		if dials.Add(1) != 1 {
			return nil, errors.New("unexpected automatic retry")
		}
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			reader := bufio.NewReader(server)
			for n := 1; n <= 2; n++ {
				req, err := http.ReadRequest(reader)
				if err != nil {
					serverDone <- err
					return
				}
				body, err := io.ReadAll(req.Body)
				req.Body.Close()
				if err != nil {
					serverDone <- err
					return
				}
				if n == 2 {
					// The request was delivered; EOF supplies no result evidence.
					serverDone <- nil
					return
				}
				m, err := wire.Parse(body)
				if err != nil {
					serverDone <- err
					return
				}
				resp := success(m.ID, selected, nil)
				raw, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				_, err = fmt.Fprintf(server, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(raw), raw)
				if err != nil {
					serverDone <- err
					return
				}
			}
		}()
		return client, nil
	}
	if _, err := a.Call(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	if err := waitValue(t, serverDone); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 1 || a.Address() != selected {
		t.Fatal("replayed or reselected")
	}
}

func TestEstablishmentFailureWithOwnedTransportIsDefinite(t *testing.T) {
	r, a := newTestRuntime(t, nil)
	r.http.Transport = r.transport
	r.transport.DialContext = func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("private dial detail") }
	for range 2 {
		if _, err := a.Call(context.Background(), nil); !errors.Is(err, ErrTransport) ||
			errors.Is(err, ErrOutcomeUnknown) || strings.Contains(err.Error(), "private") {
			t.Fatal(err)
		}
	}
}

func TestPostSSEAndSessionHeaders(t *testing.T) {
	r, _ := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("Accept") != "text/event-stream, application/json" || req.Header.Get("X-Actorx-Remote-Session") != "session-token" {
			t.Error("missing SSE/session headers")
		}
		return &http.Response{StatusCode: 204, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})
	response, err := r.post(context.Background(), []byte(`{}`), true, "session-token")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
}

func TestTransportCausesAreControlled(t *testing.T) {
	_, a := newTestRuntime(t, func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("secret endpoint: %w", io.ErrUnexpectedEOF)
	})
	_, err := a.Call(context.Background(), nil)
	if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, ErrTransport) || !errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
	var detail *TransportError
	if !errors.As(err, &detail) || detail.Unwrap() != io.ErrUnexpectedEOF {
		t.Fatal("lost controlled cause", err)
	}
}
