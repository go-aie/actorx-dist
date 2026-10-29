package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

func TestDirectoryVectorsAndInvalidResults(t *testing.T) {
	raw, err := os.ReadFile("testdata/v1.json")
	if err != nil {
		t.Fatal(err)
	}

	var vectors map[string]json.RawMessage
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	m, err := wire.Parse(vectors["actors"])
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeActors(m.Result)
	if err != nil || len(got.Actors) != 1 {
		t.Fatal(got, err)
	}

	limit, err := wire.Response(vectors["actors_limit"], "list-1")
	if err != nil || limit.Error == nil || limit.Error.Code != 1001 || wire.ValidateError(limit.Error) != nil {
		t.Fatal("invalid directory limit vector", limit, err)
	}

	empty, err := decodeActors([]byte(`{"actors":[]}`))
	if err != nil || empty.Actors == nil || len(empty.Actors) != 0 {
		t.Fatal(empty, err)
	}

	valid := `{"actors":[{"type":"demo","description":"","protocol":"actorx.remote/v1"}]}`
	for _, bad := range []string{
		strings.Replace(valid, `{"actors":`, `{"discovery":{},"actors":`, 1),
		strings.Replace(valid, `"actors":[`, `"actors":null,"unused":[`, 1),
		strings.Replace(valid, `"description":""`, `"description":null`, 1),
		strings.Replace(valid, `"actorx.remote/v1"`, `"actorx.remote/v2"`, 1),
		strings.Replace(valid, `"description":""`, `"description":"`+strings.Repeat("x", 513)+`"`, 1),
		`{}`, `{"actors":null}`, `{"actors":[{"type":"a"}]}`,
		`{"actors":[{"type":"a","description":"","protocol":"actorx.remote/v1"},{"type":"a","description":"","protocol":"actorx.remote/v1"}]}`,
	} {
		if got, err := decodeActors([]byte(bad)); !errors.Is(err, ErrProtocol) || got.Actors != nil {
			t.Fatal("accepted malformed directory", bad, got, err)
		}
	}
}

func TestActorsHTTPAndLifecycle(t *testing.T) {
	count := 0
	r, _ := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		count++
		m := rpcRequest(t, req)
		if m.Method != "runtime/actors" ||
			req.Header.Get("Accept") != "application/json" ||
			req.Header.Get("X-Actorx-Remote-Session") != "" {
			t.Error("wrong directory request", m)
		}
		if string(m.Params) != "{}" {
			t.Error("directory takes no parameters", string(m.Params))
		}
		result := ActorsResult{Actors: []ActorInfo{}}
		return jsonResponse(m.ID, result, nil), nil
	})
	got, err := r.Actors(context.Background())
	if err != nil || got.Actors == nil || count != 1 {
		t.Fatal(got, err, count)
	}

	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Actors(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func TestActorsTransportFailures(t *testing.T) {
	for _, mode := range []string{"rpc", "full", "correlation", "oversize", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			r, _ := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
				calls++
				if mode == "canceled" {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}

				m := rpcRequest(t, req)
				if mode == "rpc" {
					return jsonResponse(m.ID, nil, &wire.RPCError{
						Code:    1010,
						Message: "unavailable",
						Data:    json.RawMessage(`{"kind":"owner_unavailable"}`),
					}), nil
				}
				if mode == "full" {
					return jsonResponse(m.ID, nil, &wire.RPCError{
						Code:    1001,
						Message: "full",
						Data:    json.RawMessage(`{"kind":"full"}`),
					}), nil
				}
				if mode == "correlation" {
					return jsonResponse(json.RawMessage(`"wrong"`), ActorsResult{Actors: []ActorInfo{}}, nil), nil
				}
				return jsonResponse(m.ID, strings.Repeat("x", 8192), nil), nil
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}

			got, err := r.Actors(ctx)
			if mode == "full" {
				var detail *RemoteError
				if !errors.As(err, &detail) || detail.RPCCode != 1001 || detail.Code != "full" {
					t.Fatal(err)
				}
			}
			if err == nil || got.Actors != nil || errors.Is(err, ErrOutcomeUnknown) || calls > 1 {
				t.Fatal(got, err, calls)
			}
		})
	}
}

func TestActorsCloseCancelsInFlightQuery(t *testing.T) {
	entered := make(chan struct{})
	r, _ := newTestRuntime(t, func(req *http.Request) (*http.Response, error) {
		close(entered)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	done := make(chan error, 1)
	go func() {
		_, err := r.Actors(context.Background())
		done <- err
	}()
	waitValue(t, entered)
	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := waitValue(t, done); !errors.Is(err, context.Canceled) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
}
