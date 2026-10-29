package remotehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/internal/naming"
	"github.com/go-aie/actorx-dist/remote"
	"github.com/go-aie/actorx-dist/remote/protocol"
	"github.com/nats-io/nats.go"
)

type directoryNative struct{}

type directoryPing struct{ Value string }

func (*directoryNative) Ping(_ actorx.Context, m directoryPing) (directoryPing, error) { return m, nil }

func (a *directoryNative) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.Handle(a.Ping)}
}

func TestDirectoryHTTPClusterNATS(t *testing.T) {
	url := os.Getenv("ACTORX_REMOTE_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_REMOTE_NATS_URL to an isolated JetStream server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	namespace := directoryNamespace(t, url)
	var runtimes []*actorxdist.Runtime
	var gateways []*remote.Gateway
	for range 3 {
		rt, err := actorxdist.NewRuntime(ctx, actorxdist.WithNATS(url), actorxdist.WithNamespace(namespace))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := rt.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		g, err := remote.NewGateway(rt)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := g.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		runtimes = append(runtimes, rt)
		gateways = append(gateways, g)
	}

	register := func(index int, typ, description string) *remote.Registration {
		t.Helper()
		r, err := gateways[index].Register(ctx, protocol.RegisterParams{Type: typ, Description: description})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	register(0, "alpha", "local")
	remoteReg := register(1, "beta", "remote")
	register(0, "shared", "one")
	register(1, "shared", "two")
	register(0, "same", "same")
	register(1, "same", "same")
	if _, err := runtimes[1].Register(ctx, actorxdist.ActorSpec[*directoryNative]{
		Type:        "native",
		Description: "not Remote",
		New: func(actorxdist.ActorKey) *directoryNative {
			t.Error("discovery constructed native actor")
			return &directoryNative{}
		},
	}); err != nil {
		t.Fatal(err)
	}

	handler, err := NewHandler(gateways[0], Options{APIKey: testKey})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	query := func(params string) (int, protocol.Message) {
		t.Helper()
		body := `{"jsonrpc":"2.0","id":"directory","method":"runtime/actors"` + params + `}`
		req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/runtime/rpc", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+testKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(ProtocolHeader, protocol.Version)
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			return resp.StatusCode, protocol.Message{}
		}
		m, e := protocol.ParseMessage(raw)
		if e != nil {
			t.Fatal(string(raw), e)
		}
		return resp.StatusCode, m
	}

	decode := func(params string) protocol.ActorsResult {
		t.Helper()
		status, m := query(params)
		if status != 200 || m.Error != nil {
			t.Fatal(status, m.Error)
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(m.Result, &fields) != nil || len(fields) != 1 || fields["actors"] == nil {
			t.Fatalf("directory must contain only actors: %s", m.Result)
		}
		var result protocol.ActorsResult
		if err := json.Unmarshal(m.Result, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}

	// Both request forms use cluster discovery. Shared descriptions survive
	// only when providers agree, and native registrations are filtered out.
	for _, params := range []string{"", `,"params":{}`} {
		got := decode(params)
		if len(got.Actors) != 4 ||
			got.Actors[1].Type != "beta" ||
			got.Actors[1].Description != "remote" ||
			got.Actors[2].Description != "same" ||
			got.Actors[3].Description != "" {
			t.Fatalf("%+v", got)
		}
	}

	// Visibility does not remove the ingress Gateway's local provider check.
	_, err = gateways[0].Call(ctx, protocol.CallParams{
		Address: protocol.Address{Version: 1, Type: "beta", ID: "probe"},
		Input:   json.RawMessage(`null`),
	})
	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != protocol.ActorUnavailable {
		t.Fatal("cluster directory changed Call admission", err)
	}

	if err := remoteReg.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := decode(""); len(got.Actors) != 3 {
		t.Fatal("withdrawn registration visible", got)
	}

	for _, params := range []string{
		`,"params":{"scope":"local"}`,
		`,"params":{"scope":"cluster"}`,
		`,"params":{"scope":null}`,
		`,"params":{"scope":1}`,
		`,"params":{"scope":"all"}`,
		`,"params":{"window":1}`,
	} {
		_, m := query(params)
		if m.Error == nil || m.Error.Code != protocol.InvalidParams {
			t.Fatal(m)
		}
	}

	// A truncated node snapshot must fail the directory, even when that node
	// contains only native types that would otherwise be filtered out.
	var nativeRegistrations []*actorxdist.Registration
	for i := range 257 {
		r, err := runtimes[2].Register(ctx, actorxdist.ActorSpec[*directoryNative]{
			Type: fmt.Sprintf("native-%03d", i),
			New: func(actorxdist.ActorKey) *directoryNative {
				t.Error("directory constructed native actor")
				return &directoryNative{}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		nativeRegistrations = append(nativeRegistrations, r)
	}
	if status, m := query(""); status != 200 || m.Error == nil || m.Error.Code != protocol.Full || len(m.Result) != 0 {
		t.Fatal("truncated directory was not rejected", status, m)
	}
	for _, r := range nativeRegistrations {
		if err := r.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Full RPC encoding, including HTML escapes, must fit; no partial directory.
	for i := range 4 {
		register(1, fmt.Sprintf("large-%d", i), strings.Repeat("<", 512))
	}
	if status, _ := query(""); status != http.StatusRequestEntityTooLarge {
		t.Fatal("oversize directory was not rejected", status)
	}
}

func directoryNamespace(t *testing.T, url string) string {
	t.Helper()
	namespace := fmt.Sprintf("remote-directory-%d", time.Now().UnixNano())
	// Registered before Runtime cleanups, this runs after their Close calls.
	t.Cleanup(func() {
		nc, err := nats.Connect(url)
		if err != nil {
			t.Error(err)
			return
		}
		defer nc.Close()
		js, err := nc.JetStream()
		if err != nil {
			t.Error(err)
			return
		}
		names, _ := naming.New(namespace)
		for _, bucket := range []string{names.TypesBucket, names.CurrentBucket, names.ExitsBucket} {
			if err := js.DeleteKeyValue(bucket); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
				t.Error(err)
			}
		}
	})
	return namespace
}
