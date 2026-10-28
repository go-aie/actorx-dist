package remote

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

// Freeze the pre-unification binding sets. Receiver names do not enter actorx-dist's
// wire contract; message/result types and operation kinds do.
type legacyStreamProxy struct{ proxyActor }

func (a *legacyStreamProxy) Started(ctx actorx.Context, m actorx.Started) error {
	return a.proxyActor.Started(ctx, m)
}

func (a *legacyStreamProxy) Invoke(ctx actorx.Context, i protocol.Invoke, r actorx.Reply[protocol.Result]) error {
	return a.proxyActor.Invoke(ctx, i, r)
}

func (a *legacyStreamProxy) InvokeStream(ctx actorx.Context, i protocol.InvokeStream, r actorx.StreamReply[protocol.Chunk]) error {
	return a.proxyActor.InvokeStream(ctx, i, r)
}

func (a *legacyStreamProxy) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.HandleDeferred(a.Invoke), actorx.HandleStream(a.InvokeStream), actorx.HandleMessage(a.Started)}
}

type legacyCallProxy struct{ proxyActor }

func (a *legacyCallProxy) Started(ctx actorx.Context, m actorx.Started) error {
	return a.proxyActor.Started(ctx, m)
}

func (a *legacyCallProxy) Invoke(ctx actorx.Context, i protocol.Invoke, r actorx.Reply[protocol.Result]) error {
	return a.proxyActor.Invoke(ctx, i, r)
}

func (a *legacyCallProxy) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.HandleDeferred(a.Invoke), actorx.HandleMessage(a.Started)}
}

// Exercise the real immutable Registry contract check, including after
// Unregister. Own random Types only; never delete shared Registry data.
func TestProxyContractNATS(t *testing.T) {
	url := os.Getenv("ACTORX_REMOTE_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_REMOTE_NATS_URL to an isolated JetStream server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rt, err := actorxdist.NewRuntime(ctx, actorxdist.WithNATS(url))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rt.Close(testCtx(t)); err != nil {
			t.Error(err)
		}
	}()
	g, err := NewGateway(rt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := g.Close(testCtx(t)); err != nil {
			t.Error(err)
		}
	}()
	for _, stream := range []bool{true, false} {
		typ := fmt.Sprintf("remote-contract-%x-%t", time.Now().UnixNano(), stream)
		var old *actorxdist.Registration
		if stream {
			old, err = rt.Register(ctx, actorxdist.ActorSpec[*legacyStreamProxy]{
				Type: typ,
				New:  func(actorxdist.ActorKey) *legacyStreamProxy { return &legacyStreamProxy{} },
			})
		} else {
			old, err = rt.Register(ctx, actorxdist.ActorSpec[*legacyCallProxy]{
				Type: typ,
				New:  func(actorxdist.ActorKey) *legacyCallProxy { return &legacyCallProxy{} },
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := old.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
		reg, err := g.Register(ctx, protocol.RegisterParams{Type: typ})
		if stream {
			if err != nil {
				t.Fatalf("old Stream contract must match unified proxy: %v", err)
			}
		} else {
			rpcCode(t, err, protocol.Contract)
		}
		if reg != nil {
			if err := reg.Close(testCtx(t)); err != nil {
				t.Fatal(err)
			}
		}
	}
}
