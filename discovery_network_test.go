package actorxdist

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/nats-io/nats.go"
)

type discoveryPipeDialer struct{ conn net.Conn }

func (d discoveryPipeDialer) Dial(string, string) (net.Conn, error) { return d.conn, nil }

// Only the Core NATS connection handshake is needed to exercise endpoint
// installation failures. This in-memory peer uses no listener or external server.
func discoveryPipeConnection(t *testing.T) *nats.Conn {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		if _, err := fmt.Fprint(server, "INFO {\"max_payload\":131072}\r\n"); err != nil {
			return
		}
		reader := bufio.NewReader(server)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == "PING\r\n" {
				if _, err := fmt.Fprint(server, "PONG\r\n"); err != nil {
					return
				}
			}
		}
	}()
	nc, err := nats.Connect(nats.DefaultURL, nats.SetCustomDialer(discoveryPipeDialer{client}), nats.NoReconnect())
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nc.Close()
		await(t, done)
	})
	return nc
}

func TestDiscoveryEndpointInitializationRollback(t *testing.T) {
	for _, mode := range []string{"first subscription", "second subscription", "flush", "success"} {
		t.Run(mode, func(t *testing.T) {
			rt := testRuntime(t)
			n := discoveryNetworkFixture(t, rt)
			n.nc = discoveryPipeConnection(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			switch mode {
			case "first subscription":
				n.nc.Close()
			case "second subscription":
				rt.runtimeID = "bad..subject" // fail after the query endpoint is installed
			case "flush":
				cancel() // both subscriptions are created before Flush observes this
			}

			err := n.subscribeDiscovery(ctx)
			if mode == "success" {
				if err != nil || n.nc.NumSubscriptions() != 2 || n.discoveryQueries.Queue != "" ||
					n.discoveryReplies.Queue != "" {
					t.Fatal("endpoints are missing or use queue groups", err)
				}
				frames := rt.limits.replyFrames()
				for _, tc := range []struct {
					sub *nats.Subscription
					max int
				}{{n.discoveryQueries, maxDiscoveryQueryBytes}, {n.discoveryReplies, maxDiscoverySnapshotBytes}} {
					messages, bytes, err := tc.sub.PendingLimits()
					if err != nil || messages != frames || bytes != frames*tc.max {
						t.Fatal(messages, bytes, err)
					}
				}
			} else if err == nil || n.nc.NumSubscriptions() != 0 {
				t.Fatal("initialization failure retained endpoints", err, n.nc.NumSubscriptions())
			}

			n.closeDiscovery()
			n.closeDiscovery() // endpoint teardown is idempotent
			if n.nc.NumSubscriptions() != 0 {
				t.Fatal("close retained endpoints")
			}
		})
	}
}

func discoveryNetworkFixture(t *testing.T, rt *Runtime) *network {
	t.Helper()
	return &network{
		rt:           rt,
		diagnostics:  make(chan Diagnostic, 64),
		ordinary:     make(chan incoming, 1),
		payloadSlots: make(chan struct{}, 1),
		codecSlots:   make(chan struct{}, 1),
	}
}

type discoveryReplyProbe struct {
	nodes chan discoveryNodeReply
}

func (p *discoveryReplyProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{actorx.HandleMessage(func(_ actorx.Context, node discoveryNodeReply) error {
		p.nodes <- node
		return nil
	})}
}

func TestDiscoveryReplyRoutingAndBudgets(t *testing.T) {
	rt := testRuntime(t)
	p := &discoveryReplyProbe{nodes: make(chan discoveryNodeReply, 8)}
	var err error
	rt.discovery, err = rt.management.Spawn(p)
	if err != nil {
		t.Fatal(err)
	}

	n := discoveryNetworkFixture(t, rt)
	id, runtimeID := token(t), token(t)
	data, err := encodeDiscoveryReply(id, discoverySnapshot{RuntimeID: runtimeID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		data []byte
		want error
	}{
		{data, nil},
		{[]byte(strings.Replace(string(data), `"version":1`, `"version":2`, 1)), ErrProtocol},
		{[]byte(strings.TrimSuffix(string(data), "}") + `,"error":"full"}`), actorx.ErrFull},
		{[]byte(strings.TrimSuffix(string(data), "}") + `,"error":"unavailable"}`), ErrOwnerUnavailable},
	} {
		n.discoveryReplyCallback(&nats.Msg{Data: tc.data})
		select {
		case got := <-p.nodes:
			if got.queryID != id || !errors.Is(got.err, tc.want) || (got.err == nil && got.snapshot.RuntimeID != runtimeID) {
				t.Fatal(got, tc.want)
			}
		case <-testContext(t).Done():
			t.Fatal("reply was not delivered")
		}
		if len(n.payloadSlots) != 0 || len(n.codecSlots) != 0 {
			t.Fatal("decode retained a buffer")
		}
	}

	n.discoveryReplyCallback(&nats.Msg{Data: []byte(`{"query_id":"unreliable"}`)})
	select {
	case got := <-p.nodes:
		t.Fatal("uncorrelated frame delivered", got)
	default:
	}

	// Payload, decoder and frame-size pressure all invalidate current receive
	// baselines, without allocating a query-specific failure table.
	n.payloadSlots <- struct{}{}
	n.discoveryReplyCallback(&nats.Msg{Data: data})
	<-n.payloadSlots
	n.codecSlots <- struct{}{}
	n.discoveryReplyCallback(&nats.Msg{Data: data})
	<-n.codecSlots
	n.discoveryReplyCallback(&nats.Msg{Data: make([]byte, maxDiscoverySnapshotBytes+1)})
	if n.discoveryFailures.Load() != 3 || len(n.payloadSlots) != 0 || len(n.codecSlots) != 0 {
		t.Fatal("lost reply was not recorded or buffer leaked", n.discoveryFailures.Load())
	}
	state := discoveryTransport{failures: 0}
	if !errors.Is(n.checkDiscovery(state), actorx.ErrFull) {
		t.Fatal("old receive baseline remained usable")
	}

	n.closeDiscovery()
	n.discoveryReplyCallback(&nats.Msg{Data: data})
	select {
	case got := <-p.nodes:
		t.Fatal("reply delivered after admission closed", got)
	default:
	}
}

func TestDiscoveryQueryDispatchAndValidation(t *testing.T) {
	rt := testRuntime(t)
	n := discoveryNetworkFixture(t, rt)
	var scans atomic.Int32
	controlledDiscovery(t, rt, func(ctx context.Context) (discoverySnapshot, error) {
		scans.Add(1)
		return rt.snapshotLocal(ctx)
	})
	query := discoveryQueryFrame{1, token(t), token(t), time.Now().Add(-time.Second).UnixMilli()}
	data, _ := json.Marshal(query)
	msg := &nats.Msg{Data: data, Reply: rt.names.DiscoveryReplySubject(query.OriginRuntimeID)}
	n.discoveryQueryCallback(msg)
	n.discoveryQueryCallback(msg) // payload slot is retained by the first request
	if len(n.ordinary) != 1 || len(n.payloadSlots) != 1 {
		t.Fatal("query dispatch was not bounded")
	}
	n.handleDiscovery(<-n.ordinary) // expired: no scan and no connection required
	if len(n.payloadSlots) != 0 || scans.Load() != 0 {
		t.Fatal("expired query scanned or leaked its slot")
	}

	query.DeadlineUnixMS = time.Now().Add(time.Second).UnixMilli()
	data, _ = json.Marshal(query)
	for _, reply := range []string{
		"", "_INBOX.arbitrary", rt.names.DiscoveryReplySubject(token(t)),
		rt.names.DiscoveryReplySubject(query.OriginRuntimeID) + ".*",
		"actorxdist.other.discovery.v1.reply." + query.OriginRuntimeID,
		rt.names.DiscoveryReplySubject(query.OriginRuntimeID) + "\r\n",
	} {
		n.discoveryQueryCallback(&nats.Msg{Data: data, Reply: reply})
		n.handleDiscovery(<-n.ordinary)
	}

	query.OriginRuntimeID = rt.runtimeID
	data, _ = json.Marshal(query)
	n.discoveryQueryCallback(&nats.Msg{Data: data, Reply: rt.names.DiscoveryReplySubject(rt.runtimeID)})
	n.handleDiscovery(<-n.ordinary) // own broadcast never scans again
	if scans.Load() != 0 || len(n.payloadSlots) != 0 {
		t.Fatal("invalid or own query scanned", scans.Load())
	}

	// Ordinary dispatch pressure uses no lifecycle control reserve.
	n.ordinary <- incoming{}
	n.discoveryQueryCallback(msg)
	if len(n.payloadSlots) != 0 {
		t.Fatal("rejected dispatch retained payload capacity")
	}
	<-n.ordinary
	n.closeDiscovery()
	n.discoveryQueryCallback(msg)
	if len(n.ordinary) != 0 || len(n.payloadSlots) != 0 {
		t.Fatal("closed endpoint retained work")
	}
}

func TestDiscoveryReplyMailboxLoss(t *testing.T) {
	rt := testRuntime(t)
	p := &discoveryMailboxProbe{
		discoveryActor: &discoveryActor{rt: rt, snapshot: rt.snapshotLocal},
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
		drained:        make(chan struct{}),
		remaining:      rt.limits.managementMailbox(),
	}
	defer close(p.release)
	addr, err := rt.management.Spawn(p)
	if err != nil {
		t.Fatal(err)
	}
	rt.discovery = addr
	if err := addr.Send(testContext(t), discoveryMailboxRequest{Hold: true}); err != nil {
		t.Fatal(err)
	}
	await(t, p.entered)
	for range p.remaining {
		if err := addr.Send(testContext(t), discoveryMailboxRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	n := discoveryNetworkFixture(t, rt)
	data, _ := encodeDiscoveryReply(token(t), discoverySnapshot{RuntimeID: token(t)}, nil)
	n.discoveryReplyCallback(&nats.Msg{Data: data})
	if n.discoveryFailures.Load() != 1 || len(n.payloadSlots) != 0 {
		t.Fatal("mailbox loss was hidden or retained a buffer")
	}
}

func TestDiscoveryEpochNotification(t *testing.T) {
	var ready readiness
	if !ready.resume(ready.epoch()) {
		t.Fatal("initial resume failed")
	}
	for range 64 {
		old, lost := ready.observe()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready.Store(false)
		}()
		// observe competes with fault delivery; the returned pair must describe
		// one epoch, even if another fault or reconnect follows immediately.
		observed, signal := ready.observe()
		wg.Wait()
		await(t, lost)
		if observed == old {
			await(t, signal)
		}
		if ready.resume(old) || !ready.resume(ready.epoch()) {
			t.Fatal("fault was erased by a stale reconnect")
		}
		_, current := ready.observe()
		select {
		case <-current:
			t.Fatal("new epoch inherited the old loss signal")
		default:
		}
	}
}

func TestDiscoveryPublishChecks(t *testing.T) {
	rt := testRuntime(t)
	n := discoveryNetworkFixture(t, rt)
	n.nc = discoveryPipeConnection(t)
	if err := n.subscribeDiscovery(testContext(t)); err != nil {
		t.Fatal(err)
	}
	defer n.closeDiscovery()
	n.ready.resume(n.ready.epoch())
	state, err := n.observeDiscovery()
	if err != nil {
		t.Fatal(err)
	}
	query := discoveryQueryFrame{1, token(t), rt.runtimeID, time.Now().Add(time.Second).UnixMilli()}
	if err := n.publishDiscovery(testContext(t), query, state); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(testContext(t))
	cancel()
	if err := n.publishDiscovery(ctx, query, state); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled publication succeeded", err)
	}

	n.codecSlots <- struct{}{}
	if err := n.publishDiscovery(testContext(t), query, state); !errors.Is(err, actorx.ErrFull) {
		t.Fatal("publication escaped codec budget", err)
	}
	<-n.codecSlots
	for _, bad := range []discoveryQueryFrame{
		{2, query.QueryID, rt.runtimeID, query.DeadlineUnixMS},
		{1, "bad", rt.runtimeID, query.DeadlineUnixMS},
		{1, strings.Repeat("x", maxDiscoveryQueryBytes), rt.runtimeID, query.DeadlineUnixMS},
		{1, query.QueryID, "bad", query.DeadlineUnixMS},
		{1, query.QueryID, token(t), query.DeadlineUnixMS},
		{1, query.QueryID, rt.runtimeID, 0},
		{1, query.QueryID, rt.runtimeID, -1},
	} {
		if err := n.publishDiscovery(testContext(t), bad, state); !errors.Is(err, ErrProtocol) {
			t.Fatal("publication accepted an invalid query", bad, err)
		}
	}
	bad := query
	bad.DeadlineUnixMS = time.Now().Add(-time.Second).UnixMilli()
	if err := n.publishDiscovery(testContext(t), bad, state); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired publication succeeded", err)
	}
	n.ready.Store(false)
	await(t, state.lost)
	n.ready.resume(n.ready.epoch())
	if err := n.publishDiscovery(testContext(t), query, state); !errors.Is(err, ErrOwnerUnavailable) {
		t.Fatal("old epoch survived reconnect", err)
	}
	current, err := n.observeDiscovery()
	if err != nil {
		t.Fatal(err)
	}
	n.discoveryReceiveFailure()
	if err := n.checkDiscovery(current); !errors.Is(err, actorx.ErrFull) {
		t.Fatal("receive loss did not invalidate captured baseline", err)
	}
	rt.beginClose()
	if err := n.checkDiscovery(current); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("closed Runtime remained usable", err)
	}
}
