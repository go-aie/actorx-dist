package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/naming"
	"github.com/nats-io/nats.go"
)

// The collector belongs only to this test probe. Production keeps one inbox
// per Runtime and delivers decoded replies to its local Discovery Actor.
type discoveryNATSProbe struct {
	nc    *nats.Conn
	sub   *nats.Subscription
	query discoveryQueryFrame
	inbox string
}

func newDiscoveryNATSProbe(t *testing.T, url string, rt *Runtime, end time.Time) *discoveryNATSProbe {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	query := discoveryQueryFrame{1, token(t), token(t), end.UnixMilli()}
	inbox := rt.names.DiscoveryReplySubject(query.OriginRuntimeID)
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	if err = sub.SetPendingLimits(8, 8*maxDiscoverySnapshotBytes); err != nil {
		t.Fatal(err)
	}
	if err = nc.FlushWithContext(testContext(t)); err != nil {
		t.Fatal(err)
	}
	return &discoveryNATSProbe{nc, sub, query, inbox}
}

func (p *discoveryNATSProbe) publish(t *testing.T, rt *Runtime) {
	t.Helper()
	data, _ := json.Marshal(p.query)
	if err := p.nc.PublishRequest(rt.names.DiscoveryQuerySubject(), p.inbox, data); err != nil {
		t.Fatal(err)
	}
}

func (p *discoveryNATSProbe) next(t *testing.T) discoveryReplyFrame {
	t.Helper()
	msg, err := p.sub.NextMsgWithContext(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	frame, queryID, err := decodeDiscoveryReply(msg.Data)
	if err != nil || queryID != p.query.QueryID {
		t.Fatal(frame, queryID, err)
	}
	return frame
}

// Install controlled work before connecting, then use the same connection and
// subscription setup as NewRuntime. No Actor dependencies change while live.
func connectDiscoveryFixture(t *testing.T, rt *Runtime, url string) {
	t.Helper()
	ctx := testContext(t)
	opts, err := connectionOptions(ctx, natsConfig{url: url})
	if err == nil {
		err = rt.connect(ctx, opts)
	}
	if err == nil {
		err = rt.net.subscribe(ctx)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// Allocate only this test's namespace, then remove its Registry buckets after
// every Runtime has closed. Repeated gates do not accumulate storage reservations.
func discoveryNATSNamespace(t *testing.T, url, prefix string) string {
	t.Helper()
	namespace := fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
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

func discoveryClusterNATSRuntime(t *testing.T, url, namespace string) *Runtime {
	t.Helper()
	return testRuntimeWithOptions(t, []Option{WithNATS(url), WithNamespace(namespace)})
}

func TestDiscoveryClusterPublicNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	namespace := discoveryNATSNamespace(t, url, "dsc")
	a, b, empty := discoveryClusterNATSRuntime(t, url, namespace),
		discoveryClusterNATSRuntime(t, url, namespace),
		discoveryClusterNATSRuntime(t, url, namespace)
	registrations := make([]*Registration, 2)
	for i, rt := range []*Runtime{a, b} {
		r, err := rt.Register(testContext(t), ActorSpec[*counter]{
			Type:        "shared",
			Description: fmt.Sprintf("原生计数器 %d", i),
			New: func(ActorKey) *counter {
				t.Error("discovery activated an instance")
				return &counter{}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		registrations[i] = r
	}
	if _, err := b.Register(testContext(t), ActorSpec[*counter]{
		Type: "other",
		New:  func(ActorKey) *counter { return &counter{} },
	}); err != nil {
		t.Fatal(err)
	}

	for _, rt := range []*Runtime{a, b, empty} {
		report, err := rt.Discover(testContext(t), DiscoveryQuery{Scope: DiscoveryCluster})
		if err != nil || report.Scope != DiscoveryCluster || report.EndReason != DiscoveryEndWindow ||
			report.Truncated || len(report.Responders) != 3 || len(report.Actors) != 2 {
			t.Fatal(report, err)
		}
		actor := report.Actors[1]
		descriptions := map[string]string{}
		for _, p := range actor.Providers {
			descriptions[p.RuntimeID] = p.Description
		}
		if actor.Type != "shared" || actor.Fingerprint != a.specs["shared"].contract.fingerprint ||
			!reflect.DeepEqual(descriptions, map[string]string{a.runtimeID: "原生计数器 0", b.runtimeID: "原生计数器 1"}) {
			t.Fatal(actor)
		}
	}

	// Withdrawing a registration leaves a historical contract, but removes this source.
	if err := registrations[0].Unregister(testContext(t)); err != nil {
		t.Fatal(err)
	}
	report, err := empty.Discover(testContext(t), DiscoveryQuery{Scope: DiscoveryCluster, Window: 100 * time.Millisecond})
	if err != nil || len(report.Actors) != 2 || len(report.Actors[1].Providers) != 1 ||
		report.Actors[1].Providers[0].RuntimeID != b.runtimeID {
		t.Fatal(report, err)
	}

	// Three public queries coexist on one fixed reply inbox.
	results := make([]<-chan discoveryResult, 3)
	for i := range results {
		results[i] = startCluster(empty, testContext(t), 100*time.Millisecond)
	}
	for _, result := range results {
		got := <-result
		if got.err != nil || len(got.report.Responders) != 3 || len(got.report.Actors) != 2 {
			t.Fatal(got)
		}
	}
	for _, rt := range []*Runtime{a, b, empty} {
		if len(rt.ledger) != 0 {
			t.Fatal("discovery constructed business instances")
		}
	}

	// The empty node has no Factory or lazy Call inbox: both permanent discovery
	// subscriptions must nevertheless exist and be removed by Close.
	if empty.net.nc.NumSubscriptions() != 2 {
		t.Fatal("empty Runtime lacks its two discovery endpoints", empty.net.nc.NumSubscriptions())
	}
	if err := empty.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if empty.net.discoveryQueries.IsValid() || empty.net.discoveryReplies.IsValid() ||
		len(empty.net.payloadSlots) != 0 || len(empty.net.ordinary) != 0 {
		t.Fatal("closed Runtime retained endpoints or buffers")
	}

	// A fresh namespace with no peers still returns its local empty snapshot.
	solo := discoveryClusterNATSRuntime(t, url, discoveryNATSNamespace(t, url, "solo"))
	start := time.Now()
	report, err = solo.Discover(testContext(t), DiscoveryQuery{Scope: DiscoveryCluster, Window: 40 * time.Millisecond})
	if err != nil || len(report.Actors) != 0 || !reflect.DeepEqual(report.Responders, []string{solo.runtimeID}) ||
		report.EndReason != DiscoveryEndWindow || time.Since(start) < 40*time.Millisecond {
		t.Fatal(report, err)
	}
}

func TestDiscoveryClusterFaultsNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	for _, mode := range []string{"malformed", "contract", "duplicate", "remote full", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			rt := discoveryClusterNATSRuntime(t, url, discoveryNATSNamespace(t, url, "fault"))
			// Observe the public broadcast to obtain an actual live QueryID. Injected
			// frames still traverse the permanent inbox, decoder and Actor mailbox.
			nc, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			sub, err := nc.SubscribeSync(rt.names.DiscoveryQuerySubject())
			if err != nil {
				t.Fatal(err)
			}
			if err := nc.FlushWithContext(testContext(t)); err != nil {
				t.Fatal(err)
			}

			result := startCluster(rt, testContext(t), 500*time.Millisecond)
			msg, err := sub.NextMsgWithContext(testContext(t))
			if err != nil {
				t.Fatal(err)
			}
			query, err := decodeDiscoveryQuery(msg.Data)
			if err != nil {
				t.Fatal(err)
			}

			send := func(data []byte) {
				t.Helper()
				if err := nc.Publish(rt.net.discoveryInbox, data); err != nil {
					t.Fatal(err)
				}
				if err := nc.FlushWithContext(testContext(t)); err != nil {
					t.Fatal(err)
				}
			}
			snapshot := discoverySnapshot{token(t), discoveryEntries(1, "remote"), false}
			data, err := encodeDiscoveryReply(query.QueryID, snapshot, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := ErrOwnerUnavailable
			switch mode {
			case "malformed":
				want = ErrProtocol
				send([]byte(strings.Replace(string(data), `"version":1`, `"version":9`, 1)))
			case "contract", "duplicate":
				// Each frame arrives in wire order on the same NATS connection.
				send(data)
				if mode == "contract" {
					snapshot.RuntimeID = token(t)
					snapshot.Entries[0].Fingerprint = strings.Repeat("b", 64)
					want = ErrContract
				} else {
					snapshot.Entries[0].Description = "contradictory"
					want = ErrProtocol
				}
				data, _ = encodeDiscoveryReply(query.QueryID, snapshot, nil)
				send(data)
			case "remote full":
				want = actorx.ErrFull
				data, _ = encodeDiscoveryReply(query.QueryID, snapshot, want)
				send(data)
			case "disconnect":
				if err := rt.net.nc.ForceReconnect(); err != nil {
					t.Fatal(err)
				}
			}
			assertDiscoveryError(t, result, want)
			if mode == "disconnect" {
				// Wait for a fresh transport epoch; the old query must stay failed.
				deadline := time.Now().Add(3 * time.Second)
				for {
					if _, err := rt.net.observeDiscovery(); err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("no reconnect")
					}
					time.Sleep(time.Millisecond)
				}
				report, err := rt.Discover(testContext(t), DiscoveryQuery{Scope: DiscoveryCluster, Window: 30 * time.Millisecond})
				if err != nil || report.EndReason != DiscoveryEndWindow || len(report.Responders) != 1 {
					t.Fatal("new query after reconnect", report, err)
				}
			}
		})
	}
}

func TestDiscoveryTransportNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	t.Run("full and unavailable feedback", func(t *testing.T) {
		rt := testRuntime(t, WithNamespace(discoveryNATSNamespace(t, url, "transport")))
		capacity := rt.limits.managementTasks()
		entered, release := make(chan struct{}, capacity), make(chan struct{})
		defer close(release)
		controlledDiscovery(t, rt, func(ctx context.Context) (discoverySnapshot, error) {
			entered <- struct{}{}
			select {
			case <-ctx.Done():
				return discoverySnapshot{}, ctx.Err()
			case <-release:
				return rt.snapshotLocal(ctx)
			}
		})
		connectDiscoveryFixture(t, rt, url)
		ctx, cancel := context.WithCancel(testContext(t))
		defer cancel()
		results := make(chan error, capacity)
		for range capacity {
			go func() {
				_, err := rt.Discover(ctx, DiscoveryQuery{})
				results <- err
			}()
			await(t, entered)
		}

		p := newDiscoveryNATSProbe(t, url, rt, time.Now().Add(time.Second))
		p.publish(t, rt)
		if frame := p.next(t); frame.Error != "full" || len(frame.Entries) != 0 || frame.Truncated {
			t.Fatal("Task overload did not return full", frame)
		}
		cancel()
		for range capacity {
			if err := <-results; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		rt.net.ready.Store(false)
		p.publish(t, rt)
		if frame := p.next(t); frame.Error != "unavailable" {
			t.Fatal("unavailable transport appeared as empty directory", frame)
		}
	})
	t.Run("deadline cancels node scan and close joins it", func(t *testing.T) {
		rt := testRuntime(t, WithNamespace(discoveryNATSNamespace(t, url, "transport")))
		entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		controlledDiscovery(t, rt, func(ctx context.Context) (discoverySnapshot, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return discoverySnapshot{RuntimeID: rt.runtimeID}, nil
		})
		connectDiscoveryFixture(t, rt, url)
		p := newDiscoveryNATSProbe(t, url, rt, time.Now().Add(250*time.Millisecond))
		p.publish(t, rt)
		await(t, entered)
		await(t, canceled)
		closed := make(chan error, 1)
		closeCtx := testContext(t)
		go func() { closed <- rt.Close(closeCtx) }()
		await(t, rt.life.Done())
		select {
		case err := <-closed:
			t.Fatal("Close did not join canceled snapshot work", err)
		default:
		}
		unblock()
		if err := <-closed; err != nil || len(rt.net.payloadSlots) != 0 {
			t.Fatal("Close leaked transport work", err)
		}
		if _, err := p.sub.NextMsg(30 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
			t.Fatal("expired query received a late snapshot", err)
		}
	})
}
