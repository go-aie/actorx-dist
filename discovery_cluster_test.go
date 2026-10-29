package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

func discoveryEntries(n int, description string) []discoveryEntry {
	entries := make([]discoveryEntry, n)
	for i := range entries {
		entries[i] = discoveryEntry{fmt.Sprintf("type%03d", i), strings.Repeat("a", 64), description}
	}
	return entries
}

func TestDiscoveryBuilderAtomicMerge(t *testing.T) {
	b := newDiscoveryBuilder()
	a, c := token(t), token(t)
	entries := discoveryEntries(2, "本地")
	if ok, err := b.merge(discoverySnapshot{a, entries, false}); !ok || err != nil {
		t.Fatal(ok, err)
	}

	reversed := []discoveryEntry{entries[1], entries[0]}
	if ok, err := b.merge(discoverySnapshot{a, reversed, false}); !ok || err != nil {
		t.Fatal("reordered duplicate", ok, err)
	}
	other := discoveryEntries(2, "remote")
	if ok, err := b.merge(discoverySnapshot{c, other, true}); !ok || err != nil {
		t.Fatal(ok, err)
	}
	empty := token(t)
	if ok, err := b.merge(discoverySnapshot{RuntimeID: empty}); !ok || err != nil {
		t.Fatal(ok, err)
	}

	report := b.report(DiscoveryEndWindow)
	if len(report.Responders) != 3 || len(report.Actors) != 2 || len(report.Actors[0].Providers) != 2 ||
		!report.Truncated || report.EndReason != DiscoveryEndWindow {
		t.Fatal(report)
	}
	encoded, _ := json.Marshal(report)
	if len(encoded) > b.bytes || b.bytes-len(encoded) != 1 {
		t.Fatal("byte accounting", len(encoded), b.bytes)
	}

	// Invalid nodes must leave the previously accepted report unchanged.
	for _, tc := range []struct {
		snapshot discoverySnapshot
		want     error
	}{
		{discoverySnapshot{a, other, false}, ErrProtocol},
		{discoverySnapshot{a, entries, true}, ErrProtocol},
		{
			discoverySnapshot{
				token(t),
				[]discoveryEntry{
					{"new", strings.Repeat("a", 64), ""},
					{entries[0].Type, strings.Repeat("b", 64), ""},
				},
				false,
			},
			ErrContract,
		},
		{discoverySnapshot{token(t), []discoveryEntry{entries[0], entries[0]}, false}, ErrProtocol},
		{discoverySnapshot{RuntimeID: "invalid"}, ErrProtocol},
	} {
		if _, err := b.merge(tc.snapshot); !errors.Is(err, tc.want) {
			t.Fatal(err, tc.want)
		}
		if !reflect.DeepEqual(report, b.report(DiscoveryEndWindow)) {
			t.Fatal("invalid node partially committed")
		}
	}

	// Returned slices must not alias the builder, including nested providers.
	provider, responder := report.Actors[0].Providers[0], report.Responders[0]
	report.Actors[0].Providers[0].Description = "mutated"
	report.Responders[0] = "mutated"
	fresh := b.report(DiscoveryEndWindow)
	if fresh.Actors[0].Providers[0] != provider {
		t.Fatal("report Providers alias builder")
	}
	if fresh.Responders[0] != responder {
		t.Fatal("report Responders alias builder")
	}
}

func TestDiscoveryBuilderSnapshotByteBoundary(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		t.Run(fmt.Sprintf("truncated=%t", truncated), func(t *testing.T) {
			entries := discoveryEntries(maxDiscoveryEntries, strings.Repeat("x", 100))
			entries[0].Description = "说明\x00\n\"\\<>&"
			snapshot := discoverySnapshot{token(t), entries, truncated}
			base, _ := json.Marshal(snapshot)
			gap := maxDiscoverySnapshotBytes - len(base)
			for i := range entries {
				extra := min(gap, 512-len(entries[i].Description))
				entries[i].Description += strings.Repeat("x", extra)
				gap -= extra
			}
			data, _ := json.Marshal(snapshot)
			if gap != 0 || len(data) != maxDiscoverySnapshotBytes {
				t.Fatal("fixture could not fill the byte limit", gap, len(data))
			}
			if ok, err := newDiscoveryBuilder().merge(snapshot); !ok || err != nil {
				t.Fatal("exact snapshot boundary was rejected", ok, err)
			}

			// Add one byte beyond the exact budget; none of the node may commit.
			for i := range entries {
				if len(entries[i].Description) < 512 {
					entries[i].Description += "x"
					break
				}
			}
			b := newDiscoveryBuilder()
			if _, err := b.merge(snapshot); !errors.Is(err, ErrProtocol) || len(b.nodes) != 0 || len(b.actors) != 0 {
				t.Fatal("oversized snapshot was not rejected atomically", err)
			}
		})
	}
}

func TestDiscoveryBuilderLimits(t *testing.T) {
	for _, mode := range []string{"responders", "sources", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			b := newDiscoveryBuilder()
			count, description := 0, ""
			if mode == "sources" {
				count = maxDiscoveryEntries
			}
			if mode == "bytes" {
				count, description = 18, strings.Repeat("\x00", 512)
			}

			for i := 0; i <= maxDiscoveryResponders; i++ {
				before := b.report(DiscoveryEndWindow)
				ok, err := b.merge(discoverySnapshot{token(t), discoveryEntries(count, description), false})
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					if !reflect.DeepEqual(before, b.report(DiscoveryEndWindow)) {
						t.Fatal("capacity partially committed node")
					}
					report := b.report(DiscoveryEndLimit)
					data, _ := json.Marshal(report)
					if !report.Truncated || len(data) > maxDiscoveryReportBytes || b.sources > maxDiscoverySources ||
						len(b.nodes) > maxDiscoveryResponders {
						t.Fatal(len(data), b.sources, len(b.nodes))
					}
					if mode == "responders" && len(b.nodes) != 256 || mode == "sources" && b.sources != 4096 ||
						mode == "bytes" && (b.sources >= 4096 || len(b.nodes) >= 256) {
						t.Fatal("wrong budget ended query", mode, b.sources, len(b.nodes))
					}
					return
				}
				data, _ := json.Marshal(b.report(DiscoveryEndWindow))
				if len(data) != b.bytes {
					t.Fatal("inaccurate encoding budget", len(data), b.bytes)
				}
			}
			t.Fatal("budget was not enforced")
		})
	}
}

type discoveryFakeTransport struct {
	ready     readiness
	failures  atomic.Uint64
	published chan discoveryQueryFrame
	publish   func(context.Context) error
}

func (f *discoveryFakeTransport) observeDiscovery() (discoveryTransport, error) {
	epoch, lost := f.ready.observe()
	state := discoveryTransport{epoch, lost, f.failures.Load()}
	return state, f.checkDiscovery(state)
}

func (f *discoveryFakeTransport) checkDiscovery(s discoveryTransport) error {
	if s.failures != f.failures.Load() {
		return actorx.ErrFull
	}
	if s.epoch&1 == 0 || s.epoch != f.ready.epoch() {
		return ErrOwnerUnavailable
	}
	return nil
}

func (f *discoveryFakeTransport) publishDiscovery(ctx context.Context, q discoveryQueryFrame, s discoveryTransport) error {
	if err := f.checkDiscovery(s); err != nil {
		return err
	}
	f.published <- q
	if f.publish != nil {
		return f.publish(ctx)
	}
	return nil
}

type discoveryInspect struct{}

type discoveryCounts struct{ active, queries int }

type discoveryClusterProbe struct{ *discoveryMailboxProbe }

func (p *discoveryClusterProbe) Bindings() []actorx.Binding {
	return append(p.discoveryMailboxProbe.Bindings(), actorx.Handle(func(actorx.Context, discoveryInspect) (discoveryCounts, error) {
		return discoveryCounts{p.querySlots, len(p.queries)}, nil
	}))
}

func clusterFixture(
	t *testing.T,
	snapshot func(context.Context) (discoverySnapshot, error),
	publish func(context.Context) error,
) (*Runtime, *discoveryFakeTransport, *discoveryClusterProbe) {
	t.Helper()
	rt := testRuntime(t)
	if snapshot == nil {
		snapshot = rt.snapshotLocal
	}
	f := &discoveryFakeTransport{published: make(chan discoveryQueryFrame, 16), publish: publish}
	f.ready.resume(f.ready.epoch())
	p := &discoveryClusterProbe{&discoveryMailboxProbe{
		discoveryActor: &discoveryActor{rt: rt, snapshot: snapshot, transport: f},
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
		drained:        make(chan struct{}),
	}}
	var err error
	rt.discovery, err = rt.management.Spawn(p)
	if err != nil {
		t.Fatal(err)
	}
	return rt, f, p
}

type discoveryResult struct {
	report DiscoveryReport
	err    error
}

func startCluster(rt *Runtime, ctx context.Context, window time.Duration) <-chan discoveryResult {
	result := make(chan discoveryResult, 1)
	go func() {
		report, err := rt.Discover(ctx, DiscoveryQuery{Scope: DiscoveryCluster, Window: window})
		result <- discoveryResult{report, err}
	}()
	return result
}

func nextDiscoveryQuery(t *testing.T, f *discoveryFakeTransport) discoveryQueryFrame {
	t.Helper()
	select {
	case q := <-f.published:
		return q
	case <-testContext(t).Done():
		t.Fatal("no publication")
		return discoveryQueryFrame{}
	}
}

func assertDiscoveryError(t *testing.T, result <-chan discoveryResult, want error) {
	t.Helper()
	select {
	case got := <-result:
		if !errors.Is(got.err, want) || !reflect.DeepEqual(got.report, DiscoveryReport{}) {
			t.Fatal(got, want)
		}
	case <-testContext(t).Done():
		t.Fatal("query did not finish")
	}
}

func discoveryBarrier(t *testing.T, rt *Runtime) discoveryCounts {
	t.Helper()
	counts, err := rt.discovery.Call[discoveryCounts](testContext(t), discoveryInspect{})
	if err != nil {
		t.Fatal(err)
	}
	return counts
}

func sendDiscoveryNode(t *testing.T, rt *Runtime, q string, s discoverySnapshot, err error) {
	t.Helper()
	if err := rt.discovery.Send(testContext(t), discoveryNodeReply{q, s, err}); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryClusterAssociationAndDuplicates(t *testing.T) {
	rt, f, _ := clusterFixture(t, nil, nil)
	first := startCluster(rt, testContext(t), 100*time.Millisecond)
	q1 := nextDiscoveryQuery(t, f)
	second := startCluster(rt, testContext(t), 100*time.Millisecond)
	q2 := nextDiscoveryQuery(t, f)

	// Interleave replies for both live queries, a duplicate and an unknown ID.
	node := discoverySnapshot{token(t), discoveryEntries(2, "remote"), true}
	sendDiscoveryNode(t, rt, q1.QueryID, node, nil)
	sendDiscoveryNode(t, rt, q1.QueryID, node, nil)
	sendDiscoveryNode(t, rt, token(t), discoverySnapshot{}, ErrProtocol)
	sendDiscoveryNode(t, rt, q2.QueryID, discoverySnapshot{RuntimeID: token(t)}, nil)

	// The accepted local snapshot wins even if registration changes during collection.
	sendDiscoveryNode(t, rt, q1.QueryID, discoverySnapshot{RuntimeID: rt.runtimeID, Entries: discoveryEntries(1, "changed")}, nil)

	a, b := <-first, <-second
	if a.err != nil || b.err != nil || len(a.report.Actors) != 2 || len(b.report.Actors) != 0 ||
		len(a.report.Responders) != 2 || len(b.report.Responders) != 2 || !a.report.Truncated ||
		a.report.EndReason != DiscoveryEndWindow {
		t.Fatal(a, b)
	}

	// Completion removes the query; a late error cannot recreate it.
	sendDiscoveryNode(t, rt, q1.QueryID, node, ErrContract)
	if counts := discoveryBarrier(t, rt); counts != (discoveryCounts{}) {
		t.Fatal("late reply recreated state", counts)
	}
}

// Builder tests exercise every budget; this checks that the Actor turns a
// rejected whole-node merge into one successful, truncated Limit result.
func TestDiscoveryClusterLimit(t *testing.T) {
	rt, f, _ := clusterFixture(t, nil, nil)
	result := startCluster(rt, testContext(t), 2*time.Second)
	q := nextDiscoveryQuery(t, f)
	for range maxDiscoveryResponders {
		sendDiscoveryNode(t, rt, q.QueryID, discoverySnapshot{RuntimeID: token(t)}, nil)
		discoveryBarrier(t, rt) // pace replies without filling the mailbox
	}
	got := <-result
	if got.err != nil || !got.report.Truncated || got.report.EndReason != DiscoveryEndLimit ||
		len(got.report.Responders) != maxDiscoveryResponders {
		t.Fatal(got)
	}
}

func TestDiscoveryClusterFailures(t *testing.T) {
	for _, mode := range []string{
		"protocol",
		"contract",
		"contradictory duplicate",
		"remote full",
		"remote unavailable",
		"receive loss",
		"disconnect",
		"disconnect and reconnect",
		"publish failure",
		"cancel",
		"deadline",
		"close",
	} {
		t.Run(mode, func(t *testing.T) {
			var publish func(context.Context) error
			if mode == "publish failure" {
				publish = func(context.Context) error { return ErrOwnerUnavailable }
			}
			rt, f, _ := clusterFixture(t, nil, publish)
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(testContext(t), 80*time.Millisecond)
				defer cancel()
			}
			result := startCluster(rt, ctx, time.Second)
			q := nextDiscoveryQuery(t, f)
			node := discoverySnapshot{token(t), discoveryEntries(1, "remote"), false}
			want := ErrOwnerUnavailable
			switch mode {
			case "protocol":
				want = ErrProtocol
				sendDiscoveryNode(t, rt, q.QueryID, discoverySnapshot{}, ErrProtocol)
			case "contract":
				sendDiscoveryNode(t, rt, q.QueryID, node, nil)
				discoveryBarrier(t, rt)
				node.RuntimeID = token(t)
				node.Entries[0].Fingerprint = strings.Repeat("b", 64)
				want = ErrContract
				sendDiscoveryNode(t, rt, q.QueryID, node, nil)
			case "contradictory duplicate":
				sendDiscoveryNode(t, rt, q.QueryID, node, nil)
				discoveryBarrier(t, rt)
				node.Entries = discoveryEntries(1, "changed")
				want = ErrProtocol
				sendDiscoveryNode(t, rt, q.QueryID, node, nil)
			case "remote full":
				want = actorx.ErrFull
				sendDiscoveryNode(t, rt, q.QueryID, node, want)
			case "remote unavailable":
				sendDiscoveryNode(t, rt, q.QueryID, node, want)
			case "receive loss":
				want = actorx.ErrFull
				f.failures.Add(1)
			case "disconnect":
				f.ready.Store(false)
			case "disconnect and reconnect":
				f.ready.Store(false)
				f.ready.resume(f.ready.epoch())
			case "cancel":
				want = context.Canceled
				cancel()
			case "deadline":
				want = context.DeadlineExceeded
			case "close":
				want = actorx.ErrClosed
				rt.beginClose()
			}
			assertDiscoveryError(t, result, want)
			if mode != "close" {
				discoveryBarrier(t, rt)
			}
		})
	}
}

func TestDiscoveryClusterCancellationRetainsPreparingSlots(t *testing.T) {
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	// Use a valid empty snapshot; cancellation is checked again at completion.
	id := token(t)
	rt, _, _ := clusterFixture(t, func(ctx context.Context) (discoverySnapshot, error) {
		entered <- struct{}{}
		<-release
		return discoverySnapshot{RuntimeID: id}, ctx.Err()
	}, nil)
	results := make([]<-chan discoveryResult, 4)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	for i := range 4 {
		parent := testContext(t)
		if i == 0 {
			parent = ctx
		}
		results[i] = startCluster(rt, parent, 10*time.Millisecond)
		await(t, entered)
	}

	// The caller returns, but the blocked scan still owns its query slot.
	cancel()
	assertDiscoveryError(t, results[0], context.Canceled)
	if counts := discoveryBarrier(t, rt); counts.active != 4 {
		t.Fatal(counts)
	}
	report, err := rt.Discover(testContext(t), DiscoveryQuery{Scope: DiscoveryCluster})
	if !errors.Is(err, actorx.ErrFull) || !reflect.DeepEqual(report, DiscoveryReport{}) {
		t.Fatal(report, err)
	}
	unblock()
	for _, result := range results[1:] {
		if got := <-result; got.err != nil {
			t.Fatal(got)
		}
	}
	if counts := discoveryBarrier(t, rt); counts != (discoveryCounts{}) {
		t.Fatal(counts)
	}
}

func TestDiscoveryClusterEarlyFinishRetainsPublishingWorker(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	rt, f, _ := clusterFixture(t, nil, func(context.Context) error {
		<-release
		return nil
	})
	result := startCluster(rt, testContext(t), time.Second)
	q := nextDiscoveryQuery(t, f)
	sendDiscoveryNode(t, rt, q.QueryID, discoverySnapshot{}, ErrProtocol)
	assertDiscoveryError(t, result, ErrProtocol)
	if counts := discoveryBarrier(t, rt); counts != (discoveryCounts{1, 0}) {
		t.Fatal("worker prematurely released", counts)
	}

	// Early query completion must still leave Close waiting for publication.
	closed := make(chan error, 1)
	ctx := testContext(t)
	go func() { closed <- rt.Close(ctx) }()
	await(t, rt.life.Done())
	select {
	case err := <-closed:
		t.Fatal("Close did not join publishing task", err)
	default:
	}
	unblock()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryClusterLateAcceptanceAndFinalChecks(t *testing.T) {
	for _, mode := range []string{"late node", "cancel", "disconnect", "receive loss"} {
		t.Run(mode, func(t *testing.T) {
			rt, f, p := clusterFixture(t, nil, nil)
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			result := startCluster(rt, ctx, 50*time.Millisecond)
			q := nextDiscoveryQuery(t, f)
			var once sync.Once
			unblock := func() { once.Do(func() { close(p.release) }) }
			defer unblock()
			if err := rt.discovery.Send(testContext(t), discoveryMailboxRequest{Hold: true}); err != nil {
				t.Fatal(err)
			}
			await(t, p.entered)
			sendDiscoveryNode(t, rt, q.QueryID, discoverySnapshot{token(t), discoveryEntries(1, "late"), false}, nil)

			// Queue the timer completion behind a node already in the mailbox.
			time.Sleep(70 * time.Millisecond)
			switch mode {
			case "cancel":
				cancel()
			case "disconnect":
				f.ready.Store(false)
			case "receive loss":
				f.failures.Add(1)
			}
			unblock()
			if mode == "late node" {
				got := <-result
				if got.err != nil || len(got.report.Actors) != 0 || len(got.report.Responders) != 1 ||
					got.report.EndReason != DiscoveryEndWindow {
					t.Fatal(got)
				}
			} else {
				want := context.Canceled
				if mode == "disconnect" {
					want = ErrOwnerUnavailable
				}
				if mode == "receive loss" {
					want = actorx.ErrFull
				}
				assertDiscoveryError(t, result, want)
			}
		})
	}
}

func TestDiscoveryClusterLocalTruncationStillBroadcasts(t *testing.T) {
	id := token(t)
	rt, f, _ := clusterFixture(t, func(context.Context) (discoverySnapshot, error) {
		return discoverySnapshot{RuntimeID: id, Truncated: true}, nil
	}, nil)
	result := startCluster(rt, testContext(t), 20*time.Millisecond)
	q := nextDiscoveryQuery(t, f)
	sendDiscoveryNode(t, rt, q.QueryID, discoverySnapshot{RuntimeID: token(t)}, nil)
	got := <-result
	if got.err != nil || !got.report.Truncated || len(got.report.Responders) != 2 ||
		got.report.EndReason != DiscoveryEndWindow {
		t.Fatal(got)
	}
}

func TestDiscoveryClusterPreparingFaultsAndWindow(t *testing.T) {
	for _, mode := range []string{"window starts after scan", "disconnect before publish", "receive loss before publish"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			id := token(t)
			rt, f, _ := clusterFixture(t, func(context.Context) (discoverySnapshot, error) {
				close(entered)
				<-release
				return discoverySnapshot{RuntimeID: id}, nil
			}, nil)
			result := startCluster(rt, testContext(t), 50*time.Millisecond)
			await(t, entered)
			if mode == "disconnect before publish" {
				f.ready.Store(false)
			} else if mode == "receive loss before publish" {
				f.failures.Add(1)
			} else {
				time.Sleep(70 * time.Millisecond)
			}
			start := time.Now()
			unblock()
			if mode == "window starts after scan" {
				nextDiscoveryQuery(t, f)
				got := <-result
				if got.err != nil || got.report.EndReason != DiscoveryEndWindow || time.Since(start) < 50*time.Millisecond {
					t.Fatal(got)
				}
			} else {
				want := ErrOwnerUnavailable
				if mode == "receive loss before publish" {
					want = actorx.ErrFull
				}
				assertDiscoveryError(t, result, want)
				select {
				case q := <-f.published:
					t.Fatal("published after preparing fault", q)
				default:
				}
			}
		})
	}
}

func TestDiscoveryClusterStartTaskRollback(t *testing.T) {
	entered, release := make(chan struct{}, 8), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	id := token(t)
	rt, _, _ := clusterFixture(t, func(ctx context.Context) (discoverySnapshot, error) {
		entered <- struct{}{}
		<-release
		return discoverySnapshot{RuntimeID: id}, ctx.Err()
	}, nil)

	// Exhaust the shared Task budget with local scans before cluster admission.
	results := make(chan error, rt.limits.managementTasks())
	for range rt.limits.managementTasks() {
		go func() {
			_, err := rt.Discover(testContext(t), DiscoveryQuery{})
			results <- err
		}()
		await(t, entered)
	}
	report, err := rt.Discover(testContext(t), DiscoveryQuery{Scope: DiscoveryCluster})
	if !errors.Is(err, actorx.ErrFull) || !reflect.DeepEqual(report, DiscoveryReport{}) {
		t.Fatal(report, err)
	}
	if counts := discoveryBarrier(t, rt); counts != (discoveryCounts{}) {
		t.Fatal("failed task retained a query slot", counts)
	}
	unblock()
	for range rt.limits.managementTasks() {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestDiscoveryClusterTinyWindow(t *testing.T) {
	rt, _, _ := clusterFixture(t, nil, nil)
	report, err := rt.Discover(testContext(t), DiscoveryQuery{Scope: DiscoveryCluster, Window: time.Nanosecond})
	if err != nil || report.EndReason != DiscoveryEndWindow || len(report.Responders) != 1 {
		t.Fatal(report, err)
	}
}
