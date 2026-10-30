package actorxdist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
)

type iteratorQuery struct {
	Count      int
	Fail, Park bool
}

type iteratorPointer struct{}
type iteratorProbe struct{}

func (*iteratorProbe) Bindings() []actorx.Binding {
	return []actorx.Binding{
		actorx.HandleStream(func(ctx actorx.Context, q iteratorQuery, r actorx.StreamReply[int]) error {
			_, err := actorx.StartTask(ctx, func(work context.Context) (struct{}, error) {
				for i := 0; i < q.Count; i++ {
					if err := r.Write(work, i, 8); err != nil {
						return struct{}{}, err
					}
				}
				if q.Park {
					<-work.Done()
					return struct{}{}, work.Err()
				}
				if q.Fail {
					return struct{}{}, errors.New("iterator business failure")
				}
				return struct{}{}, nil
			}, func(_ struct{}, err error) bool { return r.Complete(err) }, nil)
			return err
		}),
		actorx.HandleStream(func(ctx actorx.Context, _ iteratorPointer, r actorx.StreamReply[*int]) error {
			if err := r.Write(ctx, nil, 0); err != nil {
				return err
			}
			r.Complete(nil)
			return nil
		}),
		actorx.Handle(func(actorx.Context, read) (int, error) { return 42, nil }),
	}
}

func streamEventually(t *testing.T, condition func() bool) {
	t.Helper()
	until := time.After(5 * time.Second)
	for !condition() {
		select {
		case <-until:
			t.Fatal("stream did not settle")
		case <-time.After(time.Millisecond):
		}
	}
}

func streamNetworkClean(rt *Runtime) bool {
	n := rt.net
	n.businessMu.Lock()
	defer n.businessMu.Unlock()
	n.rpcMu.Lock()
	defer n.rpcMu.Unlock()
	return n.streams == 0 && n.outgoing == 0 && len(n.leases) == 0 && len(n.streamReplies) == 0
}

// Optional real transport evidence. No in-memory invocation adapter participates.
func TestStreamIteratorNATS(t *testing.T) {
	url := os.Getenv("ACTORX_NATS_URL")
	if url == "" {
		t.Skip("set ACTORX_NATS_URL to an isolated JetStream server")
	}
	typ := fmt.Sprintf("iterator-%x", time.Now().UnixNano())
	var activated atomic.Int32
	worker := testRuntimeWithOptions(t, []Option{WithNATS(url)}, registerSpec(ActorSpec[*iteratorProbe]{
		Type: typ,
		New: func(ActorKey) *iteratorProbe {
			activated.Add(1)
			return &iteratorProbe{}
		},
	}))
	client := testRuntimeWithOptions(t, []Option{WithNATS(url), WithTransportLimits(TransportLimits{Streams: 1})})
	a := mustAddr(t, client, typ, "one")
	ctx := testContext(t)
	seq := a.Stream[int](ctx, iteratorQuery{Count: 3, Fail: true})
	if activated.Load() != 0 || !streamNetworkClean(client) || a.generation() != "" {
		t.Fatal("constructing sequence dispatched work")
	}

	var values []int
	var terminal error
	for value, err := range seq {
		if err != nil {
			terminal = err
			break
		}
		values = append(values, value)
		if len(values) == 1 {
			streamEventually(t, func() bool {
				client.net.rpcMu.Lock()
				defer client.net.rpcMu.Unlock()
				for _, s := range client.net.streamReplies {
					s.mu.Lock()
					done := s.end != nil
					s.mu.Unlock()
					return done
				}
				return false
			})
		}
	}
	if fmt.Sprint(values) != "[0 1 2]" || terminal == nil || !strings.Contains(terminal.Error(), "iterator business failure") {
		t.Fatal(values, terminal)
	}
	if err := streamError(seq); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal("reused sequence", err)
	}
	if a.generation() == "" {
		t.Fatal("missing generation")
	}

	streamEventually(t, func() bool { return streamNetworkClean(worker) && streamNetworkClean(client) })

	for v, err := range a.Stream[*int](ctx, iteratorPointer{}) {
		if err != nil || v != nil {
			t.Fatal(v, err)
		}
	}

	streamEventually(t, func() bool { return streamNetworkClean(worker) && streamNetworkClean(client) })

	for _, mode := range []string{"continue", "break", "return", "panic"} {
		t.Run(mode, func(t *testing.T) {
			var ack atomic.Int32
			client.net.businessFault = func(stage string, f businessFrame) error {
				if stage == "stream_frame" && f.Kind == "window" {
					ack.Add(1)
				}
				return nil
			}
			defer func() { client.net.businessFault = nil }()
			sentinel := errors.New("application return")
			consume := func() error {
				for _, err := range a.Stream[int](ctx, iteratorQuery{Count: 3}) {
					if err != nil {
						return err
					}
					switch mode {
					case "continue":
						continue
					case "break":
						break
					case "return":
						return sentinel
					case "panic":
						panic(sentinel)
					}
					break
				}
				return nil
			}
			var got error
			if mode == "panic" {
				func() {
					defer func() {
						if recover() != sentinel {
							t.Error("panic changed")
						}
					}()
					_ = consume()
				}()
			} else {
				got = consume()
			}
			if mode == "return" && got != sentinel || mode != "return" && got != nil {
				t.Fatal(got)
			}
			want := int32(0)
			if mode == "continue" {
				want = 3
			}
			if ack.Load() != want {
				t.Fatal("consumption credits", ack.Load(), want)
			}
			streamEventually(t, func() bool { return streamNetworkClean(worker) && streamNetworkClean(client) })
		})
	}

	// Application failure never stops the shared Actor.
	if got, err := a.Call[int](ctx, read{}); err != nil || got != 42 {
		t.Fatal(got, err)
	}

	for _, mode := range []string{"success", "failure", "queued", "missing-terminal"} {
		t.Run("final-ack/"+mode, func(t *testing.T) {
			var ack atomic.Int32
			client.net.businessFault = func(stage string, f businessFrame) error {
				if stage == "stream_frame" && f.Kind == "window" {
					ack.Add(1)
					return ErrOwnerUnavailable
				}
				return nil
			}
			defer func() { client.net.businessFault = nil }()
			count := 1
			if mode == "queued" {
				count = 2
			}
			ackCtx, cancelAck := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancelAck()
			var result error
			for _, err := range a.Stream[int](ackCtx, iteratorQuery{Count: count, Fail: mode == "failure", Park: mode == "missing-terminal"}) {
				if err != nil {
					result = err
					break
				}
				if mode != "missing-terminal" {
					streamEventually(t, func() bool {
						client.net.rpcMu.Lock()
						defer client.net.rpcMu.Unlock()
						for _, s := range client.net.streamReplies {
							s.mu.Lock()
							done := s.end != nil
							s.mu.Unlock()
							return done
						}
						return false
					})
				}
			}

			switch mode {
			case "success":
				if result != nil {
					t.Fatal(result)
				}
			case "failure":
				if result == nil || !strings.Contains(result.Error(), "iterator business failure") || errors.Is(result, ErrOutcomeUnknown) {
					t.Fatal(result)
				}
			default:
				if !errors.Is(result, ErrOutcomeUnknown) {
					t.Fatal("lost delivery uncertainty", result)
				}
			}
			if ack.Load() != 1 {
				t.Fatal("unexpected credit attempts", ack.Load())
			}
			streamEventually(t, func() bool { return streamNetworkClean(client) })
			// A lost final ACK leaves a bounded producer ledger until the original
			// deadline, even though the receiver has preserved its terminal result.
			streamEventually(t, func() bool { return streamNetworkClean(worker) })
		})
	}

	for _, mode := range []string{"wait-cancel", "complete-independent", "complete-request", "consumer-cancel", "runtime-close"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			flow, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var result error
				for _, err := range a.Stream[int](flow, iteratorQuery{Park: true}) {
					if err != nil {
						result = err
						break
					}
					close(entered)
					<-release
				}
				done <- result
			}()

			var p *businessReply
			streamEventually(t, func() bool {
				worker.net.businessMu.Lock()
				defer worker.net.businessMu.Unlock()
				for _, v := range worker.net.leases {
					p = v
				}
				return p != nil
			})

			// Keep WriteConsumed waiting while the application holds its chunk.
			writeCtx, stopWait := context.WithCancel(ctx)
			defer stopWait()
			if mode == "complete-request" {
				writeCtx = p.stream.ctx
			}
			receipt := make(chan error, 1)
			go func() { receipt <- p.WriteConsumed(writeCtx, 99, 8) }()
			await(t, entered)
			select {
			case err := <-receipt:
				t.Fatal("publish certified consumption", err)
			default:
			}

			switch mode {
			case "wait-cancel":
				stopWait()
				if err := <-receipt; !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case "complete-independent", "complete-request":
				completed := make(chan struct{})
				go func() {
					p.Complete(nil, nil)
					close(completed)
				}()
				await(t, completed)
				if mode == "complete-request" {
					if err := <-receipt; !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				} else {
					select {
					case err := <-receipt:
						t.Fatal("Complete settled independent receipt", err)
					default:
					}
				}
			case "consumer-cancel":
				cancel()
			case "runtime-close":
				short, c := context.WithTimeout(ctx, 30*time.Millisecond)
				err := client.Close(short)
				c()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("Close passed active body", err)
				}
			}

			if err := streamError(a.Stream[int](ctx, iteratorQuery{})); mode != "runtime-close" && !errors.Is(err, actorx.ErrFull) {
				t.Fatal("active body released budget", err)
			}

			// Only releasing the loop body allows its reservation to settle.
			unblock()
			if mode == "wait-cancel" {
				p.Complete(nil, nil)
			}
			result := <-done
			if mode == "consumer-cancel" || mode == "runtime-close" {
				if result == nil {
					t.Fatal("local abort succeeded")
				}
			} else if result != nil {
				t.Fatal(result)
			}
			if mode == "complete-independent" {
				if err := <-receipt; err != nil {
					t.Fatal("lost late consumption receipt", err)
				}
			}
			if mode == "consumer-cancel" || mode == "runtime-close" {
				if err := <-receipt; err == nil {
					t.Fatal("abandonment acknowledged")
				}
			}
			streamEventually(t, func() bool { return streamNetworkClean(worker) && streamNetworkClean(client) })
		})
	}
}
