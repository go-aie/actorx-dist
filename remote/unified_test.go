package remote

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"testing"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

func TestUnifiedProxyBusyAdmission(t *testing.T) {
	s := newSession()
	defer s.close()
	fixed := testAddress("shared")
	a := &proxyActor{session: s, address: fixed}
	rt, err := actorx.NewRuntime(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rt.Close(testCtx(t)); err != nil {
			t.Error(err)
		}
	}()
	addr, err := rt.Spawn(bindingsOnly{actorx.HandleDeferred(a.Invoke), actorx.HandleStream(a.InvokeStream)})
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range []bool{false, true} {
		s.track("active")
		done := make(chan error, 1)
		go func() {
			if stream {
				done <- streamError(addr.Stream[protocol.Chunk](testCtx(t), streamInput("active")))
			} else {
				_, err := addr.Call[protocol.Result](testCtx(t), input("active"))
				done <- err
			}
		}()
		event := await(t, s.events)

		// Both directions share the same admission flag while provider work runs.
		if stream {
			_, err = addr.Call[protocol.Result](testCtx(t), input("overlap"))
		} else {
			err = streamError(addr.Stream[protocol.Chunk](testCtx(t), streamInput("overlap")))
		}
		if !errors.Is(err, actorx.ErrFull) {
			t.Fatalf("stream=%t overlap: %v", stream, err)
		}

		reply := success(event, fixed, json.RawMessage("null"))
		if stream {
			reply = protocol.Success(event.ID, protocol.StreamResult{Address: fixed, FinalSeq: 0})
		}
		if err := s.accept(reply); err != nil {
			t.Fatal(err)
		}
		if err := await(t, done); err != nil {
			t.Fatal(err)
		}
		s.forget("active")
	}
}

func TestUnsupportedOperationEvidenceAndOtherOperation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, late := range []error{actorx.ErrStopped, context.Canceled} {
			t.Run(protocol.Kind(protocol.UnsupportedOperation)+"/"+map[bool]string{false: "call", true: "stream"}[stream]+"/"+late.Error(), func(t *testing.T) {
				g, r := fakeGateway(t, nil)
				fixed := testAddress("shared")
				cold := fixed
				cold.Generation = ""
				settled, release := make(chan struct{}), make(chan struct{})
				g.backend.call = func(ctx context.Context, _ protocol.Address, in protocol.Invoke) (protocol.Address, protocol.Result, error) {
					result, err := r.session.invoke(ctx, fixed, in)
					if stream {
						return fixed, result, err
					}
					if err != nil || result.Failure == nil || result.Failure.Code != protocol.UnsupportedOperation {
						t.Error("Call failure must use Result.Failure and nil Go error", result, err)
					}
					close(settled)
					<-release
					return fixed, protocol.Result{}, late
				}
				g.backend.stream = func(ctx context.Context, a *protocol.Address, in protocol.InvokeStream) iter.Seq2[protocol.Chunk, error] {
					return func(yield func(protocol.Chunk, error) bool) {
						*a = fixed
						err := streamError(r.session.invokeStream(ctx, fixed, in))
						if !stream {
							if err != nil {
								yield(protocol.Chunk{}, err)
							}
							return
						}
						if !errors.Is(err, errProviderStream) {
							t.Error("missing retained Stream failure", err)
						}
						close(settled)
						<-release
						yield(protocol.Chunk{}, late)
					}
				}
				ctx, cancel := context.WithCancel(testCtx(t))
				defer cancel()

				var done <-chan callOutcome
				var terminal <-chan protocol.Message
				if stream {
					s, err := g.OpenStream(ctx, json.RawMessage(`"consumer"`), protocol.CallParams{Address: cold, Input: json.RawMessage("null")})
					if err != nil {
						t.Fatal(err)
					}
					ch := make(chan protocol.Message, 1)
					terminal = ch
					go func() {
						for m := range s.Messages() {
							if m.Method == protocol.ChunkMethod {
								t.Error("unsupported Stream emitted a chunk")
							} else {
								ch <- m
							}
						}
					}()
				} else {
					done = startCall(g, ctx, cold)
				}

				// Accept definite evidence first, then expose a later backend error.
				event := await(t, r.Events())
				failure := protocol.NewError(protocol.UnsupportedOperation, "unsupported_operation", &fixed)
				if stream {
					failure = protocol.StreamError(failure, 0)
				}
				if err := g.Accept(testCtx(t), r.Info().SessionToken, protocol.ErrorResponse(event.ID, failure)); err != nil {
					t.Fatal(err)
				}
				await(t, settled)
				cancel() // Valid terminal must survive a later cancellation and actorx-dist error.
				close(release)

				var rpcErr *protocol.Error
				if stream {
					m := await(t, terminal)
					rpcErr = m.Error
					var data struct {
						FinalSeq *uint64 `json:"final_seq"`
					}
					if rpcErr == nil || json.Unmarshal(rpcErr.Data, &data) != nil || data.FinalSeq == nil || *data.FinalSeq != 0 {
						t.Fatal("missing zero-item terminal", m)
					}
				} else {
					result := await(t, done)
					if result.response.Address != fixed {
						t.Fatal("lost fixed address", result)
					}
					if !errors.As(result.err, &rpcErr) {
						t.Fatal(result.err)
					}
				}
				var data struct {
					Address protocol.Address `json:"address"`
				}
				if rpcErr.Code != protocol.UnsupportedOperation || json.Unmarshal(rpcErr.Data, &data) != nil || data.Address != fixed {
					t.Fatal(rpcErr)
				}

				// The other operation can reuse this registration and fixed identity.
				// Python Addr generation reuse is covered by the SDK consumer tests.
				if stream {
					again := startCall(g, testCtx(t), fixed)
					event = await(t, r.Events())
					if event.Method != protocol.Call {
						t.Fatal(event.Method)
					}
					if err := g.Accept(testCtx(t), r.Info().SessionToken, success(event, fixed, json.RawMessage("null"))); err != nil {
						t.Fatal(err)
					}
					if result := await(t, again); result.err != nil || result.response.Address != fixed {
						t.Fatal(result)
					}
				} else {
					again := make(chan error, 1)
					go func() {
						result := protocol.StreamResult{Address: fixed}
						err := streamError(g.stream(testCtx(t), protocol.CallParams{Address: fixed, Input: json.RawMessage("null")}, &result))
						if result.Address != fixed {
							t.Error("Stream lost fixed address", result)
						}
						again <- err
					}()
					event = await(t, r.Events())
					if event.Method != protocol.Stream {
						t.Fatal(event.Method)
					}
					if err := g.Accept(testCtx(t), r.Info().SessionToken, protocol.Success(event.ID, protocol.StreamResult{Address: fixed, FinalSeq: 0})); err != nil {
						t.Fatal(err)
					}
					if err := await(t, again); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestGatewaySharedOperationCapacity(t *testing.T) {
	g, _ := fakeGateway(t, nil)
	entered := make(chan struct{}, protocol.Capacity)
	release := make(chan struct{})
	defer close(release)
	g.backend.call = func(_ context.Context, a protocol.Address, _ protocol.Invoke) (protocol.Address, protocol.Result, error) {
		entered <- struct{}{}
		<-release
		return a, protocol.NewResult(json.RawMessage("null"), nil), nil
	}
	g.backend.stream = func(_ context.Context, a *protocol.Address, _ protocol.InvokeStream) iter.Seq2[protocol.Chunk, error] {
		return func(func(protocol.Chunk, error) bool) {
			entered <- struct{}{}
			<-release
		}
	}
	done := make(chan error, protocol.Capacity)
	request := protocol.CallParams{Address: testAddress("one"), Input: json.RawMessage("null")}
	for i := 0; i < protocol.Capacity; i++ {
		go func() {
			var err error
			if i%2 == 0 {
				_, err = g.Call(testCtx(t), request)
			} else {
				err = streamError(g.Stream(testCtx(t), request))
			}
			done <- err
		}()
	}
	for i := 0; i < protocol.Capacity; i++ {
		await(t, entered)
	}

	_, err := g.Call(testCtx(t), request)
	rpcCode(t, err, protocol.Full)
	err = streamError(g.Stream(testCtx(t), request))
	rpcCode(t, err, protocol.Full)

	// Release workers and join them before the registration's cleanup runs.
	for i := 0; i < protocol.Capacity; i++ {
		release <- struct{}{}
	}
	for i := 0; i < protocol.Capacity; i++ {
		if err := await(t, done); err != nil {
			t.Fatal(err)
		}
	}
}
