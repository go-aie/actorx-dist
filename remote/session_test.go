package remote

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

func TestSessionRequestIDsFollowQueueOrder(t *testing.T) {
	for range 2 { // a new registration starts its own sequence
		s := newSession()
		defer s.close()
		ctx := testCtx(t)
		done := make(chan error, 9)
		for i := range 9 {
			id := strconv.Itoa(i)
			a := testAddress(id)
			s.track(id)
			go func() {
				var err error
				switch i % 3 {
				case 0:
					err = s.create(ctx, a)
				case 1:
					_, err = s.invoke(ctx, a, input(id))
				case 2:
					err = streamError(s.invokeStream(ctx, a, protocol.InvokeStream(input(id))))
				}
				done <- err
			}()
		}
		messages := make([]protocol.Message, 9)
		for i := range messages {
			m := await(t, s.events)
			if string(m.ID) != `"r:`+strconv.Itoa(i+1)+`"` {
				t.Fatalf("outbound ID order: %s", m.ID)
			}
			messages[i] = m
		}
		// Completion order does not constrain subsequent request numbering.
		for i := len(messages) - 1; i >= 0; i-- {
			m := messages[i]
			var params protocol.CallParams
			if err := json.Unmarshal(m.Params, &params); err != nil {
				t.Fatal(err)
			}
			reply := success(m, params.Address, json.RawMessage(`null`))
			if m.Method == protocol.Stream {
				reply = protocol.Success(m.ID, protocol.StreamResult{Address: params.Address})
			}
			if err := s.accept(reply); err != nil {
				t.Fatal(err)
			}
		}
		for range messages {
			if err := await(t, done); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSessionRequestSequenceExhaustionDoesNotDispatch(t *testing.T) {
	s := newSession()
	defer s.close()
	s.requestSeq = protocol.MaxSequence
	a := testAddress("counter")
	s.track("counter")
	ctx := testCtx(t)
	for _, method := range []string{protocol.Create, protocol.Call, protocol.Stream} {
		var err error
		if method == protocol.Stream {
			err = streamError(s.invokeStream(ctx, a, protocol.InvokeStream(input("counter"))))
		} else {
			_, err = s.exchange(ctx, method, a, "counter", json.RawMessage(`null`))
		}
		if !errors.Is(err, actorxdist.ErrProtocol) {
			t.Fatal(method, err)
		}
	}
	if len(s.events) != 0 || len(s.pending) != 0 || len(s.streams) != 0 || s.requestSeq != protocol.MaxSequence {
		t.Fatal("exhausted sequence dispatched or retained work")
	}
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-testCtx(t).Done():
		t.Fatal("event timed out")
		var zero T
		return zero
	}
}

func testAddress(id string) protocol.Address {
	return protocol.Address{
		Version:    1,
		Type:       "demo.ping",
		ID:         id,
		Generation: "AAAAAAAAAAAAAAAAAAAAAA",
	}
}

func input(id string) protocol.Invoke {
	return protocol.Invoke{
		RequestID:   id,
		MessageType: "json",
		ContentType: "application/json",
		Content:     `{"text":"ping"}`,
	}
}

func success(m protocol.Message, a protocol.Address, value json.RawMessage) protocol.Message {
	if m.Method == protocol.Create {
		return protocol.Success(m.ID, protocol.CreateResult{Address: a})
	}
	return protocol.Success(m.ID, protocol.CallResult{Address: a, Value: value})
}

func rpcCode(t *testing.T, err error, code int) {
	t.Helper()
	var e *protocol.Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("want RPC %d, got %v", code, err)
	}
}

type fakeOwner struct {
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
	err     error
}

func (o *fakeOwner) Unregister(ctx context.Context) error {
	o.once.Do(func() {
		if o.entered != nil {
			close(o.entered)
		}
	})
	if o.release != nil {
		<-o.release
	}
	return o.err
}

func fakeGateway(t *testing.T, call func(context.Context, protocol.Address, protocol.Invoke) (protocol.Address, protocol.Result, error)) (*Gateway, *Registration) {
	t.Helper()
	g := newGateway(backend{
		register: func(context.Context, protocol.RegisterParams, *session) (registrationOwner, error) {
			return &fakeOwner{}, nil
		},
		call: call,
	})
	r, err := g.Register(testCtx(t), protocol.RegisterParams{Type: "demo.ping", Description: "test provider"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.Close(testCtx(t)); err != nil {
			t.Error(err)
		}
	})
	return g, r
}

func TestSessionCorrelationAndTerminalPublication(t *testing.T) {
	g, r := fakeGateway(t, nil)
	s := r.session
	s.track("call-1")
	done := make(chan error, 1)
	ctx := testCtx(t)
	go func() {
		_, err := s.invoke(ctx, testAddress("one"), input("call-1"))
		done <- err
	}()
	event := await(t, r.Events())
	if event.Method != protocol.Call {
		t.Fatal(event)
	}
	params, e := event.Parameters(protocol.ToProvider)
	if e != nil {
		t.Fatal(e)
	}
	p := params.(protocol.CallParams)
	deadline, _ := ctx.Deadline()
	if p.Address != testAddress("one") || p.DeadlineUnixMS == nil || *p.DeadlineUnixMS != deadline.UnixMilli() || string(event.ID) == `"call-1"` {
		t.Fatal(params)
	}

	// Reject unrelated replies without consuming the legitimate pending request.
	for _, response := range []protocol.Message{
		success(event, testAddress("other"), json.RawMessage(`null`)),
		protocol.Success(json.RawMessage(`"wrong-id"`), nil),
		protocol.ErrorResponse(event.ID, protocol.NewError(protocol.RemoteError, "wrong address", ptr(testAddress("other")))),
	} {
		if err := g.Accept(testCtx(t), r.Info().SessionToken, response); err != ErrCorrelation {
			t.Fatal(err)
		}
	}
	reply := success(event, testAddress("one"), json.RawMessage(`null`))
	if err := g.Accept(testCtx(t), "wrong-session", reply); err != ErrSession {
		t.Fatal(err)
	}
	if err := g.Accept(testCtx(t), r.Info().SessionToken, event); err != ErrResponse {
		t.Fatal(err)
	}
	if err := g.Accept(testCtx(t), r.Info().SessionToken, reply); err != nil {
		t.Fatal(err)
	}

	// Publication precedes the worker wakeup, not its later serial callback.
	evidence := s.evidence("call-1", testAddress("one"))
	if !evidence.terminal || evidence.result.Content != "null" {
		t.Fatal(evidence)
	}
	for _, other := range []protocol.Address{
		testAddress("other"),
		{
			Version:    1,
			Type:       "demo.ping",
			ID:         "one",
			Generation: "AQAAAAAAAAAAAAAAAAAAAA",
		},
	} {
		if r.session.evidence("call-1", other).terminal {
			t.Fatal("execution evidence crossed ID/generation")
		}
	}
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	if err := g.Accept(testCtx(t), r.Info().SessionToken, reply); err != ErrCorrelation {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) != 0 || s.invokes != 0 {
		t.Fatal("pending leaked")
	}
}

func ptr(a protocol.Address) *protocol.Address { return &a }

func TestCreateFailureSurvivesPendingAndIsScoped(t *testing.T) {
	g, r := fakeGateway(t, nil)
	a := testAddress("failed")
	done := make(chan error, 1)
	go func() { done <- r.session.create(testCtx(t), a) }()
	event := await(t, r.Events())
	reply := protocol.ErrorResponse(event.ID, protocol.NewError(protocol.RemoteError, "factory failed", &a))
	if err := g.Accept(testCtx(t), r.Info().SessionToken, reply); err != nil {
		t.Fatal(err)
	}
	if err := await(t, done); err != errProviderCreate {
		t.Fatal(err)
	}

	// Create pending is gone, but its confirmed failure must remain queryable.
	r.session.mu.Lock()
	n := len(r.session.pending)
	r.session.mu.Unlock()
	if n != 0 {
		t.Fatal(n)
	}
	evidence := r.session.evidence("outer-call", a)
	if !evidence.terminal || evidence.result.Failure.Code != protocol.RemoteError {
		t.Fatal(evidence)
	}
	for _, other := range []protocol.Address{
		testAddress("other"),
		{
			Version:    1,
			Type:       a.Type,
			ID:         a.ID,
			Generation: "AQAAAAAAAAAAAAAAAAAAAA",
		},
		{
			Version:    1,
			Type:       "other.type",
			ID:         a.ID,
			Generation: a.Generation,
		},
	} {
		if r.session.evidence("outer-call", other).terminal {
			t.Fatal("identity evidence leaked")
		}
	}
	second := newSession()
	defer second.close()
	if second.evidence("outer-call", a).terminal {
		t.Fatal("registration evidence leaked")
	}
	if err := g.Accept(testCtx(t), r.Info().SessionToken, reply); err != ErrCorrelation {
		t.Fatal(err)
	}
	if err := r.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if r.session.evidence("outer-call", a).terminal {
		t.Fatal("cleanup retained evidence")
	}
}

func TestProviderRefusalsKeepClassification(t *testing.T) {
	for _, code := range []int{protocol.ActorUnavailable, protocol.TransportMode} {
		for _, method := range []string{protocol.Call, protocol.Stream} {
			t.Run(protocol.Kind(code)+"/"+method, func(t *testing.T) {
				g, r := fakeGateway(t, nil)
				g.backend.call = func(ctx context.Context, a protocol.Address, in protocol.Invoke) (protocol.Address, protocol.Result, error) {
					result, err := r.session.invoke(ctx, a, in)
					if result.Validate() != nil {
						t.Error("refusal did not survive DTO validation", result)
					}
					return a, result, err
				}
				g.backend.stream = func(ctx context.Context, a *protocol.Address, in protocol.InvokeStream) iter.Seq2[protocol.Chunk, error] {
					return r.session.invokeStream(ctx, *a, in)
				}
				a := testAddress("refused")
				done := make(chan error, 1)
				ctx := testCtx(t)
				go func() {
					request := protocol.CallParams{Address: a, Input: json.RawMessage(`null`)}
					if method == protocol.Call {
						_, err := g.Call(ctx, request)
						done <- err
					} else {
						var result error
						for _, err := range g.Stream(ctx, request) {
							if err != nil {
								result = err
								break
							}
							t.Error("refusal delivered a chunk")
						}
						done <- result
					}
				}()
				m := await(t, r.Events())
				e := protocol.NewError(code, protocol.Kind(code), &a)
				if method == protocol.Stream {
					e = protocol.StreamError(e, 0)
				}
				if err := g.Accept(ctx, r.Info().SessionToken, protocol.ErrorResponse(m.ID, e)); err != nil {
					t.Fatal(err)
				}
				rpcCode(t, await(t, done), code)
			})
		}
	}
}

func TestCanceledPendingCannotPublishLateEvidence(t *testing.T) {
	for _, create := range []bool{false, true} {
		t.Run(map[bool]string{false: "invoke", true: "create"}[create], func(t *testing.T) {
			g, r := fakeGateway(t, nil)
			r.session.track("call")
			ctx, cancel := context.WithCancel(testCtx(t))
			done := make(chan error, 1)
			go func() {
				if create {
					done <- r.session.create(ctx, testAddress("late"))
				} else {
					_, err := r.session.invoke(ctx, testAddress("late"), input("call"))
					done <- err
				}
			}()
			event := await(t, r.Events())
			cancel()
			if !errors.Is(await(t, done), context.Canceled) {
				t.Fatal("cancellation lost")
			}
			if err := g.Accept(testCtx(t), r.Info().SessionToken, success(event, testAddress("late"), json.RawMessage(`true`))); err != ErrCorrelation {
				t.Fatal(err)
			}
			evidence := r.session.evidence("call", testAddress("late"))
			if evidence.terminal {
				t.Fatal("late terminal published")
			}
			if !create && !evidence.sent {
				t.Fatal("dispatch evidence lost")
			}
		})
	}
}

func TestSessionCapacityCloseAndQueue(t *testing.T) {
	g, r := fakeGateway(t, nil)
	results := make(chan error, protocol.Capacity)
	for i := 0; i < protocol.Capacity; i++ {
		id := string(rune('a' + i))
		r.session.track(id)
		go func() {
			_, err := r.session.invoke(testCtx(t), testAddress(id), input(id))
			results <- err
		}()
	}

	// Receive each dispatch, then put it back: all eight pending are registered
	// before proceeding; the pending invocation capacity is now full.
	events := make([]protocol.Message, 0, protocol.Capacity)
	for i := 0; i < protocol.Capacity; i++ {
		events = append(events, await(t, r.Events()))
	}
	r.session.mu.Lock()
	for _, event := range events {
		r.session.events <- event
	}
	r.session.mu.Unlock()
	r.session.track("overflow")
	if _, err := r.session.invoke(testCtx(t), testAddress("overflow"), input("overflow")); err != actorx.ErrFull {
		t.Fatal(err)
	}
	if r.session.evidence("overflow", testAddress("overflow")).sent {
		t.Fatal("rejected work was dispatched")
	}
	if err := r.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < protocol.Capacity; i++ {
		if err := await(t, results); err != actorx.ErrStopped {
			t.Fatal(err)
		}
	}
	r.session.mu.Lock()
	defer r.session.mu.Unlock()
	if len(r.session.pending) != 0 || r.session.creates != 0 || r.session.invokes != 0 {
		t.Fatal("pending capacity leaked")
	}
	_ = g
}

func TestProxyBindingsAndDeferredWorker(t *testing.T) {
	var zero *proxyActor
	descriptors, err := actorx.Describe(zero.Bindings())
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 2 || len(zero.Bindings()) != 3 {
		t.Fatal(descriptors)
	}
	found, foundStream := false, false
	for _, d := range descriptors {
		if d.MessageType() == reflect.TypeFor[protocol.Invoke]() &&
			d.ResultType() == reflect.TypeFor[protocol.Result]() &&
			d.Validate(actorx.CallKind) == nil {
			found = true
		}
		if d.MessageType() == reflect.TypeFor[protocol.InvokeStream]() &&
			d.ResultType() == reflect.TypeFor[protocol.Chunk]() &&
			d.Validate(actorx.StreamKind) == nil {
			foundStream = true
		}
	}
	if !found || !foundStream {
		t.Fatal("Call/Stream DTO binding missing")
	}

	// This tests core deferred work only; Started/Self/activation are covered by
	// the explicit real-NATS integration test.
	s := newSession()
	defer s.close()
	s.track("work")
	actor := &proxyActor{session: s, address: testAddress("worker")}
	rt, err := actorx.NewRuntime(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rt.Close(testCtx(t)); err != nil {
			t.Error(err)
		}
	})
	addr, err := rt.Spawn(bindingsOnly{actorx.HandleDeferred(actor.Invoke)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		result, err := addr.Call[protocol.Result](testCtx(t), input("work"))
		if err == nil && result.Content != "null" {
			err = errors.New("bad result")
		}
		done <- err
	}()
	event := await(t, s.events)

	// The first worker is still waiting on its provider response.
	_, err = addr.Call[protocol.Result](testCtx(t), input("busy"))
	if err != actorx.ErrFull {
		t.Fatal(err)
	}
	if err := s.accept(success(event, testAddress("worker"), json.RawMessage(`null`))); err != nil {
		t.Fatal(err)
	}
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
}

type bindingsOnly []actorx.Binding

func (b bindingsOnly) Bindings() []actorx.Binding { return b }

func TestMalformedProviderResponseDoesNotConsumePending(t *testing.T) {
	g, r := fakeGateway(t, nil)
	r.session.track("work")
	done := make(chan error, 1)
	go func() {
		_, err := r.session.invoke(testCtx(t), testAddress("one"), input("work"))
		done <- err
	}()
	event := await(t, r.Events())
	malformed := []protocol.Message{
		protocol.Success(event.ID, protocol.CreateResult{Address: testAddress("one")}), // missing value
		protocol.ErrorResponse(event.ID, &protocol.Error{
			Code:    protocol.Full,
			Message: "x",
			Data:    json.RawMessage(`{"kind":"remote_error"}`),
		}),
	}
	for _, response := range malformed {
		if err := g.Accept(testCtx(t), r.Info().SessionToken, response); err != ErrResponse {
			t.Fatal(err)
		}
		if r.session.evidence("work", testAddress("one")).terminal {
			t.Fatal("invalid response published terminal")
		}
	}
	if err := g.Accept(testCtx(t), r.Info().SessionToken, success(event, testAddress("one"), json.RawMessage(`true`))); err != nil {
		t.Fatal(err)
	}
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestQueueRejectionAndOuterSettlement(t *testing.T) {
	s := newSession()
	defer s.close()
	filler, _ := protocol.NewRequest(protocol.Create, "filler", protocol.CreateParams{Address: testAddress("filler"), DeadlineUnixMS: 1})
	for i := 0; i < cap(s.events); i++ {
		s.events <- filler
	}
	s.track("rejected")
	if _, err := s.invoke(testCtx(t), testAddress("queue"), input("rejected")); err != actorx.ErrFull {
		t.Fatal(err)
	}
	if s.evidence("rejected", testAddress("queue")).sent {
		t.Fatal("queue refusal recorded dispatch")
	}
	for i := 0; i < cap(s.events); i++ {
		<-s.events
	}

	// Outer settlement withdraws pending before a late provider reply can arrive.
	s.track("outer")
	done := make(chan error, 1)
	go func() {
		_, err := s.invoke(testCtx(t), testAddress("outer"), input("outer"))
		done <- err
	}()
	event := await(t, s.events)
	evidence := s.settle("outer", testAddress("outer"))
	if !evidence.sent || evidence.terminal {
		t.Fatal(evidence)
	}
	if err := s.accept(success(event, testAddress("outer"), json.RawMessage(`null`))); err != ErrCorrelation {
		t.Fatal(err)
	}
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.evidence("outer", testAddress("outer")).terminal {
		t.Fatal("settled call was resurrected")
	}
}

func TestProxyTaskAdmissionFailureResetsBusy(t *testing.T) {
	a := &proxyActor{}
	if err := a.Invoke(nil, input("work"), actorx.Reply[protocol.Result]{}); err != actorx.ErrSetup {
		t.Fatal(err)
	}
	if a.busy {
		t.Fatal("failed StartTask admission left proxy busy")
	}
}
