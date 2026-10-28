package remote

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

type callOutcome struct {
	response protocol.CallResult
	err      error
}

func startCall(g *Gateway, ctx context.Context, a protocol.Address) <-chan callOutcome {
	done := make(chan callOutcome, 1)
	go func() {
		response, err := g.Call(ctx, protocol.CallParams{Address: a, Input: json.RawMessage(`null`)})
		done <- callOutcome{response, err}
	}()
	return done
}

func TestGatewayCallEvidence(t *testing.T) {
	for _, mode := range []string{
		"success",
		"invoke_failure",
		"create_failure",
		"create_unknown",
		"invoke_unknown",
		"standard_error",
		"damaged_dto",
	} {
		t.Run(mode, func(t *testing.T) {
			var r *Registration
			created := false
			g, reg := fakeGateway(t, func(ctx context.Context, a protocol.Address, in protocol.Invoke) (protocol.Address, protocol.Result, error) {
				if a.Generation == "" {
					a.Generation = testAddress(a.ID).Generation
				}
				if !created {
					created = true
					if err := r.session.create(ctx, a); err != nil {
						return a, protocol.Result{}, errors.Join(actorxdist.ErrActivationUnknown, err)
					}
				}
				if evidence := r.session.evidence(in.RequestID, a); evidence.terminal {
					return a, protocol.Result{}, actorx.ErrStopped
				}
				result, err := r.session.invoke(ctx, a, in)
				if mode == "invoke_failure" {
					if err != nil || result.Failure == nil {
						t.Error("business failure did not travel as Result with nil Go error")
					}
					return a, protocol.Result{}, actorx.ErrStopped // simulate loss of actorx-dist terminal
				}
				if mode == "damaged_dto" {
					result.Content = ""
				}
				return a, result, err
			})
			r = reg
			cold := testAddress("session-42")
			cold.Generation = ""
			ctx, cancel := context.WithCancel(testCtx(t))
			defer cancel()
			done := startCall(g, ctx, cold)
			create := await(t, r.Events())
			if create.Method != protocol.Create {
				t.Fatal(create)
			}
			fixed := testAddress(cold.ID)
			if mode == "create_unknown" {
				cancel()
			} else {
				response := success(create, fixed, nil)
				if mode == "create_failure" {
					response = protocol.ErrorResponse(create.ID, protocol.NewError(protocol.RemoteError, "factory failed", &fixed))
				}
				if err := g.Accept(testCtx(t), r.Info().SessionToken, response); err != nil {
					t.Fatal(err)
				}
				if mode != "create_failure" {
					invoke := await(t, r.Events())
					if invoke.Method != protocol.InvokeMethod {
						t.Fatal(invoke)
					}
					switch mode {
					case "invoke_unknown":
						// Session close occurs after possible dispatch, with no provider terminal.
						r.session.close()
					case "invoke_failure":
						if err := g.Accept(
							testCtx(t), r.Info().SessionToken,
							protocol.ErrorResponse(invoke.ID, protocol.NewError(protocol.RemoteError, "handler failed", &fixed)),
						); err != nil {
							t.Fatal(err)
						}
					case "standard_error":
						if err := g.Accept(
							testCtx(t), r.Info().SessionToken,
							protocol.ErrorResponse(invoke.ID, protocol.NewError(protocol.MethodNotFound, "wrong wiring", nil)),
						); err != nil {
							t.Fatal(err)
						}
					default:
						if err := g.Accept(testCtx(t), r.Info().SessionToken, success(invoke, fixed, json.RawMessage(`null`))); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			outcome := await(t, done)
			if outcome.response.Address != fixed {
				t.Fatalf("lost selected address: %+v", outcome)
			}
			want := map[string]int{
				"invoke_failure": protocol.RemoteError,
				"create_failure": protocol.RemoteError,
				"create_unknown": protocol.ActivationUnknown,
				"invoke_unknown": protocol.OutcomeUnknown,
				"standard_error": protocol.ProtocolError,
				"damaged_dto":    protocol.ProtocolError,
			}[mode]
			if want == 0 {
				if outcome.err != nil || string(outcome.response.Value) != "null" {
					t.Fatal(outcome)
				}
			} else {
				rpcCode(t, outcome.err, want)
			}
			if mode == "create_failure" {
				// A second call to the same failed generation uses the retained evidence;
				// it must never issue invoke or recreate merely because pending was removed.
				again := await(t, startCall(g, testCtx(t), fixed))
				rpcCode(t, again.err, protocol.RemoteError)
				select {
				case event := <-r.Events():
					t.Fatalf("failed generation dispatched %s", event.Method)
				default:
				}
			}
		})
	}
}

func TestCloseRetainsEvidenceUntilOuterCallSettles(t *testing.T) {
	release := make(chan struct{})
	settled := make(chan struct{})
	var r *Registration
	g, reg := fakeGateway(t, func(ctx context.Context, a protocol.Address, in protocol.Invoke) (protocol.Address, protocol.Result, error) {
		_, err := r.session.invoke(ctx, a, in)
		if err != nil {
			t.Error(err)
		}
		close(settled)
		<-release
		return a, protocol.Result{}, actorx.ErrStopped
	})
	r = reg
	done := startCall(g, testCtx(t), testAddress("one"))
	event := await(t, r.Events())
	if err := g.Accept(
		testCtx(t), r.Info().SessionToken,
		protocol.ErrorResponse(event.ID, protocol.NewError(protocol.RemoteError, "known failure", nil)),
	); err != nil {
		t.Fatal(err)
	}
	await(t, settled)

	// Closing may stop admission now, but cannot reclaim the outer Call's evidence.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-r.Done():
		t.Fatal("close lost outer-call cleanup responsibility")
	default:
	}
	r.session.mu.Lock()
	count := len(r.session.calls)
	r.session.mu.Unlock()
	if count != 1 {
		t.Fatal("evidence was reclaimed before outer Call settled")
	}

	close(release)
	rpcCode(t, await(t, done).err, protocol.RemoteError)
	if err := r.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	r.session.mu.Lock()
	count = len(r.session.calls) + len(r.session.activations)
	r.session.mu.Unlock()
	if count != 0 {
		t.Fatal("evidence leaked after cleanup")
	}
}

func TestRegistrationOwnershipAndCloseWait(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	owner := &fakeOwner{entered: entered, release: release}
	g := newGateway(backend{register: func(context.Context, string, *session) (registrationOwner, error) { return owner, nil }})
	ctx, cancel := context.WithCancel(testCtx(t))
	r, err := g.Register(ctx, protocol.RegisterParams{Type: "demo.ping", Description: "original"})
	if err != nil {
		t.Fatal(err)
	}
	cancel() // install ctx does not own the session

	list := g.Services()
	if len(list) != 1 {
		t.Fatal(list)
	}
	list[0].Description = "mutated"
	if g.Services()[0].Description != "original" {
		t.Fatal("directory exposed mutable storage")
	}
	if _, err := g.Register(testCtx(t), protocol.RegisterParams{Type: "demo.ping"}); err == nil {
		t.Fatal("duplicate registration admitted")
	} else {
		rpcCode(t, err, protocol.RegistrationConflict)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := g.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	await(t, entered)
	if len(g.Services()) != 0 {
		t.Fatal("closing registration in directory")
	}
	if _, err := g.Register(testCtx(t), protocol.RegisterParams{Type: "other.type"}); err == nil {
		t.Fatal("closed gateway accepted registration")
	}
	if err := g.Accept(testCtx(t), r.Info().SessionToken, protocol.Success(json.RawMessage(`"old"`), nil)); err != ErrSession {
		t.Fatal(err)
	}
	select {
	case <-r.Done():
		t.Fatal("cleanup returned before Unregister")
	default:
	}
	close(release)
	if err := g.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
}

func TestInstallationFailureAndReregistration(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	owner := &fakeOwner{entered: entered, release: release}
	fail := true
	g := newGateway(backend{register: func(context.Context, string, *session) (registrationOwner, error) {
		if fail {
			return owner, actorx.ErrFull
		}
		return &fakeOwner{}, nil
	}})
	r, err := g.Register(testCtx(t), protocol.RegisterParams{Type: "demo.ping"})
	rpcCode(t, err, protocol.Full)
	if r == nil {
		t.Fatal("installation discarded cleanup handle")
	}
	await(t, entered)
	if len(g.Services()) != 0 {
		t.Fatal("failed install published")
	}
	_, err = g.Register(testCtx(t), protocol.RegisterParams{Type: "demo.ping"})
	rpcCode(t, err, protocol.RegistrationConflict)
	close(release)
	if err := r.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	fail = false

	// A replacement registration gets a new session; the stale handle is harmless.
	second, err := g.Register(testCtx(t), protocol.RegisterParams{Type: "demo.ping"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Info().SessionToken == r.Info().SessionToken {
		t.Fatal("session token reused")
	}
	if err := r.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if len(g.Services()) != 1 {
		t.Fatal("stale handle withdrew replacement")
	}
	if err := g.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayAdmissionAndErrorPrecedence(t *testing.T) {
	g, r := fakeGateway(t, nil)
	_ = r
	past := int64(0)
	request := protocol.CallParams{
		Address:        testAddress("one"),
		Input:          json.RawMessage(`null`),
		DeadlineUnixMS: &past,
	}
	_, err := g.Call(testCtx(t), request)
	rpcCode(t, err, protocol.DeadlineExceeded)
	request.DeadlineUnixMS = nil
	request.Address.Type = "missing.type"
	_, err = g.Call(testCtx(t), request)
	rpcCode(t, err, protocol.ServiceUnavailable)
	request.Address.Type = "demo.ping"
	g.mu.Lock()
	g.active = protocol.Capacity
	g.mu.Unlock()
	_, err = g.Call(testCtx(t), request)
	rpcCode(t, err, protocol.Full)
	g.mu.Lock()
	g.active = 0
	g.mu.Unlock()
	rpcCode(t, mapError(errors.Join(context.Canceled, actorxdist.ErrActivationUnknown, actorxdist.ErrOutcomeUnknown), nil), protocol.OutcomeUnknown)
	if _, err := NewGateway(nil); err == nil {
		t.Fatal("nil Runtime accepted")
	}
}

func TestCloseDuringInstallationKeepsHandle(t *testing.T) {
	installing := make(chan struct{})
	installationStopped := make(chan struct{})
	release := make(chan struct{})
	g := newGateway(backend{register: func(ctx context.Context, _ string, _ *session) (registrationOwner, error) {
		close(installing)
		<-ctx.Done()
		close(installationStopped)
		<-release
		return &fakeOwner{}, ctx.Err()
	}})
	type registrationOutcome struct {
		r   *Registration
		err error
	}
	installed := make(chan registrationOutcome, 1)
	go func() {
		r, err := g.Register(testCtx(t), protocol.RegisterParams{Type: "demo.ping"})
		installed <- registrationOutcome{r, err}
	}()
	await(t, installing)

	// Close cancels the installer, but must still wait for its cleanup handle.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	await(t, installationStopped)
	select {
	case <-g.done:
		t.Fatal("Close abandoned in-flight installation")
	default:
	}
	close(release)
	outcome := await(t, installed)
	if outcome.r == nil || outcome.err == nil {
		t.Fatal("installation lost cleanup handle")
	}
	if err := g.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if err := outcome.r.Close(testCtx(t)); err != nil {
		t.Fatal(err)
	}
}
