// Package remote coordinates cross-language providers with an existing actorx-dist
// Runtime. It owns registrations and pending work, never the borrowed Runtime.
package remote

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

// registrationOwner keeps cleanup responsibility even when installation fails.
type registrationOwner interface{ Unregister(context.Context) error }

// backend is a private test seam. Production always uses actorxdist. In-memory tests
// cannot establish activation ownership, real Self or network routing.
type backend struct {
	register func(context.Context, string, *session) (registrationOwner, error)
	stream   func(context.Context, protocol.Address, protocol.InvokeStream, func(protocol.Address, protocol.Chunk) error) (protocol.Address, error)
	call     func(context.Context, protocol.Address, protocol.Invoke) (protocol.Address, protocol.Result, error)
}

// Gateway owns provider registrations and admission to their shared Runtime.
// mu protects registration visibility, the active Call/Stream count and shutdown.
type Gateway struct {
	mu              sync.Mutex
	backend         backend
	registrations   map[string]*Registration
	active          int
	consumerStreams map[string]*ConsumerStream
	closed          bool
	once            sync.Once
	done            chan struct{}
	err             error
}

// NewGateway borrows rt. Its creator closes rt after Gateway and transport
// shutdown. Construction validates the Runtime without network I/O.
func NewGateway(rt *actorxdist.Runtime) (*Gateway, error) {
	if _, err := rt.Addr("remote.probe", "probe"); err != nil {
		return nil, err
	}
	return newGateway(backend{
		register: func(ctx context.Context, typ string, s *session) (registrationOwner, error) {
			r, err := rt.Register(ctx, actorxdist.ActorSpec[*proxyActor]{
				Type: typ,
				New:  func(actorxdist.ActorKey) *proxyActor { return &proxyActor{session: s} },
			})
			if r == nil {
				return nil, err
			}
			return r, err
		},
		stream: func(ctx context.Context, address protocol.Address, input protocol.InvokeStream, yield func(protocol.Address, protocol.Chunk) error) (protocol.Address, error) {
			raw, _ := json.Marshal(address)
			a, err := rt.DecodeAddr(raw)
			if err != nil {
				return address, err
			}
			err = a.Stream[protocol.Chunk](ctx, input, func(c protocol.Chunk) error {
				raw, _ := a.MarshalJSON()
				var selected protocol.Address
				_ = json.Unmarshal(raw, &selected)
				return yield(selected, c)
			})
			if raw, e := a.MarshalJSON(); e == nil {
				_ = json.Unmarshal(raw, &address)
			}
			return address, err
		},
		call: func(ctx context.Context, address protocol.Address, input protocol.Invoke) (protocol.Address, protocol.Result, error) {
			raw, _ := json.Marshal(address)
			addr, err := rt.DecodeAddr(raw)
			if err != nil {
				return address, protocol.Result{}, err
			}
			result, err := addr.Call[protocol.Result](ctx, input)
			// A failed Call may still have selected a generation.
			if raw, e := addr.MarshalJSON(); e == nil {
				_ = json.Unmarshal(raw, &address)
			}
			return address, result, err
		},
	}), nil
}

func newGateway(b backend) *Gateway {
	return &Gateway{
		backend:         b,
		registrations:   map[string]*Registration{},
		consumerStreams: map[string]*ConsumerStream{},
		done:            make(chan struct{}),
	}
}

// Registration must not be copied. Events has one reader and closes when session
// admission ends. Done closes after Unregister and all outer Call/Stream operations finish.
type Registration struct {
	gateway     *Gateway
	session     *session
	info        protocol.RegistrationInfo
	description string
	owner       registrationOwner
	installed   chan struct{}
	done        chan struct{}
	calls       sync.WaitGroup
	once        sync.Once

	// gateway.mu protects ready/closing. installed publishes owner.
	ready, closing bool
	err            error
}

// Info returns this registration's immutable protocol data.
func (r *Registration) Info() protocol.RegistrationInfo {
	return r.info
}

// Events has one transport reader; closing the session closes the channel.
func (r *Registration) Events() <-chan protocol.Message { return r.session.events }

// Done closes after Unregister and all admitted Call/Stream operations finish cleanup.
func (r *Registration) Done() <-chan struct{} { return r.done }

// Register bounds installation with ctx; ctx does not own the provider lifetime.
// A non-nil handle accompanying an error must still be joined with Close.
func (g *Gateway) Register(ctx context.Context, request protocol.RegisterParams) (*Registration, error) {
	if g == nil || ctx == nil {
		return nil, actorx.ErrSetup
	}
	if request.Validate() != nil {
		return nil, protocol.NewError(protocol.InvalidParams, "Invalid registration", nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, mapError(err, nil)
	}
	token, err := opaque()
	if err != nil {
		return nil, err
	}
	r := &Registration{
		gateway: g,
		session: newSession(),
		info: protocol.RegistrationInfo{
			Protocol:     protocol.Version,
			Type:         request.Type,
			SessionToken: token,
		},
		description: request.Description,
		installed:   make(chan struct{}),
		done:        make(chan struct{}),
	}

	// Reserve the Type before installation so another register cannot race it.
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil, mapError(actorx.ErrClosed, nil)
	}
	if g.registrations[request.Type] != nil {
		g.mu.Unlock()
		return nil, protocol.NewError(protocol.RegistrationConflict, "Type already registered or closing", nil)
	}
	g.registrations[request.Type] = r
	g.mu.Unlock()

	// Session shutdown cancels installation; a successful install outlives ctx.
	installCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.session.life, cancel)
	r.owner, err = g.backend.register(installCtx, request.Type, r.session)
	stop()
	cancel()

	g.mu.Lock()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && (g.closed || r.closing) {
		err = actorx.ErrClosed
	}
	r.ready = err == nil
	close(r.installed)
	g.mu.Unlock()

	if err != nil {
		r.beginClose()
		return r, mapError(err, nil)
	}
	return r, nil
}

func (r *Registration) beginClose() {
	r.once.Do(func() {
		g := r.gateway
		g.mu.Lock()
		r.closing = true
		r.ready = false
		g.mu.Unlock()
		r.session.close()
		go func() {
			// Installation publishes owner before cleanup may unregister it.
			<-r.installed
			if r.owner != nil {
				r.err = r.owner.Unregister(context.Background())
			}
			r.calls.Wait()
			r.session.clearEvidence()

			g.mu.Lock()
			if g.registrations[r.info.Type] == r {
				delete(g.registrations, r.info.Type)
			}
			close(r.done)
			g.mu.Unlock()
		}()
	})
}

// Close is idempotent. Timeout ends only this wait; cleanup continues.
func (r *Registration) Close(ctx context.Context) error {
	if r == nil || r.gateway == nil || ctx == nil {
		return actorx.ErrSetup
	}
	r.beginClose()
	return waitClose(ctx, r.done, func() error { return r.err })
}

// Close rejects new work and joins every owned registration. The borrowed
// Runtime remains the caller's responsibility, even if this wait times out.
func (g *Gateway) Close(ctx context.Context) error {
	if g == nil || ctx == nil {
		return actorx.ErrSetup
	}
	g.once.Do(func() {
		g.mu.Lock()
		g.closed = true
		for _, s := range g.consumerStreams {
			s.cancel()
		}
		regs := make([]*Registration, 0, len(g.registrations))
		for _, r := range g.registrations {
			regs = append(regs, r)
		}
		g.mu.Unlock()
		for _, r := range regs {
			r.beginClose()
		}
		go func() {
			var errs []error
			for _, r := range regs {
				<-r.done
				errs = append(errs, r.err)
			}
			g.err = errors.Join(errs...)
			close(g.done)
		}()
	})
	return waitClose(ctx, g.done, func() error { return g.err })
}

func waitClose(ctx context.Context, done <-chan struct{}, result func() error) error {
	select {
	case <-done:
		return result()
	default:
	}
	select {
	case <-done:
		return result()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Actors returns an independently allocated snapshot of ready Actor registrations
// on this Gateway. It does not enumerate running instances.
func (g *Gateway) Actors() []protocol.Actor {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]protocol.Actor, 0, len(g.registrations))
	for _, r := range g.registrations {
		if r.ready && !g.closed {
			out = append(out, protocol.Actor{
				Type:        r.info.Type,
				Description: r.description,
				Protocol:    protocol.Version,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// Call retains the selected address on failure. It never retries or replaces a
// generation. Confirmed provider evidence wins over later cancellation/stop.
func (g *Gateway) Call(ctx context.Context, request protocol.CallParams) (protocol.CallResult, error) {
	response := protocol.CallResult{Address: request.Address}
	if g == nil || ctx == nil {
		return response, actorx.ErrSetup
	}
	if request.Address.Validate(false) != nil || protocol.ValidateJSON(request.Input) != nil {
		return response, protocol.NewError(protocol.InvalidParams, "Invalid call", nil)
	}
	if _, err := protocol.NewRequest(protocol.Call, "validation", request); err != nil {
		return response, protocol.NewError(protocol.InvalidParams, "Call exceeds message limit", nil)
	}

	ctx, cancel := callBudget(ctx, request.DeadlineUnixMS, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return response, mapError(err, addressEvidence(response.Address))
	}
	id, err := opaque()
	if err != nil {
		return response, err
	}

	// Account for the Call before releasing mu; Close must join it.
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return response, mapError(actorx.ErrClosed, addressEvidence(response.Address))
	}
	r := g.registrations[request.Address.Type]
	if r == nil || !r.ready {
		g.mu.Unlock()
		return response, protocol.NewError(protocol.ActorUnavailable, "Actor unavailable", addressEvidence(response.Address))
	}
	if g.active >= protocol.Capacity {
		g.mu.Unlock()
		return response, mapError(actorx.ErrFull, addressEvidence(response.Address))
	}
	g.active++
	r.calls.Add(1)
	r.session.track(id)
	g.mu.Unlock()

	defer func() {
		r.session.forget(id)
		g.mu.Lock()
		g.active--
		g.mu.Unlock()
		r.calls.Done()
	}()

	input := protocol.Invoke{
		RequestID:   id,
		MessageType: "json",
		ContentType: "application/json",
		Content:     string(request.Input),
	}
	address, result, err := g.backend.call(ctx, request.Address, input)
	response.Address = address

	// Seal the evidence window before interpreting actorx-dist's possibly later error.
	evidence := r.session.settle(id, address)
	if evidence.terminal {
		if evidence.err != nil {
			return response, mapError(evidence.err, addressEvidence(address))
		}
		if evidence.result.Failure != nil {
			return response, protocol.NewError(evidence.result.Failure.Code, evidence.result.Failure.Message, addressEvidence(address))
		}
	} else if evidence.sent {
		return response, protocol.NewError(protocol.OutcomeUnknown, "Provider outcome is unknown", addressEvidence(address))
	}
	if err != nil {
		return response, mapError(err, addressEvidence(address))
	}
	if result.Validate() != nil {
		return response, mapError(actorxdist.ErrProtocol, addressEvidence(address))
	}
	if result.Failure != nil {
		return response, protocol.NewError(result.Failure.Code, result.Failure.Message, addressEvidence(address))
	}
	response.Value = json.RawMessage(result.Content)
	return response, nil
}

func addressEvidence(a protocol.Address) *protocol.Address {
	if a.Generation == "" {
		return nil
	}
	return &a
}

func callBudget(ctx context.Context, unixMS *int64, limit time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(limit)
	if unixMS != nil && time.UnixMilli(*unixMS).Before(deadline) {
		deadline = time.UnixMilli(*unixMS)
	}
	return context.WithDeadline(ctx, deadline)
}

func opaque() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func mapError(err error, address *protocol.Address) *protocol.Error {
	code := protocol.InternalError
	switch {
	case errors.Is(err, actorxdist.ErrOutcomeUnknown):
		code = protocol.OutcomeUnknown
	case errors.Is(err, actorxdist.ErrActivationUnknown):
		code = protocol.ActivationUnknown
	case errors.Is(err, actorxdist.ErrProtocol):
		code = protocol.ProtocolError
	case errors.Is(err, actorx.ErrFull):
		code = protocol.Full
	case errors.Is(err, actorxdist.ErrTypeUnavailable):
		code = protocol.ActorUnavailable
	case errors.Is(err, actorxdist.ErrOwnerUnavailable):
		code = protocol.OwnerUnavailable
	case errors.Is(err, actorxdist.ErrContract):
		code = protocol.Contract
	case errors.Is(err, context.DeadlineExceeded):
		code = protocol.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = protocol.Canceled
	case errors.Is(err, actorx.ErrStopped), errors.Is(err, actorx.ErrClosed):
		code = protocol.Stopped
	}
	return protocol.NewError(code, protocol.Kind(code), address)
}
