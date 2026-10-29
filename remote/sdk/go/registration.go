package actorxremote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

const providerCapacity = 8

type registerOptions struct{ description string }

// RegisterOption configures a registration.
type RegisterOption func(*registerOptions)

// WithDescription sets a description of at most 512 UTF-8 bytes.
func WithDescription(description string) RegisterOption {
	return func(o *registerOptions) { o.description = description }
}

// Register installs a provider session within five seconds (or ctx's earlier
// deadline). Success transfers the SSE lifetime to Runtime; canceling ctx later
// does not unregister it. Register does not call factory.
func (r *Runtime) Register(ctx context.Context, actorType string, factory ActorFactory, opts ...RegisterOption) error {
	if err := wire.Identity(actorType, "registration"); err != nil {
		return err
	}
	o := registerOptions{}
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("actorxremote: invalid registration option")
		}
		opt(&o)
	}
	if factory == nil || !utf8.ValidString(o.description) || len(o.description) > 512 {
		return fmt.Errorf("actorxremote: expected factory and bounded description")
	}

	install, stop := context.WithTimeout(ctx, callTimeout)
	defer stop()
	if err := install.Err(); err != nil {
		return beforeDispatch(err)
	}

	// Reserve the type before opening SSE so concurrent registrations cannot
	// install a second session. run rolls the reservation back on failure.
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return remoteError(1006, nil)
	}
	if r.registrations[actorType] != nil {
		r.mu.Unlock()
		return remoteError(1007, nil)
	}
	life, cancel := context.WithCancel(r.ctx)
	p := &registration{
		runtime:   r,
		actorType: actorType,
		factory:   factory,
		ctx:       life,
		cancel:    cancel,
		ready:     make(chan error, 1),
		accepted:  make(chan struct{}),
		done:      make(chan struct{}),
		instances: make(map[Address]*providerInstance),
		active:    make(map[*providerWork]struct{}),
		streams:   make(map[string]*providerStream),
		replies:   make(chan providerReply, providerCapacity),
	}
	r.registrations[actorType] = p
	r.work.Add(1)
	r.mu.Unlock()
	go p.run(o.description)

	var err error
	select {
	case err = <-p.ready:
	case <-install.Done():
		err = beforeDispatch(install.Err())
	}
	if err == nil {
		// Only a completed installation transfers the SSE lifetime to Runtime.
		r.mu.Lock()
		if install.Err() != nil {
			err = beforeDispatch(install.Err())
		} else if r.closing {
			err = remoteError(1006, nil)
		} else {
			close(p.accepted)
		}
		r.mu.Unlock()
	}
	if err != nil {
		p.cancel()
		<-p.done
	}
	return err
}

type registration struct {
	runtime   *Runtime
	actorType string
	factory   ActorFactory
	token     string
	ctx       context.Context
	cancel    context.CancelFunc
	ready     chan error
	accepted  chan struct{}
	done      chan struct{}
	jobs      sync.WaitGroup

	mu             sync.Mutex // protects instances, active execution slots and streams
	instances      map[Address]*providerInstance
	active         map[*providerWork]struct{}
	streams        map[string]*providerStream
	lastRequestSeq uint64 // only the SSE reader accesses this session's sequence
	replies        chan providerReply
}

func (p *registration) run(description string) {
	defer p.runtime.work.Done()
	defer close(p.done)
	defer func() {
		p.cancel()
		p.jobs.Wait() // includes handlers which have ignored cancellation
		p.mu.Lock()
		clear(p.instances)
		clear(p.active)
		p.factory = nil
		p.mu.Unlock()
		p.token = ""
		p.runtime.mu.Lock()
		if p.runtime.registrations[p.actorType] == p {
			delete(p.runtime.registrations, p.actorType)
		}
		p.runtime.mu.Unlock()
	}()

	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		p.ready <- transportError(nil)
		return
	}
	id := "p:" + hex.EncodeToString(random[:])
	raw, err := wire.Request("runtime/register", id, struct {
		Type        string `json:"type"`
		Description string `json:"description"`
	}{p.actorType, description})
	if err != nil {
		p.ready <- protocolError()
		return
	}
	response, err := p.runtime.post(p.ctx, raw, true, "")
	if err != nil {
		p.ready <- transportError(err)
		return
	}
	defer response.Body.Close()

	// Explicitly close a pending SSE read on cancellation, also for bounded
	// custom bodies whose Read does not observe the HTTP request context.
	closedBody := make(chan struct{})
	stopBody := context.AfterFunc(p.ctx, func() {
		response.Body.Close()
		close(closedBody)
	})
	defer func() {
		if !stopBody() {
			<-closedBody
		}
	}()

	if response.StatusCode != 200 {
		p.ready <- statusError(response.StatusCode)
		return
	}

	reader := newSSEReader(response.Body)
	var first []byte
	switch mediaType(response.Header) {
	case "application/json":
		first, err = readBody(response.Body)
	case "text/event-stream":
		first, err = reader.next()
	default:
		err = protocolError()
	}
	if err == nil {
		var reply wire.Message
		reply, err = wire.Response(first, id)
		if err != nil {
			err = protocolError()
		} else if reply.Error != nil {
			if wire.ValidateError(reply.Error) != nil {
				err = protocolError()
			} else {
				err = &RemoteError{Code: wire.Kind(reply.Error.Code), RPCCode: reply.Error.Code, Data: reply.Error.Data}
			}
		} else if mediaType(response.Header) != "text/event-stream" {
			err = protocolError()
		} else {
			p.token, err = registrationToken(reply.Result, p.actorType)
		}
	}
	if err != nil {
		p.ready <- err
		return
	}

	p.ready <- nil
	select {
	case <-p.accepted:
	case <-p.ctx.Done():
		return
	}

	p.jobs.Go(p.replyLoop)
	for {
		raw, err := reader.next()
		if err == nil {
			var message wire.Message
			message, err = wire.Parse(raw)
			if err != nil {
				err = protocolError()
			} else {
				err = p.dispatch(message)
			}
		}
		if err != nil {
			if p.ctx.Err() == nil {
				p.runtime.fail(err)
			}
			return
		}
	}
}

func registrationToken(raw []byte, actorType string) (string, error) {
	f, err := wire.Fields(raw, []string{"protocol", "type", "session_token"}, "protocol", "type", "session_token")
	var protocol, gotType, token string
	if err != nil || json.Unmarshal(f["protocol"], &protocol) != nil || protocol != wire.Version ||
		json.Unmarshal(f["type"], &gotType) != nil || gotType != actorType ||
		json.Unmarshal(f["session_token"], &token) != nil || len(token) == 0 || len(token) > 256 {
		return "", protocolError()
	}
	for _, c := range token {
		if c < 33 || c > 126 || c == ',' {
			return "", protocolError()
		}
	}
	return token, nil
}

type providerReply struct {
	message  wire.Message
	deadline time.Time
}

// Immediate rejections have a separate bounded queue so a slow response POST
// cannot stall SSE admission or create unbounded response goroutines.
func (p *registration) enqueue(message wire.Message, deadline time.Time) error {
	if message.ID == nil || !time.Now().Before(deadline) {
		return nil
	}
	select {
	case p.replies <- providerReply{message, deadline}:
		return nil
	default:
		return remoteError(1001, nil)
	}
}

func (p *registration) replyLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case reply := <-p.replies:
			p.reply(reply.message, reply.deadline)
		}
	}
}

func (p *registration) reply(message wire.Message, deadline time.Time) {
	if message.ID == nil || p.ctx.Err() != nil || !time.Now().Before(deadline) {
		return
	}
	ctx, cancel := context.WithDeadline(p.ctx, deadline)
	defer cancel()
	raw, err := wire.Encode(message)
	if err != nil {
		p.runtime.fail(protocolError())
		return
	}
	response, err := p.runtime.post(ctx, raw, false, p.token)
	if err == nil {
		defer response.Body.Close()
		if response.StatusCode == 204 || response.StatusCode == 409 {
			return
		}
		err = statusError(response.StatusCode)
	} else {
		err = transportError(err)
	}
	// Deadline expiry does not open a fresh POST budget or produce another
	// result. Other upstream failures invalidate the provider session.
	if ctx.Err() == nil {
		p.runtime.fail(err)
	}
}
