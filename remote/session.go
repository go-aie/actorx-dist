package remote

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

// Transport-neutral rejections: HTTP maps these to 403, 409 and 400.
var (
	ErrSession     = errors.New("remote: invalid or closed session")
	ErrCorrelation = errors.New("remote: unknown, settled or mismatched response")
	ErrResponse    = errors.New("remote: malformed provider response")
)

// terminal records what is known about one provider exchange. sent means the
// request entered the outbound queue; terminal means a validated reply arrived.
type terminal struct {
	sent, terminal bool
	address        protocol.Address
	result         protocol.Result
	finalSeq       uint64
	err            error
}

type activationRecord struct{ terminal }

type pending struct {
	method    string
	address   protocol.Address
	requestID string
	done      chan struct{}
	ctx       context.Context
	result    protocol.Result
	err       error
}

type session struct {
	mu               sync.Mutex
	life             context.Context
	cancel           context.CancelFunc
	closed           bool
	requestSeq       uint64 // assigned under mu in outbound queue order
	streams          map[string]*providerStream
	events           chan protocol.Message
	pending          map[string]*pending
	creates, invokes int

	// Records are scoped to this registration and the complete fixed identity.
	// They outlive pending; cleanup joins Unregister and outer Calls first.
	activations map[protocol.Address]*activationRecord
	calls       map[string]*terminal
}

func newSession() *session {
	life, cancel := context.WithCancel(context.Background())
	return &session{
		life:   life,
		cancel: cancel,
		// One queue for both operations, with room for creates, credits and cancels.
		events:      make(chan protocol.Message, protocol.Capacity*8),
		pending:     map[string]*pending{},
		streams:     map[string]*providerStream{},
		activations: map[protocol.Address]*activationRecord{},
		calls:       map[string]*terminal{},
	}
}

func (s *session) track(id string) {
	s.mu.Lock()
	s.calls[id] = &terminal{}
	s.mu.Unlock()
}

func (s *session) forget(id string) {
	s.mu.Lock()
	delete(s.calls, id)
	s.mu.Unlock()
}

func (s *session) evidence(id string, a protocol.Address) terminal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evidenceLocked(id, a)
}

func (s *session) evidenceLocked(id string, a protocol.Address) terminal {
	if c := s.calls[id]; c != nil && c.address == a && (c.sent || c.terminal) {
		return *c
	}
	if c := s.activations[a]; c != nil && c.terminal.terminal && (c.err != nil || c.result.Failure != nil) {
		return c.terminal
	}
	return terminal{}
}

// settle closes the outer Call/Stream evidence window atomically with pending
// withdrawal. A delayed worker/response cannot publish after this boundary.
func (s *session) settle(id string, a protocol.Address) terminal {
	s.mu.Lock()
	defer s.mu.Unlock()
	evidence := s.evidenceLocked(id, a)
	for rpcID, p := range s.pending {
		if p.method == protocol.Call && p.requestID == id {
			s.remove(rpcID, p)
			p.err = context.Canceled
			close(p.done)
		}
	}
	delete(s.calls, id)
	return evidence
}

func (s *session) clearEvidence() {
	s.mu.Lock()
	clear(s.activations)
	clear(s.calls)
	s.mu.Unlock()
}

func (s *session) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.cancel()
	for id, p := range s.pending {
		s.remove(id, p)
		p.err = actorx.ErrStopped
		close(p.done)
	}
	for _, p := range s.streams {
		p.stop(actorx.ErrStopped)
	}
	close(s.events)
}

// remove releases pending capacity. The caller must hold s.mu.
func (s *session) remove(id string, p *pending) {
	delete(s.pending, id)
	if p.method == protocol.Create {
		s.creates--
	} else {
		s.invokes--
	}
}

func (s *session) create(ctx context.Context, a protocol.Address) error {
	_, err := s.exchange(ctx, protocol.Create, a, "", nil)
	return err
}

func (s *session) invoke(ctx context.Context, a protocol.Address, input protocol.Invoke) (protocol.Result, error) {
	if input.Validate() != nil {
		return protocol.Result{}, actorxdist.ErrProtocol
	}
	return s.exchange(ctx, protocol.Call, a, input.RequestID, json.RawMessage(input.Content))
}

// requestLocked allocates a session-local ID. Gaps are legal, but IDs never wrap
// or go backwards. The caller holds mu through enqueue to preserve wire order.
func (s *session) requestLocked(method string, params any) (protocol.Message, error) {
	if s.requestSeq == protocol.MaxSequence {
		return protocol.Message{}, actorxdist.ErrProtocol
	}
	id := "r:" + strconv.FormatUint(s.requestSeq+1, 10)
	m, err := protocol.NewRequest(method, id, params)
	if err != nil {
		return protocol.Message{}, actorxdist.ErrProtocol
	}
	s.requestSeq++
	return m, nil
}

func (s *session) exchange(ctx context.Context, method string, a protocol.Address, requestID string, input json.RawMessage) (protocol.Result, error) {
	if ctx == nil || a.Validate(true) != nil {
		return protocol.Result{}, actorxdist.ErrProtocol
	}
	if err := ctx.Err(); err != nil {
		return protocol.Result{}, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return protocol.Result{}, actorxdist.ErrProtocol
	}
	var params any = protocol.CreateParams{Address: a, DeadlineUnixMS: deadline.UnixMilli()}
	if method == protocol.Call {
		ms := deadline.UnixMilli()
		params = protocol.CallParams{Address: a, Input: input, DeadlineUnixMS: &ms}
	}
	p := &pending{
		method:    method,
		address:   a,
		requestID: requestID,
		done:      make(chan struct{}),
		ctx:       ctx,
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return protocol.Result{}, actorx.ErrStopped
	}
	if method == protocol.Create {
		if s.activations[a] != nil {
			s.mu.Unlock()
			return protocol.Result{}, actorxdist.ErrProtocol
		}
		if s.creates >= protocol.Capacity || len(s.activations) >= protocol.Capacity {
			s.mu.Unlock()
			return protocol.Result{}, actorx.ErrFull
		}
	} else if s.invokes >= protocol.Capacity {
		s.mu.Unlock()
		return protocol.Result{}, actorx.ErrFull
	} else if s.calls[requestID] == nil {
		s.mu.Unlock()
		return protocol.Result{}, actorxdist.ErrProtocol
	} else if s.calls[requestID].sent {
		s.mu.Unlock()
		return protocol.Result{}, actorxdist.ErrProtocol
	}
	message, err := s.requestLocked(method, params)
	if err != nil {
		s.mu.Unlock()
		return protocol.Result{}, err
	}
	var id string
	_ = json.Unmarshal(message.ID, &id)

	// Publish pending before visibility. Accept/close and nonblocking enqueue share
	// this mutex, so a full queue cannot record a dispatch or race channel close.
	s.pending[id] = p
	if method == protocol.Create {
		s.creates++
	} else {
		s.invokes++
	}
	select {
	case s.events <- message:
		if method == protocol.Create {
			s.activations[a] = &activationRecord{terminal: terminal{sent: true}}
		} else {
			s.calls[requestID].sent = true
			s.calls[requestID].address = a
		}
	default:
		s.remove(id, p)
		s.mu.Unlock()
		return protocol.Result{}, actorx.ErrFull
	}
	s.mu.Unlock()

	select {
	case <-p.done:
	case <-ctx.Done():
	}
	s.mu.Lock()
	// Accepted terminal evidence wins a later cancellation. Removed pending cannot
	// be resurrected by a late response, including while outer Call is still alive.
	if s.pending[id] == p {
		s.remove(id, p)
		p.err = ctx.Err()
	}
	result, err := p.result, p.err
	s.mu.Unlock()
	return result, err
}

// Accept validates and hands off a provider response without touching Actor
// fields or Reply. Invalid correlation never consumes a legitimate pending item.
func (g *Gateway) Accept(ctx context.Context, sessionToken string, response protocol.Message) error {
	if g == nil || ctx == nil {
		return actorx.ErrSetup
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := response.Encode()
	if err != nil {
		return ErrResponse
	}
	return g.AcceptEncoded(ctx, sessionToken, raw)
}

// AcceptEncoded preserves the actual UTF-8 byte size of provider chunk POSTs.
// Transports must use it rather than decode/re-encode before window accounting.
func (g *Gateway) AcceptEncoded(ctx context.Context, sessionToken string, raw []byte) error {
	if g == nil || ctx == nil {
		return actorx.ErrSetup
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(raw) > protocol.MaxMessageBytes {
		return ErrResponse
	}
	parsed, rpcErr := protocol.ParseMessage(raw)
	if rpcErr != nil || parsed.IsRequest() && (!parsed.IsNotification() || parsed.Method != protocol.ChunkMethod) {
		return ErrResponse
	}
	g.mu.Lock()
	var found *session
	for _, r := range g.registrations {
		if r.info.SessionToken == sessionToken && !r.closing {
			found = r.session
			break
		}
	}
	g.mu.Unlock()

	if found == nil {
		return ErrSession
	}
	if parsed.IsRequest() {
		return found.acceptChunk(parsed, len(raw))
	}
	return found.accept(parsed)
}

func (s *session) accept(response protocol.Message) error {
	var id string
	if json.Unmarshal(response.ID, &id) != nil || id == "" {
		return ErrCorrelation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSession
	}
	if p := s.streams[id]; p != nil {
		return s.acceptStream(p, response)
	}
	p := s.pending[id]
	if p == nil || p.ctx.Err() != nil {
		return ErrCorrelation
	}

	// Validate the full response before removing pending or publishing evidence.
	var result protocol.Result
	var err error
	if response.Error != nil {
		if e := protocol.ErrorAddress(response.Error.Data, p.address); e != nil {
			if errors.Is(e, protocol.ErrAddressMismatch) {
				return ErrCorrelation
			}
			return ErrResponse
		}
		failure, e := protocol.NormalizeProviderError(response.Error)
		if e != nil {
			if !errors.Is(e, protocol.ErrProviderWiring) {
				return ErrResponse
			}
			err = actorxdist.ErrProtocol
		} else {
			result = protocol.NewResult(nil, failure)
		}
	} else {
		value, e := protocol.ProviderResult(response.Result, p.method, p.address)
		if e != nil {
			if errors.Is(e, protocol.ErrAddressMismatch) {
				return ErrCorrelation
			}
			return ErrResponse
		}
		if p.method == protocol.Call {
			result = protocol.NewResult(value, nil)
		}
	}

	// Save evidence before waking Started/worker. Create failures must survive actorx-dist
	// lifecycle error conversion and deletion of their original pending request.
	evidence := terminal{
		sent:     true,
		terminal: true,
		address:  p.address,
		result:   result,
		err:      err,
	}
	if p.method == protocol.Create {
		s.activations[p.address].terminal = evidence
		if err == nil && result.Failure != nil {
			err = errProviderCreate
		}
	} else if call := s.calls[p.requestID]; call != nil {
		*call = evidence
	}
	p.result, p.err = result, err
	s.remove(id, p)
	close(p.done)
	return nil
}

var errProviderCreate = errors.New("remote: provider creation failed (details retained by registration)")
