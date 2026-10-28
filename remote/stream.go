package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

// Stream shares the Call proxy and retains selected identity on error.
// yield's return acknowledges consumption of one chunk; it must honor ctx.
func (g *Gateway) Stream(
	ctx context.Context,
	request protocol.CallParams,
	yield func(protocol.Address, json.RawMessage) error,
) (protocol.StreamResult, error) {
	response := protocol.StreamResult{Address: request.Address}
	if g == nil || ctx == nil || yield == nil {
		return response, actorx.ErrSetup
	}
	if request.Address.Validate(false) != nil || protocol.ValidateJSON(request.Input) != nil {
		return response, protocol.NewError(protocol.InvalidParams, "Invalid stream", nil)
	}
	if _, err := protocol.NewRequest(protocol.Stream, "validation", request); err != nil {
		return response, protocol.NewError(protocol.InvalidParams, "Stream exceeds message limit", nil)
	}

	limit := 5 * time.Second
	if request.DeadlineUnixMS != nil {
		limit = 60 * time.Second
	}
	ctx, cancel := callBudget(ctx, request.DeadlineUnixMS, limit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return response, mapError(err, addressEvidence(response.Address))
	}
	id, err := opaque()
	if err != nil {
		return response, err
	}

	// Call and Stream share admission; registration shutdown joins both.
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return response, mapError(actorx.ErrClosed, addressEvidence(response.Address))
	}
	r := g.registrations[request.Address.Type]
	if r == nil || !r.ready {
		g.mu.Unlock()
		return response, protocol.NewError(protocol.ServiceUnavailable, "Service unavailable", addressEvidence(response.Address))
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

	input := protocol.InvokeStream{
		RequestID:   id,
		MessageType: "json",
		ContentType: "application/json",
		Content:     string(request.Input),
	}
	address, err := g.backend.stream(ctx, request.Address, input, func(a protocol.Address, c protocol.Chunk) error {
		if a.Validate(true) != nil || c.Validate() != nil {
			return actorxdist.ErrProtocol
		}
		response.Address = a
		response.FinalSeq++
		return yield(a, json.RawMessage(c.Content))
	})
	response.Address = address

	// Confirmed provider evidence takes precedence over later transport errors.
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
	return response, nil
}

// ConsumerStream correlates an HTTP-independent consumer segment. Run owns its
// lifetime; Close requests cancellation without stopping the shared Actor.
type ConsumerStream struct {
	gateway  *Gateway
	request  protocol.CallParams
	id       json.RawMessage
	token    string
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	seq, ack uint64
	terminal bool
	wake     chan struct{}
}

// OpenStream validates a consumer request and reserves its correlation token.
// The caller sends Open before Run and closes the stream on transport exit.
func (g *Gateway) OpenStream(ctx context.Context, id json.RawMessage, request protocol.CallParams) (*ConsumerStream, error) {
	if g == nil || ctx == nil || len(id) == 0 {
		return nil, actorx.ErrSetup
	}
	m := protocol.Notification(protocol.Stream, request)
	m.ID = id
	raw, err := m.Encode()
	if err != nil {
		return nil, protocol.NewError(protocol.InvalidParams, "Invalid stream", nil)
	}
	parsed, e := protocol.ParseMessage(raw)
	if e != nil {
		return nil, e
	}
	if _, e = parsed.Parameters(protocol.ToGateway); e != nil {
		return nil, e
	}

	limit := 5 * time.Second
	if request.DeadlineUnixMS != nil {
		limit = 60 * time.Second
	}
	ctx, cancel := callBudget(ctx, request.DeadlineUnixMS, limit)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, mapError(err, addressEvidence(request.Address))
	}
	token, err := opaque()
	if err != nil {
		cancel()
		return nil, err
	}
	s := &ConsumerStream{
		gateway: g,
		request: request,
		id:      append(json.RawMessage(nil), id...),
		token:   token,
		ctx:     ctx,
		cancel:  cancel,
		wake:    make(chan struct{}, 1),
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		cancel()
		return nil, mapError(actorx.ErrClosed, nil)
	}
	if len(g.consumerStreams) >= protocol.Capacity {
		cancel()
		return nil, mapError(actorx.ErrFull, nil)
	}
	g.consumerStreams[token] = s
	return s, nil
}

// Open is the first notification, carrying correlation and window parameters.
func (s *ConsumerStream) Open() protocol.Message {
	return protocol.Notification(protocol.StreamOpen, protocol.OpenParams{
		RequestID:   s.id,
		StreamToken: s.token,
		Window:      protocol.StreamWindow(),
	})
}

// Close cancels this segment and releases its gateway entry. It is idempotent.
func (s *ConsumerStream) Close() {
	s.cancel()
	s.gateway.mu.Lock()
	delete(s.gateway.consumerStreams, s.token)
	s.gateway.mu.Unlock()
}

// Run writes chunks and returns the terminal response after consumption settles.
// write must honor the context supplied to OpenStream.
func (s *ConsumerStream) Run(write func(protocol.Message) error) protocol.Message {
	defer s.Close()
	result, err := s.gateway.Stream(s.ctx, s.request, func(a protocol.Address, value json.RawMessage) error {
		s.mu.Lock()
		s.seq++
		seq := s.seq
		s.mu.Unlock()
		m := protocol.Notification(protocol.ChunkMethod, protocol.ChunkParams{
			RequestID:   s.id,
			StreamToken: s.token,
			Address:     a,
			Seq:         seq,
			Value:       value,
		})
		if err := write(m); err != nil {
			return err
		}

		// Flushing SSE is delivery only. Wait for consumer credit before
		// acknowledging this chunk to the native stream and the provider.
		for {
			s.mu.Lock()
			ack := s.ack
			s.mu.Unlock()
			if ack >= seq {
				return nil
			}
			select {
			case <-s.wake:
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		}
	})

	s.mu.Lock()
	s.terminal = true
	seq := s.seq
	s.mu.Unlock()
	result.FinalSeq = seq
	if err != nil {
		return protocol.ErrorResponse(s.id, protocol.StreamError(mapStreamError(err, result.Address), seq))
	}
	return protocol.Success(s.id, result)
}

func mapStreamError(err error, a protocol.Address) *protocol.Error {
	if e, ok := err.(*protocol.Error); ok {
		return e
	}
	return mapError(err, addressEvidence(a))
}

// Control admits only correlated notifications. Invalid controls never settle
// the business request or overwrite an already established terminal.
func (g *Gateway) Control(ctx context.Context, m protocol.Message) error {
	if ctx == nil {
		return actorx.ErrSetup
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	params, e := m.Parameters(protocol.ToGateway)
	if e != nil {
		return ErrResponse
	}
	c, ok := params.(protocol.ControlParams)
	if !ok {
		return ErrResponse
	}
	g.mu.Lock()
	s := g.consumerStreams[c.StreamToken]
	g.mu.Unlock()
	if s == nil {
		return ErrSession
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !bytes.Equal(s.id, c.RequestID) {
		return ErrCorrelation
	}
	if m.Method == protocol.Cancel {
		if !s.terminal {
			s.cancel()
		}
		return nil
	}
	if c.AckSeq > s.seq {
		return ErrCorrelation
	}
	if c.AckSeq > s.ack {
		s.ack = c.AckSeq
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	return nil
}
