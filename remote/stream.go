package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

// StreamChunk retains the selected generation alongside the JSON value.
type StreamChunk struct {
	Address protocol.Address
	Value   json.RawMessage
}

// Stream starts on first iteration and may be consumed once. A normal loop body
// acknowledges its chunk. Early exit cancels only this request.
func (g *Gateway) Stream(ctx context.Context, request protocol.CallParams) iter.Seq2[StreamChunk, error] {
	var used atomic.Bool

	return func(yield func(StreamChunk, error) bool) {
		if !used.CompareAndSwap(false, true) {
			yield(StreamChunk{}, actorx.ErrSetup)
			return
		}
		result := protocol.StreamResult{Address: request.Address}
		g.stream(ctx, request, &result)(yield)
	}
}

// stream is the single consumption path shared by the public iterator and HTTP.
// result also carries generation evidence for empty and failed streams.
func (g *Gateway) stream(ctx context.Context, request protocol.CallParams, result *protocol.StreamResult) iter.Seq2[StreamChunk, error] {
	return func(yield func(StreamChunk, error) bool) {
		fail := func(err error) { yield(StreamChunk{}, err) }
		if g == nil || ctx == nil {
			fail(actorx.ErrSetup)
			return
		}
		if request.Address.Validate(false) != nil || protocol.ValidateJSON(request.Input) != nil {
			fail(protocol.NewError(protocol.InvalidParams, "Invalid stream", nil))
			return
		}
		if _, err := protocol.NewRequest(protocol.Stream, "validation", request); err != nil {
			fail(protocol.NewError(protocol.InvalidParams, "Stream exceeds message limit", nil))
			return
		}

		limit := 5 * time.Second
		if request.DeadlineUnixMS != nil {
			limit = 60 * time.Second
		}
		ctx, cancel := callBudget(ctx, request.DeadlineUnixMS, limit)
		defer cancel()
		if err := ctx.Err(); err != nil {
			fail(mapError(err, addressEvidence(result.Address)))
			return
		}

		id, err := opaque()
		if err != nil {
			fail(err)
			return
		}

		g.mu.Lock()
		r := g.registrations[request.Address.Type]
		switch {
		case g.closed:
			err = actorx.ErrClosed
		case r == nil || !r.ready:
			err = actorxdist.ErrTypeUnavailable
		case g.active >= protocol.Capacity:
			err = actorx.ErrFull
		}
		if err != nil {
			g.mu.Unlock()
			fail(mapError(err, addressEvidence(result.Address)))
			return
		}
		g.active++
		r.calls.Add(1)
		r.session.track(id)
		g.mu.Unlock()

		closeScope := sync.OnceFunc(func() {
			cancel()
			r.session.forget(id)
			g.mu.Lock()
			g.active--
			g.mu.Unlock()
			r.calls.Done()
		})
		defer closeScope()

		input := protocol.InvokeStream{
			RequestID:   id,
			MessageType: "json",
			ContentType: "application/json",
			Content:     string(request.Input),
		}
		var streamErr error
		for c, err := range g.backend.stream(ctx, &result.Address, input) {
			if err != nil {
				streamErr = err
				break
			}
			if result.Address.Validate(true) != nil || c.Validate() != nil {
				streamErr = actorxdist.ErrProtocol
				break
			}
			if !yield(StreamChunk{Address: result.Address, Value: json.RawMessage(c.Content)}, nil) {
				return
			}
			result.FinalSeq++
		}

		// A valid provider terminal cannot erase a failed local delivery or
		// canceled drain. Business failure evidence is restored only on its path.
		// Empty terminals have no pending delivery to cancel.
		evidence := r.session.settle(id, result.Address)
		if evidence.terminal && evidence.err != nil {
			streamErr = evidence.err
		} else if evidence.terminal && evidence.result.Failure != nil &&
			result.FinalSeq == evidence.finalSeq &&
			(evidence.finalSeq == 0 || ctx.Err() == nil) &&
			!errors.Is(streamErr, actorxdist.ErrProtocol) && !errors.Is(streamErr, actorxdist.ErrOutcomeUnknown) {
			streamErr = protocol.NewError(evidence.result.Failure.Code, evidence.result.Failure.Message, addressEvidence(result.Address))
		} else if !evidence.terminal && evidence.sent {
			streamErr = actorxdist.ErrOutcomeUnknown
		}

		closeScope()
		if streamErr != nil {
			fail(mapStreamError(streamErr, result.Address))
		}
	}
}

// ConsumerStream correlates an HTTP-independent consumer segment. Messages owns its
// lifetime; Close requests cancellation without stopping the shared Actor.
type ConsumerStream struct {
	gateway   *Gateway
	request   protocol.CallParams
	id        json.RawMessage
	token     string
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	seq, ack  uint64
	terminal  bool
	used      atomic.Bool
	closeOnce sync.Once
	wake      chan struct{}
}

// OpenStream validates a consumer request and reserves its correlation token.
// The caller sends Open before Messages and closes the stream on transport exit.
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
	s.closeOnce.Do(func() {
		s.cancel()
		s.gateway.mu.Lock()
		delete(s.gateway.consumerStreams, s.token)
		s.gateway.mu.Unlock()
	})
}

// Messages yields HTTP chunks and the final wire response. The transport must
// stop iteration on a failed write. Only a correlated client ACK completes the
// upstream loop body; SSE flush alone does not acknowledge consumption.
func (s *ConsumerStream) Messages() iter.Seq[protocol.Message] {
	return func(yield func(protocol.Message) bool) {
		if !s.used.CompareAndSwap(false, true) {
			return
		}
		defer s.Close()

		result := protocol.StreamResult{Address: s.request.Address}
		var streamErr error
		for chunk, err := range s.gateway.stream(s.ctx, s.request, &result) {
			if err != nil {
				streamErr = err
				break
			}
			s.mu.Lock()
			s.seq++
			seq := s.seq
			s.mu.Unlock()
			m := protocol.Notification(protocol.ChunkMethod, protocol.ChunkParams{
				RequestID:   s.id,
				StreamToken: s.token,
				Address:     chunk.Address,
				Seq:         seq,
				Value:       chunk.Value,
			})
			if !yield(m) {
				return
			}

			// Keep the upstream body active until the client confirms consumption.
			for {
				s.mu.Lock()
				ack := s.ack
				s.mu.Unlock()
				if err := s.ctx.Err(); err != nil {
					streamErr = err
					break
				}
				if ack >= seq {
					break
				}
				select {
				case <-s.wake:
				case <-s.ctx.Done():
				}
			}
			if streamErr != nil {
				break
			}
		}

		s.mu.Lock()
		s.terminal = true
		seq := s.seq
		s.mu.Unlock()
		result.FinalSeq = seq
		s.Close()
		if streamErr != nil {
			yield(protocol.ErrorResponse(s.id, protocol.StreamError(mapStreamError(streamErr, result.Address), seq)))
		} else {
			yield(protocol.Success(s.id, result))
		}
	}
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
	if !sameRequestID(s.id, c.RequestID) {
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

// sameRequestID decodes string escapes while preserving ID types and exact
// numeric tokens. No numeric ID is converted through float64.
func sameRequestID(a, b json.RawMessage) bool {
	a, b = bytes.TrimSpace(a), bytes.TrimSpace(b)
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	if a[0] == '"' && b[0] == '"' {
		var left, right string
		return json.Unmarshal(a, &left) == nil && json.Unmarshal(b, &right) == nil && left == right
	}
	return bytes.Equal(a, b)
}
