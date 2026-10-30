package remote

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

type providerChunk struct {
	value json.RawMessage
	size  int
	seq   uint64
}

// All shared state is protected by session.mu. The queue includes at most four
// accepted items; bytes includes the item currently inside StreamReply.Write.
type providerStream struct {
	id, token, requestID string
	address              protocol.Address
	ctx                  context.Context
	queue                chan providerChunk
	done                 chan struct{}
	terminal             bool
	err                  error
	seq                  uint64
	sizes                map[uint64]int
	bytes                int
}

func (p *providerStream) stop(err error) {
	if !p.terminal {
		p.terminal = true
		p.err = err
		close(p.done)
	}
}

// abortError excludes producer failure, which still permits draining accepted
// chunks. The caller must hold session.mu.
func (p *providerStream) abortError() error {
	if p.err == errProviderStream {
		return nil
	}
	return p.err
}

// streamControl uses the control reserve in the shared outbound queue.
// The caller must hold s.mu to serialize sends with session shutdown.
func (s *session) streamControl(p *providerStream, method string, seq uint64) error {
	// At most 16 business requests, 32 credits and 8 cancels can be queued.
	params := map[string]any{"request_id": p.id, "stream_token": p.token}
	if method == protocol.Credit {
		params["ack_seq"] = seq
	}
	if s.closed {
		return actorx.ErrStopped
	}
	select {
	case s.events <- protocol.Notification(method, params):
		return nil
	default:
		return actorx.ErrFull
	}
}

func (s *session) openProviderStream(ctx context.Context, a protocol.Address, input protocol.InvokeStream) (*providerStream, error) {
	token, err := opaque()
	if err != nil {
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, actorxdist.ErrProtocol
	}
	ms := deadline.UnixMilli()
	params := protocol.StreamParams{
		CallParams: protocol.CallParams{
			Address:        a,
			Input:          json.RawMessage(input.Content),
			DeadlineUnixMS: &ms,
		},
		StreamToken: token,
		Window:      protocol.StreamWindow(),
	}
	p := &providerStream{
		token:     token,
		requestID: input.RequestID,
		address:   a,
		ctx:       ctx,
		queue:     make(chan providerChunk, 4),
		done:      make(chan struct{}),
		sizes:     map[uint64]int{},
	}

	// Publish correlation and dispatch evidence under the same lock as accept.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, actorx.ErrStopped
	}
	c := s.calls[input.RequestID]
	if c == nil || c.sent {
		s.mu.Unlock()
		return nil, actorxdist.ErrProtocol
	}
	if len(s.streams) >= protocol.Capacity {
		s.mu.Unlock()
		return nil, actorx.ErrFull
	}
	m, err := s.requestLocked(protocol.Stream, params)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var id string
	_ = json.Unmarshal(m.ID, &id)
	p.id = id
	s.streams[id] = p
	select {
	case s.events <- m:
		c.sent = true
		c.address = a
	default:
		delete(s.streams, id)
		s.mu.Unlock()
		return nil, actorx.ErrFull
	}
	s.mu.Unlock()

	return p, nil
}

// invokeStream owns provider delivery and credits. Ingress only queues and seals;
// a validated business failure drains before its error is delivered.
func (s *session) invokeStream(ctx context.Context, a protocol.Address, input protocol.InvokeStream) iter.Seq2[protocol.Chunk, error] {
	var used atomic.Bool

	return func(yield func(protocol.Chunk, error) bool) {
		if !used.CompareAndSwap(false, true) {
			yield(protocol.Chunk{}, actorx.ErrSetup)
			return
		}
		p, err := s.openProviderStream(ctx, a, input)
		if err != nil {
			yield(protocol.Chunk{}, err)
			return
		}

		var consumed uint64
		closeScope := sync.OnceFunc(func() {
			s.mu.Lock()
			if ctx.Err() != nil || !p.terminal || consumed != p.seq || p.abortError() != nil {
				_ = s.streamControl(p, protocol.Cancel, 0)
			}
			delete(s.streams, p.id)
			p.queue = nil
			p.sizes = nil
			p.bytes = 0
			s.mu.Unlock()
		})
		defer closeScope()

		fail := func(err error) {
			closeScope()
			yield(protocol.Chunk{}, err)
		}

		for {
			if err := ctx.Err(); err != nil {
				fail(err)
				return
			}

			s.mu.Lock()
			terminal, finalSeq, endErr := p.terminal, p.seq, p.err
			abortErr := p.abortError()
			s.mu.Unlock()
			if terminal && (abortErr != nil || consumed == finalSeq) {
				if endErr != nil {
					fail(endErr)
				}
				return
			}

			var chunk providerChunk
			select {
			case chunk = <-p.queue:
			case <-ctx.Done():
				continue
			case <-p.done:
				// The done channel stays closed while accepted data drains.
				select {
				case chunk = <-p.queue:
				default:
					continue
				}
			}

			if err := ctx.Err(); err != nil {
				fail(err)
				return
			}

			s.mu.Lock()
			abortErr = p.abortError()
			s.mu.Unlock()
			if abortErr != nil {
				fail(abortErr)
				return
			}

			if !yield(protocol.Chunk{
				MessageType: "json",
				ContentType: "application/json",
				Content:     string(chunk.value),
			}, nil) {
				return
			}

			if err := ctx.Err(); err != nil {
				fail(err)
				return
			}

			s.mu.Lock()
			if err := p.abortError(); err != nil {
				s.mu.Unlock()
				fail(err)
				return
			}

			consumed = chunk.seq
			delete(p.sizes, chunk.seq)
			p.bytes -= chunk.size
			err = s.streamControl(p, protocol.Credit, chunk.seq)
			complete := p.terminal && p.abortError() == nil && consumed == p.seq
			s.mu.Unlock()
			if err != nil && !complete {
				fail(err)
				return
			}
		}
	}
}

// failStream retains an uncertain outcome when provider evidence is contradictory.
// The caller must hold s.mu.
func (s *session) failStream(p *providerStream) error {
	err := errors.Join(actorxdist.ErrOutcomeUnknown, actorxdist.ErrProtocol)
	p.stop(err)
	p.err = err
	if c := s.calls[p.requestID]; c != nil {
		*c = terminal{sent: true, terminal: true, address: p.address, err: err}
	}
	return ErrResponse
}

func (s *session) acceptChunk(m protocol.Message, size int) error {
	params, e := m.Parameters(protocol.ToGateway)
	if e != nil {
		return ErrResponse
	}
	chunk := params.(protocol.ChunkParams)
	var id string
	if json.Unmarshal(chunk.RequestID, &id) != nil {
		return ErrCorrelation
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.streams[id]
	if p == nil {
		return ErrCorrelation
	}
	if p.token != chunk.StreamToken {
		return ErrSession
	}
	if p.terminal || p.ctx.Err() != nil {
		return ErrCorrelation
	}

	// sizes includes the chunk being written, so queue removal alone cannot
	// reopen the provider's four-item / 32 KiB window.
	if chunk.Address != p.address || chunk.Seq != p.seq+1 ||
		len(p.sizes) >= 4 || p.bytes+size > 32768 {
		return s.failStream(p)
	}
	p.seq = chunk.Seq
	p.sizes[p.seq] = size
	p.bytes += size
	p.queue <- providerChunk{value: chunk.Value, size: size, seq: chunk.Seq}
	return nil
}

// Caller holds s.mu. A valid terminal uses its own slot, outside the data window.
func (s *session) acceptStream(p *providerStream, m protocol.Message) error {
	if p.terminal || p.ctx.Err() != nil {
		return ErrCorrelation
	}
	var result protocol.Result
	var endErr error
	if m.Error != nil {
		if protocol.ErrorAddress(m.Error.Data, p.address) != nil {
			return s.failStream(p)
		}
		var data struct {
			FinalSeq *uint64 `json:"final_seq"`
		}

		// Execution-time output contradicts a pre-execution rejection, even
		// when final_seq otherwise matches the received data.
		refused := m.Error.Code == protocol.ActorUnavailable ||
			m.Error.Code == protocol.TransportMode ||
			m.Error.Code == protocol.UnsupportedOperation
		if json.Unmarshal(m.Error.Data, &data) != nil || data.FinalSeq == nil || *data.FinalSeq != p.seq ||
			refused && p.seq != 0 {
			return s.failStream(p)
		}
		failure, err := protocol.NormalizeProviderError(m.Error)
		if err != nil {
			return s.failStream(p)
		}
		result = protocol.NewResult(nil, failure)

		// The normalized business error is retained as evidence; no arbitrary error
		// type crosses actorx-dist. Accepted chunks drain before this error.
		endErr = errProviderStream
	} else if protocol.ParseStreamResult(m.Result, p.address, p.seq) != nil {
		return s.failStream(p)
	}

	p.stop(endErr)
	if c := s.calls[p.requestID]; c != nil {
		*c = terminal{sent: true, terminal: true, address: p.address, result: result, finalSeq: p.seq}
	}
	return nil
}

var errProviderStream = errors.New("remote: provider stream failed (evidence retained)")
