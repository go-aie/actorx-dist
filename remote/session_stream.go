package remote

import (
	"context"
	"encoding/json"
	"errors"

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

func (s *session) invokeStream(
	ctx context.Context,
	a protocol.Address,
	input protocol.InvokeStream,
	write func(protocol.Chunk) error,
) error {
	id, err := opaque()
	if err != nil {
		return err
	}
	token, err := opaque()
	if err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return actorxdist.ErrProtocol
	}
	ms := deadline.UnixMilli()
	m, err := protocol.NewRequest(protocol.Stream, id, protocol.StreamParams{
		CallParams: protocol.CallParams{
			Address: a, Input: json.RawMessage(input.Content), DeadlineUnixMS: &ms,
		},
		StreamToken: token,
		Window:      protocol.StreamWindow(),
	})
	if err != nil {
		return actorxdist.ErrProtocol
	}
	p := &providerStream{
		id:        id,
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
		return actorx.ErrStopped
	}
	c := s.calls[input.RequestID]
	if c == nil || c.sent {
		s.mu.Unlock()
		return actorxdist.ErrProtocol
	}
	if len(s.streams) >= protocol.Capacity {
		s.mu.Unlock()
		return actorx.ErrFull
	}
	s.streams[id] = p
	select {
	case s.events <- m:
		c.sent = true
		c.address = a
	default:
		delete(s.streams, id)
		s.mu.Unlock()
		return actorx.ErrFull
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if ctx.Err() != nil || !p.terminal ||
			p.err != nil && p.err != errProviderStream {
			_ = s.streamControl(p, protocol.Cancel, 0)
		}
		delete(s.streams, id)
		s.mu.Unlock()
	}()

	var consumed uint64
	for {
		s.mu.Lock()
		terminal, finalSeq, endErr := p.terminal, p.seq, p.err
		s.mu.Unlock()
		if terminal && (endErr != nil || consumed == finalSeq) {
			return endErr
		}

		var chunk providerChunk
		select {
		case chunk = <-p.queue:
		case <-ctx.Done():
			return ctx.Err()
		case <-p.done:
			// Success can arrive before queued chunks have been consumed.
			// Drain them before returning; a failed terminal needs no queue drain.
			s.mu.Lock()
			endErr = p.err
			finalSeq = p.seq
			s.mu.Unlock()
			if endErr != nil || consumed == finalSeq {
				return endErr
			}
			select {
			case chunk = <-p.queue:
			default:
				return actorxdist.ErrProtocol
			}
		}

		if err := write(protocol.Chunk{
			MessageType: "json",
			ContentType: "application/json",
			Content:     string(chunk.value),
		}); err != nil {
			return err
		}

		// Native Write acknowledges consumption, not just queue admission.
		// Keep its item and byte charges until that acknowledgement arrives.
		consumed = chunk.seq
		s.mu.Lock()
		delete(p.sizes, chunk.seq)
		p.bytes -= chunk.size
		err = s.streamControl(p, protocol.Credit, chunk.seq)
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

// failStream retains an uncertain outcome when provider evidence is contradictory.
// The caller must hold s.mu.
func (s *session) failStream(p *providerStream) error {
	err := errors.Join(actorxdist.ErrOutcomeUnknown, actorxdist.ErrProtocol)
	p.stop(err)
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

		// Unsupported means the business method was never entered: no chunk
		// may precede it, even if final_seq otherwise matches the received data.
		if json.Unmarshal(m.Error.Data, &data) != nil || data.FinalSeq == nil || *data.FinalSeq != p.seq ||
			m.Error.Code == protocol.UnsupportedOperation && p.seq != 0 {
			return s.failStream(p)
		}
		failure, err := protocol.NormalizeProviderError(m.Error)
		if err != nil {
			return s.failStream(p)
		}
		result = protocol.NewResult(nil, failure)
		// The normalized business error is retained as evidence; no arbitrary error
		// type crosses actorx-dist. Stop forwarding queued chunks on the error path.
		endErr = errProviderStream
	} else if protocol.ParseStreamResult(m.Result, p.address, p.seq) != nil {
		return s.failStream(p)
	}

	p.stop(endErr)
	if c := s.calls[p.requestID]; c != nil {
		*c = terminal{sent: true, terminal: true, address: p.address, result: result}
	}
	return nil
}

var errProviderStream = errors.New("remote: provider stream failed (evidence retained)")
