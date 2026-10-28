package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/errwire"
	"github.com/nats-io/nats.go"
)

const (
	streamItems = 4
	streamBytes = 64 << 10
)

// Reserve the full window before starting work, including the active callback.
// Both directions share the Runtime bound; encoding scratch is charged separately.
func (n *network) reserveStream() bool {
	n.businessMu.Lock()
	defer n.businessMu.Unlock()
	if n.streams >= n.rt.limits.transport.Streams {
		return false
	}
	n.streams++
	return true
}

func (n *network) releaseStream() {
	n.businessMu.Lock()
	n.streams--
	n.businessMu.Unlock()
}

// receiveStream validates sequence numbers and bounds unconsumed chunks. The
// inbox callback validates and queues only. The invoking goroutine alone runs
// user code and retains its reservation through callback completion, even on cancellation.
type receiveStream struct {
	n      *network
	ctx    context.Context
	f      businessFrame
	result reflect.Type
	yield  func(any) error
	epoch  uint64

	// mu protects receive state shared with the inbox and cancellation callbacks.
	mu           sync.Mutex
	queue        chan businessFrame
	wake         chan struct{}
	seq          uint64
	items, bytes int
	end          *businessFrame
	control      string
	cancelOnce   sync.Once
	err          error
}

func newReceiveStream(n *network, ctx context.Context, f businessFrame, result reflect.Type, yield func(any) error) *receiveStream {
	f.Payload = nil
	return &receiveStream{
		n:      n,
		ctx:    ctx,
		f:      f,
		result: result,
		yield:  yield,
		epoch:  n.ready.epoch(),
		queue:  make(chan businessFrame, streamItems),
		wake:   make(chan struct{}, 1),
	}
}

func (s *receiveStream) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *receiveStream) accept(msg *nats.Msg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return
	}
	bad := func(err error) {
		s.err = errors.Join(ErrOutcomeUnknown, err)
		s.signal()
	}
	if msg.Header.Get("Status") == "503" {
		bad(ErrOwnerUnavailable)
		return
	}
	var f businessFrame
	if decodeBusiness(msg.Data, &f) != nil || !f.matches(s.f) || f.Reply != "" || s.end != nil {
		bad(ErrProtocol)
		return
	}

	switch f.Kind {
	case "chunk":
		if !validBusinessReply(f.Control, f.Correlation) ||
			s.control != "" && s.control != f.Control ||
			f.Seq != s.seq+1 ||
			f.Items != 0 ||
			f.Bytes != len(f.Payload) ||
			f.Bytes <= 0 ||
			f.Bytes > streamBytes-s.bytes ||
			s.items >= streamItems ||
			len(f.Error) != 0 ||
			f.Status != "" {
			bad(ErrProtocol)
			return
		}
		s.control = f.Control
		s.seq = f.Seq
		s.items++
		s.bytes += f.Bytes
		s.queue <- f // Whole window includes the active callback, so this never blocks.
	case "complete":
		if f.Control != "" || f.Seq != s.seq || f.Items != 0 || f.Bytes != 0 {
			bad(ErrProtocol)
			return
		}
		switch f.Status {
		case "ok":
			if len(f.Error) != 0 || string(f.Payload) != "null" {
				bad(ErrProtocol)
				return
			}
		case "error":
			var evidence struct {
				Admission string `json:"admission"`
			}
			remote, err := errwire.Decode(f.Error)
			if err != nil ||
				remote == nil ||
				decodeRecord(f.Payload, &evidence) != nil ||
				(evidence.Admission != "unknown" && evidence.Admission != "not_admitted") ||
				evidence.Admission == "not_admitted" && s.seq != 0 {
				bad(ErrProtocol)
				return
			}

			// Definite business errors outrank cancellation while a callback is running.
			s.err = remote
		default:
			bad(ErrProtocol)
			return
		}
		s.end = &f
		s.signal()
	default:
		bad(ErrProtocol)
	}
}

func (s *receiveStream) stopped() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	var err error
	switch {
	case s.ctx.Err() != nil:
		err = s.ctx.Err()
	case s.n.rt.life.Err() != nil:
		err = actorx.ErrClosed
	case !s.n.ready.Load() || !s.n.nc.IsConnected() || s.n.ready.epoch() != s.epoch:
		err = ErrOwnerUnavailable
	}
	if err != nil {
		s.err = err
		if s.end == nil {
			s.err = errors.Join(ErrOutcomeUnknown, err)
		}
	}
	return s.err
}

func (s *receiveStream) cancelRemote(err error) {
	s.cancelOnce.Do(func() { s.n.cancelBusiness(s.f, err) })
}

func (s *receiveStream) run() error {
	// One bounded monitor propagates cancellation even while user code is blocked.
	// It never invokes yield and is joined before the caller's reservation returns.
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			if err := s.stopped(); err != nil {
				s.cancelRemote(err)
				return
			}
			select {
			case <-stop:
				return
			case <-s.ctx.Done():
			case <-s.n.rt.life.Done():
			case <-ticker.C:
			}
		}
	}()
	defer func() {
		close(stop)
		<-joined
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.stopped(); err != nil {
			return err
		}
		s.mu.Lock()
		done := s.end != nil && s.items == 0
		s.mu.Unlock()
		if done {
			return nil
		}
		select {
		case f := <-s.queue:
			if err := s.stopped(); err != nil {
				return err
			}
			if !takeSlot(s.n.codecSlots) {
				return errors.Join(ErrOutcomeUnknown, actorx.ErrFull)
			}
			value, err := decodeValue(s.n.rt, f.Payload, s.result)
			<-s.n.codecSlots
			if err != nil {
				return errors.Join(ErrOutcomeUnknown, err)
			}
			if err = s.stopped(); err != nil {
				return err
			}

			// No separate callback goroutine: cancellation/Close cannot return early.
			err = s.yield(value)
			if err != nil {
				s.mu.Lock()
				terminal := s.end != nil
				prior := s.err
				s.mu.Unlock()
				if !terminal {
					err = errors.Join(ErrOutcomeUnknown, err)
				}
				return errors.Join(prior, err)
			}
			if err = s.stopped(); err != nil {
				return err
			}

			// Return this exact chunk's credit only after the callback has finished.
			s.mu.Lock()
			s.items--
			s.bytes -= f.Bytes
			s.mu.Unlock()
			control := s.f
			control.Kind, control.Seq, control.Items, control.Bytes = "window", f.Seq, 1, f.Bytes
			control.Payload = nil
			if err = s.n.publishBusiness(f.Control, s.f.Reply, control); err != nil {
				return errors.Join(ErrOutcomeUnknown, err)
			}
		case <-s.wake:
		case <-s.ctx.Done():
		case <-s.n.rt.life.Done():
		case <-ticker.C:
		}
	}
}

// sendStream keeps only encoded sizes for unconsumed chunks. Serializing before
// encoding prevents concurrent writers from allocating outside the window.
// Cancel before joining writeMu so a writer waiting for credit can exit.
type sendStream struct {
	p       *businessReply
	ctx     context.Context
	writeMu sync.Mutex

	// mu protects the window and wake channel; writeMu serializes entire writes.
	mu       sync.Mutex
	seq, ack uint64
	sizes    []int
	bytes    int
	wake     chan struct{}
	ended    bool
	err      error
	epoch    uint64
}

func newSendStream(p *businessReply, ctx context.Context) *sendStream {
	return &sendStream{p: p, ctx: ctx, wake: make(chan struct{}), epoch: p.n.ready.epoch()}
}

func (s *sendStream) signal() {
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *sendStream) write(ctx context.Context, value any, size int, consumed bool) error {
	if ctx == nil || size < 0 {
		return actorx.ErrChunkSize
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	n := s.p.n
	if !takeSlot(n.payloadSlots) {
		return actorx.ErrFull
	}
	defer func() { <-n.payloadSlots }()
	if !takeSlot(n.codecSlots) {
		return actorx.ErrFull
	}
	payload, err := encodeValue(value)
	<-n.codecSlots
	if err != nil {
		return err
	}
	if len(payload) > streamBytes {
		return actorx.ErrChunkSize
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if n.ready.epoch() != s.epoch {
			return ErrOwnerUnavailable
		}
		s.mu.Lock()
		if s.ended {
			s.mu.Unlock()
			return context.Canceled
		}
		if len(s.sizes) < streamItems && len(payload) <= streamBytes-s.bytes {
			f := s.p.f
			f.Kind, f.Seq, f.Bytes, f.Items, f.Payload, f.Reply, f.Control =
				"chunk", s.seq+1, len(payload), 0, payload, "", n.inbox+"control."+f.Correlation
			s.seq++
			s.bytes += len(payload)
			s.sizes = append(s.sizes, len(payload))
			s.mu.Unlock()
			if err := n.publishBusiness(s.p.f.Reply, "", f); err != nil {
				return err
			}
			if !consumed {
				return nil
			}
			for {
				s.mu.Lock()
				acked, wake := s.ack >= f.Seq, s.wake
				s.mu.Unlock()
				if acked {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-s.ctx.Done():
					return s.ctx.Err()
				case <-wake:
				}
			}
		}
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-wake:
		}
	}
}

func (s *sendStream) complete(err error, rejected bool) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	s.ended = true
	seq := s.seq
	s.sizes = nil
	s.bytes = 0
	s.signal()
	if s.err != nil {
		err = s.err
	}
	s.mu.Unlock()
	f := s.p.f
	f.Seq = seq
	s.p.n.respondBusiness(f, nil, err, rejected)
}

func (s *sendStream) credit(f businessFrame) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	valid := f.Items == 1 && f.Seq == s.ack+1 && len(s.sizes) > 0 && f.Bytes == s.sizes[0]
	if valid {
		s.ack = f.Seq
		s.bytes -= s.sizes[0]
		s.sizes = s.sizes[1:]
		s.signal()
	}
	if !valid {
		s.err = ErrProtocol
	}
	s.mu.Unlock()
	if !valid {
		s.p.cancel() // core completes the existing request; never block the SDK callback
	}
}

func (n *network) publishBusiness(subject, reply string, f businessFrame) error {
	if !takeSlot(n.codecSlots) {
		return actorx.ErrFull
	}
	defer func() { <-n.codecSlots }()
	if !n.ready.Load() || !n.nc.IsConnected() {
		return ErrOwnerUnavailable
	}
	if n.businessFault != nil {
		if err := n.businessFault("stream_frame", f); err != nil {
			return err
		}
	}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if len(data) > 128<<10 {
		return actorx.ErrChunkSize
	}
	return n.nc.PublishRequest(subject, reply, data)
}

// Credits preserve ordered SDK delivery on the Runtime's second inbox. The
// callback only validates an existing capability and wakes a writer.
func (n *network) streamCredit(msg *nats.Msg) {
	var f businessFrame
	if decodeBusiness(msg.Data, &f) != nil ||
		f.Kind != "window" ||
		f.Mode != "stream" ||
		msg.Subject != n.inbox+"control."+f.Correlation ||
		f.Reply != msg.Reply ||
		f.Control != f.Reply ||
		!validBusinessReply(f.Reply, f.Correlation) ||
		len(f.Payload) != 0 ||
		len(f.Error) != 0 ||
		f.Status != "" {
		return
	}
	n.businessMu.Lock()
	p := n.leases[f.Reply]
	n.businessMu.Unlock()
	if p != nil && p.stream != nil && p.f.matches(f) {
		p.stream.credit(f)
	}
}

// Cancel only existing network streams on transport loss. The core still owns
// request/task completion; resuming this connection cannot resume an old flow.
func (n *network) interruptStreams() {
	n.businessMu.Lock()
	defer n.businessMu.Unlock()
	for _, p := range n.leases {
		if p.stream != nil {
			p.cancel()
		}
	}
}

func (n *network) cancelBusiness(f businessFrame, cause error) {
	f.Kind = "cancel"
	f.Items, f.Bytes, f.Seq = 0, 0, 0
	f.Payload = nil
	f.Error, _ = errwire.Encode(cause, errwire.Source{Operation: "cancel", Type: f.Type, ID: f.ID, Generation: f.Generation})
	data, _ := json.Marshal(f)
	_ = n.nc.PublishRequest(actorSubject(ActorKey{f.Type, f.ID}), f.Reply, data)
}
