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

// Reserve the full window before starting work, including the active loop body.
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
// inbox callback validates and queues only. The iterating goroutine alone runs
// user code and retains its reservation through loop body completion, even on
// cancellation.
type receiveStream struct {
	n      *network
	ctx    context.Context
	f      businessFrame
	result reflect.Type
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
	err          error // local abort, distinct from the producer terminal
	endErr       error // validated producer failure, reported after draining
	release      func()
	closeOnce    sync.Once
	stopMonitor  chan struct{}
	joined       chan struct{}
}

func newReceiveStream(n *network, ctx context.Context, f businessFrame, result reflect.Type) *receiveStream {
	f.Payload = nil
	return &receiveStream{
		n:      n,
		ctx:    ctx,
		f:      f,
		result: result,
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
		s.queue <- f // Whole window includes the active loop body, so this never blocks.
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

			// Producer failures drain through the same queue as successful completion.
			s.endErr = remote
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

// The monitor can stop transport while a loop body is active. Only the
// iterator closes the scope and returns its window/Runtime reservations.
func (s *receiveStream) startMonitor() {
	s.stopMonitor, s.joined = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(s.joined)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		for {
			if err := s.stopped(); err != nil {
				s.cancelRemote(err)
				s.signal()
				return
			}
			select {
			case <-s.stopMonitor:
				return
			case <-s.ctx.Done():
			case <-s.n.rt.life.Done():
			case <-ticker.C:
			}
		}
	}()
}

func (s *receiveStream) close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		complete := s.end != nil && s.items == 0 && s.err == nil
		if !complete && s.err == nil {
			s.err = context.Canceled
		}
		err := s.err

		// No application body is active here. Discard without successful credit.
		for len(s.queue) > 0 {
			<-s.queue
		}
		s.items, s.bytes = 0, 0
		s.mu.Unlock()

		if !complete {
			s.cancelRemote(err)
		}
		if s.stopMonitor != nil {
			close(s.stopMonitor)
			<-s.joined
		}
		if s.release != nil {
			s.release()
		}
	})
}

// next returns accepted data before a valid producer terminal. Local aborts
// interrupt draining; they never masquerade as a producer failure.
func (s *receiveStream) next() (businessFrame, bool, error) {
	for {
		if err := s.stopped(); err != nil {
			return businessFrame{}, false, err
		}

		s.mu.Lock()
		done, endErr := s.end != nil && s.items == 0, s.endErr
		s.mu.Unlock()
		if done {
			return businessFrame{}, false, endErr
		}

		select {
		case f := <-s.queue:
			if err := s.stopped(); err != nil {
				return businessFrame{}, false, err
			}
			return f, true, nil
		case <-s.wake:
		case <-s.ctx.Done():
		case <-s.n.rt.life.Done():
		}
	}
}

func (s *receiveStream) acknowledge(f businessFrame) error {
	if err := s.stopped(); err != nil {
		return err
	}
	s.mu.Lock()
	s.items--
	s.bytes -= f.Bytes
	s.mu.Unlock()

	// Queue removal did not return credit; the loop body has now completed.
	control := s.f
	control.Kind, control.Seq, control.Items, control.Bytes = "window", f.Seq, 1, f.Bytes
	control.Payload = nil
	if err := s.n.publishBusiness(f.Control, s.f.Reply, control); err != nil {
		s.mu.Lock()
		complete := s.end != nil && s.items == 0 && s.err == nil
		s.mu.Unlock()

		// A lost final credit cannot erase a fully delivered, validated terminal.
		if !complete {
			return errors.Join(ErrOutcomeUnknown, err)
		}
	}
	return nil
}

// sendStream keeps only encoded sizes for unconsumed chunks. Serializing before
// encoding prevents concurrent writers from allocating outside the window.
// Cancel production before joining the admission gate so waiting writes exit.
type sendStream struct {
	p            *businessReply
	ctx          context.Context
	writeGate    chan struct{}
	settlement   context.Context
	settleCancel context.CancelFunc
	done         chan struct{}
	settleOnce   sync.Once
	abortErr     error

	// mu protects the ledger and wake channel; writeGate serializes admission.
	mu           sync.Mutex
	seq, ack     uint64
	sizes        []int
	bytes        int
	wake         chan struct{}
	ended        bool
	terminalSent bool
	err          error
	epoch        uint64
}

func newSendStream(p *businessReply, ctx context.Context) *sendStream {
	settlement, cancel := context.WithDeadline(p.n.rt.life, deadline(ctx))
	return &sendStream{
		p:            p,
		ctx:          ctx,
		settlement:   settlement,
		settleCancel: cancel,
		writeGate:    make(chan struct{}, 1),
		done:         make(chan struct{}),
		wake:         make(chan struct{}),
		epoch:        p.n.ready.epoch(),
	}
}

func (s *sendStream) signal() {
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *sendStream) write(ctx context.Context, value any, size int, consumed bool) error {
	seq, err := s.admit(ctx, value, size)
	if err != nil || !consumed {
		return err
	}

	// Waiting does not hold the admission gate. Complete can seal production
	// while an independent write context still awaits its consumption receipt.
	for {
		s.mu.Lock()
		acked, abortErr, wake := s.ack >= seq, s.abortErr, s.wake
		s.mu.Unlock()
		if acked {
			return nil
		}
		if abortErr != nil {
			return abortErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

func (s *sendStream) admit(ctx context.Context, value any, size int) (uint64, error) {
	if ctx == nil || size < 0 {
		return 0, actorx.ErrChunkSize
	}
	select {
	case s.writeGate <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	}
	defer func() { <-s.writeGate }()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}

	n := s.p.n
	if !takeSlot(n.payloadSlots) {
		return 0, actorx.ErrFull
	}
	defer func() { <-n.payloadSlots }()
	if !takeSlot(n.codecSlots) {
		return 0, actorx.ErrFull
	}
	payload, err := encodeValue(value)
	<-n.codecSlots
	if err != nil {
		return 0, err
	}
	if len(payload) > streamBytes {
		return 0, actorx.ErrChunkSize
	}

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := s.ctx.Err(); err != nil {
			return 0, err
		}
		if n.ready.epoch() != s.epoch {
			return 0, ErrOwnerUnavailable
		}

		s.mu.Lock()
		if s.ended {
			s.mu.Unlock()
			return 0, context.Canceled
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
				return 0, err
			}
			return f.Seq, nil
		}
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.ctx.Done():
			return 0, s.ctx.Err()
		case <-wake:
		}
	}
}

// monitor bounds post-terminal receipts by the original request deadline and
// transport lifetime. It never treats producer completion as consumer discard.
func (s *sendStream) monitor() {
	// The admitted stream still holds work here, before Deliver can complete.
	// Close also joins this monitor after receipt cleanup releases that stream.
	s.p.n.rt.work.Add(1)
	go func() {
		defer s.p.n.rt.work.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-s.done:
				return
			case <-s.settlement.Done():
				cause := s.settlement.Err()
				if s.p.n.rt.life.Err() != nil {
					cause = actorx.ErrClosed
				}
				s.abort(errors.Join(ErrOutcomeUnknown, cause))
				return
			case <-ticker.C:
				if !s.p.n.ready.Load() || !s.p.n.nc.IsConnected() || s.p.n.ready.epoch() != s.epoch {
					s.abort(errors.Join(ErrOutcomeUnknown, ErrOwnerUnavailable))
					return
				}
			}
		}
	}()
}

func (s *sendStream) abort(err error) {
	s.mu.Lock()
	if s.abortErr == nil {
		s.abortErr = err
		s.signal()
	}
	s.mu.Unlock()
	s.p.cancel()
	s.releaseIfSettled()
}

func (s *sendStream) releaseIfSettled() {
	s.mu.Lock()
	settled := s.terminalSent && (len(s.sizes) == 0 || s.abortErr != nil)
	s.mu.Unlock()
	if !settled {
		return
	}
	s.settleOnce.Do(func() {
		s.settleCancel()
		s.p.n.businessMu.Lock()
		delete(s.p.n.leases, s.p.key)
		s.p.n.businessMu.Unlock()
		s.p.n.releaseStream()
		s.p.n.rt.work.Done()
		close(s.done)
	})
}

func (s *sendStream) complete(err error, rejected bool) {
	s.writeGate <- struct{}{}
	defer func() { <-s.writeGate }()

	s.mu.Lock()
	s.ended = true
	seq := s.seq
	s.signal()
	if s.err != nil {
		err = s.err
	}
	s.mu.Unlock()

	f := s.p.f
	f.Seq = seq
	s.p.n.respondBusiness(f, nil, err, rejected)

	s.mu.Lock()
	s.terminalSent = true
	s.mu.Unlock()

	s.releaseIfSettled()
}

func (s *sendStream) credit(f businessFrame) {
	s.mu.Lock()
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
		s.abort(errors.Join(ErrOutcomeUnknown, ErrProtocol)) // never block the inbox callback
	}
	if s.done != nil {
		s.releaseIfSettled()
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
	var streams []*sendStream
	for _, p := range n.leases {
		if p.stream != nil {
			streams = append(streams, p.stream)
		}
	}
	n.businessMu.Unlock()
	for _, s := range streams {
		s.abort(errors.Join(ErrOutcomeUnknown, ErrOwnerUnavailable))
	}
}

func (n *network) cancelBusiness(f businessFrame, cause error) {
	f.Kind = "cancel"
	f.Items, f.Bytes, f.Seq = 0, 0, 0
	f.Payload = nil
	f.Error, _ = errwire.Encode(cause, errwire.Source{Operation: "cancel", Type: f.Type, ID: f.ID, Generation: f.Generation})
	data, _ := json.Marshal(f)
	_ = n.nc.PublishRequest(n.actorSubject(ActorKey{f.Type, f.ID}), f.Reply, data)
}
