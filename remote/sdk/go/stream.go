package actorxremote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

type streamOptions struct{ timeout time.Duration }

// StreamOption configures one Stream's total budget, including consumption.
type StreamOption func(*streamOptions)

// WithStreamTimeout sets a total timeout in (0, 60s]. The default is five seconds.
func WithStreamTimeout(timeout time.Duration) StreamOption {
	return func(o *streamOptions) { o.timeout = timeout }
}

type streamItem struct {
	seq   uint64
	value any
}

// streamReceiver owns only transport and bounded ingress state. The sequence
// owns delivery, credit, disposal and the Runtime/Addr reservations.
type streamReceiver struct {
	addr     *Addr
	id       string
	token    string
	ctx      context.Context
	budget   context.Context // original caller budget, also used for best-effort cancellation
	cancelIO context.CancelFunc
	body     io.ReadCloser
	reader   *sseReader

	mu             sync.Mutex
	queue          []streamItem
	sizes          map[uint64]int // includes the active loop body
	seq, consumed  uint64
	remoteTerminal bool
	endErr         error // validated producer failure, delivered after draining
	err            error // local abort, interrupts draining
	closing        bool

	wake       chan struct{}
	readerDone chan struct{}
	stopWatch  func() bool
	watchDone  chan struct{}
	stopBody   func() bool
	bodyDone   chan struct{}
	cancelOnce sync.Once
	controlMu  sync.Mutex
}

// Stream returns a lazy, single-use sequence of JSON values and errors. Its
// budget, input snapshot and HTTP request start on iteration. Only a normally
// returning loop body acknowledges a chunk; break, return and panic cancel it.
// Keep message and its referenced data stable until iteration starts.
func (a *Addr) Stream(parent context.Context, message any, opts ...StreamOption) iter.Seq2[any, error] {
	var used atomic.Bool

	return func(yield func(any, error) bool) {
		if !used.CompareAndSwap(false, true) {
			yield(nil, ErrStreamUsed)
			return
		}
		if a == nil || a.runtime == nil || parent == nil {
			yield(nil, fmt.Errorf("actorxremote: invalid stream context or address"))
			return
		}

		o := streamOptions{callTimeout}
		for _, opt := range opts {
			if opt == nil {
				yield(nil, fmt.Errorf("actorxremote: invalid stream option"))
				return
			}
			opt(&o)
		}
		if o.timeout <= 0 || o.timeout > 60*time.Second {
			yield(nil, fmt.Errorf("actorxremote: stream timeout must be in (0, 60s]"))
			return
		}

		budget, cancel := context.WithTimeout(parent, o.timeout)
		ctx, end, err := a.runtime.begin(budget)
		if err != nil {
			cancel()
			yield(nil, err)
			return
		}

		var s *streamReceiver
		locked := false
		closeScope := sync.OnceFunc(func() {
			if s != nil {
				s.close()
			}
			if locked {
				<-a.operation
			}
			end()
			cancel()
		})
		defer closeScope() // also handles early return and panic without recovering it

		fail := func(err error) {
			closeScope()
			yield(nil, err)
		}

		input, err := snapshot(message)
		if err != nil {
			fail(err)
			return
		}

		select {
		case a.operation <- struct{}{}:
			locked = true
		case <-ctx.Done():
			fail(beforeDispatch(ctx.Err()))
			return
		}
		if ctx.Err() != nil {
			fail(beforeDispatch(ctx.Err()))
			return
		}
		a.mu.Lock()
		uncertain := a.uncertain
		a.mu.Unlock()
		if uncertain {
			fail(&RemoteError{Code: "address_uncertain"})
			return
		}

		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			fail(transportError(nil))
			return
		}
		id := "c:" + hex.EncodeToString(random[:])
		deadline, _ := ctx.Deadline()
		raw, err := wire.Request("actor/stream", id, struct {
			Address  Address         `json:"address"`
			Input    json.RawMessage `json:"input"`
			Deadline int64           `json:"deadline_unix_ms"`
		}{a.Address(), input, deadline.UnixMilli()})
		if err != nil {
			fail(err)
			return
		}
		if ctx.Err() != nil {
			fail(beforeDispatch(ctx.Err()))
			return
		}

		ioCtx, cancelIO := context.WithCancel(ctx)
		s = &streamReceiver{
			addr:     a,
			id:       id,
			ctx:      ctx,
			budget:   budget,
			cancelIO: cancelIO,
			sizes:    make(map[uint64]int),
			wake:     make(chan struct{}, 1),
		}
		response, err := a.runtime.post(ioCtx, raw, true, "")
		if err != nil {
			if connectionFailed(err) {
				if ctx.Err() != nil {
					err = beforeDispatch(ctx.Err())
				} else {
					err = transportError(errConnection)
				}
			} else {
				err = a.noEvidence(ctx, transportError(err))
			}
			fail(err)
			return
		}
		s.body = response.Body

		// Also interrupt bounded custom transports which ignore request context.
		s.bodyDone = make(chan struct{})
		s.stopBody = context.AfterFunc(ioCtx, func() {
			s.body.Close()
			close(s.bodyDone)
		})
		if response.StatusCode != 200 {
			err := statusError(response.StatusCode)
			if response.StatusCode >= 500 {
				err = a.noEvidence(ctx, err)
			}
			fail(err)
			return
		}

		s.reader = newSSEReader(response.Body)
		if mediaType(response.Header) == "application/json" {
			raw, err := readBody(response.Body)
			if err == nil {
				var m wire.Message
				m, err = wire.Response(raw, id)
				if err == nil && m.Error != nil {
					err = s.acceptTerminal(m)
				} else {
					err = protocolError()
				}
			}
			if s.remoteTerminal {
				fail(s.endErr)
			} else {
				fail(a.noEvidence(ctx, err))
			}
			return
		}
		if mediaType(response.Header) != "text/event-stream" {
			fail(a.noEvidence(ctx, protocolError()))
			return
		}

		first, err := s.reader.next()
		if err == nil {
			err = s.open(first)
		}
		if err != nil {
			fail(a.noEvidence(ctx, err))
			return
		}

		s.readerDone, s.watchDone = make(chan struct{}), make(chan struct{})
		s.stopWatch = context.AfterFunc(ctx, func() {
			s.mu.Lock()
			s.checkContext()
			s.mu.Unlock()
			s.notify()
			s.cancelIO()
			s.cancelRemote()
			close(s.watchDone)
		})
		go s.read()

		// The only delivery loop: queue removal alone returns no credit. User
		// code runs synchronously here, with the full reservation still held.
		for {
			s.mu.Lock()
			s.checkContext()
			err := s.err
			complete := s.complete()
			endErr := s.endErr
			var item streamItem
			if err == nil && !complete && len(s.queue) > 0 {
				item = s.queue[0]
				s.queue[0] = streamItem{}
				s.queue = s.queue[1:]
			}
			s.mu.Unlock()
			if err != nil {
				fail(err)
				return
			}
			if complete {
				closeScope()
				if endErr != nil {
					yield(nil, endErr)
				}
				return
			}
			if item.seq == 0 {
				select {
				case <-s.wake:
				case <-ctx.Done():
				}
				continue
			}

			if !yield(item.value, nil) {
				return
			}

			s.mu.Lock()
			s.checkContext()
			err = s.err
			if err == nil {
				s.consumed = item.seq
				delete(s.sizes, item.seq)
			}
			s.mu.Unlock()
			if err != nil {
				fail(err)
				return
			}

			s.controlMu.Lock()
			err = s.control(ioCtx, "stream/ack", item.seq)
			s.controlMu.Unlock()
			if err != nil {
				s.mu.Lock()
				// Only complete delivery plus valid terminal evidence can make
				// the final ACK failure harmless, for success or business failure.
				if !s.complete() && s.err == nil {
					s.err = a.noEvidence(ctx, err)
				}
				s.mu.Unlock()
			}
		}
	}
}

// Caller holds mu. Producer completion and local consumption are independent.
func (s *streamReceiver) complete() bool {
	return s.remoteTerminal && s.consumed == s.seq && s.err == nil
}

func (s *streamReceiver) checkContext() {
	if s.ctx.Err() == nil || s.err != nil || s.complete() {
		return
	}
	if s.ctx.Err() == context.Canceled {
		s.err = context.Canceled
	} else if s.remoteTerminal {
		s.err = remoteError(1005, s.ctx.Err())
	} else {
		s.err = s.addr.noEvidence(s.ctx, s.ctx.Err())
	}
}

func (s *streamReceiver) cancelRemote() {
	s.cancelOnce.Do(func() {
		s.mu.Lock()
		incomplete := !s.complete()
		s.mu.Unlock()
		if incomplete && s.token != "" && s.budget.Err() == nil {
			s.controlMu.Lock()
			_ = s.control(s.budget, "stream/cancel", 0)
			s.controlMu.Unlock()
		}
	})
}

// Only the iterator calls close, after the active body has exited. A watcher
// may stop IO earlier, but cannot discard that body's value or release its slot.
func (s *streamReceiver) close() {
	s.mu.Lock()
	s.closing = true
	if s.token != "" && !s.remoteTerminal {
		s.addr.unknown()
	}
	s.mu.Unlock()

	s.cancelIO()
	if s.body != nil {
		s.body.Close()
		if !s.stopBody() {
			<-s.bodyDone
		}
	}
	if s.stopWatch != nil && !s.stopWatch() {
		<-s.watchDone
	}
	s.cancelRemote()
	if s.readerDone != nil {
		<-s.readerDone
	}

	s.queue = nil
	s.sizes = nil
}

func (s *streamReceiver) open(raw []byte) error {
	m, err := wire.Parse(raw)
	if err != nil || !m.Request || m.Method != "stream/opened" || m.ID != nil {
		return protocolError()
	}
	names := []string{"request_id", "stream_token", "window"}
	f, err := wire.Fields(m.Params, names, names...)
	if err != nil || !wire.SameID(f["request_id"], mustID(s.id)) || wire.CheckWindow(f["window"]) != nil {
		return protocolError()
	}
	s.token, err = wire.Token(f["stream_token"])
	if err != nil {
		return protocolError()
	}
	return nil
}

func mustID(id string) json.RawMessage {
	raw, _ := json.Marshal(id)
	return raw
}

func (s *streamReceiver) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// The reader only validates, queues and records evidence. It never runs user
// code or releases the iterator's Runtime work or Addr operation lock.
func (s *streamReceiver) read() {
	defer close(s.readerDone)
	for {
		raw, err := s.reader.next()
		if err == nil {
			var m wire.Message
			m, err = wire.Parse(raw)
			if err != nil {
				err = protocolError()
			} else {
				s.mu.Lock()
				if s.closing {
					s.mu.Unlock()
					return
				}
				if m.Request {
					err = s.chunk(m, len(raw))
				} else {
					m, err = wire.Response(raw, s.id)
					if err == nil {
						err = s.acceptTerminal(m)
					}
				}
				terminal := s.remoteTerminal
				s.mu.Unlock()
				s.notify()
				if err == nil && terminal {
					return
				}
			}
		}
		if err != nil {
			s.mu.Lock()
			if !s.closing && s.err == nil {
				s.err = s.addr.noEvidence(s.ctx, err)
			}
			s.mu.Unlock()
			s.notify()
			s.cancelIO()
			return
		}
	}
}

// Called under mu once published; during setup no other goroutine has access.
func (s *streamReceiver) chunk(m wire.Message, size int) error {
	if m.Method != "stream/chunk" || m.ID != nil {
		return protocolError()
	}
	names := []string{"request_id", "stream_token", "address", "seq", "value"}
	f, err := wire.Fields(m.Params, names, names...)
	if err != nil || !wire.SameID(f["request_id"], mustID(s.id)) {
		return protocolError()
	}
	token, err := wire.Token(f["stream_token"])
	if err != nil || token != s.token {
		return protocolError()
	}
	seq, err := wire.Sequence(f["seq"])
	if err != nil || seq != s.seq+1 {
		return protocolError()
	}
	if err = s.addr.save(f["address"]); err != nil {
		return err
	}
	total := size
	for _, n := range s.sizes {
		total += n
	}
	if len(s.sizes) >= wire.WindowItems || total > wire.WindowBytes {
		return protocolError()
	}
	value, err := wire.Decode(f["value"])
	if err != nil {
		return protocolError()
	}
	s.seq = seq
	s.sizes[seq] = size
	s.queue = append(s.queue, streamItem{seq, value})
	return nil
}

// acceptTerminal runs under mu after setup. A validated remote terminal fixes
// the outcome, but successful iteration still requires consuming queued items.
func (s *streamReceiver) acceptTerminal(m wire.Message) error {
	if m.Error != nil {
		if wire.ValidateError(m.Error) != nil {
			return protocolError()
		}
		e := m.Error
		var data map[string]json.RawMessage
		_ = json.Unmarshal(e.Data, &data)
		refused := e.Code == 1002 || e.Code == 1008
		if refused && s.seq != 0 {
			return protocolError()
		}
		if e.Code == 1013 {
			seq, err := wire.Sequence(data["final_seq"])
			if err != nil || seq != 0 || s.seq != 0 || data["address"] == nil {
				return protocolError()
			}
		}
		if s.token != "" || refused && data["final_seq"] != nil {
			seq, err := wire.Sequence(data["final_seq"])
			if err != nil || seq != s.seq {
				return protocolError()
			}
		}
		if data["address"] != nil {
			if err := s.addr.save(data["address"]); err != nil {
				return err
			}
		}
		remote := &RemoteError{Code: wire.Kind(e.Code), RPCCode: e.Code, Data: e.Data}
		if s.addr.Address().Generation == "" && (e.Code == 1000 || e.Code == -32603 || e.Code == 1012) {
			return remote
		}
		if e.Code == 1003 || e.Code == 1004 {
			s.addr.unknown()
		}
		s.endErr = remote
	} else {
		f, err := wire.Fields(m.Result, []string{"address", "final_seq"}, "address", "final_seq")
		if err != nil {
			return protocolError()
		}
		seq, err := wire.Sequence(f["final_seq"])
		if err != nil || seq != s.seq {
			return protocolError()
		}
		if err = s.addr.save(f["address"]); err != nil {
			return err
		}
	}
	s.remoteTerminal = true
	return nil
}

func (s *streamReceiver) control(ctx context.Context, method string, seq uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.token == "" {
		return nil
	}
	p := map[string]any{"request_id": s.id, "stream_token": s.token}
	if method == "stream/ack" {
		p["ack_seq"] = seq
	}
	raw, err := wire.Notification(method, p)
	if err != nil {
		return protocolError()
	}
	resp, err := s.addr.runtime.post(ctx, raw, false, "")
	if err != nil {
		return transportError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		return statusError(resp.StatusCode)
	}
	return nil
}
