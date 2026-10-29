package actorxremote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
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

// Stream owns an SSE connection and its Addr's operation lock until exhausted,
// closed or expired. Next/Value/Err have one consumer; Close may run concurrently.
// Calling Next acknowledges the previously delivered item. Always check Err
// after Next returns false, and Close when stopping before exhaustion.
type Stream struct {
	addr     *Addr
	id       string
	token    string
	ctx      context.Context
	ioCtx    context.Context
	cancelIO context.CancelFunc
	body     io.ReadCloser
	reader   *sseReader

	// mu protects consumer delivery, reader progress and terminal state.
	mu             sync.Mutex
	queue          []streamItem
	sizes          map[uint64]int // queued and delivered items awaiting consumption
	seq            uint64
	previous       uint64
	value          any
	terminal       bool
	remoteTerminal bool
	err            error
	closing        bool

	wake        chan struct{}
	closeSignal chan struct{}
	readerDone  chan struct{}
	closeDone   chan struct{}
	closeOnce   sync.Once
	controlMu   sync.Mutex // serializes credit and cancellation POSTs
}

// Stream establishes HTTP and validates stream/opened before returning. Requests
// and chunks follow encoding/json; decoded values preserve json.Number.
func (a *Addr) Stream(parent context.Context, message any, opts ...StreamOption) (*Stream, error) {
	o := streamOptions{callTimeout}
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("actorxremote: invalid stream option")
		}
		opt(&o)
	}
	if o.timeout <= 0 || o.timeout > 60*time.Second {
		return nil, fmt.Errorf("actorxremote: stream timeout must be in (0, 60s]")
	}

	budget, cancel := context.WithTimeout(parent, o.timeout)
	ctx, end, err := a.runtime.begin(budget)
	if err != nil {
		cancel()
		return nil, err
	}

	// Setup owns cleanup until stream/opened is validated. After that, the
	// stream's close goroutine owns the budget, Runtime work and Addr lock.
	owned := false
	locked := false
	defer func() {
		if !owned {
			if locked {
				<-a.operation
			}
			end()
			cancel()
		}
	}()

	input, err := snapshot(message)
	if err != nil {
		return nil, err
	}
	select {
	case a.operation <- struct{}{}:
		locked = true
	case <-ctx.Done():
		return nil, beforeDispatch(ctx.Err())
	}
	if ctx.Err() != nil {
		return nil, beforeDispatch(ctx.Err())
	}

	a.mu.Lock()
	uncertain := a.uncertain
	a.mu.Unlock()
	if uncertain {
		return nil, &RemoteError{Code: "address_uncertain"}
	}

	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, transportError(nil)
	}
	id := "c:" + hex.EncodeToString(random[:])
	deadline, _ := ctx.Deadline()
	raw, err := wire.Request("actor/stream", id, struct {
		Address  Address         `json:"address"`
		Input    json.RawMessage `json:"input"`
		Deadline int64           `json:"deadline_unix_ms"`
	}{a.Address(), input, deadline.UnixMilli()})
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, beforeDispatch(ctx.Err())
	}

	ioCtx, cancelIO := context.WithCancel(ctx)
	s := &Stream{
		addr:        a,
		id:          id,
		ctx:         ctx,
		ioCtx:       ioCtx,
		cancelIO:    cancelIO,
		sizes:       make(map[uint64]int),
		wake:        make(chan struct{}, 1),
		closeSignal: make(chan struct{}),
		readerDone:  make(chan struct{}),
		closeDone:   make(chan struct{}),
	}
	response, err := a.runtime.post(ioCtx, raw, true, "")
	if err != nil {
		cancelIO()
		if connectionFailed(err) {
			if ctx.Err() != nil {
				return nil, beforeDispatch(ctx.Err())
			}
			return nil, transportError(errConnection)
		}
		return nil, a.noEvidence(ctx, transportError(err))
	}
	s.body = response.Body

	// Closing the body also interrupts controlled transports which do not
	// observe the request context.
	bodyClosed := make(chan struct{})
	stopBody := context.AfterFunc(ioCtx, func() {
		s.body.Close()
		close(bodyClosed)
	})
	cleanupBody := func() {
		cancelIO()
		s.body.Close()
		if !stopBody() {
			<-bodyClosed
		}
	}

	if response.StatusCode != 200 {
		cleanupBody()
		e := statusError(response.StatusCode)
		if response.StatusCode >= 500 {
			return nil, a.noEvidence(ctx, e)
		}
		return nil, e
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
		cleanupBody()
		if s.remoteTerminal {
			return nil, s.err
		}
		return nil, a.noEvidence(ctx, err)
	}
	if mediaType(response.Header) != "text/event-stream" {
		cleanupBody()
		return nil, a.noEvidence(ctx, protocolError())
	}
	first, err := s.reader.next()
	if err == nil {
		err = s.open(first)
	}
	if err != nil {
		cleanupBody()
		return nil, a.noEvidence(ctx, err)
	}

	owned = true
	go s.read()
	go func() {
		select {
		case <-s.closeSignal:
		case <-ctx.Done():
		}
		s.mu.Lock()
		s.closing = true
		if !s.terminal || (s.err == nil && (len(s.queue) > 0 || s.previous != 0)) {
			if ctx.Err() != nil {
				if ctx.Err() == context.Canceled {
					s.err = context.Canceled
				} else if s.remoteTerminal {
					s.err = remoteError(1005, ctx.Err())
				} else {
					s.err = a.noEvidence(ctx, ctx.Err())
				}
			} else {
				s.err = remoteError(1009, nil)
				if !s.remoteTerminal {
					a.unknown()
				}
			}
			s.terminal = true
		}
		remote := s.remoteTerminal
		s.mu.Unlock()
		s.notify()
		cancelIO()
		s.body.Close()

		// The cancellation POST uses only the original remaining budget. It is
		// best effort; no new timeout or retry is started after caller cancellation.
		s.controlMu.Lock()
		if !remote && budget.Err() == nil {
			_ = s.control(budget, "stream/cancel", 0)
		}
		s.controlMu.Unlock()

		<-s.readerDone
		cleanupBody()
		<-a.operation
		end()
		cancel()
		close(s.closeDone)
	}()
	return s, nil
}

func (s *Stream) open(raw []byte) error {
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

func (s *Stream) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Stream) requestClose() { s.closeOnce.Do(func() { close(s.closeSignal) }) }

func (s *Stream) read() {
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
				terminal := s.terminal
				s.mu.Unlock()
				s.notify()
				if err == nil && terminal {
					return
				}
			}
		}
		if err != nil {
			s.mu.Lock()
			if !s.closing && !s.remoteTerminal {
				s.err = s.addr.noEvidence(s.ctx, err)
				s.terminal = true
			}
			s.mu.Unlock()
			s.notify()
			s.requestClose()
			return
		}
	}
}

// Called under mu once published; during setup no other goroutine has access.
func (s *Stream) chunk(m wire.Message, size int) error {
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
func (s *Stream) acceptTerminal(m wire.Message) error {
	if m.Error != nil {
		if wire.ValidateError(m.Error) != nil {
			return protocolError()
		}
		e := m.Error
		var data map[string]json.RawMessage
		_ = json.Unmarshal(e.Data, &data)
		if e.Code == 1013 {
			seq, err := wire.Sequence(data["final_seq"])
			if err != nil || seq != 0 || s.seq != 0 || data["address"] == nil {
				return protocolError()
			}
		}
		if s.token != "" {
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
		s.err = remote
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
	s.terminal = true
	return nil
}

func (s *Stream) control(ctx context.Context, method string, seq uint64) error {
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

// Next marks the previous value consumed before waiting for the next value.
func (s *Stream) Next() bool {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		<-s.closeDone
		return false
	}
	previous := s.previous
	s.previous = 0
	delete(s.sizes, previous)
	s.mu.Unlock()

	// Delivery alone does not free credit. The caller confirms consumption by
	// asking for the next item, including after processing the final item.
	if previous != 0 {
		s.controlMu.Lock()
		err := s.control(s.ioCtx, "stream/ack", previous)
		s.controlMu.Unlock()
		if err != nil {
			s.mu.Lock()
			// A credit transport failure cannot erase an already valid terminal.
			if !s.terminal {
				s.err = s.addr.noEvidence(s.ctx, err)
				s.terminal = true
			}
			s.mu.Unlock()
		}
	}

	for {
		s.mu.Lock()
		if s.closing || s.err != nil {
			s.mu.Unlock()
			s.Close()
			return false
		}
		if s.terminal && len(s.queue) == 0 {
			s.mu.Unlock()
			s.Close()
			return false
		}
		if s.ctx.Err() != nil {
			if s.ctx.Err() == context.Canceled {
				s.err = context.Canceled
			} else if s.remoteTerminal {
				s.err = remoteError(1005, s.ctx.Err())
			} else {
				s.err = s.addr.noEvidence(s.ctx, s.ctx.Err())
			}
			s.mu.Unlock()
			s.Close()
			return false
		}
		if len(s.queue) > 0 {
			item := s.queue[0]
			s.queue = s.queue[1:]
			s.previous = item.seq
			s.value = item.value
			s.mu.Unlock()
			return true
		}
		terminal := s.terminal
		s.mu.Unlock()
		if terminal {
			s.Close()
			return false
		}
		select {
		case <-s.wake:
		case <-s.ctx.Done():
		}
	}
}

// Value returns the most recently delivered JSON value.
func (s *Stream) Value() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
}

// Err reports why iteration ended, or nil after successful exhaustion.
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close is idempotent, wakes Next and joins the reader and control POSTs.
func (s *Stream) Close() error {
	s.requestClose()
	<-s.closeDone
	return nil
}
