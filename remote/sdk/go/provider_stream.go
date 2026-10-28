package actorxremote

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

// Each segment owns independent sequence and byte accounting. A finished ledger
// stays registered until cumulative credit, the original deadline or shutdown.
type providerStream struct {
	registration *registration
	work         *providerWork
	token        string

	mu         sync.Mutex
	sendMu     sync.Mutex // orders chunk POSTs before the terminal POST
	writing    sync.Mutex // rejects concurrent calls to Write
	seq        uint64
	ack        uint64
	sizes      map[uint64]int
	finished   bool
	failure    error
	wake       chan struct{}
	removed    chan struct{}
	removeOnce sync.Once
}

func newProviderStream(p *registration, w *providerWork, token string) *providerStream {
	return &providerStream{
		registration: p,
		work:         w,
		token:        token,
		sizes:        make(map[uint64]int),
		wake:         make(chan struct{}, 1),
		removed:      make(chan struct{}),
	}
}

func streamFailure(m wire.Message, seq uint64) wire.Message {
	var data map[string]json.RawMessage
	_ = json.Unmarshal(m.Error.Data, &data)
	if data == nil {
		data = make(map[string]json.RawMessage)
	}
	data["final_seq"], _ = json.Marshal(seq)
	copy := *m.Error
	copy.Data, _ = wire.Encode(data)
	m.Error = &copy
	return m
}

func (s *providerStream) remove() {
	s.removeOnce.Do(func() {
		p := s.registration
		key, _ := wire.IDKey(s.work.request.ID)
		p.mu.Lock()
		if p.streams[key] == s {
			delete(p.streams, key)
		}
		p.mu.Unlock()
		close(s.removed)
	})
}

func (s *providerStream) expire() {
	timer := time.NewTimer(time.Until(s.work.deadline))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-s.registration.ctx.Done():
	case <-s.removed:
		return
	}
	s.remove()
}

func (p *registration) control(m wire.Message) error {
	id, token, seq, err := wire.Control(m)
	if err != nil {
		return protocolError()
	}
	key, _ := wire.IDKey(id)
	p.mu.Lock()
	s := p.streams[key]
	p.mu.Unlock()

	// Structurally valid controls after ledger expiry are harmless.
	if s == nil {
		return nil
	}

	s.mu.Lock()
	if token != s.token {
		s.mu.Unlock()
		return protocolError()
	}
	if m.Method == "actor/cancel" {
		if !s.finished {
			s.work.cancel()
		}
		s.mu.Unlock()
		return nil
	}

	if seq > s.seq {
		s.mu.Unlock()
		return protocolError()
	}
	if seq > s.ack {
		s.ack = seq
		for n := range s.sizes {
			if n <= seq {
				delete(s.sizes, n)
			}
		}
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	remove := s.finished && len(s.sizes) == 0
	s.mu.Unlock()
	if remove {
		s.remove()
	}
	return nil
}

func (s *providerStream) failed(err error) error {
	s.mu.Lock()
	if s.failure == nil {
		s.failure = err
	}
	s.mu.Unlock()
	return err
}

// Write snapshots and validates the entire chunk before charging the window.
// POST completion is handoff, never consumption. All failures remain sticky.
func (s *providerStream) Write(value any) error {
	if !s.writing.TryLock() {
		return s.failed(protocolError())
	}
	defer s.writing.Unlock()

	s.mu.Lock()
	stopped := s.finished
	failure := s.failure
	s.mu.Unlock()
	if failure != nil {
		return failure
	}
	if stopped {
		return s.failed(remoteError(1009, nil))
	}
	if err := s.work.ctx.Err(); err != nil {
		return s.failed(err)
	}

	rawValue, err := snapshot(value)
	if err != nil {
		return s.failed(remoteError(1000, nil))
	}
	s.mu.Lock()
	seq := s.seq + 1
	s.mu.Unlock()
	if seq > wire.MaxSequence {
		return s.failed(protocolError())
	}
	raw, err := wire.Notification("actor/chunk", struct {
		RequestID   json.RawMessage `json:"request_id"`
		StreamToken string          `json:"stream_token"`
		Address     Address         `json:"address"`
		Seq         uint64          `json:"seq"`
		Value       json.RawMessage `json:"value"`
	}{s.work.request.ID, s.token, s.work.address, seq, rawValue})
	if err != nil {
		return s.failed(remoteError(1000, nil))
	}

	// Reserve the complete envelope size before sending. Only cumulative
	// credit releases this charge, including after the terminal is posted.
	for {
		s.mu.Lock()
		total := len(raw)
		for _, n := range s.sizes {
			total += n
		}
		if s.finished || s.work.ctx.Err() != nil || s.failure != nil {
			s.mu.Unlock()
			return s.failed(remoteError(1009, s.work.ctx.Err()))
		}
		if len(s.sizes) < wire.WindowItems && total <= wire.WindowBytes {
			s.seq = seq
			s.sizes[seq] = len(raw)
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-s.work.ctx.Done():
			return s.failed(s.work.ctx.Err())
		}
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.work.ctx.Err() != nil {
		return s.failed(s.work.ctx.Err())
	}
	response, err := s.registration.runtime.post(s.work.ctx, raw, false, s.registration.token)
	if err != nil {
		err = transportError(err)
		if s.work.ctx.Err() == nil {
			s.registration.runtime.fail(err)
		}
		return s.failed(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 204 {
		err = statusError(response.StatusCode)
		// A 409 is a late rejection, not another outcome or a session failure.
		if response.StatusCode == 409 {
			s.work.cancel()
		} else if s.work.ctx.Err() == nil {
			s.registration.runtime.fail(err)
		}
		return s.failed(err)
	}
	return nil
}

func handleStream(ctx context.Context, actor StreamActor, message any, writer StreamWriter) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return actor.HandleStream(ctx, message, writer) == nil
}
