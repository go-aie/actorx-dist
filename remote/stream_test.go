package remote

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/go-aie/actorx-dist"
	"github.com/go-aie/actorx-dist/remote/protocol"
)

func streamInput(id string) protocol.InvokeStream {
	return protocol.InvokeStream{
		RequestID:   id,
		MessageType: "json",
		ContentType: "application/json",
		Content:     "null",
	}
}

func streamRequest(t *testing.T, s *session, write func(protocol.Chunk) error) (protocol.Message, <-chan error) {
	t.Helper()
	s.track("stream")
	done := make(chan error, 1)
	go func() { done <- s.invokeStream(testCtx(t), testAddress("one"), streamInput("stream"), write) }()
	return await(t, s.events), done
}

func providerChunkMessage(m protocol.Message, seq uint64) protocol.Message {
	var p protocol.InvokeStreamParams
	_ = json.Unmarshal(m.Params, &p)
	return protocol.Notification(protocol.ChunkMethod, protocol.ChunkParams{
		RequestID:   m.ID,
		StreamToken: p.StreamToken,
		Address:     p.Address,
		Seq:         seq,
		Value:       json.RawMessage("null"),
	})
}

func acceptTestChunk(s *session, m protocol.Message) error {
	raw, _ := m.Encode()
	return s.acceptChunk(m, len(raw))
}

func TestProviderStreamWindowAndTerminal(t *testing.T) {
	s := newSession()
	defer s.close()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	m, done := streamRequest(t, s, func(protocol.Chunk) error {
		entered <- struct{}{}
		<-release
		return nil
	})

	// Fill the window while native Write is blocked. Queue admission must not
	// release credit, and an early success terminal must not skip queued items.
	for seq := uint64(1); seq <= 4; seq++ {
		if err := acceptTestChunk(s, providerChunkMessage(m, seq)); err != nil {
			t.Fatal(err)
		}
	}
	await(t, entered)
	select {
	case <-s.events:
		t.Fatal("credit released before Write returned")
	default:
	}

	if err := s.accept(protocol.Success(m.ID, protocol.StreamResult{Address: testAddress("one"), FinalSeq: 4})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("terminal overtook queued chunks")
	default:
	}

	// Each completed Write releases exactly one credit through the final item.
	for seq := uint64(1); seq <= 4; seq++ {
		release <- struct{}{}
		credit := await(t, s.events)
		var p protocol.ControlParams
		_ = json.Unmarshal(credit.Params, &p)
		if credit.Method != protocol.Credit || p.AckSeq != seq {
			t.Fatal(credit)
		}
		if seq < 4 {
			await(t, entered)
		}
	}
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	if evidence := s.settle("stream", testAddress("one")); !evidence.terminal || evidence.err != nil {
		t.Fatal(evidence)
	}
}

func TestProviderStreamInvalidAndEmpty(t *testing.T) {
	for _, mode := range []string{
		"empty",
		"wrong-token", "wrong-address",
		"duplicate", "gap", "overflow", "bad-final",
		"failure", "unsupported-after-chunk", "disconnect",
	} {
		t.Run(mode, func(t *testing.T) {
			s := newSession()
			defer s.close()
			ctx, cancel := context.WithCancel(testCtx(t))
			defer cancel()

			// Block native Write to keep all four data slots occupied.
			s.track("stream")
			done := make(chan error, 1)
			go func() {
				done <- s.invokeStream(ctx, testAddress("one"), streamInput("stream"), func(protocol.Chunk) error {
					<-ctx.Done()
					return ctx.Err()
				})
			}()
			m := await(t, s.events)
			if mode == "empty" {
				if err := s.accept(protocol.Success(m.ID, protocol.StreamResult{Address: testAddress("one"), FinalSeq: 0})); err != nil {
					t.Fatal(err)
				}
				if err := await(t, done); err != nil {
					t.Fatal(err)
				}
				return
			}
			if mode == "wrong-token" {
				chunk := providerChunkMessage(m, 1)
				var p protocol.ChunkParams
				_ = json.Unmarshal(chunk.Params, &p)
				p.StreamToken, _ = opaque()
				if err := acceptTestChunk(s, protocol.Notification(protocol.ChunkMethod, p)); err != ErrSession {
					t.Fatal(err)
				}
				s.mu.Lock()
				if s.streams[stringID(m.ID)].seq != 0 {
					t.Fatal("wrong token consumed pending")
				}
				s.mu.Unlock()
			} else if mode == "disconnect" {
				s.close()
			} else if mode == "bad-final" {
				if err := s.accept(protocol.Success(m.ID, protocol.StreamResult{Address: testAddress("one"), FinalSeq: 1})); err != ErrResponse {
					t.Fatal(err)
				}
			} else if mode == "unsupported-after-chunk" {
				if err := acceptTestChunk(s, providerChunkMessage(m, 1)); err != nil {
					t.Fatal(err)
				}
				fixed := testAddress("one")
				if err := s.accept(protocol.ErrorResponse(m.ID, protocol.StreamError(protocol.NewError(protocol.UnsupportedOperation, "unsupported_operation", &fixed), 1))); err != ErrResponse {
					t.Fatal("1013 after a chunk must not become definite unsupported evidence", err)
				}
				if e := s.evidence("stream", fixed); !errors.Is(e.err, actorxdist.ErrOutcomeUnknown) {
					t.Fatal(e)
				}
			} else if mode == "failure" {
				err := s.accept(protocol.ErrorResponse(m.ID, protocol.StreamError(protocol.NewError(protocol.RemoteError, "failure", nil), 0)))
				if err != nil {
					t.Fatal(err)
				}
				if err := await(t, done); !errors.Is(err, errProviderStream) {
					t.Fatal(err)
				}
				if e := s.settle("stream", testAddress("one")); e.result.Failure == nil {
					t.Fatal(e)
				}
				return
			} else {
				seq := uint64(2)
				if mode == "duplicate" {
					if err := acceptTestChunk(s, providerChunkMessage(m, 1)); err != nil {
						t.Fatal(err)
					}
					seq = 1
				}
				if mode == "overflow" {
					for i := uint64(1); i <= 4; i++ {
						if err := acceptTestChunk(s, providerChunkMessage(m, i)); err != nil {
							t.Fatal(err)
						}
					}
					seq = 5
				}
				chunk := providerChunkMessage(m, seq)
				if mode == "wrong-address" {
					var p protocol.ChunkParams
					_ = json.Unmarshal(chunk.Params, &p)
					p.Address = testAddress("other")
					chunk = protocol.Notification(protocol.ChunkMethod, p)
				}
				if err := acceptTestChunk(s, chunk); err != ErrResponse {
					t.Fatal(err)
				}
				s.mu.Lock()
				err := s.streams[stringID(m.ID)].err
				s.mu.Unlock()
				if !errors.Is(err, actorxdist.ErrOutcomeUnknown) {
					t.Fatal(err)
				}
			}
			cancel()
			_ = await(t, done)
		})
	}
}

func stringID(raw []byte) string {
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

func TestConsumerControlCorrelationAndConsumption(t *testing.T) {
	g, _ := fakeGateway(t, nil)
	g.backend.stream = func(ctx context.Context, a protocol.Address, in protocol.InvokeStream, yield func(protocol.Address, protocol.Chunk) error) (protocol.Address, error) {
		a = testAddress(a.ID)
		return a, yield(a, protocol.Chunk{
			MessageType: "json",
			ContentType: "application/json",
			Content:     "null",
		})
	}
	s, err := g.OpenStream(testCtx(t), json.RawMessage(`null`), protocol.CallParams{Address: testAddress("one"), Input: json.RawMessage("null")})
	if err != nil {
		t.Fatal(err)
	}
	chunks := make(chan protocol.Message, 1)
	done := make(chan protocol.Message, 1)
	go func() {
		done <- s.Run(func(m protocol.Message) error {
			chunks <- m
			return nil
		})
	}()
	chunk := await(t, chunks)
	if chunk.Method != protocol.ChunkMethod {
		t.Fatal(chunk)
	}
	control := func(id json.RawMessage, token string, ack uint64) error {
		return g.Control(testCtx(t), protocol.Notification(protocol.Credit, protocol.ControlParams{RequestID: id, StreamToken: token, AckSeq: ack}))
	}
	if err := control(s.id, s.token, 2); err != ErrCorrelation {
		t.Fatal(err)
	}
	if err := control(json.RawMessage(`"wrong"`), s.token, 1); err != ErrCorrelation {
		t.Fatal(err)
	}
	wrong, _ := opaque()
	if err := control(s.id, wrong, 1); err != ErrSession {
		t.Fatal(err)
	}
	if err := control(s.id, s.token, 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("returned before consumption")
	default:
	}
	if err := control(s.id, s.token, 1); err != nil {
		t.Fatal(err)
	}
	if terminal := await(t, done); terminal.Error != nil {
		t.Fatal(terminal)
	}
	if len(g.consumerStreams) != 0 {
		t.Fatal("retained consumer state")
	}
}

func TestProviderChunkAccountsOriginalEncoding(t *testing.T) {
	g, r := fakeGateway(t, nil)
	release := make(chan struct{})
	m, done := streamRequest(t, r.session, func(protocol.Chunk) error {
		<-release
		return nil
	})
	chunk := providerChunkMessage(m, 1)
	compact, _ := chunk.Encode()
	raw := append([]byte(" \n"), compact...)
	if err := g.AcceptEncoded(testCtx(t), r.Info().SessionToken, raw); err != nil {
		t.Fatal(err)
	}
	r.session.mu.Lock()
	size := r.session.streams[stringID(m.ID)].sizes[1]
	r.session.mu.Unlock()
	if size != len(raw) {
		t.Fatalf("accounted %d, original %d", size, len(raw))
	}
	r.session.close()
	close(release)
	_ = await(t, done)
}
