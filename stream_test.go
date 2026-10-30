package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-aie/actorx"
	"github.com/nats-io/nats.go"
)

type streamQuery struct {
	Count, Size int
	Park, Fail  bool
}

func TestStreamReceiveSequenceAndWindow(t *testing.T) {
	base := businessFrame{
		Version:     1,
		Kind:        "chunk",
		Operation:   "invoke",
		Type:        "stream",
		ID:          "one",
		Generation:  token(t),
		Correlation: token(t),
		Deadline:    time.Now().Add(time.Second).UnixNano(),
		Message:     "test",
		Mode:        "stream",
		Fingerprint: strings.Repeat("a", 64),
		Seq:         1,
		Payload:     json.RawMessage(`"x"`),
		Bytes:       3,
	}
	base.Control = "_INBOX.test.control." + base.Correlation
	accept := func(s *receiveStream, frame businessFrame) {
		t.Helper()

		data, err := json.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		s.accept(&nats.Msg{Data: data})
	}

	for _, tc := range []struct {
		name   string
		mutate func(*businessFrame)
	}{
		{"gap", func(f *businessFrame) { f.Seq = 3 }},
		{"duplicate", func(f *businessFrame) { f.Seq = 1 }},
		{"wrong terminal sequence", func(f *businessFrame) {
			f.Kind = "complete"
			f.Control = ""
			f.Seq = 9
			f.Status = "ok"
			f.Payload = json.RawMessage("null")
			f.Bytes = 0
		}},
		{"different generation", func(f *businessFrame) { f.Generation = token(t) }},
		{"inflated bytes", func(f *businessFrame) { f.Bytes++ }},
		{"different control route", func(f *businessFrame) { f.Control = "_INBOX.other.control." + f.Correlation }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newReceiveStream(&network{}, context.Background(), base, reflect.TypeFor[string]())
			accept(s, base)
			if s.err != nil || len(s.queue) != 1 {
				t.Fatal("first chunk", s.err)
			}

			next := base
			next.Seq = 2
			tc.mutate(&next)
			accept(s, next)
			if !errors.Is(s.err, ErrProtocol) || !errors.Is(s.err, ErrOutcomeUnknown) {
				t.Fatal("invalid stream did not fail with protocol and outcome evidence", s.err)
			}
			if len(s.queue) != 1 {
				t.Fatal("invalid chunk reached callback queue")
			}
		})
	}

	t.Run("full window includes callback", func(t *testing.T) {
		s := newReceiveStream(&network{}, context.Background(), base, reflect.TypeFor[string]())
		for i := range streamItems {
			f := base
			f.Seq = uint64(i + 1)
			accept(s, f)
		}
		if s.err != nil {
			t.Fatal(s.err)
		}

		<-s.queue // An active loop body still consumes its window reservation.
		f := base
		f.Seq = streamItems + 1
		accept(s, f)
		if !errors.Is(s.err, ErrProtocol) {
			t.Fatal("active callback released credit early", s.err)
		}
	})

	t.Run("byte window includes active body", func(t *testing.T) {
		s := newReceiveStream(&network{}, context.Background(), base, reflect.TypeFor[string]())
		payload, err := json.Marshal(strings.Repeat("x", streamBytes/2-2))
		if err != nil {
			t.Fatal(err)
		}

		// Two encoded values fill the byte budget before the four-item limit.
		for seq := uint64(1); seq <= 2; seq++ {
			f := base
			f.Seq, f.Payload, f.Bytes = seq, payload, len(payload)
			accept(s, f)
		}
		if s.err != nil || s.items != 2 || s.bytes != streamBytes {
			t.Fatal("valid chunks did not fill the byte window", s.err, s.items, s.bytes)
		}

		<-s.queue // The active body still owns its encoded bytes.
		f := base
		f.Seq = 3
		accept(s, f)
		if !errors.Is(s.err, ErrProtocol) || !errors.Is(s.err, ErrOutcomeUnknown) {
			t.Fatal("byte overflow was not rejected", s.err)
		}
		if s.seq != 2 || s.items != 2 || s.bytes != streamBytes || len(s.queue) != 1 {
			t.Fatal("rejected chunk changed the receive window", s.seq, s.items, s.bytes, len(s.queue))
		}
	})
}

func TestStreamCreditRequiresExactConsumedChunk(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame businessFrame
		valid bool
	}{
		{"consumed", businessFrame{Seq: 1, Items: 1, Bytes: 3}, true},
		{"duplicate", businessFrame{Seq: 0, Items: 1, Bytes: 3}, false},
		{"gap", businessFrame{Seq: 2, Items: 1, Bytes: 3}, false},
		{"inflated bytes", businessFrame{Seq: 1, Items: 1, Bytes: 4}, false},
		{"inflated items", businessFrame{Seq: 1, Items: 2, Bytes: 3}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &sendStream{
				p:     &businessReply{cancel: cancel},
				sizes: []int{3, 5},
				bytes: 8,
				wake:  make(chan struct{}),
			}
			s.credit(tc.frame)
			if tc.valid {
				if s.err != nil || s.ack != 1 || s.bytes != 5 || len(s.sizes) != 1 || ctx.Err() != nil {
					t.Fatal("credit did not release exactly the consumed chunk", s.err)
				}
			} else if !errors.Is(s.err, ErrProtocol) || ctx.Err() == nil || s.bytes != 8 || s.ack != 0 {
				t.Fatal("invalid credit changed window or failed to cancel", s.err)
			}
		})
	}
}

func TestStreamContractAndFrames(t *testing.T) {
	contract, err := buildContract([]actorx.CallDescriptor{actorx.Streaming[streamQuery, string]()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkInvocation(contract, streamQuery{}, actorx.StreamKind, reflect.TypeFor[string]()); err != nil {
		t.Fatal(err)
	}

	if _, err := checkInvocation(contract, streamQuery{}, actorx.StreamKind, reflect.TypeFor[int]()); !errors.Is(err, actorx.ErrReplyType) {
		t.Fatal(err)
	}

	f := businessFrame{
		Version:     1,
		Kind:        "complete",
		Operation:   "invoke",
		Type:        "stream",
		ID:          "one",
		Generation:  token(t),
		Correlation: token(t),
		Deadline:    time.Now().Add(time.Second).UnixNano(),
		Message:     "test",
		Mode:        "stream",
		Fingerprint: contract.fingerprint,
		Seq:         0,
		Status:      "ok",
		Payload:     json.RawMessage("null"),
	}
	data, _ := json.Marshal(f)
	var decoded businessFrame
	if err := decodeBusiness(data, &decoded); err != nil {
		t.Fatal("empty stream terminal", err)
	}

	// Even an empty stream must explicitly carry a valid final sequence number.
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	for _, bad := range []json.RawMessage{nil, json.RawMessage("null"), json.RawMessage("-1"), json.RawMessage("1.5")} {
		if bad == nil {
			delete(fields, "seq")
		} else {
			fields["seq"] = bad
		}
		encoded, _ := json.Marshal(fields)
		if err := decodeBusiness(encoded, &decoded); !errors.Is(err, ErrProtocol) {
			t.Fatal("invalid final seq", string(bad), err)
		}
	}
}

func streamError[T any](seq iter.Seq2[T, error]) error {
	for _, err := range seq {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestStreamIteratorLazyAndConcurrentUse(t *testing.T) {
	rt := testRuntime(t)
	a := mustAddr(t, rt, "probe", "one")
	entered, release := make(chan struct{}), make(chan struct{})
	sentinel := errors.New("open failed")
	var opens atomic.Int32
	rt.openStream = func(context.Context, Addr, any, reflect.Type) (*receiveStream, error) {
		opens.Add(1)
		close(entered)
		<-release
		return nil, sentinel
	}

	seq := a.Stream[int](testContext(t), series{})
	if opens.Load() != 0 {
		t.Fatal("eager open")
	}

	done := make(chan error, 1)
	go func() { done <- streamError(seq) }()
	await(t, entered)
	if err := streamError(seq); !errors.Is(err, actorx.ErrSetup) {
		t.Fatal(err)
	}

	close(release)
	if err := <-done; err != sentinel || opens.Load() != 1 {
		t.Fatal(err, opens.Load())
	}
}

func TestStreamValuesPreserveNilAndInterfaces(t *testing.T) {
	if v, err := streamValueAs[any](nil); v != nil || err != nil {
		t.Fatal(v, err)
	}
	if v, err := streamValueAs[*int](nil); v != nil || err != nil {
		t.Fatal(v, err)
	}

	var pointer *int
	if v, err := streamValueAs[any](pointer); v != any(pointer) || err != nil {
		t.Fatal(v, err)
	}

	var value error = errors.New("interface value")
	if v, err := streamValueAs[error](value); v != value || err != nil {
		t.Fatal(v, err)
	}
	if _, err := streamValueAs[int](nil); !errors.Is(err, actorx.ErrReplyType) {
		t.Fatal(err)
	}
	if _, err := streamValueAs[int]("wrong"); !errors.Is(err, actorx.ErrReplyType) {
		t.Fatal(err)
	}
}
