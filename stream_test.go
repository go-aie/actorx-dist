package actorxdist

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
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
			s := newReceiveStream(&network{}, context.Background(), base, reflect.TypeFor[string](), nil)
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
		s := newReceiveStream(&network{}, context.Background(), base, reflect.TypeFor[string](), nil)
		for i := range streamItems {
			f := base
			f.Seq = uint64(i + 1)
			accept(s, f)
		}
		if s.err != nil {
			t.Fatal(s.err)
		}

		<-s.queue // A running callback still consumes its window reservation.
		f := base
		f.Seq = streamItems + 1
		accept(s, f)
		if !errors.Is(s.err, ErrProtocol) {
			t.Fatal("active callback released credit early", s.err)
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
			s := &sendStream{p: &businessReply{cancel: cancel}, sizes: []int{3, 5}, bytes: 8, wake: make(chan struct{})}
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
