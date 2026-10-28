package protocol

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestStreamParameters(t *testing.T) {
	token := strings.Repeat("A", 43)
	address := `{"version":1,"type":"demo.chat","id":"one","generation":"AAAAAAAAAAAAAAAAAAAAAA"}`
	vectors := []struct {
		method, params string
		direction      Direction
		notification   bool
		valid          bool
	}{
		{Register, `{"type":"demo.chat"}`, ToGateway, false, true},
		{Register, `{"type":"demo.chat","capabilities":{"stream":true}}`, ToGateway, false, false},
		{Register, `{"type":"demo.chat","capabilities":{"stream":false}}`, ToGateway, false, false},
		{Register, `{"type":"demo.chat","capabilities":null}`, ToGateway, false, false},
		{Register, `{"type":"demo.chat","capabilities":false}`, ToGateway, false, false},
		{Register, `{"type":"demo.chat","capabilities":{}}`, ToGateway, false, false},
		{Register, `{"type":"demo.chat","capabilities":{"stream":null}}`, ToGateway, false, false},
		{Register, `{"type":"demo.chat","capabilities":{"stream":true,"other":true}}`, ToGateway, false, false},
		{Stream, `{"address":` + address + `,"input":null}`, ToGateway, false, true},
		{InvokeStreamMethod, `{"address":` + address + `,"input":null,"deadline_unix_ms":1,"stream_token":"` + token + `","window":{"items":4,"bytes":32768}}`, ToProvider, false, true},
		{ChunkMethod, `{"request_id":null,"stream_token":"` + token + `","address":` + address + `,"seq":1,"value":null}`, ToGateway, true, true},
		{Credit, `{"request_id":1.250,"stream_token":"` + token + `","ack_seq":0}`, ToGateway, true, true},
		{Credit, `{"request_id":null,"stream_token":"` + token + `","ack_seq":9007199254740992}`, ToGateway, true, false},
		{Cancel, `{"request_id":null,"stream_token":"` + token + `"}`, ToProvider, true, true},
		{Cancel, `{"request_id":null,"stream_token":"` + token + `","ack_seq":0}`, ToProvider, true, false},
		{ChunkMethod, `{"request_id":"s","stream_token":"` + token + `","address":` + address + `,"seq":0,"value":null}`, ToGateway, true, false},
	}
	for _, v := range vectors {
		id := `"id":"request",`
		if v.notification {
			id = ""
		}
		m, e := ParseMessage([]byte(`{"jsonrpc":"2.0",` + id + `"method":"` + v.method + `","params":` + v.params + `}`))
		if e != nil {
			t.Fatal(e)
		}
		_, err := m.Parameters(v.direction)
		if (err == nil) != v.valid {
			t.Fatalf("%s %s: %v", v.method, v.params, err)
		}
	}
}

func TestStreamCannotOpenFromBatchOrNotification(t *testing.T) {
	for _, raw := range []string{`{"jsonrpc":"2.0","method":"actor/stream","params":{}}`, `[{"jsonrpc":"2.0","id":null,"method":"actor/stream","params":{}}]`} {
		called := false
		_, err := ProcessRequests(t.Context(), []byte(raw), func(_ context.Context, _ Message, _ bool) (any, *Error) {
			called = true
			return nil, nil
		})
		if err != nil || called {
			t.Fatal("stream dispatched without single request", err)
		}
	}
}

func TestStreamTerminalValidation(t *testing.T) {
	a := Address{
		Version:    1,
		Type:       "demo.chat",
		ID:         "one",
		Generation: "AAAAAAAAAAAAAAAAAAAAAA",
	}
	raw, _ := json.Marshal(StreamResult{Address: a, FinalSeq: 0})
	if err := ParseStreamResult(raw, a, 0); err != nil {
		t.Fatal(err)
	}
	if err := ParseStreamResult(raw, a, 1); err == nil {
		t.Fatal("missing chunk accepted")
	}
}
