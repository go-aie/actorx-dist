package protocol

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// Both Go and Python read these vectors so protocol revisions cannot silently
// diverge in registration, error evidence or the unchanged actorx-dist DTO shapes.
func TestV1WireVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors map[string]json.RawMessage
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	a := Address{
		Version:    1,
		Type:       "demo.ping",
		ID:         "session-42",
		Generation: "AAAAAAAAAAAAAAAAAAAAAA",
	}
	registration, err := NewRequest(Register, "p:register-1", RegisterParams{Type: a.Type, Description: "Ping/Pong provider"})
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, id string, params any) Message {
		t.Helper()
		m, err := NewRequest(method, id, params)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	logical := a
	logical.Generation = ""
	ms := int64(1890000000000)
	consumer := CallParams{Address: logical, Input: json.RawMessage(`{"text":"ping"}`)}
	provider := CallParams{Address: a, Input: consumer.Input, DeadlineUnixMS: &ms}
	values := map[string]any{
		"consumer_call":   request(Call, "c:call-1", consumer),
		"provider_call":   request(Call, "g:call-1", provider),
		"consumer_stream": request(Stream, "c:stream-1", consumer),
		"provider_stream": request(Stream, "g:stream-1", StreamParams{
			CallParams:  provider,
			StreamToken: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Window:      StreamWindow(),
		}),
		"stream_opened": Notification(StreamOpen, OpenParams{
			RequestID:   json.RawMessage(`"c:stream-1"`),
			StreamToken: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Window:      StreamWindow(),
		}),
		"stream_chunk": Notification(ChunkMethod, ChunkParams{
			RequestID:   json.RawMessage(`"c:stream-1"`),
			StreamToken: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			Address:     a,
			Seq:         1,
			Value:       json.RawMessage(`null`),
		}),
		"stream_ack": Notification(Credit, ControlParams{
			RequestID:   json.RawMessage(`"c:stream-1"`),
			StreamToken: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			AckSeq:      1,
		}),
		"stream_cancel": Notification(Cancel, CancelParams{
			RequestID:   json.RawMessage(`"c:stream-1"`),
			StreamToken: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		}),
		"register": registration,
		"registered": Success(json.RawMessage(`"p:register-1"`), RegistrationInfo{
			Protocol: Version, Type: a.Type, SessionToken: "session-token",
		}),
		"actors": Success(json.RawMessage(`"list-1"`), ActorsResult{Actors: []Actor{
			{
				Type:        a.Type,
				Description: "Ping/Pong provider",
				Protocol:    Version,
			},
		}}),
		"call_unsupported":   ErrorResponse(json.RawMessage(`"c:1"`), NewError(UnsupportedOperation, Kind(UnsupportedOperation), &a)),
		"actor_unavailable":  ErrorResponse(json.RawMessage(`"c:2"`), NewError(ActorUnavailable, Kind(ActorUnavailable), nil)),
		"stream_unsupported": ErrorResponse(json.RawMessage(`"s:1"`), StreamError(NewError(UnsupportedOperation, Kind(UnsupportedOperation), &a), 0)),
		"call_failure":       NewResult(nil, &Failure{Code: UnsupportedOperation, Message: Kind(UnsupportedOperation)}),
		"invoke": Invoke{
			RequestID:   "remote-call-1",
			MessageType: "json",
			ContentType: "application/json",
			Content:     `{"text":"ping"}`,
		},
		"invoke_stream": InvokeStream{
			RequestID:   "remote-stream-1",
			MessageType: "json",
			ContentType: "application/json",
			Content:     `{"text":"ping"}`,
		},
		"chunk": Chunk{
			MessageType: "json",
			ContentType: "application/json",
			Content:     "null",
		},
	}
	if len(values) != len(vectors) {
		t.Fatal("uncovered wire vectors")
	}
	for name, value := range values {
		t.Run(name, func(t *testing.T) {
			got, err := json.Marshal(value)
			var want bytes.Buffer
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Compact(&want, vectors[name]); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want.Bytes()) {
				t.Fatalf("got %s, want %s", got, want.Bytes())
			}
		})
	}
}

func TestV1ProviderFailureWhitelist(t *testing.T) {
	allowed := map[int]bool{
		1000: true,
		1001: true,
		1004: true,
		1005: true,
		1009: true,
		1013: true,
	}
	for code := 999; code <= 1014; code++ {
		e := NewError(code, "provider failure", nil)
		failure, err := NormalizeProviderError(e)
		if err != nil {
			t.Fatal(err)
		}
		want := RemoteError
		if allowed[code] {
			want = code
		}
		if failure.Code != want {
			t.Errorf("provider code %d normalized to %d, want %d", code, failure.Code, want)
		}
		if err := NewResult(nil, &Failure{Code: code, Message: "failure"}).Validate(); (err == nil) != allowed[code] {
			t.Errorf("Failure code %d: %v", code, err)
		}
	}
	if _, err := NormalizeProviderError(&Error{
		Code: UnsupportedOperation,
		Data: json.RawMessage(`{"kind":"remote_error"}`),
	}); err == nil {
		t.Fatal("accepted mismatched unsupported_operation kind")
	}
}

func TestJSONBoundaryVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/json-boundaries.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct{ Valid, Invalid []string }
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, raw := range vectors.Valid {
		if _, err := ParseMessage([]byte(raw)); err != nil {
			t.Fatalf("valid vector %s: %v", raw, err)
		}
	}
	for _, raw := range vectors.Invalid {
		if _, err := ParseMessage([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid vector %s", raw)
		}
	}
}
