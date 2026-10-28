package protocol

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const fixedAddress = `{"version":1,"type":"demo.ping","id":"session-42","generation":"AAAAAAAAAAAAAAAAAAAAAA"}`

func TestJSONRPCEnvelope(t *testing.T) {
	for _, id := range []string{`"c:1"`, `900719925474099312345`, `1.234567890123456789`, `null`} {
		m, e := ParseMessage([]byte(`{"jsonrpc":"2.0","id":` + id + `,"method":"runtime/services"}`))
		if e != nil || m.IsNotification() || string(m.ID) != id {
			t.Fatalf("id %s: %+v %v", id, m, e)
		}
		raw, err := Success(m.ID, nil).Encode()
		if err != nil {
			t.Fatal(err)
		}
		reply, e := ParseMessage(raw)
		if e != nil || string(reply.ID) != id || string(reply.Result) != "null" {
			t.Fatalf("reply %s: %v", raw, e)
		}
	}
	m, e := ParseMessage([]byte(`{"jsonrpc":"2.0","method":"unknown","extension":true}`))
	if e != nil || !m.IsNotification() {
		t.Fatalf("notification: %v %v", m, e)
	}
	for _, raw := range []string{
		`{}`, `null`, `1`, `[]`, `{"jsonrpc":"1.0","method":"x"}`,
		`{"jsonrpc":"2.0","method":null}`, `{"jsonrpc":"2.0","method":1}`,
		`{"jsonrpc":"2.0","method":"x","id":true}`,
		`{"jsonrpc":"2.0","method":"x","params":null}`,
		`{"jsonrpc":"2.0","method":"x","result":null}`,
		`{"jsonrpc":"2.0","id":1,"result":null,"error":null}`,
		`{"jsonrpc":"2.0","id":1}`, `{"jsonrpc":"2.0","result":null}`,
		`{"jsonrpc":"2.0","id":1,"error":null}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":1.1,"message":"x"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":null,"message":"x"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":1000,"message":null}}`,
	} {
		if _, e := ParseMessage([]byte(raw)); e == nil || e.Code != InvalidRequest {
			t.Errorf("accepted invalid envelope %s: %v", raw, e)
		}
	}
	for _, raw := range []string{`{`, `{"jsonrpc":"2.0","id":1,"id":2}`, `{"x":{"a":1,"a":2}}`, `NaN`, `{} {}`, "\xff"} {
		if _, e := ParseMessage([]byte(raw)); e == nil || e.Code != ParseError {
			t.Errorf("accepted malformed JSON %q: %v", raw, e)
		}
	}
}

func TestActorMethodVectors(t *testing.T) {
	tests := []struct {
		raw       string
		direction Direction
	}{
		{
			`{"jsonrpc":"2.0","id":"p:register-1","method":"runtime/register","params":{"type":"demo.ping","description":"Ping/Pong provider"}}`,
			ToGateway,
		},
		{`{"jsonrpc":"2.0","id":"list-1","method":"runtime/services"}`, ToGateway},
		{
			`{"jsonrpc":"2.0","id":"c:1","method":"actor/call","params":{"address":{"version":1,"type":"demo.ping","id":"session-42"},"input":{"n":900719925474099312345},"deadline_unix_ms":1890000000000}}`,
			ToGateway,
		},
		{
			`{"jsonrpc":"2.0","id":"g:create-1","method":"actor/create","params":{"address":` + fixedAddress + `,"deadline_unix_ms":1890000000000}}`,
			ToProvider,
		},
		{
			`{"jsonrpc":"2.0","id":"g:invoke-1","method":"actor/invoke","params":{"address":` + fixedAddress + `,"input":null,"deadline_unix_ms":1890000000000}}`,
			ToProvider,
		},
	}
	for _, tt := range tests {
		m, e := ParseMessage([]byte(tt.raw))
		if e != nil {
			t.Fatal(e)
		}
		p, e := m.Parameters(tt.direction)
		if e != nil {
			t.Fatalf("%s: %v", tt.raw, e)
		}
		if c, ok := p.(CallParams); ok && string(c.Input) != `{"n":900719925474099312345}` {
			t.Fatalf("precision lost: %s", c.Input)
		}
	}
	cases := []struct {
		method, params string
		direction      Direction
		code           int
	}{
		{Services, `[]`, ToGateway, InvalidParams},
		{Services, `{"other":1}`, ToGateway, InvalidParams},
		{Register, `{"Type":"demo.ping"}`, ToGateway, InvalidParams},
		{Register, `{"type":null}`, ToGateway, InvalidParams},
		{Register, `{"type":"INVALID"}`, ToGateway, InvalidParams},
		{Call, `{"address":` + fixedAddress + `}`, ToGateway, InvalidParams},
		{Call, `{"address":` + fixedAddress + `,"input":null,"extra":true}`, ToGateway, InvalidParams},
		{
			Call,
			`{"address":{"version":1,"type":"demo.ping","id":"x","other":1},"input":null}`,
			ToGateway,
			InvalidParams,
		},
		{Call, `{"address":` + fixedAddress + `,"input":null,"deadline_unix_ms":null}`, ToGateway, InvalidParams},
		{
			Create,
			`{"address":{"version":1,"type":"demo.ping","id":"x"},"deadline_unix_ms":1}`,
			ToProvider,
			InvalidParams,
		},
		{InvokeMethod, `{"address":` + fixedAddress + `,"input":null}`, ToProvider, InvalidParams},
		{Create, `{}`, ToGateway, MethodNotFound}, {Services, `{}`, ToProvider, MethodNotFound},
		{"actor/stream", `{}`, ToGateway, MethodNotFound}, {"runtime.services", `{}`, ToGateway, MethodNotFound},
	}
	for _, tt := range cases {
		m, err := NewRequest(tt.method, "test", json.RawMessage(tt.params))
		if err != nil {
			t.Fatal(err)
		}
		if _, e := m.Parameters(tt.direction); e == nil || e.Code != tt.code {
			t.Errorf("%s %s: %v", tt.method, tt.params, e)
		}
	}
}

func TestNotificationAndBatch(t *testing.T) {
	dispatches := 0
	dispatch := func(ctx context.Context, m Message, batch bool) (any, *Error) {
		dispatches++
		p, e := m.Parameters(ToGateway)
		_ = p
		if e != nil {
			return nil, e
		}
		if m.Method == Register {
			return nil, NewError(TransportMode, "SSE required", nil)
		}
		return ServicesResult{Services: []Service{}}, nil
	}
	tests := []struct {
		raw   string
		calls int
		want  string
	}{
		{`[]`, 0, `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}`},
		{`[{"jsonrpc":"2.0","method":"unknown"}]`, 1, ``},
		{
			`[{"jsonrpc":"2.0","method":"runtime/services"},{"jsonrpc":"2.0","id":null,"method":"runtime/services"},1]`,
			2,
			`[{"jsonrpc":"2.0","id":null,"result":{"services":[]}},{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid_request"}}]`,
		},
		{
			`{"jsonrpc":"2.0","method":"runtime/services","id":900719925474099312345}`,
			1,
			`{"jsonrpc":"2.0","id":900719925474099312345,"result":{"services":[]}}`,
		},
	}
	for _, tt := range tests {
		dispatches = 0
		got, err := ProcessRequests(context.Background(), []byte(tt.raw), dispatch)
		if err != nil || string(got) != tt.want || dispatches != tt.calls {
			t.Errorf("%s: %s, calls %d, err %v", tt.raw, got, dispatches, err)
		}
	}
	for _, raw := range []string{strings.Repeat(" ", MaxMessageBytes+1), `[1,2,3,4,5,6,7,8,9]`} {
		if _, err := ProcessRequests(context.Background(), []byte(raw), dispatch); err != ErrLimit {
			t.Fatal(err)
		}
	}
	if _, err := NewRequest(Call, "id", map[string]string{"input": strings.Repeat("<", 3000)}); err != ErrLimit {
		t.Fatalf("encoded escaping limit: %v", err)
	}
}

func TestResultFailureAndProviderErrors(t *testing.T) {
	for _, v := range []Result{
		NewResult(json.RawMessage(`null`), nil),
		NewResult(json.RawMessage(`{"n":900719925474099312345}`), nil),
		NewResult(nil, &Failure{Code: RemoteError, Message: "business failed"}),
	} {
		if err := v.Validate(); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(v)
		var round Result
		_ = json.Unmarshal(raw, &round)
		if round.Validate() != nil {
			t.Fatal(string(raw))
		}
	}
	for _, v := range []Result{
		{},
		NewResult(nil, nil),
		NewResult(json.RawMessage(`null`), &Failure{Code: RemoteError}),
		NewResult(nil, &Failure{Code: ProtocolError}),
		NewResult(json.RawMessage(`{"x":1,"x":2}`), nil),
		NewResult(nil, &Failure{Code: RemoteError, Message: strings.Repeat("x", 513)}),
	} {
		if v.Validate() == nil {
			t.Fatalf("accepted damaged DTO: %+v", v)
		}
	}
	var a Address
	_ = json.Unmarshal([]byte(fixedAddress), &a)
	for _, tt := range []struct {
		method, raw string
		valid       bool
	}{
		{Create, `{"address":` + fixedAddress + `}`, true},
		{InvokeMethod, `{"address":` + fixedAddress + `,"value":null}`, true},
		{InvokeMethod, `{"address":` + fixedAddress + `}`, false},
		{Create, `{"address":` + fixedAddress + `,"value":null}`, false},
		{
			InvokeMethod,
			`{"address":` + strings.Replace(fixedAddress, "session-42", "other", 1) + `,"value":null}`,
			false,
		},
	} {
		if _, err := ProviderResult([]byte(tt.raw), tt.method, a); (err == nil) != tt.valid {
			t.Errorf("%s: %v", tt.raw, err)
		}
	}
	for _, code := range []int{RemoteError, Full, OutcomeUnknown, DeadlineExceeded, Canceled, 42, Stopped} {
		e := NewError(code, strings.Repeat("界", 180), nil)
		f, err := NormalizeProviderError(e)
		if err != nil || len(f.Message) > 512 {
			t.Fatalf("%d: %v", code, err)
		}
		want := code
		if !failureCode(code) {
			want = RemoteError
		}
		if f.Code != want {
			t.Fatal(f)
		}
	}
	for _, e := range []*Error{
		NewError(MethodNotFound, "wrong method", nil),
		{Code: Full, Message: "x", Data: json.RawMessage(`{"kind":"remote_error"}`)},
	} {
		if _, err := NormalizeProviderError(e); err == nil {
			t.Fatalf("accepted %v", e)
		}
	}
}

func TestRegistrationTransportPreconditions(t *testing.T) {
	for _, raw := range []string{
		`{"jsonrpc":"2.0","method":"runtime/register","params":{"type":"demo.ping"}}`,
		`[{"jsonrpc":"2.0","method":"runtime/register","params":{"type":"demo.ping"}}]`,
		`[{"jsonrpc":"2.0","id":"r","method":"runtime/register","params":{"type":"demo.ping"}}]`,
	} {
		dispatched := false
		result, err := ProcessRequests(context.Background(), []byte(raw), func(context.Context, Message, bool) (any, *Error) {
			dispatched = true
			return nil, nil
		})
		if err != nil || dispatched {
			t.Fatalf("registration reached dispatch: %s, %v", raw, err)
		}
		if strings.Contains(raw, `"id"`) {
			var responses []Message
			_ = json.Unmarshal(result, &responses)
			if len(responses) != 1 || responses[0].Error == nil || responses[0].Error.Code != TransportMode {
				t.Fatal(string(result))
			}
		} else if len(result) != 0 {
			t.Fatal(string(result))
		}
	}
}

func TestStableDistDTOVectors(t *testing.T) {
	tests := []struct {
		value any
		want  string
	}{
		{
			Invoke{
				RequestID:   "remote-call-1",
				MessageType: "json",
				ContentType: "application/json",
				Content:     `{"text":"ping"}`,
			},
			`{"request_id":"remote-call-1","message_type":"json","content_type":"application/json","content":"{\"text\":\"ping\"}"}`,
		},
		{
			NewResult(json.RawMessage(`null`), nil),
			`{"message_type":"json","content_type":"application/json","content":"null"}`,
		},
		{
			NewResult(nil, &Failure{Code: RemoteError, Message: "handler failed"}),
			`{"message_type":"json","content_type":"application/json","content":"","failure":{"code":1000,"message":"handler failed"}}`,
		},
	}
	for _, tt := range tests {
		raw, err := json.Marshal(tt.value)
		if err != nil || string(raw) != tt.want {
			t.Fatalf("DTO %s: %v, want %s", raw, err, tt.want)
		}
	}
}
