package wire

import (
	"encoding/json"
	"fmt"
)

// Message retains raw IDs: nil means absent, while explicit null is an ID.
type Message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
	Request bool            `json:"-"`
}

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func Parse(raw []byte) (Message, error) {
	bad := fmt.Errorf("invalid RPC envelope")
	if err := Validate(raw); err != nil {
		return Message{}, bad
	}
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil || f == nil {
		return Message{}, bad
	}
	var m Message
	if json.Unmarshal(f["jsonrpc"], &m.JSONRPC) != nil || m.JSONRPC != "2.0" {
		return Message{}, bad
	}
	m.ID = f["id"]
	if m.ID != nil {
		id, _ := Decode(m.ID)
		switch id.(type) {
		case nil, string, json.Number:
		default:
			return Message{}, bad
		}
	}
	method, request := f["method"]
	result, hasResult := f["result"]
	e, hasError := f["error"]
	if request {
		if string(method) == "null" || json.Unmarshal(method, &m.Method) != nil || hasResult || hasError {
			return Message{}, bad
		}
		m.Request = true
		m.Params = f["params"]
		if m.Params != nil && m.Params[0] != '{' && m.Params[0] != '[' {
			return Message{}, bad
		}
	} else {
		if m.ID == nil || f["params"] != nil || hasResult == hasError {
			return Message{}, bad
		}
		if hasResult {
			m.Result = result
		} else {
			var ef map[string]json.RawMessage
			if json.Unmarshal(e, &ef) != nil || ef == nil ||
				ef["code"] == nil || ef["message"] == nil ||
				string(ef["code"]) == "null" || string(ef["message"]) == "null" {
				return Message{}, bad
			}
			m.Error = new(RPCError)
			// encoding/json struct matching is case-insensitive; decode the exact
			// required fields explicitly and ignore envelope/error extensions.
			if json.Unmarshal(ef["code"], &m.Error.Code) != nil || json.Unmarshal(ef["message"], &m.Error.Message) != nil {
				return Message{}, bad
			}
			m.Error.Data = ef["data"]
		}
	}
	return m, nil
}

func Response(raw []byte, id string) (Message, error) {
	m, err := Parse(raw)
	var got string
	if err != nil || m.Request || string(m.ID) == "null" || json.Unmarshal(m.ID, &got) != nil || got != id {
		return Message{}, fmt.Errorf("RPC correlation mismatch")
	}
	return m, nil
}

func Request(method, id string, params any) ([]byte, error) {
	p, err := Encode(params)
	if err != nil || id == "" {
		return nil, fmt.Errorf("invalid RPC request")
	}
	i, _ := json.Marshal(id)
	return Encode(Message{JSONRPC: "2.0", ID: i, Method: method, Params: p})
}

func Kind(code int) string {
	if code >= 1000 && code <= 1013 {
		return [...]string{
			"remote_error",
			"full",
			"actor_unavailable",
			"activation_unknown",
			"outcome_unknown",
			"deadline_exceeded",
			"stopped",
			"registration_conflict",
			"transport_mode",
			"canceled",
			"owner_unavailable",
			"contract",
			"protocol",
			"unsupported_operation",
		}[code-1000]
	}
	switch code {
	case -32700:
		return "parse_error"
	case -32600:
		return "invalid_request"
	case -32601:
		return "method_not_found"
	case -32602:
		return "invalid_params"
	case -32603:
		return "internal_error"
	}
	return "unknown"
}

func ValidateError(e *RPCError) error {
	var data map[string]json.RawMessage
	if json.Unmarshal(e.Data, &data) == nil && data != nil && data["kind"] != nil && Kind(e.Code) != "unknown" {
		var kind string
		if json.Unmarshal(data["kind"], &kind) != nil || kind != Kind(e.Code) {
			return fmt.Errorf("RPC kind/code mismatch")
		}
	}
	return nil
}
