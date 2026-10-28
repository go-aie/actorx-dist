package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrLimit is a transport rejection, rather than a JSON-RPC error response.
var ErrLimit = errors.New("remote: message or batch limit exceeded")

// ErrAddressMismatch leaves the legitimate pending request available for a reply
// carrying its expected complete identity.
var ErrAddressMismatch = errors.New("remote: response address mismatch")

// Message retains raw IDs: absence denotes notification; explicit null is an ID.
// Use ParseMessage/NewRequest/Success/ErrorResponse before passing to a Gateway.
type Message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
	request bool
}

func (m Message) IsRequest() bool { return m.request }

func (m Message) IsNotification() bool { return m.request && len(m.ID) == 0 }

func (m Message) Encode() ([]byte, error) {
	raw, err := json.Marshal(m)
	if err == nil && len(raw) > MaxMessageBytes {
		err = ErrLimit
	}
	return raw, err
}

func validID(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&v) != nil {
		return false
	}
	switch v.(type) {
	case nil, string, json.Number:
		return true
	}
	return false
}

// ParseMessage validates the JSON-RPC envelope without decoding business values
// or rounding numeric IDs. Method-specific validation belongs to Parameters.
func ParseMessage(raw []byte) (Message, *Error) {
	bad := func(code int) (Message, *Error) { return Message{}, NewError(code, Kind(code), nil) }
	if ValidateJSON(raw) != nil {
		return bad(ParseError)
	}
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil || f == nil {
		return bad(InvalidRequest)
	}
	var version string
	if json.Unmarshal(f["jsonrpc"], &version) != nil || version != JSONRPC {
		return bad(InvalidRequest)
	}
	id := f["id"]
	if !validID(id) {
		return bad(InvalidRequest)
	}
	m := Message{JSONRPC: JSONRPC, ID: id}
	method, request := f["method"]
	result, hasResult := f["result"]
	errorRaw, hasError := f["error"]

	// Field presence matters: result:null is a result, and id:null is a request ID.
	if request {
		if hasResult || hasError || string(method) == "null" || json.Unmarshal(method, &m.Method) != nil {
			return bad(InvalidRequest)
		}
		m.request = true
		m.Params = f["params"]
		if len(m.Params) > 0 && m.Params[0] != '{' && m.Params[0] != '[' {
			return bad(InvalidRequest)
		}
	} else {
		if len(id) == 0 || hasResult == hasError || f["params"] != nil {
			return bad(InvalidRequest)
		}
		if hasResult {
			m.Result = result
		} else {
			var ef map[string]json.RawMessage
			if json.Unmarshal(errorRaw, &ef) != nil || ef == nil {
				return bad(InvalidRequest)
			}
			var code int
			var message string
			if json.Unmarshal(ef["code"], &code) != nil ||
				string(ef["code"]) == "null" ||
				json.Unmarshal(ef["message"], &message) != nil ||
				string(ef["message"]) == "null" {
				return bad(InvalidRequest)
			}
			m.Error = &Error{Code: code, Message: message, Data: ef["data"]}
		}
	}
	return m, nil
}

func NewRequest(method, id string, params any) (Message, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return Message{}, err
	}
	idRaw, _ := json.Marshal(id)
	m := Message{JSONRPC: JSONRPC, ID: idRaw, Method: method, Params: raw, request: true}
	if id == "" {
		return Message{}, fmt.Errorf("empty generated RPC id")
	}
	_, err = m.Encode()
	return m, err
}

func Success(id json.RawMessage, result any) Message {
	raw, err := json.Marshal(result)
	if err != nil {
		return ErrorResponse(id, NewError(InternalError, "Internal error", nil))
	}
	return Message{JSONRPC: JSONRPC, ID: id, Result: raw}
}

func ErrorResponse(id json.RawMessage, e *Error) Message {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	return Message{JSONRPC: JSONRPC, ID: id, Error: e}
}

type Direction int

const (
	ToGateway Direction = iota
	ToProvider
)

// object enforces exact, case-sensitive field names and presence vs JSON null.
func object(raw []byte, allowed []string, required ...string) (map[string]json.RawMessage, error) {
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil || f == nil {
		return nil, fmt.Errorf("expected object")
	}
	for k := range f {
		found := false
		for _, name := range allowed {
			if name == k {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown parameter")
		}
	}
	for _, k := range required {
		if _, ok := f[k]; !ok {
			return nil, fmt.Errorf("missing parameter")
		}
	}
	return f, nil
}

func decodeParams(raw []byte, out any, allowed []string, required ...string) error {
	f, err := object(raw, allowed, required...)
	if err != nil {
		return err
	}
	for k, v := range f {
		if string(v) == "null" && k != "input" && k != "value" {
			return fmt.Errorf("null parameter")
		}
	}
	return json.Unmarshal(raw, out)
}

func validateAddressField(raw []byte, fixed bool) error {
	var f map[string]json.RawMessage
	_ = json.Unmarshal(raw, &f)
	var a Address
	if err := decodeParams(f["address"], &a, []string{"version", "type", "id", "generation"}, "version", "type", "id"); err != nil {
		return err
	}
	return a.Validate(fixed)
}

// Parameters decodes only methods allowed in this direction and rejects unknown
// remote fields. Business input/value fields remain opaque JSON.
func (m Message) Parameters(direction Direction) (any, *Error) {
	invalid := NewError(InvalidParams, "Invalid params", nil)
	if !m.request {
		return nil, NewError(InvalidRequest, "Expected request", nil)
	}
	raw := m.Params
	var out any
	var err error
	switch {
	case direction == ToGateway && m.Method == Register:
		var p RegisterParams
		err = decodeParams(raw, &p, []string{"type", "description"}, "type")
		if err == nil {
			err = p.Validate()
		}
		out = p
	case direction == ToGateway && m.Method == Services:
		if len(raw) == 0 {
			raw = []byte("{}")
		}
		_, err = object(raw, nil)
		out = struct{}{}
	case direction == ToGateway && m.Method == Call:
		var p CallParams
		err = decodeParams(raw, &p, []string{"address", "input", "deadline_unix_ms"}, "address", "input")
		if err == nil {
			err = validateAddressField(raw, false)
		}
		out = p
	case direction == ToProvider && m.Method == Create:
		var p CreateParams
		err = decodeParams(raw, &p, []string{"address", "deadline_unix_ms"}, "address", "deadline_unix_ms")
		if err == nil {
			err = validateAddressField(raw, true)
		}
		out = p
	case direction == ToProvider && m.Method == InvokeMethod:
		var p InvokeParams
		err = decodeParams(raw, &p, []string{"address", "input", "deadline_unix_ms"}, "address", "input", "deadline_unix_ms")
		if err == nil {
			err = validateAddressField(raw, true)
		}
		out = p
	default:
		return nil, NewError(MethodNotFound, "Method not found", nil)
	}
	if err != nil {
		return nil, invalid
	}
	return out, nil
}

// ProviderResult validates the exact successful shape for the pending method.
func ProviderResult(raw []byte, method string, expected Address) (json.RawMessage, error) {
	allowed := []string{"address"}
	required := []string{"address"}
	if method == InvokeMethod {
		allowed = append(allowed, "value")
		required = append(required, "value")
	} else if method != Create {
		return nil, fmt.Errorf("invalid pending method")
	}
	f, err := object(raw, allowed, required...)
	if err != nil {
		return nil, err
	}
	if err = validateAddressField(raw, true); err != nil {
		return nil, err
	}
	var a Address
	_ = json.Unmarshal(f["address"], &a)
	if a != expected {
		return nil, ErrAddressMismatch
	}
	return f["value"], nil
}

// ErrorAddress checks optional provider data.address without constraining other data.
func ErrorAddress(raw []byte, expected Address) error {
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil || f == nil {
		return nil
	}
	if a, ok := f["address"]; ok {
		wrapper, _ := json.Marshal(map[string]json.RawMessage{"address": a})
		if err := validateAddressField(wrapper, true); err != nil {
			return err
		}
		var address Address
		_ = json.Unmarshal(a, &address)
		if address != expected {
			return ErrAddressMismatch
		}
	}
	return nil
}

// ProcessRequests implements transport-independent JSON-RPC notification/batch
// rules. A transport handles provider responses separately and enforces SSE for
// a single registration. Batch/notification registration never reaches dispatch.
func ProcessRequests(ctx context.Context, raw []byte, dispatch func(context.Context, Message, bool) (any, *Error)) ([]byte, error) {
	if len(raw) > MaxMessageBytes {
		return nil, ErrLimit
	}
	singleError := func(e *Error) ([]byte, error) { return ErrorResponse(nil, e).Encode() }
	if ValidateJSON(raw) != nil {
		return singleError(NewError(ParseError, "Parse error", nil))
	}
	raw = bytes.TrimSpace(raw)
	batch := len(raw) > 0 && raw[0] == '['
	items := []json.RawMessage{raw}
	if batch {
		_ = json.Unmarshal(raw, &items)
		if len(items) == 0 {
			return singleError(NewError(InvalidRequest, "Invalid Request", nil))
		}
		if len(items) > MaxBatchItems {
			return nil, ErrLimit
		}
	}

	responses := make([]Message, 0, len(items))
	for _, item := range items {
		m, e := ParseMessage(item)
		if e != nil {
			responses = append(responses, ErrorResponse(nil, e))
			continue
		}
		if !m.IsRequest() {
			return nil, fmt.Errorf("remote: response in request document")
		}
		var result any
		var rpcErr *Error
		if m.Method == Register && (batch || m.IsNotification()) {
			rpcErr = NewError(TransportMode, "Registration requires a single SSE request", nil)
		} else {
			result, rpcErr = dispatch(ctx, m, batch)
		}

		// Notifications execute but never contribute a success or error response.
		if m.IsNotification() {
			continue
		}
		response := Success(m.ID, result)
		if rpcErr != nil {
			response = ErrorResponse(m.ID, rpcErr)
		}
		if _, err := response.Encode(); err != nil {
			return nil, err
		}
		responses = append(responses, response)
	}

	if len(responses) == 0 {
		return nil, nil
	}
	if !batch {
		return responses[0].Encode()
	}
	encoded, err := json.Marshal(responses)
	if len(encoded) > MaxBatchBytes {
		return nil, ErrLimit
	}
	return encoded, err
}
