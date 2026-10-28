package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

const (
	Stream             = "actor/stream"
	StreamOpen         = "actor/stream_open"
	InvokeStreamMethod = "actor/invoke_stream"
	ChunkMethod        = "actor/chunk"
	Credit             = "actor/credit"
	Cancel             = "actor/cancel"
	MaxSequence        = uint64(1<<53 - 1)
)

// Window bounds unacknowledged chunks by count and encoded JSON byte size.
type Window struct {
	Items int `json:"items"`
	Bytes int `json:"bytes"`
}

// StreamWindow is the fixed window for each remote v1 stream segment.
func StreamWindow() Window { return Window{Items: 4, Bytes: 32768} }

// InvokeStream is the actorx-dist Stream DTO. It remains separate from Invoke
// so the Call DTO schema stays unchanged; adding a Stream binding changes the
// Type contract of a previously Call-only proxy.
type InvokeStream struct {
	RequestID   string `json:"request_id"`
	MessageType string `json:"message_type"`
	ContentType string `json:"content_type"`
	Content     string `json:"content"`
}

func (i InvokeStream) Validate() error {
	return (Invoke{
		RequestID:   i.RequestID,
		MessageType: i.MessageType,
		ContentType: i.ContentType,
		Content:     i.Content,
	}).Validate()
}

type Chunk struct {
	MessageType string `json:"message_type"`
	ContentType string `json:"content_type"`
	Content     string `json:"content"`
}

func (c Chunk) Validate() error {
	if c.MessageType != "json" || c.ContentType != "application/json" {
		return fmt.Errorf("invalid chunk encoding")
	}
	return ValidateJSON([]byte(c.Content))
}

// OpenParams opens the consumer segment with its own correlation and window.
type OpenParams struct {
	RequestID   json.RawMessage `json:"request_id"`
	StreamToken string          `json:"stream_token"`
	Window      Window          `json:"window"`
}

// InvokeStreamParams opens a separate provider segment with independent credit.
type InvokeStreamParams struct {
	Address        Address         `json:"address"`
	Input          json.RawMessage `json:"input"`
	DeadlineUnixMS int64           `json:"deadline_unix_ms"`
	StreamToken    string          `json:"stream_token"`
	Window         Window          `json:"window"`
}

type ChunkParams struct {
	RequestID   json.RawMessage `json:"request_id"`
	StreamToken string          `json:"stream_token"`
	Address     Address         `json:"address"`
	Seq         uint64          `json:"seq"`
	Value       json.RawMessage `json:"value"`
}

// CancelParams cancels a correlated segment without acknowledging data.
type CancelParams struct {
	RequestID   json.RawMessage `json:"request_id"`
	StreamToken string          `json:"stream_token"`
}

// ControlParams acknowledges consumption of every chunk up to AckSeq.
type ControlParams struct {
	RequestID   json.RawMessage `json:"request_id"`
	StreamToken string          `json:"stream_token"`
	AckSeq      uint64          `json:"ack_seq"`
}

// StreamResult confirms success through FinalSeq, including zero for an empty stream.
type StreamResult struct {
	Address  Address `json:"address"`
	FinalSeq uint64  `json:"final_seq"`
}

func ValidStreamToken(token string) bool {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	return err == nil && len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == token
}

func Notification(method string, params any) Message {
	raw, _ := json.Marshal(params)
	return Message{JSONRPC: JSONRPC, Method: method, Params: raw, request: true}
}

func (m Message) streamParameters(direction Direction) (any, *Error) {
	bad := NewError(InvalidParams, "Invalid stream params", nil)
	wrong := NewError(MethodNotFound, "Method not found", nil)
	var out any
	var err error
	switch m.Method {
	case InvokeStreamMethod:
		if direction != ToProvider || m.IsNotification() {
			return nil, wrong
		}
		var p InvokeStreamParams
		err = decodeParams(
			m.Params, &p,
			[]string{"address", "input", "deadline_unix_ms", "stream_token", "window"},
			"address", "input", "deadline_unix_ms", "stream_token", "window",
		)
		if err == nil {
			err = validateAddressField(m.Params, true)
		}
		if !ValidStreamToken(p.StreamToken) || p.Window != StreamWindow() {
			return nil, bad
		}
		var f map[string]json.RawMessage
		_ = json.Unmarshal(m.Params, &f)
		var w Window
		if e := decodeParams(f["window"], &w, []string{"items", "bytes"}, "items", "bytes"); e != nil {
			return nil, bad
		}
		out = p
	case ChunkMethod:
		if direction != ToGateway || !m.IsNotification() {
			return nil, wrong
		}
		var p ChunkParams
		err = decodeParams(
			m.Params, &p,
			[]string{"request_id", "stream_token", "address", "seq", "value"},
			"request_id", "stream_token", "address", "seq", "value",
		)
		if err == nil {
			err = validateAddressField(m.Params, true)
		}
		if !validID(p.RequestID) || len(p.RequestID) == 0 ||
			!ValidStreamToken(p.StreamToken) || p.Seq == 0 || p.Seq > MaxSequence {
			return nil, bad
		}
		out = p
	case Credit, Cancel:
		if !m.IsNotification() {
			return nil, wrong
		}
		var p ControlParams
		allowed := []string{"request_id", "stream_token"}
		required := []string{"request_id", "stream_token"}
		if m.Method == Credit {
			allowed = append(allowed, "ack_seq")
			required = append(required, "ack_seq")
		}
		// request_id:null is legal; decodeParams deliberately rejects other nulls.
		err = decodeParams(m.Params, &p, allowed, required...)
		if !validID(p.RequestID) || len(p.RequestID) == 0 || !ValidStreamToken(p.StreamToken) || p.AckSeq > MaxSequence {
			return nil, bad
		}
		out = p
	default:
		return nil, wrong
	}
	if err != nil {
		return nil, bad
	}
	return out, nil
}

// ParseStreamResult requires the terminal identity and sequence to match all
// accepted chunks. EOF alone cannot establish successful completion.
func ParseStreamResult(raw []byte, expected Address, seq uint64) error {
	var r StreamResult
	if err := decodeParams(raw, &r, []string{"address", "final_seq"}, "address", "final_seq"); err != nil {
		return err
	}
	if err := validateAddressField(raw, true); err != nil {
		return err
	}
	if r.Address != expected || r.FinalSeq != seq || seq > MaxSequence {
		return fmt.Errorf("invalid stream terminal")
	}
	return nil
}

// StreamError adds terminal sequence evidence without modifying the input error.
func StreamError(e *Error, seq uint64) *Error {
	var data map[string]json.RawMessage
	_ = json.Unmarshal(e.Data, &data)
	if data == nil {
		data = map[string]json.RawMessage{}
	}
	data["final_seq"], _ = json.Marshal(seq)
	copy := *e
	copy.Data, _ = json.Marshal(data)
	return &copy
}
