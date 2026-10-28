package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// MaxSequence is the largest integer represented exactly by all protocol peers.
const MaxSequence uint64 = 1<<53 - 1
const WindowItems = 4
const WindowBytes = 32768

type Window struct {
	Items int `json:"items"`
	Bytes int `json:"bytes"`
}

func StreamWindow() Window { return Window{WindowItems, WindowBytes} }

func Token(raw json.RawMessage) (string, error) {
	var token string
	if json.Unmarshal(raw, &token) != nil {
		return "", fmt.Errorf("invalid stream token")
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(b) != 32 || base64.RawURLEncoding.EncodeToString(b) != token {
		return "", fmt.Errorf("invalid stream token")
	}
	return token, nil
}

func CheckWindow(raw json.RawMessage) error {
	f, err := Fields(raw, []string{"items", "bytes"}, "items", "bytes")
	var items, size int
	if err != nil || json.Unmarshal(f["items"], &items) != nil || json.Unmarshal(f["bytes"], &size) != nil || items != WindowItems || size != WindowBytes {
		return fmt.Errorf("invalid stream window")
	}
	return nil
}

func Sequence(raw json.RawMessage) (uint64, error) {
	var n uint64
	if len(raw) == 0 || string(raw) == "null" || json.Unmarshal(raw, &n) != nil || n > MaxSequence {
		return 0, fmt.Errorf("invalid sequence")
	}
	return n, nil
}

// IDKey preserves ID types and string escapes without float conversion.
func IDKey(raw json.RawMessage) (string, error) {
	v, err := Decode(raw)
	if err != nil {
		return "", err
	}
	switch v := v.(type) {
	case nil:
		return "null", nil
	case string:
		return "s:" + v, nil
	case json.Number:
		return "n:" + v.String(), nil
	}
	return "", fmt.Errorf("invalid ID")
}

func SameID(a, b json.RawMessage) bool {
	x, e := IDKey(a)
	y, f := IDKey(b)
	return e == nil && f == nil && x == y
}

func Notification(method string, params any) ([]byte, error) {
	p, err := Encode(params)
	if err != nil {
		return nil, err
	}
	return Encode(Message{JSONRPC: "2.0", Method: method, Params: p})
}

func Control(m Message) (json.RawMessage, string, uint64, error) {
	bad := fmt.Errorf("invalid stream control")
	if !m.Request || m.ID != nil || (m.Method != "actor/credit" && m.Method != "actor/cancel") {
		return nil, "", 0, bad
	}
	names := []string{"request_id", "stream_token"}
	if m.Method == "actor/credit" {
		names = append(names, "ack_seq")
	}
	f, err := Fields(m.Params, names, names...)
	if err != nil {
		return nil, "", 0, bad
	}
	if _, err := IDKey(f["request_id"]); err != nil {
		return nil, "", 0, bad
	}
	token, err := Token(f["stream_token"])
	if err != nil {
		return nil, "", 0, bad
	}
	var seq uint64
	if m.Method == "actor/credit" {
		seq, err = Sequence(f["ack_seq"])
	}
	return bytes.Clone(f["request_id"]), token, seq, err
}
