// Package protocol defines the actorx.remote/v1 Call and Stream contracts. Business JSON is
// preserved as raw text at the boundary and as strings in the stable actorx-dist DTOs.
package protocol

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/go-aie/actorx-dist/internal/identity"
	"github.com/go-aie/actorx-dist/internal/strictjson"
)

// ErrProviderWiring denotes a valid standard RPC error from a provider that was
// sent one of our known methods. It is a terminal protocol failure.
var ErrProviderWiring = errors.New("remote: provider RPC wiring failure")

const (
	Version = "actorx.remote/v1"
	JSONRPC = "2.0"

	MaxMessageBytes = 8 << 10
	MaxBatchBytes   = 64 << 10
	MaxBatchItems   = 8
	Capacity        = 8

	Register     = "runtime/register"
	Actors       = "runtime/actors"
	Call         = "actor/call"
	Create       = "actor/create"
	InvokeMethod = "actor/invoke"

	// Standard JSON-RPC errors describe envelope, method and parameter failures.
	ParseError     = -32700
	InvalidRequest = -32600
	MethodNotFound = -32601
	InvalidParams  = -32602
	InternalError  = -32603

	// Positive codes describe remote execution and lifecycle outcomes.
	RemoteError          = 1000
	Full                 = 1001
	ActorUnavailable     = 1002
	ActivationUnknown    = 1003
	OutcomeUnknown       = 1004
	DeadlineExceeded     = 1005
	Stopped              = 1006
	RegistrationConflict = 1007
	TransportMode        = 1008
	Canceled             = 1009
	OwnerUnavailable     = 1010
	Contract             = 1011
	ProtocolError        = 1012
	UnsupportedOperation = 1013
)

// These field types and package identity form the actorx-dist Call schema. Changing
// them or adding a binding requires a separately planned Type contract upgrade.
type Invoke struct {
	RequestID   string `json:"request_id"`
	MessageType string `json:"message_type"`
	ContentType string `json:"content_type"`
	Content     string `json:"content"`
}

// Result carries a business value or a normalized provider failure over actorx-dist.
// Provider failures use Failure with a nil Go error; framework errors stay separate.
type Result struct {
	MessageType string   `json:"message_type"`
	ContentType string   `json:"content_type"`
	Content     string   `json:"content"`
	Failure     *Failure `json:"failure,omitempty"`
}

type Failure struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func ValidateJSON(raw []byte) error {
	return strictjson.Validate(raw, MaxMessageBytes, 64, MaxMessageBytes)
}

func (v Invoke) Validate() error {
	if v.RequestID == "" || len(v.RequestID) > 128 || v.MessageType != "json" || v.ContentType != "application/json" {
		return fmt.Errorf("invalid Invoke markers")
	}
	return ValidateJSON([]byte(v.Content))
}

func (v Result) Validate() error {
	if v.MessageType != "json" || v.ContentType != "application/json" {
		return fmt.Errorf("invalid Result markers")
	}
	if v.Failure != nil {
		if v.Content != "" ||
			!failureCode(v.Failure.Code) ||
			!utf8.ValidString(v.Failure.Message) ||
			len(v.Failure.Message) > 512 {
			return fmt.Errorf("invalid Failure")
		}
		return nil
	}
	return ValidateJSON([]byte(v.Content))
}

func NewResult(value json.RawMessage, failure *Failure) Result {
	return Result{
		MessageType: "json",
		ContentType: "application/json",
		Content:     string(value),
		Failure:     failure,
	}
}

func failureCode(code int) bool {
	switch code {
	case RemoteError, Full, OutcomeUnknown, DeadlineExceeded, Canceled, UnsupportedOperation:
		return true
	}
	return false
}

// Address preserves the actorx-dist identity. An omitted generation is selected on
// first Call or Stream; a fixed address must never silently select another generation.
type Address struct {
	Version    int    `json:"version"`
	Type       string `json:"type"`
	ID         string `json:"id"`
	Generation string `json:"generation,omitempty"`
}

// Validate requires a generation when fixed is true.
func (a Address) Validate(fixed bool) error {
	if a.Version != 1 {
		return fmt.Errorf("invalid address version")
	}
	if err := identity.Validate(a.Type, a.ID); err != nil {
		return err
	}
	if a.Generation == "" {
		if fixed {
			return fmt.Errorf("generation required")
		}
		return nil
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(a.Generation)
	if err != nil || len(raw) != 16 || base64.RawURLEncoding.EncodeToString(raw) != a.Generation {
		return fmt.Errorf("invalid generation")
	}
	return nil
}

type RegisterParams struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

func (p RegisterParams) Validate() error {
	if err := identity.Validate(p.Type, "registration"); err != nil {
		return err
	}
	if !utf8.ValidString(p.Description) || len(p.Description) > 512 {
		return fmt.Errorf("invalid description")
	}
	return nil
}

type RegistrationInfo struct {
	Protocol     string `json:"protocol"`
	Type         string `json:"type"`
	SessionToken string `json:"session_token"`
}

// Actor describes a registered Actor in the directory, not a running instance.
type Actor struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	Protocol    string `json:"protocol"`
}

type ActorsResult struct {
	Actors []Actor `json:"actors"`
}

type CallParams struct {
	Address Address         `json:"address"`
	Input   json.RawMessage `json:"input"`

	// A pointer distinguishes an omitted deadline from an explicit zero deadline.
	DeadlineUnixMS *int64 `json:"deadline_unix_ms,omitempty"`
}

type CreateParams struct {
	Address        Address `json:"address"`
	DeadlineUnixMS int64   `json:"deadline_unix_ms"`
}

type InvokeParams struct {
	Address        Address         `json:"address"`
	Input          json.RawMessage `json:"input"`
	DeadlineUnixMS int64           `json:"deadline_unix_ms"`
}

type CreateResult struct {
	Address Address `json:"address"`
}

type CallResult struct {
	Address Address         `json:"address"`
	Value   json.RawMessage `json:"value"`
}

type ErrorData struct {
	Kind    string   `json:"kind"`
	Address *Address `json:"address,omitempty"`
}

type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("remote: %d %s", e.Code, e.Message) }

func Kind(code int) string {
	switch code {
	case ParseError:
		return "parse_error"
	case InvalidRequest:
		return "invalid_request"
	case MethodNotFound:
		return "method_not_found"
	case InvalidParams:
		return "invalid_params"
	case InternalError:
		return "internal_error"
	case RemoteError:
		return "remote_error"
	case Full:
		return "full"
	case ActorUnavailable:
		return "actor_unavailable"
	case ActivationUnknown:
		return "activation_unknown"
	case OutcomeUnknown:
		return "outcome_unknown"
	case DeadlineExceeded:
		return "deadline_exceeded"
	case Stopped:
		return "stopped"
	case RegistrationConflict:
		return "registration_conflict"
	case TransportMode:
		return "transport_mode"
	case Canceled:
		return "canceled"
	case OwnerUnavailable:
		return "owner_unavailable"
	case Contract:
		return "contract"
	case ProtocolError:
		return "protocol"
	case UnsupportedOperation:
		return "unsupported_operation"
	}
	return ""
}

func NewError(code int, message string, address *Address) *Error {
	e := &Error{Code: code, Message: SafeMessage(message)}
	if code > 0 {
		e.Data, _ = json.Marshal(ErrorData{Kind: Kind(code), Address: address})
	}
	return e
}

func SafeMessage(message string) string {
	message = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, message)
	if len(message) > 512 {
		message = message[:512]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	return message
}

// NormalizeProviderError validates known kind/code pairs. Standard RPC errors
// indicate protocol wiring failure; arbitrary application codes become 1000.
// Caller must validate any data.address against its pending identity first.
func NormalizeProviderError(e *Error) (*Failure, error) {
	if e == nil {
		return nil, fmt.Errorf("missing error")
	}
	if len(e.Data) > 0 && string(e.Data) != "null" {
		var fields map[string]json.RawMessage
		if json.Unmarshal(e.Data, &fields) == nil && fields != nil {
			if raw, ok := fields["kind"]; ok && Kind(e.Code) != "" {
				var kind string
				if json.Unmarshal(raw, &kind) != nil || kind != Kind(e.Code) {
					return nil, fmt.Errorf("kind/code mismatch")
				}
			}
		}
	}
	if e.Code >= -32768 && e.Code <= -32000 {
		return nil, ErrProviderWiring
	}
	code := e.Code
	if !failureCode(code) {
		code = RemoteError
	}
	return &Failure{Code: code, Message: SafeMessage(e.Message)}, nil
}
