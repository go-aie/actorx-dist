package actorxremote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/go-aie/actorx-dist/remote/sdk/go/internal/wire"
)

type category string

func (e category) Error() string { return "actorxremote: " + string(e) }

// Error categories support errors.Is. Details are available through errors.As
// with *RemoteError or *TransportError. Remote diagnostics are never echoed.
var (
	// ErrStreamUsed is a local usage error; it has no wire RPC code.
	ErrStreamUsed           error = category("stream_used")
	ErrUnsupportedOperation error = category("unsupported_operation")
	ErrOutcomeUnknown       error = category("outcome_unknown")
	ErrActivationUnknown    error = category("activation_unknown")
	ErrDeadlineExceeded     error = category("deadline_exceeded")
	ErrAddressUncertain     error = category("address_uncertain")
	ErrStopped              error = category("stopped")
	ErrTransport            error = category("transport")
	ErrAuthentication       error = category("authentication")
	ErrSession              error = category("session")
	ErrProtocol             error = category("protocol")
)

// RemoteError preserves an RPC error's code and raw extension data. Data may be
// absent or JSON null. Error reports only a controlled classification, never
// the peer's message. Local address_uncertain has RPCCode zero and no Data.
type RemoteError struct {
	Code    string
	RPCCode int
	Data    json.RawMessage
	cause   error
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("actorxremote: %s (rpc %d)", e.Code, e.RPCCode)
}

func (e *RemoteError) Unwrap() error { return e.cause }

func (e *RemoteError) Is(target error) bool {
	c, ok := target.(category)
	return ok && string(c) == e.Code
}

// TransportError describes an HTTP, authentication, session or protocol failure.
// Status is the HTTP status, or zero when no HTTP rejection was received.
type TransportError struct {
	Code   string
	Status int
	cause  error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("actorxremote: %s (http %d)", e.Code, e.Status)
}

func (e *TransportError) Unwrap() error { return e.cause }

func (e *TransportError) Is(target error) bool {
	c, ok := target.(category)
	return ok && (string(c) == e.Code || c == category("transport"))
}

func protocolError() error { return &TransportError{Code: "protocol"} }

// Preserve safe sentinel causes without retaining url.Error, network endpoints
// or arbitrary diagnostics from a body/transport implementation.
func transportError(err error) *TransportError {
	e := &TransportError{Code: "transport"}
	for _, safe := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		io.ErrUnexpectedEOF,
		io.EOF,
		net.ErrClosed,
		errConnection,
	} {
		if errors.Is(err, safe) {
			e.cause = safe
			break
		}
	}
	return e
}

func remoteError(code int, cause error) *RemoteError {
	return &RemoteError{Code: wire.Kind(code), RPCCode: code, cause: cause}
}
