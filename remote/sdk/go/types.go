package actorxremote

import "context"

// ActorFactory constructs an instance on its first create request, not during
// registration. It must return promptly. The result must implement Actor or
// StreamActor and must not be nil (including a typed nil).
type ActorFactory func() (any, error)

// Actor handles Calls. The SDK serializes methods on the same instance.
// Handlers should observe ctx; cancellation cannot forcibly stop Go code.
type Actor interface {
	Handle(ctx context.Context, message any) (any, error)
}

// StreamActor handles Streams on the same instance and lock as Actor.Handle.
// Returning nil completes the stream; returning an error fails it.
type StreamActor interface {
	HandleStream(ctx context.Context, message any, stream StreamWriter) error
}

// StreamWriter is the output interface accepted by StreamActor.
// Write may block on credit and inherits the request context. Return promptly
// after a Write error. Errors remain sticky even if the handler ignores them.
// Use it sequentially within HandleStream; it expires when the method returns.
type StreamWriter interface {
	Write(value any) error
}

// Options configures the runtime's owned HTTP transport.
type Options struct {
	APIKey string
}

// Address identifies an actor. Generation is empty until the gateway selects it.
// Address values are independent snapshots, safe for callers to modify.
type Address struct {
	Version    int    `json:"version"`
	Type       string `json:"type"`
	ID         string `json:"id"`
	Generation string `json:"generation,omitempty"`
}
