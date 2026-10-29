# actorx remote Go SDK

English | [中文](README.zh.md)

Build remote Actor providers and consumers over HTTP/SSE, with Call and Stream. The module is `github.com/go-aie/actorx-dist/remote/sdk/go`, the package is `actorxremote`, and it requires Go 1.27 with only standard library dependencies. Applications need a gateway origin and API key, without actorx, the actorx-dist root module or a NATS client.

## Run

Use source for now; released-package consumption has not been verified. [Counter](../../examples/counter/README.md) and [Chat](../../examples/chat/README.md) provide full independent-process commands with either language in each role. With a gateway and Go provider running, start a consumer from the repository root:

```sh
(cd remote/examples/counter/go && GOWORK=off go run ./consumer)
```

Inject the same `REMOTE_API_KEY` into each process through the environment. `--server` defaults to `http://127.0.0.1:8080`, and `--type` to `demo.counter`.

To use the current source in your own Go project:

```sh
go mod init example.com/my-remote-app
go mod edit -require=github.com/go-aie/actorx-dist/remote/sdk/go@v0.0.0
go mod edit -replace=github.com/go-aie/actorx-dist/remote/sdk/go=/absolute/path/to/actorx-dist/remote/sdk/go
```

Import the public entry point:

```go
import actorxremote "github.com/go-aie/actorx-dist/remote/sdk/go"
```

The SDK tests independently, without a gateway, NATS or listening sockets:

```sh
cd remote/sdk/go
GOWORK=off go test -race ./...
```

## Expected results

A fresh Counter consumer prints `5`, `10`, `5`; another run with the same IDs prints `15`, `20`, `10`. Chat prints six chunks forming `Actors use messages to coordinate work.` Chat calls and streams on the same Addr using successive turns. Both languages produce the same output.

## API and implementation

| API | Usage and semantics |
| --- | --- |
| `NewRuntime(origin, Options{APIKey: key})` | HTTP/HTTPS origin; local validation only; Runtime owns its HTTP transport |
| `rt.Register(ctx, actorType, factory, opts...)` | Register a provider; ctx bounds installation only, at most five seconds; optional WithDescription |
| `ActorFactory func() (any, error)` | Called at first create; return promptly with a non-nil Actor or StreamActor |
| `Actor.Handle(ctx, message) (any, error)` | Call handler returning one JSON value |
| `StreamActor.HandleStream(ctx, message, writer) error` | Write chunks with writer.Write; the SDK submits the terminal after return |
| `rt.Addr(type, id)` / `addr.Address()` | Local handle / independent address snapshot; generation starts empty |
| `addr.Call(ctx, message)` | Five-second total budget, including Addr queuing and HTTP; earlier context deadlines win |
| `addr.Stream(ctx, message, opts...)` | Establish and validate stream/opened; WithStreamTimeout(d) accepts `(0, 60s]`, default five seconds |
| `stream.Next()` / `Value()` / `Err()` | Single-consumer iteration; check Err after Next returns false; Value is generic JSON |
| `stream.Close()` | Idempotent; may interrupt Next concurrently; required on early break |
| `rt.WaitClosed(ctx)` | Wait for closure or provider disconnection, retaining the reason |
| `rt.Close(ctx)` | Reject new work, cancel and join tasks, close connections and release instances |

Ordinary structs implement one or both interfaces. Register a closure such as `func() (any, error) { return &ChatActor{}, nil }`. There is no SDK base class, capability declaration or separate registration for each operation. Call and Stream share the Type/ID/generation object and instance lock; different identities are independent. Registration does not construct objects.

Each registration has at most eight instances and eight execution slots, including queued work. Cancellation or RPC timeout cannot forcibly stop a Go goroutine: handlers must observe ctx, and retain locks and slots until they actually return. Factory, handler and serialization errors/panics use fixed diagnostics without original messages or stacks.

StreamWriter.Write inherits the handler context, accounts for the complete encoded chunk, and may wait on credit or a POST. Return immediately on error; ignored Write errors cannot become success. Use the writer sequentially inside the current handler, without concurrent or post-return writes. Returning nil completes successfully; there is no Complete method.

Inputs use encoding/json.Marshal: structs, JSON tags, pointers, maps, slices, json.Number, json.RawMessage and standard custom serialization are supported. Outputs and provider messages decode as nil, bool, string, json.Number, []any and map[string]any, preserving number precision without restoring Go types. The SDK validates encoded JSON for UTF-8, duplicate keys, depth and complete-message size. Business type conversion belongs to the application.

## Addresses and errors

Use errors.Is for classification and errors.As for *RemoteError (Code, RPCCode, Data) or *TransportError (Code, Status). Caller cancellation preserves context.Canceled. Other categories include ErrDeadlineExceeded, ErrOutcomeUnknown, ErrActivationUnknown, ErrAddressUncertain, ErrUnsupportedOperation, ErrStopped, ErrAuthentication, ErrSession, ErrProtocol and ErrTransport.

The first trustworthy success or error pins generation. A missing method returns 1013/unsupported_operation with a complete address; a Stream must have no items and final_seq=0. The SDK saves generation before returning the error, so a failed Counter Stream leaves the original Addr usable for Call. For example:

```go
if errors.Is(err, actorxremote.ErrUnsupportedOperation) {
    var detail *actorxremote.RemoteError
    if errors.As(err, &detail) {
        fmt.Println(detail.RPCCode, actor.Address().Generation)
    }
}
```

Possible admission without trustworthy generation evidence makes an Addr uncertain; later operations fail locally with ErrAddressUncertain. Creating another Addr does not deduplicate previous work. A pinned address never changes generation automatically. HTTP 200/204 cannot substitute for a business terminal, and requests are not retried automatically.

## Streams and shutdown

Stream validates stream/opened before returning and returns pre-SSE JSON RPC errors directly. stream/opened does not pin generation. Next delivers an item; the next call acknowledges it, including the call after processing the final item. Each segment has its own four-item/32 KiB window, counting queued and current items. Exhaustion requires an explicit terminal; EOF is not success. Partial failure does not undo consumed content. The total deadline includes queuing, connection, execution and consumption, without renewal per chunk.

Use Next/Value/Err from one consumer. Close may run concurrently with Next, cancels an unfinished stream and joins its reader. Always defer stream.Close so an early break also cleans up. A Stream owns its Addr operation slot until consumption ends or it closes; other operations on that Addr wait.

Use an independent cleanup context for Runtime.Close and preserve its error, for example `cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)`. Canceling a Close/WaitClosed waiting context ends only that wait: Runtime retains background cleanup and can be waited on again. User code ignoring cancellation may prevent complete shutdown.

Provider SSE failure closes the entire Runtime and WaitClosed reports the reason; there is no automatic reconnection. A Call-only consumer has no SSE, and each consumer Stream has a separate SSE. Send, WS, remote Call cancellation, retries and persistence/recovery are not provided.

## Links

- [Counter](../../examples/counter/README.md) / [Chat](../../examples/chat/README.md): complete commands and source.
- [Runtime model](../../docs/runtime.md) / [Protocol](../../docs/protocol.md) / [Python SDK](../python/README.md).
- [Development and releases](../../docs/development.md): vectors, module CI, interoperability and release-consumption boundaries.
