# Actorx Remote

English | [中文](README.zh.md)

Remote connects cross-language Actor providers and consumers to a Go gateway over HTTP. The gateway uses dist/NATS internally; external programs need only its URL and API key.

The Python SDK provides `Runtime.register(type, factory)` and `Runtime.addr(type, id).call(message)`. Registration installs a factory; the first Call creates the business object on demand. Objects keep state between calls on the same generation.

Start with the [Ping/Pong example](examples/pingpong/README.md) for installation, process commands and expected output. The [Python SDK guide (Chinese)](sdk/python/README.zh.md) covers its API and exceptions.

| Guide | Contents |
| --- | --- |
| [Protocol](docs/protocol.md) | JSON-RPC methods, addresses, HTTP/SSE, authentication, errors and wire limits. |
| [Runtime and lifecycle](docs/runtime.md) | Activation, fixed generations, deadlines, concurrency, Go embedding and cleanup ownership. |

The current implementation supports Call over HTTP/SSE and a Python SDK installed from source. Send, Stream, WebSocket, automatic retry/reconnection and provider recovery are outside this implementation.
