# Python remote Actor streaming chat

English | [中文](README.zh.md)

A Python provider yields a simulated answer in six fragments; a separate consumer prints them as they arrive. The Go gateway routes through internal actorx-dist/NATS. Python programs need only its URL and a shared API key, with no model service or NATS client.

## Run

Requires Go 1.27, Python 3.11+ and a JetStream-enabled `nats-server`. Run from the repository root:

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
```

Generate a random API key with `secrets.token_urlsafe(32)` and inject the same `REMOTE_API_KEY` into the gateway, provider and consumer terminals through a trusted environment. Do not pass the key as a command-line argument.

Terminal 1, internal NATS:

```sh
nats-server -c examples/nats.conf -sd /tmp/actorx-remote-chat
```

Terminal 2, the general gateway shared with Counter:

```sh
GOWORK=off go run ./remote/examples/counter/gateway \
  -nats nats://127.0.0.1:4222 -listen 127.0.0.1:8080
```

Terminal 3, provider:

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/chat/provider.py
```

Terminal 4, consumer:

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/chat/consumer.py
```

Add `--consume-delay 0.1` to spend 100ms processing each item. The SDK acknowledges an item when iteration requests the next one. The consumer exits on completion; stop provider, gateway, then NATS with Ctrl-C.

## Expected output

The answer appears incrementally, followed by its item count:

```text
answer: Actors use messages to coordinate work.
chunks: 6
```

The provider prints `Chat actor created` and `Chat turn 1` on the first request. Running the consumer again with the same ID prints `Chat turn 2`, reusing that generation's object. `--id another` creates a separate instance.

## Implementation

[provider.py](provider.py) implements the `handle_stream(message)` async generator and uses `register(type, factory)`, just like Counter. Registration saves the factory; the first Stream creates the object. Six fragments exceed the four-item sending window. Slow consumption propagates backpressure through the gateway and actorx-dist.

[consumer.py](consumer.py) uses `async with actor.stream(...) as chunks` and `async for`. Exhaustion means an explicit terminal was received and all items were processed. Context exit after a break or exception, and Runtime shutdown, cancel unfinished streams. The default total deadline is five seconds; `timeout` accepts up to 60 seconds. This example uses 30 seconds.

This is a source-based local example without reconnection, retry or resume. Finishing a consumer does not stop its provider.

ChatActor only implements `handle_stream`. It can also implement `handle` for a single JSON reply; both operations then share one instance and run serially. Calling `.call(...)` on this Stream-only example raises `UnsupportedOperationError` (1013) with the selected address, after which the same Addr can still stream. See the [SDK guide](../../sdk/python/README.md) for an Actor with both methods.

The gateway and SDK use `actorx.remote/v1` together. An existing Stream-capable Type keeps its actorx-dist contract when its DTOs and bindings are unchanged; a former Call-only Type requires a new Type. See [protocol and Type migration](../../docs/runtime.md#protocol-and-type-migration).

## Links

- [Python SDK](../../sdk/python/README.md)
- [Remote protocol: Stream](../../docs/protocol.md#stream-exchange) and [runtime model](../../docs/runtime.md)
- [Call-based Counter](../counter/README.md)
