# Remote Actor streaming Chat

English | [中文](README.zh.md)

A Go or Python provider replies in six fragments, and a consumer displays them incrementally. The same Actor supports Call to demonstrate shared session state. Both sides need only a gateway URL and API key. They connect over HTTP/SSE, without listening on a port or installing NATS clients.

## Run

Requires Go 1.27, Python 3.11+ for either Python role, and a JetStream-enabled `nats-server` for the gateway. Run these commands from the repository root. Each Go example is an independent module with only the standard library and SDK; a local replace selects SDK source.

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
```

Generate a random key with `secrets.token_urlsafe(32)` and inject the same `REMOTE_API_KEY` through a trusted environment into gateway, provider and consumer terminals. Keep keys out of command arguments.

Terminal 1, NATS with separate development storage:

```sh
nats-server -c examples/nats.conf -sd /tmp/actorx-remote-chat
```

Terminal 2, the shared gateway:

```sh
GOWORK=off go run ./remote/examples/counter/gateway \
  -nats nats://127.0.0.1:4222 -listen 127.0.0.1:8080
```

Terminal 3, choose one provider:

```sh
# Go
(cd remote/examples/chat/go && GOWORK=off go run ./provider)

# Python
/tmp/actorx-remote-demo-venv/bin/python remote/examples/chat/python/provider.py
```

Terminal 4, choose one consumer:

```sh
# Go
(cd remote/examples/chat/go && GOWORK=off go run ./consumer)

# Python
/tmp/actorx-remote-demo-venv/bin/python remote/examples/chat/python/consumer.py
```

Choose each role independently; Go and Python can be mixed.

Both roles accept `--server` and `--type`, defaulting to `http://127.0.0.1:8080` and `demo.chat`. Keep the Type identical on both sides; consumer `--id` selects the logical instance. Run one provider per Type at a time. Consumers exit when done; stop provider → gateway → NATS with Ctrl-C.

## Expected output

For a fresh ID:

```text
call turn: 1
answer: Actors use messages to coordinate work.
```

The consumer calls once, then streams an answer from the same Actor. The provider prints `Chat turn 1` and `Chat turn 2`; reusing the ID keeps advancing its turn.

## Implementation

[Go provider](go/provider/main.go) implements `Actor.Handle` and `StreamActor.HandleStream`, using `StreamWriter.Write`; [Python provider](python/provider.py) implements `handle` and the asynchronous generator `handle_stream`. Both methods share the turns field and instance lock. No model service is needed.

[Go consumer](go/consumer/main.go) uses `for chunk, err := range actor.Stream(ctx, message)` with automatic cleanup on loop exit; [Python consumer](python/consumer.py) uses an async context and async for. Both close the stream when finished.

This demonstrates local source use. The Go local replace does not prove released-package consumption; providers do not reconnect automatically. Use matching gateway and SDK `actorx.remote/v1` implementations. If an old Call-only Type contract exists, choose a new Type for both roles rather than clearing a shared Registry; see [Protocol and Type migration](../../docs/runtime.md#protocol-and-type-migration).

## Links

- [Go SDK](../../sdk/go/README.md) / [Python SDK](../../sdk/python/README.md)
- [Remote](../../README.md) / [Runtime model](../../docs/runtime.md)
