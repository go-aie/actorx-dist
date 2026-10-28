# Python remote Actor Ping/Pong

English | [中文](README.zh.md)

An independent Python provider registers `demo.ping`, and a consumer calls it over HTTP. The Go gateway uses internal actorx-dist/NATS services to activate proxies on demand. The provider and consumer need only the gateway URL and a shared API Key. Neither listens on a port or installs a NATS client.

## Run

Requires Go 1.27, Python 3.11+, and a `nats-server` with JetStream support. Run all commands below from the actorx-dist repository root.

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
```

Generate a random API Key with `secrets.token_urlsafe(32)` and set the same `REMOTE_API_KEY` through a trusted environment in the gateway, provider, and consumer terminals. The Key is not a command-line argument; the gateway refuses to start without it.

Terminal 1: start internal NATS with a separate development storage directory:

```sh
nats-server -c examples/nats.conf -sd /tmp/actorx-remote-pingpong
```

Terminal 2: start the gateway:

```sh
GOWORK=off go run ./remote/examples/pingpong/gateway \
  -nats nats://127.0.0.1:4222 -listen 127.0.0.1:8080
```

Terminal 3: register the provider:

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/pingpong/provider.py \
  --server http://127.0.0.1:8080 --type demo.ping
```

Terminal 4: call the same Addr twice, then call another ID:

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/pingpong/consumer.py \
  --server http://127.0.0.1:8080 --type demo.ping --id session-42 --count 2
```

The consumer exits when its calls finish. Use Ctrl-C to stop the remaining processes in this order: provider → gateway → NATS. Running the consumer again with the same ID continues incrementing the existing instance's counter.

## Expected output

The first consumer run prints three Pong results. Placeholders stand for random instance identifiers:

```json
{"text": "pong", "instance": "<A>", "calls": 1}
{"text": "pong", "instance": "<A>", "calls": 2}
{"text": "pong", "instance": "<B>", "calls": 1}
```

The first two calls reuse instance A for `session-42`. The third call uses `session-42-other`, creating a separate instance B. The provider prints `registered` first, then `created` and `invoke` when the first call arrives, and `closed` on exit.

## Implementation

[provider.py](provider.py) defines the stateful `PingActor.handle` and registers a factory with no arguments. The SDK manages object creation, serial execution, and replies. `instance` and `calls` are example business data that make object lifetimes visible.

[consumer.py](consumer.py) uses `Runtime.addr(...).call(...)` to call the same Addr repeatedly, then another ID. The instance identifiers and counters show object reuse and independent state. [gateway/main.go](gateway/main.go) reads the Key from the environment, assembles a shared actorxdist.Runtime and HTTP handler, and shuts down in this order: Gateway → HTTP Server → Runtime.

This example runs from local source and uses Call. Restart the provider to register again after a disconnect.

PingActor only implements `handle`. It uses the same `register(type, factory)` API as [Chat](../chat/README.md), which implements `handle_stream`. An Actor can implement both methods and share state through one Addr. Requesting a missing operation raises `UnsupportedOperationError` (1013) with the selected address; that Addr can still use the supported operation. See the [SDK guide](../../sdk/python/README.md) for both methods and error handling.

The gateway and SDK use `actorx.remote/v1` together. If this Type was previously registered with Call-only bindings, choose a new Type such as `demo.ping.v2` for both provider and consumer; see [protocol and Type migration](../../docs/runtime.md#protocol-and-type-migration).

## Links

- [Python SDK: installation, API, and error semantics](../../sdk/python/README.md)
- [Remote protocol](../../docs/protocol.md) and [runtime model and lifecycle](../../docs/runtime.md)
