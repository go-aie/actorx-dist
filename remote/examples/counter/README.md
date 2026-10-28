# Python remote Actor Counter

English | [中文](README.zh.md)

An independent Python provider registers the `demo.counter` Actor. A consumer sends an increment over HTTP and receives the new total. The Go gateway uses internal actorx-dist/NATS to activate proxies on demand. The provider and consumer need only the gateway URL and a shared API Key. Neither listens on a port or installs a NATS client.

## Run

Requires Go 1.27, Python 3.11+, and a `nats-server` with JetStream support. Run all commands below from the actorx-dist repository root.

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
```

Generate a random API Key with `secrets.token_urlsafe(32)` and set the same `REMOTE_API_KEY` through a trusted environment in the gateway, provider, and consumer terminals. The Key is not a command-line argument; the gateway refuses to start without it.

Terminal 1: start internal NATS with a separate development storage directory:

```sh
nats-server -c examples/nats.conf -sd /tmp/actorx-remote-counter
```

Terminal 2: start the gateway:

```sh
GOWORK=off go run ./remote/examples/counter/gateway \
  -nats nats://127.0.0.1:4222 -listen 127.0.0.1:8080
```

Terminal 3: register the provider:

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/counter/provider.py \
  --server http://127.0.0.1:8080 --type demo.counter
```

Terminal 4: call the same Addr twice, then call another ID:

```sh
/tmp/actorx-remote-demo-venv/bin/python remote/examples/counter/consumer.py \
  --server http://127.0.0.1:8080 --type demo.counter --id orders-42 --count 2
```

The consumer exits when its calls finish. Use Ctrl-C to stop the remaining processes in this order: provider → gateway → NATS. Running the consumer again with the same ID continues incrementing the existing instance's counter.

## Expected output

The first consumer run prints three totals:

```text
5
10
5
```

Each Call sends `{"n": 5}`. The first two calls reuse the counter for `orders-42`; the third uses `orders-42-other`, which has independent state. Running the same consumer command again while the provider remains running prints `15`, `20`, `10`. The provider prints `registered` first, then `created` and `invoke` when the first call arrives, and `closed` on exit.

## Implementation

With the provider running, query the registered Actors through `runtime/actors`:

```sh
curl -sS http://127.0.0.1:8080/runtime/rpc \
  -H "Authorization: Bearer $REMOTE_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'X-Actorx-Remote-Protocol: actorx.remote/v1' \
  -d '{"jsonrpc":"2.0","id":"list:1","method":"runtime/actors","params":{}}'
```

`result.actors` contains an entry with `type: "demo.counter"`. This lists registrations, not running counter instances; different IDs do not add directory entries.

[provider.py](provider.py) defines `Counter.handle`, which adds the integer `n` to its `count` and returns the new total, and registers the Actor with `runtime.register(type, Counter)`. The SDK manages object creation, serial execution, and replies.

[consumer.py](consumer.py) uses `Runtime.addr(...).call(...)` to call the same Addr repeatedly, then another ID. The totals show state reuse and independence. `--count` controls the number of calls to the first ID; each call adds 5. [gateway/main.go](gateway/main.go) reads the Key from the environment, assembles a shared actorxdist.Runtime and HTTP handler, and shuts down in this order: Gateway → HTTP Server → Runtime.

This example runs from local source and uses Call. Restart the provider to register again after a disconnect.

Counter only implements `handle`. It uses the same `runtime.register` API as [Chat](../chat/README.md), which implements `handle_stream`. An Actor can implement both methods and share state through one Addr. Requesting a missing operation raises `UnsupportedOperationError` (1013) with the selected address; that Addr can still use the supported operation. See the [SDK guide](../../sdk/python/README.md) for both methods and error handling.

The gateway and SDK use `actorx.remote/v1` together. If this Type was previously registered with Call-only bindings, choose a new Type such as `demo.counter.v2` for both provider and consumer; see [protocol and Type migration](../../docs/runtime.md#protocol-and-type-migration).

## Links

- [Python SDK: installation, API, and error semantics](../../sdk/python/README.md)
- [Remote protocol](../../docs/protocol.md) and [runtime model and lifecycle](../../docs/runtime.md)
