# actorx remote Python SDK

English | [中文](README.zh.md)

The `actorx.remote/v1` HTTP Call/Stream SDK provides unified remote Actor registration, addressing and invocation. Requires Python 3.11+ and `httpx>=0.27,<0.29`. The package currently installs from local source and has not been publicly released.

## Install and run

From the `actorx-dist/` repository:

```sh
python3 -m venv /tmp/actorx-remote-demo-venv
/tmp/actorx-remote-demo-venv/bin/python -m pip install -e remote/sdk/python
/tmp/actorx-remote-demo-venv/bin/python -m unittest discover -s remote/sdk/python/tests
```

A successful unittest run prints `OK`. These tests use controlled HTTP transports and SSE byte streams without listening ports or NATS. For complete independent gateway/provider/consumer commands and expected output, see [Counter](../../examples/counter/README.md) for Call and [Chat](../../examples/chat/README.md) for Stream.

## One Actor, two operations

Import the public API from `actorx_remote`. An Actor may implement `handle`, `handle_stream`, or both. Register it by passing its class to `runtime.register`, as shown below. The following runs inside existing Runtimes; provider and consumer can be separate processes:

```python
from actorx_remote import Runtime

class ChatActor:
    def __init__(self):
        self.turns = 0

    async def handle(self, message):
        self.turns += 1
        return {"text": "complete answer", "turn": self.turns}

    async def handle_stream(self, message):
        self.turns += 1
        for text in ("first fragment", "second fragment"):
            yield {"text": text, "turn": self.turns}

# Provider
await runtime.register("demo.chat", ChatActor)

# Consumer
actor = runtime.addr("demo.chat", "session-42")
answer = await actor.call({"text": "hello"})
async with actor.stream({"text": "hello"}, timeout=30) as chunks:
    async for chunk in chunks:
        print(chunk["text"], chunk["turn"])
```

The first Call returns `turn: 1`; both subsequent Stream items carry `turn: 2`. Both operations reuse the same generation's object, lock and execution slot. Different IDs/generations create independent objects. Call waits for one JSON value; Stream consumes zero or more JSON items and an explicit terminal. The SDK does not switch operations based on return values.

Registration establishes a session without creating an Actor instance or probing its methods. On the first Call or Stream for a new ID, the SDK creates the corresponding object. It must have at least one callable `handle` / `handle_stream`; creation failure or neither method being callable produces error 1000.

The requested method is checked under the instance lock. `handle(message)` must return an awaitable producing one JSON value; `handle_stream(message)` must directly return an async iterator, including async generators and custom async iterators. Invalid return shapes, non-JSON output and business exceptions after invocation produce 1000.

## API and types

| API | Behavior |
| --- | --- |
| `Runtime(url, api_key=..., transport="http")` | URL is the gateway HTTP/HTTPS origin. Every POST includes the Bearer key and v1 protocol header; no redirects or retries. Supports `async with` |
| `await runtime.register(type, factory, description="")` | Registers an Actor; `factory` creates its objects. Returns when registration is ready, before instance creation. No `stream` argument |
| `runtime.addr(type, id)` | Creates a local `Addr` without network I/O |
| `await addr.call(message)` | Five-second total budget; saves the selected address from the first result or valid error, fixing subsequent operations to that generation |
| `addr.stream(message, timeout=5)` | Returns a stream context with a deadline in (0, 60] seconds; consume with `async with` and `async for` |
| `addr.address` | Independent address snapshot; no generation before selection |
| `await runtime.wait_closed()` | Waits for shutdown; provider stream failure closes Runtime and propagates to waiters |
| `await runtime.close()` | Idempotently closes SSE, cancels and joins SDK tasks, and releases objects; context exit calls it automatically |

For providers, the second registration parameter, `factory`, is a synchronous callable that takes no arguments and creates an Actor object. Pass a class with a no-argument constructor, as in `runtime.register("demo.chat", ChatActor)`, or a creation function or closure, such as `runtime.register("demo.configured", lambda: ChatActor())`. The SDK calls it only when an instance is needed, and it must return promptly. This is the Python SDK's object creation API; no factory is sent over the remote protocol.

Messages and results must be JSON data. Null and large integers are supported; fractions decode to `decimal.Decimal` to preserve precision. Encoding accepts finite Python float and Decimal values. Arbitrary Python objects are unsupported. Asynchronous handlers must cooperate with cancellation.

The same package exports `JSONValue`, `Actor`, `StreamActor`, `ActorFactory` and `Address`. `JSONValue` is recursive JSON data including Decimal. `Actor` / `StreamActor` are static Protocol typing aids describing `handle` returning `Awaitable[JSONValue]` and `handle_stream` returning `AsyncIterator[JSONValue]`. No inheritance is required; these types do not divide registrations or lifetimes. `ActorFactory` is a synchronous factory with no arguments; `Address` is a TypedDict with optional generation. The package includes `py.typed` for inline type checking.

Each registration holds at most eight complete-identity instances and eight execution slots. Handlers on the same instance run serially; different instances progress independently. Queued requests already consume slots, and the deadline covers queueing, execution and response POST. Expiry before handler entry produces 1005; missing terminal evidence after entry produces 1004. Expiry does not open a new response budget. Timeout settles the RPC, but locks and slots remain held until the handler actually exits. User code that ignores cancellation indefinitely also prevents close from finishing.

Stream ledgers have a separate limit of eight per registration, including finished streams awaiting ack. A full ledger set rejects new Streams with 1001/full; Call can still use available execution slots. Ack, the original deadline or registration shutdown releases retained ledgers.

## Directory queries

```python
report = await runtime.actors()
print(report["actors"])
```

Queries observe compatible Remote registrations across the distributed Runtime during a collection window, without a scope parameter. Types are deduplicated; descriptions are empty when sources disagree. Entries describe registered types, not running instances. Responses contain only `actors`. The finite observation window does not guarantee all nodes were observed. If core limits truncate a snapshot/report, the query fails with 1001/full instead of returning a partial directory.

Queries use at most five seconds, are canceled and joined on Runtime shutdown, return no partial directory on failure, and never retry. Complete responses are limited to 8 KiB; the Gateway rejects oversize responses. Cross-Gateway call ingress remains to be completed, so directory visibility currently does not guarantee that any ingress can Call/Stream that type. See the [directory protocol](../../docs/protocol.md#runtimeactors-list-registered-actor-types).

## Errors and fixed addresses

`RemoteError.code` is a string kind, `rpc_code` the integer RPC code, and `data` preserves valid extension data. Known kind/code mismatches are rejected; unknown codes use generic `RemoteError` with `code="unknown"`. Other exceptions include `AuthenticationError` (401), `SessionError` (403), `TransportError`, `ProtocolError`, `OutcomeUnknownError`, `ActivationUnknownError`, `DeadlineExceededError` and `UnsupportedOperationError`. Asyncio cancellation still raises `CancelledError`.

A missing or non-callable requested method raises `UnsupportedOperationError(RemoteError)` with `rpc_code == 1013` and `code == "unsupported_operation"`. It carries the selected complete address; Stream must have zero items and `final_seq: 0`. This proves the requested business method was not entered, although object construction may have occurred. The SDK saves the generation, allowing the same Addr to use a supported operation. For example, with the Counter provider running:

```python
from actorx_remote import UnsupportedOperationError

actor = runtime.addr("demo.counter", "orders-42")
try:
    async with actor.stream({"n": 5}) as chunks:
        async for chunk in chunks:
            print(chunk)
except UnsupportedOperationError as exc:
    print(exc.rpc_code, actor.address["generation"])

count = await actor.call({"n": 5})  # Same Addr and generation
```

Missing address evidence or a contradictory Stream terminal is treated as 1004. Error 1013 is distinct from instance creation or business failure 1000 and unknown JSON-RPC method -32601.

If the first Call or Stream may have been accepted without valid generation evidence, it reports an unknown outcome and leaves the handle uncertain. Later operations fail locally with `AddressUncertainError`. Obtaining a fresh handle is an explicit caller decision and does not deduplicate earlier work. Authentication/parameter rejection and definite connection-establishment failure do not contaminate the handle. Fixed addresses never change generation automatically on error. Error messages use fixed diagnostics without forwarding user stack traces or raw server diagnostics.

## Stream consumption and close

Creating a `Stream` validates the timeout and snapshots its JSON input. Entering `async with` or starting iteration sends one request and starts the total timeout budget, including the Addr lock wait. Repeated `__aiter__` calls do not send another request. Direct iteration is also supported when the caller explicitly owns `await chunks.aclose()`.

The SDK acknowledges an item after processing, when iteration requests the next one. The default total deadline is five seconds; `timeout` accepts (0, 60] seconds and chunks never extend it. Normal exhaustion requires a valid terminal and all items processed; empty streams also need an explicit terminal. A valid producer failure drains previously accepted items in order before raising the original remote exception. Cancellation, deadline expiry or invalid protocol evidence can abort that drain. A final ack failure or timeout preserves validated success or the original producer failure only after all local data has been delivered and iteration has requested the next item.

An early `break` must leave the context or explicitly `await chunks.aclose()`. Close discards pending items without acknowledging them, attempts cancellation within the original budget, and joins the reader and control POSTs, even if a terminal already arrived. Waiting and later iterations then raise `RemoteError` with `rpc_code == 1009`; successful exhaustion remains exhausted. Context exit preserves application exceptions and `CancelledError`. Runtime shutdown also cleans up SDK tasks, but cannot finish an independent application's loop body or claim its current item was consumed. Do not rely on garbage collection to end streams.

Cancellation preserves the shared Actor and does not prove external side effects have ended. Providers close their async iterator on success, failure and cancellation; instance locks and execution slots remain held until that cleanup actually exits. A generator that suppresses cancellation cannot send further chunks. Terminal POST completion releases no consumption credit: outstanding ledgers remain bounded until ack, the original deadline or registration shutdown.

A pure Call consumer opens no SSE connection; each Stream consumer opens its own SSE. Providers do not reconnect after disconnection. A Runtime may both register and call Actors. Send, WS and remote Call cancellation APIs are unavailable; Stream supports cancellation per request.

## Migration and reference

The gateway and SDK retain `actorx.remote/v1` during early development and must be used together. The SDK automatically sends the version header and validates registration results; authors do not configure protocol or capability switches. Remove `stream=True` and `capabilities` from earlier development code; the required version header remains v1. A former Call-only Type needs a new Type because Stream adds a binding; a former Stream-capable Type retains its contract when DTOs/bindings are unchanged. See [protocol and Type migration](../../docs/runtime.md#protocol-and-type-migration) for details and development storage handling.

- [Counter](../../examples/counter/README.md) and [Chat](../../examples/chat/README.md): independent-process examples.
- [Runtime and lifecycle](../../docs/runtime.md): addressing, concurrency and cleanup semantics.
- [Remote protocol](../../docs/protocol.md): Call/Stream exchanges, wire limits, the two credit segments and windows.

- [Go SDK](../go/README.md): interoperates with Python providers/consumers through the same gateway.
