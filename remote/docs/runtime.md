# Remote runtime and lifecycle

English | [中文](runtime.zh.md)

Remote supplies cross-language access on top of actorxdist. The [protocol guide](protocol.md) defines the wire contract; the [Counter example](../examples/counter/README.md) gives runnable commands.

## Components and activation

```text
consumer SDK → HTTP Call/Stream → Gateway → actorx-dist ProxyActor
                                              ↓
provider object ← provider SDK ← registration SSE create/invoke/invoke_stream
                       ↓
                HTTP result/chunk POST → Gateway → consumer result/chunks
```

| Component | Responsibility |
| --- | --- |
| actorx | Mailbox, Deferred Reply, owned tasks and local lifecycle |
| actorx-dist | Type/ID addressing, generation selection, unique proxy ownership, Call and Stream |
| remote | Gateway, registrations, provider sessions, ProxyActor and response correlation |
| remote/http | Authentication, routing, JSON conversion, SSE and connection/write deadlines |
| Python SDK | Actor instance creation, serial handlers, address state and HTTP/SSE transport |

The gateway borrows one existing `*actorxdist.Runtime`. External programs do not need a NATS client or listener. HTTP/SSE is an access adapter, not another actorx Channel implementation. The SDK Runtime manages external connections and Python objects; actorxdist.Runtime manages proxy ownership. The SDK never claims ownership or generates actorx-dist generations.

The provider registers an Actor through the SDK and opens a registration SSE stream. The SDK's registration call returns after validating the result, before any business object is created. `addr(type, id)` only creates a local handle. Its first Call or Stream asks actorx-dist to find or activate a ProxyActor. During `Started`, the proxy obtains its complete Self address and waits for `actor/create` to succeed. Only then can `actor/invoke` or `actor/invoke_stream` run. An existing ready proxy skips creation.

The SDK keeps objects per registration and complete address (Type/ID/generation). Repeated create for that identity reuses the object and responds to the new RPC ID; different IDs or generations have separate objects. Registration does not create an object. In Python, the SDK calls the class or creation function supplied at registration only when an instance is needed; see the [SDK guide](../sdk/python/README.md#api-and-types) for the `factory` parameter contract. On the first create, the actual object must have at least one callable `handle` / `handle_stream`; otherwise creation fails with 1000.

Python registers all Actors through `runtime.register`, with no `stream` switch. Call/Stream share the actual object, instance lock and execution slot; the requested method is checked under that lock. `handle(message)` must return an awaitable producing one JSON value. `handle_stream(message)` must directly return an async iterator, including async generators and custom iterators. Invalid return shapes and business exceptions after method invocation produce 1000 without switching operations. `Actor` / `StreamActor` are static typing aids, not separate registrations or lifetimes. Handlers on the same instance run serially; different instances can progress independently.

## Addresses and errors

The SDK saves a valid selected address from `result.address` or `error.data.address` before returning a value or raising an RPC exception. A business failure can therefore fix an Addr's generation. Once fixed, the handle rejects a different generation and never silently retries against a new one.

A missing or non-callable requested method produces 1013 `UnsupportedOperationError` with a complete address; Stream requires a zero-item terminal with `final_seq: 0`. The SDK saves that generation, allowing the same Addr to use the other supported operation. This proves the requested method was not entered, although object construction may have occurred. Missing address evidence or a contradictory Stream terminal is treated as 1004 rather than a trusted 1013.

If a first Call or Stream may have been accepted but yields no valid selection evidence, the handle becomes uncertain. That operation reports uncertainty (or preserves caller cancellation); later calls fail locally with `AddressUncertainError`. Obtaining a fresh handle is an explicit caller decision and does not deduplicate earlier work. Authentication/parameter rejection and definite connection-establishment failure do not contaminate the handle.

Python `RemoteError.code` is a string kind; `rpc_code` is the integer wire code. Known kind/code mismatches are protocol errors. An unknown legal wire code uses the generic RemoteError with `code="unknown"`. HTTP authentication/session failures use `AuthenticationError`/`SessionError`; transport and malformed-response failures have separate exceptions. Full API details are in the [SDK guide](../sdk/python/README.md).

## Deadlines and concurrency

SDK Call has one five-second budget covering its handle lock, HTTP dispatch and response. It uses a monotonic clock locally and sends an absolute UTC `deadline_unix_ms`. The gateway uses the earliest of its five-second HTTP-entry budget, the supplied deadline and its parent context deadline. An already-expired request fails before dispatch. Later stages carry the remaining deadline; they do not start a new Call budget.

Creation belongs to actorx-dist's shared activation. Proxy `Started` caps its create wait at three seconds or the earlier lifecycle deadline. A single consumer's cancellation limits that caller's readiness wait; it does not directly cancel shared activation. Successful registration ends the five-second registration wait and leaves a long-lived SSE connection owned by Runtime. Cross-host clock assumptions have not been validated by the local example.

For provider invocation:

1. Instance-lock waiting, handler execution and response POST share the incoming deadline. Queued work consumes an execution slot immediately. Capacity rejection before execution is 1001.
2. Expiry before handler start is 1005 and that request must never run later. Definite cancellation before start is 1009.
3. Expiry/cancellation after start without a valid terminal result is 1004. The SDK requests task cancellation and settles the RPC, but holds the instance lock and slot until the handler actually exits.
4. A final response is submitted at most once and only within the original budget. There is no new budget or retry to report expiry. A handler returning after settlement only releases resources; it cannot send a second result.

SSE reading and response I/O progress independently of instance locks. An uncooperative handler can keep its instance occupied and prevent SDK close from finishing. Client return, a canceled await, and provider EOF are not evidence that external side effects stopped. Call has no remote cancel method; Stream cancellation targets only its request.

The Go proxy admits one active business operation (Call/Stream) per instance and returns full on overlap; the SDK serial lock also protects tasks that outlive a settled RPC. One Python Addr serializes its own calls, but independently obtained handles do not provide a shared client-side lock.

## Resource limits

| Resource | Current limit |
| --- | --- |
| Active Gateway operations (Call/Stream) | 8 across the Gateway |
| Active consumer streams | 8 across the Gateway |
| Outbound messages per registration | One bounded queue of 64, including credit/cancel reserve |
| Pending creates / invokes per registration | 8 each |
| Retained activation records per registration | 8 |
| Active proxy business operation (Call/Stream) per instance | 1 |
| Python objects / execution slots per registration | 8 each |

Slots include queued calls and handlers still running after timeout. RPC settlement alone cannot bypass capacity. Dist has its own [execution and transport limits](../../docs/runtime.md); its default instance capacity is 64 and payload limit is 64 KiB. These remain separate from remote's complete-message [wire limits](protocol.md). The current remote constants are not public tuning options.

Validated creation failures are retained by registration and complete address until cleanup and related outer Calls finish, so a later stop/cancel cannot erase a confirmed failure. Invocation dispatch/terminal evidence is retained by gateway request ID until that Call settles. These records preserve error classification; successful values still return through the normal actorx-dist Result DTO.

## Go embedding and ownership

Go code is part of the existing actorx-dist module. [Gateway](../gateway.go) coordinates an existing Runtime; [remotehttp.NewHandler](../http/handler.go) returns a `net/http.Handler` and does not start a server. The HTTP package import name is `remotehttp`.

| API | Ownership and completion |
| --- | --- |
| `remote.NewGateway(rt)` | Borrows Runtime; its creator remains responsible for closing it |
| `Gateway.Register(ctx, params)` | Context bounds installation, not the later provider connection lifetime |
| `Registration.Info()` | Immutable registration data, including the session token |
| `Registration.Events()` | One transport reader; closes when session admission ends |
| `Registration.Done()` | Closes after actorx-dist Unregister and all admitted outer Calls finish cleanup |
| `Registration.Close(ctx)` | Idempotently closes the session and waits for owned cleanup |
| `Gateway.Accept(ctx, token, response)` | Validates/correlates a provider response; does not run user handlers |
| `Gateway.Call(ctx, params)` | Uses actorx-dist Call; preserves selected address even on error |
| `Gateway.Actors()` | Independent snapshot of registered Actors on this Gateway; does not enumerate running instances |
| `Gateway.Close(ctx)` | Rejects new work and joins owned registrations; leaves borrowed Runtime open |
| `remotehttp.NewHandler(gateway, options)` | Requires `Options{APIKey: key}`; takes ownership of neither Gateway nor Runtime |

Register may return a non-nil handle with an error after installation began. The caller must still join that handle with Close. A close timeout ends only that wait; cleanup responsibility persists, and waiting can be repeated.

SSE requires ResponseController write deadlines and Flush. Middleware wrapping ResponseWriter should implement `Unwrap`. Set per-frame deadlines; a short server-wide WriteTimeout would terminate healthy registrations. See the [gateway entry point](../examples/counter/gateway/main.go) for assembly and independent cleanup contexts.

## Disconnect and shutdown

Canceling the provider HTTP request, a write failure or SSE EOF starts registration shutdown: withdraw the directory entry, reject new work, wake pending requests and Unregister owned proxies. The token becomes unusable; a same-Type local registration remains conflicting while cleanup is in progress. The SDK closes on provider stream failure and does not automatically re-register.

Close in this order, with independent bounded contexts that are not already canceled:

1. `Gateway.Close`: end registrations and provider SSE streams, and join admitted work.
2. `http.Server.Shutdown`: wait for HTTP handlers.
3. The creator's `actorxdist.Runtime.Close`: close the borrowed distributed runtime.

Python `Runtime.close()` closes connections, cancels and joins SDK-owned tasks, then releases object references. Canceling a close waiter does not discard cleanup responsibility. No persistent Python state, provider takeover, automatic replay or lease recovery is supplied. The static API key has no hot rotation or automatic expiry; revocation would also need to end existing registration connections.

The independent-process example demonstrates the local source chain, not released-package, cross-platform or production-recovery guarantees.

## Protocol and Type migration

The Actor directory uses `runtime/actors` and the result field `actors`; the Go API is `Gateway.Actors()`. The unavailable error is `actor_unavailable` (1002). Earlier development names have been removed, so directory callers and error handling must be updated together. This naming change does not alter actorx-dist DTOs or Type contracts.

During early development, the unified gateway and Python SDK retain `actorx.remote/v1`. Use matching gateway and SDK implementations. The SDK adds `X-Actorx-Remote-Protocol: actorx.remote/v1` to every POST and validates the registration result. After authentication, HTTP rejects missing or mismatched versions with 400 before dispatch. Remove the old `stream=True` registration argument and all `capabilities` fields. Implement `handle`, `handle_stream`, or both, and use `runtime.register` for every Actor.

The remote wire version and actorx-dist Type contract are separate. All registrations now have fixed Call, Stream and Started bindings. A former Call-only Type gains a binding and changes its fingerprint: on an existing Registry, choose a new Type such as `demo.counter.v2` for both provider and consumer. Restarting or Unregister does not erase a published contract. A former Stream-capable Type retains its fingerprint if DTOs and bindings are unchanged; renaming the Go proxy struct alone does not require a new Type. See the [contract reference](../../docs/contract.md).

Development examples may keep their business Type names on fresh, isolated NATS storage. Use a new development storage directory if an earlier run published a Call-only contract; do not clear a shared Registry to bypass a conflict. Upgrading the wire version alone does not resolve that conflict.

## Go bridge

All registrations use one `proxyActor` with fixed Call, Stream and Started bindings. `Started → actor/create` completes before either operation. External JSON-RPC envelopes remain outside actorx-dist's private wire; the bridge uses separate `Invoke → Result` and `InvokeStream → Chunk` DTOs. See [Type migration](#protocol-and-type-migration) for their contract implications.

Business content is JSON text in DTO string fields, marked `message_type="json"` and `content_type="application/json"`. A successful Call `Result.content` contains JSON, including the string `"null"`, and has no Failure. Provider failures use a non-nil `Result.Failure` and empty Content, completing actorx-dist Call with a nil Go error; framework failures use the Go error path. Stream uses its retained terminal evidence to preserve definite provider errors. Invalid marker/failure/content combinations are protocol failures.

DTO `Invoke.request_id` is internal gateway correlation, separate from provider RPC IDs and actorx-dist correlation; it provides no deduplication. DTO package identity and generated Bindings participate in Type fingerprints; the Go proxy struct name does not.

`Gateway.Stream(ctx, params, yield)` consumes actorx-dist Stream; returning from the callback acknowledges that item. HTTP uses `OpenStream` / `ConsumerStream.Run` and `Gateway.Control` to correlate consumer credits and cancellation. `AcceptEncoded` preserves actual provider message bytes for window accounting. Flow-control rules are defined once in the [protocol](protocol.md#flow-control-and-terminal-results); the [Chat example](../examples/chat/README.md) shows consumption.
