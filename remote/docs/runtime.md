# Remote runtime and lifecycle

English | [中文](runtime.zh.md)

Remote supplies cross-language access on top of actorxdist. The [protocol guide](protocol.md) defines the wire contract; the [Ping/Pong example](../examples/pingpong/README.md) gives runnable commands.

## Components and activation

```text
consumer SDK → HTTP actor/call → Gateway → dist ProxyActor
                                              ↓
provider object ← provider SDK ← registration SSE create/invoke
                       ↓
                HTTP response POST → Gateway → original Call result
```

| Component | Responsibility |
| --- | --- |
| actorx | Mailbox, Deferred Reply, owned tasks and local lifecycle |
| dist | Type/ID addressing, generation selection, unique proxy ownership and Call |
| remote | Gateway, registrations, provider sessions, ProxyActor and response correlation |
| remote/http | Authentication, routing, JSON conversion, SSE and connection/write deadlines |
| Python SDK | Factories, business objects, serial handlers, address state and HTTP/SSE transport |

The gateway borrows one existing `*actorxdist.Runtime`. External programs do not need a NATS client or listener. HTTP/SSE is an access adapter, not another actorx Channel implementation. The SDK Runtime manages external connections and Python objects; actorxdist.Runtime manages proxy ownership. The SDK never claims ownership or generates dist generations.

`register(type, factory)` installs creation capability and opens a provider SSE stream. It returns after validating the registration result, before any business object is created. `addr(type, id)` only creates a local handle. Its first Call asks dist to find or activate a ProxyActor. During `Started`, the proxy obtains its complete Self address and waits for `actor/create` to succeed. Only then can `actor/invoke` run. An existing ready proxy skips creation.

The SDK keeps objects per registration and complete address (Type/ID/generation). Repeated create for that identity reuses the object and responds to the new RPC ID; different IDs or generations have separate objects. Factories are short synchronous no-argument functions returning objects with `async def handle(message)`. Handlers on the same instance run serially; different instances can progress independently.

## Addresses and errors

The SDK saves a valid selected address from `result.address` or `error.data.address` before returning a value or raising an RPC exception. A business failure can therefore fix an Addr's generation. Once fixed, the handle rejects a different generation and never silently retries against a new one.

If a first Call may have been accepted but yields no valid selection evidence, the handle becomes uncertain. That call reports uncertainty (or preserves caller cancellation); later calls fail locally with `AddressUncertainError`. Obtaining a fresh handle is an explicit caller decision and does not deduplicate earlier work. Authentication/parameter rejection and definite connection-establishment failure do not contaminate the handle.

Python `RemoteError.code` is a string kind; `rpc_code` is the integer wire code. Known kind/code mismatches are protocol errors. An unknown legal wire code uses the generic RemoteError with `code="unknown"`. HTTP authentication/session failures use `AuthenticationError`/`SessionError`; transport and malformed-response failures have separate exceptions. Full API details are in the [SDK guide (Chinese)](../sdk/python/README.zh.md).

## Deadlines and concurrency

SDK Call has one five-second budget covering its handle lock, HTTP dispatch and response. It uses a monotonic clock locally and sends an absolute UTC `deadline_unix_ms`. The gateway uses the earliest of its five-second HTTP-entry budget, the supplied deadline and its parent context deadline. An already-expired request fails before dispatch. Later stages carry the remaining deadline; they do not start a new Call budget.

Creation belongs to dist's shared activation. Proxy `Started` caps its create wait at three seconds or the earlier lifecycle deadline. A single consumer's cancellation limits that caller's readiness wait; it does not directly cancel shared activation. Successful registration ends the five-second registration wait and leaves a long-lived SSE connection owned by Runtime. Cross-host clock assumptions have not been validated by the local example.

For provider invocation:

1. Instance-lock waiting, handler execution and response POST share the incoming deadline. Queued work consumes an execution slot immediately. Capacity rejection before execution is 1001.
2. Expiry before handler start is 1005 and that request must never run later. Definite cancellation before start is 1009.
3. Expiry/cancellation after start without a valid terminal result is 1004. The SDK requests task cancellation and settles the RPC, but holds the instance lock and slot until the handler actually exits.
4. A final response is submitted at most once and only within the original budget. There is no new budget or retry to report expiry. A handler returning after settlement only releases resources; it cannot send a second result.

SSE reading and response I/O progress independently of instance locks. An uncooperative handler can keep its instance occupied and prevent SDK close from finishing. Client return, a canceled await, and provider EOF are not evidence that external side effects stopped. There is no remote cancel method.

The Go proxy admits one active business Call per instance and returns full on overlap; the SDK serial lock also protects tasks that outlive a settled RPC. One Python Addr serializes its own calls, but independently obtained handles do not provide a shared client-side lock.

## Resource limits

| Resource | Current limit |
| --- | --- |
| Active Gateway Calls | 8 across the Gateway |
| Outbound requests per registration | Queue of 8 |
| Pending creates / invokes per registration | 8 each |
| Retained activation records per registration | 8 |
| Active proxy business Call per instance | 1 |
| Python objects / execution slots per registration | 8 each |

Slots include queued calls and handlers still running after timeout. RPC settlement alone cannot bypass capacity. Dist has its own [execution and transport limits](../../docs/runtime.md); its default instance capacity is 64 and payload limit is 64 KiB. These remain separate from remote's complete-message [wire limits](protocol.md). The current remote constants are not public tuning options.

Validated creation failures are retained by registration and complete address until cleanup and related outer Calls finish, so a later stop/cancel cannot erase a confirmed failure. Invocation dispatch/terminal evidence is retained by gateway request ID until that Call settles. These records preserve error classification; successful values still return through the normal dist Result DTO.

## Go embedding and ownership

Go code is part of the existing dist module. [Gateway](../gateway.go) coordinates an existing Runtime; [remotehttp.NewHandler](../http/handler.go) returns a `net/http.Handler` and does not start a server. The HTTP package import name is `remotehttp`.

| API | Ownership and completion |
| --- | --- |
| `remote.NewGateway(rt)` | Borrows Runtime; its creator remains responsible for closing it |
| `Gateway.Register(ctx, params)` | Context bounds installation, not the later provider connection lifetime |
| `Registration.Info()` | Immutable registration data, including the session token |
| `Registration.Events()` | One transport reader; closes when session admission ends |
| `Registration.Done()` | Closes after dist Unregister and all admitted outer Calls finish cleanup |
| `Registration.Close(ctx)` | Idempotently closes the session and waits for owned cleanup |
| `Gateway.Accept(ctx, token, response)` | Validates/correlates a provider response; does not run user handlers |
| `Gateway.Call(ctx, params)` | Uses dist Call; preserves selected address even on error |
| `Gateway.Services()` | Independently allocated snapshot of ready local registrations |
| `Gateway.Close(ctx)` | Rejects new work and joins owned registrations; leaves borrowed Runtime open |
| `remotehttp.NewHandler(gateway, options)` | Requires `Options{APIKey: key}`; takes ownership of neither Gateway nor Runtime |

Register may return a non-nil handle with an error after installation began. The caller must still join that handle with Close. A close timeout ends only that wait; cleanup responsibility persists, and waiting can be repeated.

SSE requires ResponseController write deadlines and Flush. Middleware wrapping ResponseWriter should implement `Unwrap`. Set per-frame deadlines; a short server-wide WriteTimeout would terminate healthy registrations. See the [gateway entry point](../examples/pingpong/gateway/main.go) for assembly and independent cleanup contexts.

## Disconnect and shutdown

Canceling the provider HTTP request, a write failure or SSE EOF starts registration shutdown: withdraw the directory entry, reject new work, wake pending requests and Unregister owned proxies. The token becomes unusable; a same-Type local registration remains conflicting while cleanup is in progress. The SDK closes on provider stream failure and does not automatically re-register.

Close in this order, with independent bounded contexts that are not already canceled:

1. `Gateway.Close`: end registrations and provider SSE streams, and join admitted work.
2. `http.Server.Shutdown`: wait for HTTP handlers.
3. The creator's `actorxdist.Runtime.Close`: close the borrowed distributed runtime.

Python `Runtime.close()` closes connections, cancels and joins SDK-owned tasks, then releases object references. Canceling a close waiter does not discard cleanup responsibility. No persistent Python state, provider takeover, automatic replay or lease recovery is supplied. The static API key has no hot rotation or automatic expiry; revocation would also need to end existing registration connections.

The independent-process example demonstrates the local source chain, not released-package, cross-platform or production-recovery guarantees.
