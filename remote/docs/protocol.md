# Remote protocol

English | [中文](protocol.zh.md)

This document describes the implemented `actorx.remote/v1` Call contract. [Runtime and lifecycle](runtime.md) explains execution and cleanup. The wire structures and validation live in [protocol](../protocol/messages.go) and [rpc.go](../protocol/rpc.go); the HTTP binding lives in [handler.go](../http/handler.go).

## Envelope and data

Messages use JSON-RPC 2.0: requests contain `jsonrpc`, `method`, named-object `params` and `id`; responses contain the same `id` and exactly one of `result` or `error`, including when the result is null. Methods match their full slash-separated names; there are no dot aliases.

Gateway/SDK-generated IDs are nonempty strings. Parsers also accept numeric and null IDs and echo them without rounding. Only an absent `id` denotes a notification; `id:null` still requires a response. IDs correlate requests within one communication segment and provide no business deduplication.

JSON must be UTF-8, without duplicate keys or NaN/Infinity. Known envelope fields are validated; unknown envelope fields are ignored. Unknown method parameters are rejected with -32602; business `input`/`value` fields remain application-defined. Both fields are required where listed below, but may be null. Business JSON and numeric IDs retain precision at the boundary.

## Identity and methods

An address has the dist snapshot shape:

```json
{"version":1,"type":"demo.ping","id":"session-42","generation":"<dist generation>"}
```

The first `actor/call` may omit `generation`. Once selected, the SDK fixes that Addr to the returned generation. `actor/create` and `actor/invoke` require a complete address. A generation is a canonical unpadded base64url encoding of 16 bytes, assigned by actorxdist. Address version 1, JSON-RPC version 2.0 and `actorx.remote/v1` describe separate layers.

| Method and direction | Params | Successful result |
| --- | --- | --- |
| `runtime/register`, provider → gateway | `type`, optional `description` (default empty) | `protocol`, `type`, `session_token` |
| `runtime/services`, consumer → gateway | Empty object; params may be omitted | `services`: array of `type`/`description`/`protocol` |
| `actor/call`, consumer → gateway | `address`, `input`, optional `deadline_unix_ms` | `address`, `value` |
| `actor/create`, gateway → provider | Fixed `address`, `deadline_unix_ms` | Same `address` |
| `actor/invoke`, gateway → provider | Fixed `address`, `input`, `deadline_unix_ms` | Same `address`, `value` |

Deadlines are integer UTC Unix milliseconds. Omission and an explicit zero differ: zero is already expired. Registration confirms a factory is available; it does not create an object. The service list is a snapshot, not a guarantee of later availability. A Call result proves the invoked handler returned, not that additional background work finished.

Methods received in the wrong direction return -32601. Direction does not define credential roles: one Runtime may both register and call. Provider responses are correlated by registration and outbound RPC ID; successful addresses, and `error.data.address` when present, must match the pending complete address. Each result is consumed at most once. Bad or late responses cannot consume a legitimate pending request.

## HTTP and SSE

All traffic uses `POST /runtime/rpc` with these headers:

```http
Authorization: Bearer <api-key>
Content-Type: application/json
X-Actorx-Remote-Protocol: actorx.remote/v1
```

An absent protocol header selects v1; an unsupported explicit value returns HTTP 400 before dispatch. Ordinary calls accept `application/json`. Registration uses `Accept: text/event-stream, application/json` so both a successful stream and a JSON error can be read.

| Incoming document | Response |
| --- | --- |
| Single `runtime/register` request with ID and SSE accepted | HTTP 200 SSE; first frame is its RPC result, then only this registration's create/invoke requests |
| Call, services, or supported request batch | HTTP 200 JSON RPC result/error or response array |
| Notification or batch containing only notifications | HTTP 204 after dispatch; no RPC response or execution-completion ACK |
| Single provider response with session header | HTTP 204 after validation and handoff; no RPC response |

Registration without the single-request/SSE prerequisites creates no registration: a request with ID receives 1008; a notification receives no RPC response. Failure before stream establishment returns a normal HTTP 200 JSON RPC error. An unacceptable response format returns HTTP 406.

Each SSE frame is `data: <one complete single-line JSON-RPC message>` followed by a blank line, flushed immediately. SSE event names and IDs do not replace RPC method/ID. EOF ends the connection; it never proves RPC success. There are no resume cursors or automatic reconnections. The gateway uses the existing SSE stream and never opens an inbound connection to the provider.

Provider response POSTs include `X-Actorx-Remote-Session: <session_token>` as well as the API key. The token comes from the first registration result and is not a JSON-RPC envelope field. It is used only for provider responses, not ordinary consumer calls. A request/notification with this header returns 400; a response without it returns 403. Provider responses cannot be batched; the gateway also sends no batches to providers.

Request batches contain at most eight items, execute sequentially within one HTTP budget, and are not atomic. Empty batches and invalid elements follow JSON-RPC error rules; notifications produce no response entries. Registration in a batch never installs a service (1008 for an item with ID). A provider response embedded in a request batch rejects the whole document before dispatch. The Python SDK exposes no batch API.

## Call exchange

The following messages are illustrative; generation, token and deadline placeholders must be replaced with runtime values. Each line in a JSON block is a separate message.

Provider registers; the first SSE `data` message is its result:

```json
{"jsonrpc":"2.0","id":"p:1","method":"runtime/register","params":{"type":"demo.ping"}}
{"jsonrpc":"2.0","id":"p:1","result":{"protocol":"actorx.remote/v1","type":"demo.ping","session_token":"<token>"}}
```

Consumer calls without a generation. The gateway first creates the object over SSE, waits for a matching create result, then invokes it:

```json
{"jsonrpc":"2.0","id":"c:1","method":"actor/call","params":{"address":{"version":1,"type":"demo.ping","id":"session-42"},"input":{"text":"ping"}}}
{"jsonrpc":"2.0","id":"g:create-1","method":"actor/create","params":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"},"deadline_unix_ms":1890000000000}}
{"jsonrpc":"2.0","id":"g:create-1","result":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"}}}
{"jsonrpc":"2.0","id":"g:invoke-1","method":"actor/invoke","params":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"},"input":{"text":"ping"},"deadline_unix_ms":1890000000000}}
{"jsonrpc":"2.0","id":"g:invoke-1","result":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"},"value":{"text":"pong"}}}
{"jsonrpc":"2.0","id":"c:1","result":{"address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"},"value":{"text":"pong"}}}
```

Create/invoke responses travel by authenticated response POST, each acknowledged with 204. The final `c:1` response travels on the original consumer HTTP request. Consumer and provider RPC IDs are independent; a provider's 204 does not prove the consumer received its result. Subsequent calls on that Addr include the selected generation.

## Authentication and transport errors

Every POST authenticates before reading its body, looking up a session or doing Actor work. One shared static key grants service access, without consumer/provider roles, Type ACLs or tenant isolation. A session token associates responses with one registration; it is not another access credential or an idempotency key.

Missing keys return 401 with `WWW-Authenticate: Bearer realm="actorx-remote"`; invalid keys add `error="invalid_token"`. Duplicate or ambiguous Authorization headers are rejected. Key comparison is constant-time. Configure a key generated from at least 32 random bytes through a trusted environment; keep keys/tokens out of URLs and logs. Use HTTPS with certificate validation beyond the loopback demo, and protect the link after TLS termination. The SDK does not follow redirects.

| HTTP status | Transport rejection |
| --- | --- |
| 400 | Invalid/ambiguous protocol or session header; malformed provider response |
| 401 | Missing, invalid or ambiguous API key |
| 403 | Invalid/closed session, or response without session header |
| 405 | Method other than POST |
| 406 | Unacceptable response format |
| 409 | Unknown/settled provider RPC ID or mismatched address |
| 413 | Body, item count or encoded response limit exceeded |
| 415 | Content-Type is not application/json |

These rejections use HTTP status rather than RPC errors. Invalid JSON requests, invalid RPC envelopes and application failures use RPC errors. SSE setup can also fail with HTTP 500 if write deadlines are unavailable. Business failures remain HTTP 200; they do not become 502/504.

## RPC errors

Standard errors are -32700 `parse_error`, -32600 `invalid_request`, -32601 `method_not_found`, -32602 `invalid_params`, and -32603 `internal_error`. Application errors use integer positive codes:

| Code | `error.data.kind` | Meaning |
| --- | --- | --- |
| 1000 | `remote_error` | Provider factory/handler exception |
| 1001 | `full` | Definite capacity rejection |
| 1002 | `service_unavailable` | No registered service; business work not dispatched |
| 1003 | `activation_unknown` | Activation/generation selection is uncertain |
| 1004 | `outcome_unknown` | Work may have executed without a valid terminal result |
| 1005 | `deadline_exceeded` | Definite deadline failure |
| 1006 | `stopped` | Definite stopped/closed failure |
| 1007 | `registration_conflict` | Same Type already registered or closing locally |
| 1008 | `transport_mode` | Registration transport prerequisites not met |
| 1009 | `canceled` | Cancellation without an uncertainty classification |
| 1010 | `owner_unavailable` | dist owner unreachable; no automatic takeover |
| 1011 | `contract` | dist DTO/Type contract mismatch |
| 1012 | `protocol` | Internal protocol/wiring failure |

Errors echo the request ID, or null if it cannot be determined. Gateway application errors contain `data.kind` and, when known, `data.address`; address is never an extra top-level field:

```json
{"jsonrpc":"2.0","id":"c:1","error":{"code":1004,"message":"Provider outcome is unknown","data":{"kind":"outcome_unknown","address":{"version":1,"type":"demo.ping","id":"session-42","generation":"<generation>"}}}}
```

Validated provider errors 1000/1001/1004/1005/1009 retain their classification. Other legal application codes normalize to 1000; standard RPC errors from a provider for a known outbound method become 1012. Known kind/code pairs must agree. Language exception types, arbitrary provider data and stack traces are not forwarded through actorxdist. Error messages are safe diagnostics bounded to 512 UTF-8 bytes.

Confirmed provider failure evidence takes priority over a later stop/cancel. Without a definite terminal result, uncertainty takes priority: outcome_unknown, then activation_unknown, then specific framework errors. A timeout or closed connection alone cannot prove the handler did not execute. No error triggers transparent retries; see [address and deadline rules](runtime.md).

## Wire limits and compatibility

Request bodies and individual encoded messages are limited to 8 KiB, including envelopes. A batch still has an 8 KiB inbound limit, at most eight items and a 64 KiB encoded response limit. JSON nesting is limited to 64 levels. SSE frames have a five-second write deadline; a registration stream has no five-second lifetime limit.

The external JSON-RPC envelope does not enter dist's private wire protocol. Business data uses the stable [Invoke/Result/Failure DTOs](../protocol/messages.go), with `message_type="json"` and `content_type="application/json"`. `Invoke.content` is JSON text; successful `Result.content` is JSON text (including the string `"null"`), with no Failure. A provider failure uses a non-nil Failure and empty Content, completed over dist with a nil Go error. Framework failures use the Go error path. Invalid marker/failure/content combinations are protocol failures.

`Invoke.request_id` is gateway-generated internal correlation, separate from provider RPC IDs and dist's private correlation. It provides no deduplication. The DTO package identity and generated Bindings participate in the dist Type contract; changes require a planned contract upgrade. See [resource limits](runtime.md) for execution capacity. Stream, Send, WebSocket and remote cancellation are not implemented by this contract.
