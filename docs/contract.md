# Wire contract and resource limits

English | [中文](contract.zh.md)

This reference defines current v1 identities, schemas, envelopes, errors and resource limits. See [call semantics](runtime.md) for completion and cancellation. Values form a locally calibrated serialization/development profile, not measured production capacity.

## Names and canonical identity

Type is 1–64 ASCII bytes matching `[a-z][a-z0-9]*([.-][a-z0-9]+)*`. ID is 1–128 valid UTF-8 bytes with no Unicode control characters. Whitespace, dots, slash and wildcard characters in IDs are data: they never become raw subject segments. No trimming, case folding or Unicode normalization occurs; canonically equivalent Unicode strings remain distinct IDs. Omitted ID, generation, factory-token, correlation and operation IDs use 16 crypto/rand bytes encoded as unpadded base64url (22 characters); explicit empty ID is invalid.

Type/ID segments use canonical RFC 4648 raw base64url of the original bytes. Strict decode rejects padding, ignored newlines and nonzero pad bits; decode/re-encode must match. `counter` → `Y291bnRlcg`, `orders-42` → `b3JkZXJzLTQy`. Opaque random tokens are already encoded, not encoded twice. KV keys are `t.<type-token>`, `a.<type-token>.<id-token>` and `e.<generation>` in the separate buckets. Errors during validation are ErrSetup. Tokens identify routes, never authority.

Subjects are `actorxdist.default.factory.<type-token>`, `actorxdist.default.factory.<type-token>.<factory-token>.control.confirm`, `.control.stop` and `actorxdist.default.actor.<type-token>.<id-token>`. The factory queue group is `actorxdist.default.factory.<type-token>.workers`, not a publish destination. One ordinary `control.*` subscription per Factory spans all its generations and FAILED records, without a queue group. It is installed/flushed before any STARTING claim and removed only after all owned work settles. Business subscriptions are per active instance, without a queue group. Confirm/stop permissions are separate; neither falls back to a shared queue.

These names use the default namespace, `default`. `WithNamespace` configures message routes, queue groups and KV buckets together. See the [NATS reference](nats.md) for complete names and configuration.

## Schema subset and fingerprint

| Go shape | v1 wire rule |
| --- | --- |
| bool, string; named forms | exact type identity; valid UTF-8 strings |
| int/uint | portable 32-bit signed/unsigned range; retain identity distinct from int32/uint32 |
| int8/16/32/64, uint8/16/32/64 | exact range; integer decimal syntax, no fraction/exponent; no float64 intermediate |
| float32/64 | finite values only; typed overflow rejected; no NaN/Inf |
| pointers, slices, arrays, map[string]T | structural element identities; pointers/slices/maps nullable; []byte uses strict padded standard base64, arrays retain length |
| struct | exported non-embedded fields; exact JSON names; missing non-omitempty fields rejected; zero value for omitted optional fields |
| JSON tags | empty/default field name, explicit name and optional omitempty; `-` excludes field; duplicate names, embedding, unknown options and `,string` rejected |
| recursive named types | definitions plus references; repeated schema IDs do not expand forever; cyclic values rejected |
| actorxdist.Addr | built-in `addr-v1` node; strict version/type/id/optional nonempty generation; decoder associates receiving Runtime at any nesting depth |
| nil | allowed only for nullable nodes; an error-only result is distinct from a nullable typed result |
| interfaces, func, chan, complex, unsafe.Pointer, uintptr | rejected, including nested forms; Call[any] only for the core's no-result descriptor, returning nil |
| custom JSON/Text marshalers/unmarshalers, time.Time, anonymous fields, instantiated generic named types | rejected in v1; no invocation of user codecs; use explicit DTOs |
| actorx.Addr, either Context/Runtime, Reply/StreamReply and execution capabilities | rejected before generic struct handling, including pointers and nested fields |

Aliases use Go's reflected underlying identity. Named IDs are `named(<JSON-quoted package path>,<JSON-quoted name>)`; builtins use their Go name. Composite IDs are `ptr(ID)`, `slice(ID)`, `array(N,ID)`, `map(string,ID)`. Anonymous struct IDs are `struct(<canonical fields JSON>)`, each field `[jsonName,typeID,omitemptyBool]` sorted by UTF-8 name bytes. Named identities include package path and name even when schemas match. Maximum identity length is 1024 bytes; larger identities are rejected.

Canonical schema nodes are JSON arrays: `["scalar",goKind]`, `["ptr",elementID]`, `["slice",elementID]`, `["array",N,elementID]`, `["map",elementID]`, `["struct",fields]`, `["addr-v1"]`. Each definition is `[typeID,node]`; a named definition records its underlying node under its named ID. Definitions contain references by typeID, not reflect pointers. For []byte use `["bytes"]`. No-result uses JSON null, with no definition.

Fingerprint input is the compact UTF-8 JSON array `[1,handlers,definitions]`. A handler is `[messageTypeID,resultTypeID-or-null,modes]`; modes are `['send','call']` or `['stream']` (JSON double quotes in actual bytes). Sort handlers by messageTypeID, definitions by typeID and struct fields by JSON name, all bytewise. Array/scalar ordering is fixed; use encoding/json string escaping with HTML escaping enabled, no whitespace or terminal newline. SHA-256, lowercase 64 hex digits, covers exactly these bytes. Lifecycle bindings, functions, instance values and constructor addresses never enter it. Registration and live binding snapshots must match completely. One Type has one full immutable contract; even adding a handler requires a new Type. Contract lookup reads a known Type; Runtime.Discover separately observes ready registrations without proving capacity or future availability.

Codec must reject duplicate/unknown JSON fields, invalid UTF-8, trailing values, invalid nulls, incorrect types and numbers, value cycles, depth >32, value nodes >8192, >256 schema definitions or >64 ordinary handlers. Contract bytes ≤64 KiB. Encoding uses a limited writer and bounded traversal, not unbounded Marshal followed by a size check. Request payload/chunk ≤64 KiB encoded; outer frame ≤128 KiB. Schema traversal, codec temporary buffers and nested Addr decoding are included in resource reservations.

Sender checks msg, mode and expected R/C against the published full contract before sending; receiver verifies fingerprint, key/generation, message/schema and mode before mailbox admission. The internal Call[any]/Stream[any] bridge cannot bypass either check. Addr JSON export snapshots one generation under its state lock; default UnmarshalJSON returns ErrSetup, Runtime.DecodeAddr is strict and does no I/O. Send/Call/Stream enforce these checks.

## Internal discovery transport

Discovery uses separate v1 JSON frames, not the business envelope or single-response Call correlation table. A query (at most 1 KiB) requires `version=1`, `query_id`, `origin_runtime_id` and positive `deadline_unix_ms` (Unix milliseconds). Both IDs follow the 22-character opaque identity rule. NATS Reply must exactly equal `actorxdist.<namespace>.discovery.v1.reply.<origin_runtime_id>`; the broadcast subject is `actorxdist.<namespace>.discovery.v1.query`.

A reply requires `version=1`, `query_id` and `runtime_id`; optional `entries` contains `{type,fingerprint,description?}`, and optional `truncated` marks an omitted suffix. Description is valid UTF-8, at most 512 bytes; Type and fingerprint follow the rules above, and duplicate Types are rejected. An empty directory is a successful reply with no entries. Optional `error` permits only `full` or `unavailable`, with neither entries nor truncated present. Strict decoding rejects duplicate/unknown fields, nulls, incorrect types, invalid identities and oversized frames. One reply contains at most 256 entries and **64 KiB for the complete encoded frame**, including its QueryID; encoding retains a prefix and sets truncated when needed.

Query callbacks use shared ordinary dispatch and retained-payload slots; snapshot Calls use the local Discovery Actor's Task budget. Reply callbacks reserve payload/codec slots and deliver typed messages to that Actor. Lost local replies, including SDK slow-consumer loss, advance one shared receive-failure counter. Connection faults invalidate the transport epoch and close its captured notification signal; reconnect verification permits new epochs. No query table or collector exists in the transport. Close removes both endpoints, joins entered callbacks and dispatch, then management joins snapshot Tasks. Public Cluster discovery uses Actor-owned QueryID associations and atomic whole-node merges. Cancellation, Runtime closure, receive failures and epoch invalidation take precedence over successful completion; late replies do not recreate queries.

## Unified envelope

Outer field order is `version,kind,operation,type,id,generation,factory,correlation,deadline,message,mode,fingerprint,reply,control,payload,seq,items,bytes,status,error`. Omit only inapplicable fields; version=1, kind and correlation are always required. Exact names below are fixed; unknown versions/kinds/fields or illegal field combinations fail ErrProtocol. Strict JSON includes duplicate-key rejection. Reply/control must be ≤256 ASCII bytes, start with the receiver Runtime's configured inbox prefix when routing responses to itself, contain no wildcard/whitespace, and never substitute for key/generation/correlation checks. Replies echo operation, key, generation, fingerprint, correlation and deadline; Factory responses also echo factory-token. A response cannot mutate the request identity.

| kind / operation | Required additional fields and meaning |
| --- | --- |
| request / activate | type,id,correlation,deadline,fingerprint,reply,payload `{create:bool}`; generation/factory absent on first selection; create=false never constructs |
| request / confirm or stop | type,id,generation,factory,deadline,reply; stop payload contains bounded cause error envelope; subject Type/token must agree |
| request / invoke | type,id,generation,deadline,message,mode,fingerprint,reply,payload; Call/Stream also control endpoint; Stream carries initial items/bytes window |
| complete / any | status=`ok` or `error`; error required only for error status; Send success has no result, Call success has typed result or null for no-result, Stream requires final seq; Factory success payload contains confirmed state/revision/generation/factory |
| chunk / invoke | original request identity, seq starting at 1, payload, bytes equal to encoded payload length; Stream only |
| cancel / invoke | original request identity and bounded cause, Call/Stream only; no new result or admission meaning |
| window / invoke | original identity, seq=last consecutively consumed chunk, items/bytes=exact newly returned credit, Stream only; replayed/inflated credit is a protocol failure |

Completion status `error` includes a rejection evidence field inside payload: `{"admission":"not_admitted"}` only from the exact target before mailbox admission; otherwise `{"admission":"unknown"}`. Error-only outcomes never imply rollback. Stream final seq is the number of chunks produced, including on errors; missing/duplicate/out-of-order chunks cannot yield success. Window counts include the active callback, returned only when it exits. After cancellation/failure no new callback starts; every return path joins the active callback before freeing its reservation. Terminal/control/cancel/window frames cannot create a new association.

Send completes only after local Addr.Send accepts. Call needs a validated final result, without an extra ACK. Stream needs successful terminal sequence plus complete consumption. Stop proves closed admission (FAILED also requires release); Wait uses only the registry's COMMITTED result; Close includes all owned cleanup. Same-process requests take the same NATS path.

If all attempts are proved unsent, return the original validation/capacity/context error. An exact target's confirmed rejection proves only that attempt was not admitted. Possible send without required ACK/result/terminal yields ErrOutcomeUnknown plus the transport/context cause; never transparently replay it. Stale-generation/non-owner replies cannot certify rejection. There is no business result store. Activation uncertainty uses ErrActivationUnknown and original key/generation verification, independently of business uncertainty.

## Bounded errors

`internal/errwire` implements `{"version":1,"codes":[],"message":"...","sources":[],"truncated":true?}`. Code order is fixed: closed, stopped, full, unhandled, reply_type, mode, setup, phase, chunk_size, type_unavailable, not_activated, contract, protocol, owner_unavailable, activation_unknown, outcome_unknown, exit_unknown, canceled, deadline. Codes map to the corresponding actorx/actorxdist/context sentinels, are unique and complete even if diagnostics truncate. Unknown codes fail ErrProtocol; arbitrary business errors preserve only text.

Message ≤512 UTF-8 bytes; ≤4 sources containing operation ≤32, Type ≤64, ID ≤128 and generation ≤22 bytes; total encoded error ≤8192 bytes. Retain the nearest source plus deepest three on overflow, set truncated. UTF-8-safe truncation affects diagnostics only; message text is shortened further when JSON escaping would exceed the total byte budget. Decode creates an error with errors.Is support; wrapping and returning it through another Actor preserves all codes and bounded downstream sources. Receiving a definite reply from B does not erase B→C outcome_unknown/deadline. Diagnostics distinguish this operation's result from downstream uncertainty and contain no business payload. Real tests verify actual A→B propagation across independent processes.

## Time and development resource profile

Default request=5s, maximum total request=60s, control/flush=2s, KV attempt=1s, initialization=10s when initCtx has no earlier deadline. One monotonic local budget covers contract lookup, selection, activation and communication; never reset at phase boundaries. Wire deadline is UTC Unix nanoseconds capped by local start+60s. Supported hosts synchronize clocks within ±250ms; receipt uses min(wire deadline−wall clock,60s), never extends it for skew. Skew can cause early rejection; no skew assumption grants ownership or proves non-execution. Deployments outside that assumption need independent validation.

Management owns creation lifetime. Construction/start cancellation is requested after 30s; non-returning New/Started still occupies its original token. KV batches try at most 3 times within the caller/stage deadline, with 20/40/80ms capped backoff. Same-generation owner confirmation retries CAS conflicts within the caller deadline and a 2s internal cap; other failures stop by the third overall attempt. Post-claim cleanup/commit batches retain one token per generation and retry at 1,2,4,…30s intervals with at most 3 KV attempts per batch. These background retries stop only on evidence of completion or explicit recovery, never on the last waiter leaving; no repeated goroutine allocation per retry. A hard application callback/constructor deadline cannot be enforced by Go cancellation.

The table shows the default development profile. Public execution/transport limits and their meanings are documented under [resource configuration](runtime.md#resource-configuration). Other protocol, storage and management limits remain internal.

For custom limits, let I=Instances, R=Requests and S=Streams. Execution instances and held activations each have ceiling I; per-Actor mailbox/pending/tasks use the configured fields. Core business calls, outgoing requests, incoming leases, shared reply correlations and resolution jobs each have ceiling R. Send ACK waits use ceil(R/2); total resolution/Wait observers use max(256,2R), still at most16 per address state. Streams share ceiling S and reserve S×4 items / S×64 KiB across both directions.

Internal business dispatch and retained-payload capacity each use max(32,ceil(R/4)) slots; control dispatch uses max(64,ceil(R/2)). Each business subscription and each of the two lazy Call/Stream inbox subscriptions permits max(8,ceil(R/16),ceil(S/2)) pending frames, at most128 KiB/frame. Let T be the number of locally registered Types, C=Creating and K=CreatingPerType. The two permanent discovery subscriptions each permit the same pending frame count, with query/reply bytes bounded at 1/64 KiB per frame. Application subscriptions total at most I+2T+4. Factory task capacity W=max(8,min(C,K,I)+4), mailbox capacity max(64,2W), pending capacity max(128,2W), and caller associations max(64,C); the extra task slots retain control/cleanup headroom. Types optionally caps T; its default imposes no count limit. Registry capacity and worker counts do not grow automatically. Startup rejects negative values and slot/byte arithmetic overflow before I/O.

Let H=ContractCacheTypes (default64). The caller cache holds at most H Type indexes and at most H distinct contracts, each ≤64 KiB; fingerprints share canonical bytes only after full byte comparison. Resident canonical bytes are therefore ≤H×64 KiB (default4 MiB; 64 MiB at H=1024). This excludes Type strings, map/list/object overhead, SDK read buffers and in-flight snapshots, and is not a Runtime memory limit or an independent byte quota. Entries grow on demand, without preallocating H contracts. Misses retain the existing request/payload/codec and Registry I/O budgets; Registry reads run outside the cache lock. Failed reads are not cached; conflicting fills return ErrContract.

| Resource and holder | Local / Runtime bound | Acquire → release; saturation |
| --- | --- | --- |
| declarations / Factory | Types optionally bounds local declarations (default unlimited); 64 handlers/Type; 256 definitions/contract; 64 KiB/contract | local registration slot → Unregister/Close settlement; published contracts remain; invalid/duplicate registration ErrSetup, Type quota ErrFull |
| caller contract cache / network | ContractCacheTypes Type indexes (default64); shared contracts ≤Type count; resident canonical bytes ≤H×64 KiB (default4 MiB), excluding overhead and in-flight snapshots | miss → LRU eviction or Close after work drains; hits refresh recency; evict one Type when full, delete shared bytes on last cached reference; capacity alone never returns ErrFull |
| business engine / core | 64 business instances; independently, 64 held activations including constructing, running, FAILED and pending commit; mailbox16, pending32, worker4 per instance; calls128; local stream8×64 KiB plus active chunk | core reserves at actual ownership; release only execution/cleanup completion; existing core ErrFull |
| management engine / core | T Factory Actors plus one local Discovery Actor; mailbox/pending/tasks derived above (defaults 64/128/8 per Actor); calls max(64,C); stream1×4 KiB plus active | independent of business engine; Discovery has its own reserved instance and does not consume user Types; remain through submission; ErrFull before starting work |
| local discovery snapshot / Discovery Actor | at most 256 entries and 64 KiB encoded snapshot data per task; shares the management Actor's task/completion budget | scan ready registrations → serial completion or discard; sorted prefix on entry/byte overflow, Truncated=true and EndReason=limit; canceled/failed queries return zero report |
| Runtime activation ledger | Creating concurrent creation works (default16), CreatingPerType per Type (default4); subset of 64 generation slots | reserve before CAS/New → committed/adopted or full rollback; FAILED keeps generation slot; ErrFull before claim |
| resolution waiters / Addr and Runtime | 16/state, 256 total | before joining resolution → own cancellation/result; work continues without waiters; ErrFull |
| local child instances / business engine | included in 64 live instance slots, separate from persistent-generation slots | reserve under the existing core Spawn lock before Bindings and local startup → core complete exit; never an unbounded bypass |
| network associations / Runtime | outgoing≤128, Send ACK subset≤64; incoming business leases≤128; shared reply correlations≤128 | reserve before publish/dispatch → network settled and callbacks returned; ErrFull if identified; no inference after SDK loss |
| app subscriptions / Runtime | ≤I+2T+4: T shared +T dedicated +I business +2 lazy inbox +2 discovery; business pending8 frames/1 MiB each, Factory subscriptions pending8 frames/1 MiB each; at default execution/transport limits aggregate544+16T frames / ((66+2T) MiB + 520 KiB) | before subscribe → Unsubscribe and entered callbacks joined; SDK loss is async fault/possible unknown |
| SDK registry requests / Runtime | 8 concurrent total (ordinary≤6, control/commit reserve2); forced UseOldRequestStyle, transient subscription≤8, channel8 each; ≤64 frames/8 MiB | reserve before contextual SDK call → return/unsubscribe; SDK requests do not use the app association map |
| dispatch / network | business queue32 frames/4 MiB, 8 workers; control/cancel queue64 frames/8 MiB, 4 workers | charge copied frame before enqueue → dispatch returns; identified full reject ErrFull; no per-arrival goroutine |
| codec / Runtime | 8 concurrent encode/decode slots, each≤128 KiB input +128 KiB output; 2 MiB total; depth32/nodes8192 | reserve before traversal/copy → bytes handed to another charged owner or discarded; ErrFull/ErrChunkSize |
| retained network payloads / Runtime | 32 buffers /4 MiB; associations retain metadata≤2 KiB each separately | before hold → final owner releases; no uncharged payload copy |
| Stream consumer windows / Runtime | 16 incoming/outgoing streams combined, each4 items/64 KiB; reserve total64 items/1 MiB; active callback included | whole window before stream → callback joined and association closed; ErrFull; frame queues charged separately |
| Registry scans / provider Runtime | 1 scanner; 64 sequence positions/page, ≤1 page/s, ≤32 retained candidates; no KV watches | before scan → page/attempt returns; pause on full; no write maintenance on callers |
| commit/control tasks / Runtime | existing64 generation entries, one work/generation; management/control queue and2 SDK slots reserved | cleanup start → verified commit/release; never discard evidence when full |
| diagnostics / Runtime | nonblocking64 events, ≤1 KiB/event, overflow coalesced counter | callback → drain/close; no arbitrary user callback on SDK thread |

With default execution/transport limits and T registered Types, the two core engines imply at most 1088+64T mailbox slots, 2176+128T pending requests, 264+8T owned workers and 192 caller associations, before lower network/admission limits apply. Business objects and payload references retained by application code are not a whole-process memory guarantee. SDK socket/parser buffers, one in-flight frame per dispatcher and transport write buffers are separate: NATS server max_payload=128 KiB, SDK normal write buffer=32 KiB, reconnect buffer disabled (SDK value -1; zero restores its default); a decoder cannot undo allocation already made by the SDK. Only trusted configured servers are in this profile. SDK fault delivery callbacks remain bounded (including cancellation of at most the configured Streams incoming streams), nonblocking and forward into the bounded diagnostic sink; raw user callbacks that replace lifecycle/error control are rejected during option normalization.

Cache eviction releases only cached references; active calls retain immutable snapshots without pinning LRU entries. Re-reading an evicted contract may briefly coexist with an older snapshot. Eviction does not delete Registry contracts, withdraw registrations, stop Actors or change generations. Close joins admitted work before clearing the cache and rejects late fills.

`TestSizingBaseline` measures serialized representative maximum frame/record/error fixtures and verifies budget arithmetic. The default 64 generations × eight 33 KiB state/exit/tombstone write allowances =16.5 MiB, below 30% of each 128 MiB bucket; this is sizing headroom only, not a global reservation. Timing, CPU, real record overhead, throughput and concurrent multi-provider storage headroom require deployment-specific calibration. Do not advertise these values as general recommendations.

## Reply and flow-control resources

Resolution jobs and waiters are charged separately: at most Requests in-flight jobs (default 128) remain reserved until completion even after all waiters cancel. Each generation has at most one lifecycle monitor and one local Wait helper, at most Instances of each (default 64), joined when its ledger entry settles.

Activation and Send/Call share one lazy Runtime inbox with at most Requests correlations (default 128); single-result correlations hold one frame. Call cancellation publishes to the original business subject and matches reply capability and identity through control dispatch. The second Runtime inbox handles ordered Stream windows (`_INBOX.<runtime>.control.<correlation>`), advertised in chunks. Chunks/termination use the first inbox. Windows bypass shared dispatch workers; there are no per-stream subscriptions.

Streams use a reserved four-item/64 KiB chunk queue and one terminal/error slot. At most Streams transport monitors (default 16) settle before budget release. Buffers and correlations remain subject to the table's aggregate limits.


## Decode allocation limits

Static reflected types are at most128 KiB. Decode conservatively charges128 KiB cumulative value storage before reflect.New/MakeSlice/MakeMap, failing ErrChunkSize on overflow. Encoding and retained replies share32 slots with eight codec jobs. Strict JSON checks and reflection metadata separately obey input-byte/type/node bounds; encoded-buffer arithmetic is not a whole-Go-heap bound.
