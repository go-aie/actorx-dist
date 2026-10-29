# Call semantics

English | [中文](runtime.zh.md)

Send/Call/Stream follow one path: **validate the contract → select and fix a generation → wait for READY → send through NATS**. The target runs business code through actorx mailboxes, including same-process calls. See [concepts](concepts.md) for the address model.

## Registration and withdrawal

Startup and later registration use the same Runtime method:

```go
rt, err := actorxdist.NewRuntime(ctx)
// Check err and arrange Runtime.Close before registering worker types.
registration, err := rt.Register(ctx, CounterSpec)
// Check err before opening application admission.

// To withdraw this Runtime's registration independently:
err = registration.Unregister(cleanupCtx)
```

Runtime owns registrations, capacity, Factories and shutdown cleanup, so registration uses `rt.Register(ctx, spec)`. The returned Registration identifies that particular registration and owns independent withdrawal; an old handle cannot affect a replacement with the same Type. `ActorSpec[T]` retains the concrete Actor type through a Go 1.27 generic method. Its optional Description accepts valid UTF-8 up to 512 bytes and does not affect the contract.

NewRuntime initializes execution, the connection and Registry. Register validates ActorSpec, publishes the immutable contract and installs the Factory and control/activation subscriptions. Success includes Flush and constructs no business instances. Even when all Types are known before startup, register them through this API. Registration uses a five-second default total budget, capped at sixty seconds with an explicit deadline; Runtime.Close also cancels installation.

Duplicate local Types, including registrations still draining, return ErrSetup; exceeding Types returns ErrFull. Different Runtimes can register the same Type with an identical contract. Installation failures trigger automatic cleanup and may return a non-nil handle alongside the error; Unregister can join that cleanup. Already published immutable contracts remain and are verified on retry.

Unregister immediately closes the registration's new-activation gate, stops its logical instances, and joins in-flight construction, actual exit and required terminal commits before removing controls and releasing Type capacity. Other registrations and other Runtimes' same-Type capability remain available. Withdrawal currently stops owned instances; applications that need draining should drain their business entry first. Independently spawned local children retain their application-defined cleanup responsibility.

Unregister is idempotent. Timeout ends only the wait; cleanup continues and can be joined again. Re-register a local Type only after its old registration has settled. The new registration gets a fresh token; old handles cannot withdraw it, and old Addr values remain fixed to their original generation. Runtime.Close joins all unfinished registrations.

## Completion, ordering and errors

| Call or observation | Meaning |
| --- | --- |
| Successful Send | Target generation admitted the message; its handler may still be running |
| Call returns | Valid final result or definite business error; no rollback implied |
| Successful Stream | Valid successful termination, consecutive sequences, all chunks consumed and active callback returned |
| Local validation, capacity or explicit pre-publish failure | This business frame was not sent; return the original error |
| Exact target refuses before mailbox admission | Definite non-admission for this attempt; no transparent retry |
| Possible send without required ACK, result or termination | ErrOutcomeUnknown plus cancellation, deadline or transport cause |

Ordinary business errors end only the request, retain state changes and leave the generation available. Handler errors after Send ACK use core asynchronous logging without revoking admission. Panic stops the instance under core rules; [Wait](lifecycle.md) observes complete exit.

Handlers within an instance run serially; concurrent calls and different senders have no global order. Counter waits for Send ACK before Call and uses its synchronous handler to observe the preceding increment. ACK itself is no business-completion barrier. StartTask external work can overlap subsequent handlers; this ordering does not prove task completion.

## Deadlines and cancellation

Requests default to five seconds without a deadline; explicit deadlines are capped at sixty seconds. Contract reads, selection, activation and response waits consume one total deadline, never reset per stage. Admitted Send follows target instance lifetime; caller cancellation only ends the ACK wait. Call/Stream propagate cancellation best effort, with receiving deadlines as a fallback. Cancellation does not prove handlers, tasks or effects have stopped.

Disconnected business messages are neither buffered nor replayed. Applications handle uncertain outcomes through business queries, idempotency or compensation. The framework neither transparently resends nor stores queryable business results. See the [wire contract](contract.md) for deadline encoding and cancellation identity checks.

## Stream

`addr.Stream[C](ctx, msg, yield)` invokes yield serially in order. Missing, duplicate, out-of-order or malformed chunks/termination cannot succeed. Valid business errors are definite failures and may stop queued consumption without undoing consumed data. Without valid termination, ErrOutcomeUnknown remains. Cancellation after valid termination adds no remote uncertainty. Callback errors retain the original Go error, plus uncertainty when terminal evidence is absent.

Every return path joins an active callback. Cancellation/failure starts no further callback, and an observed business/protocol error survives a later timeout during that join. Blocked callbacks retain their window, correlation and Runtime work reservation and prevent Close success; network cancellation can still propagate while they block. Callbacks should return promptly or use captured context for cancellation. Never synchronously wait for the enclosing Stream or Runtime.Close inside a callback.

Each stream reserves four items/64 KiB encoded payload, including its active callback; a Runtime allows 16 incoming/outgoing streams combined by default (configurable with TransportLimits.Streams). Credit returns only after consumption, using actual encoded bytes. `Responder.WriteConsumed` additionally waits for this acknowledgement. Partial failure never resumes automatically; disconnect cancels existing incoming streams and reconnect does not restore them. Late frames cannot recreate associations or consumers. See the [wire envelope](contract.md#unified-envelope) and [resource table](contract.md#time-and-development-resource-profile) for sequences, window acknowledgements and aggregate budgets.

For long streams, use a short `actorx.HandleStream` handler to start `actorx.StartTask(ctx, ...)`. The worker uses snapshots and its task context for I/O or downstream Stream, forwards chunks through `reply.Write`, and the serial completion calls `reply.Complete(err)`. State changes stay in serial handlers/completions, leaving the Actor able to handle ordinary Notify. Obtain Addr/Self before starting the task; workers do not retain distributed context views.

Streams share the five-second default and sixty-second maximum total deadline; choose an explicit subscription deadline. Canceling a stream ends only that request, without unsubscribing Self or stopping collaborators. See [lifetime responsibility](lifecycle.md#object-lifetime).

## Contracts and capacity

Callers and workers use shared DTOs at stable package paths. Named Go identities, schemas and modes must match for messages and results/chunks. One Type has an immutable complete contract; adding a handler requires a new Type. See [contract upgrades](deployment.md#contract-upgrades). Nested Addr values automatically bind the receiving Runtime.

Wire values use a strict JSON subset with a 64 KiB limit per payload/chunk. Custom codecs, execution capabilities, invalid numbers and unsupported shapes fail. The [wire reference](contract.md#schema-subset-and-fingerprint) owns the complete compatibility table and depth/node/allocation limits.

Local capacity failures return ErrFull. SDK overflow provides only asynchronous diagnostics, never evidence that a particular request did not execute. Explicit refusal also requires a valid reply path; saturated systems may still produce uncertainty. Observe faults through `rt.Diagnostics()`. Network work and core handlers/tasks have separate budgets, released only when actual work settles. See the [complete limits](contract.md#time-and-development-resource-profile).

New activations use a Core NATS queue group to choose one Runtime. That Runtime checks Instances, Creating and CreatingPerType before claiming ownership or constructing the Actor; a refusal returns ErrFull and emits an `activate-rejected` diagnostic naming the exhausted limit. There is no capacity wait queue or automatic candidate retry: activation may fail even when another Runtime has room. A full activation budget does not itself prevent calls to existing instances. Factory response timeouts and uncertain claims return ErrActivationUnknown with the underlying cause; admitted work remains owned by the Runtime. A generic ErrFull or timeout is not permission to retry an entire business call. Diagnostics are bounded events that can overflow, not complete capacity counters.

## Resource configuration

Set limits when creating each worker or caller Runtime; omitted fields retain the defaults:

```go
rt, err := actorxdist.NewRuntime(ctx,
    actorxdist.WithExecutionLimits(actorxdist.ExecutionLimits{
        Types:           1024,
        Creating:        32,
        CreatingPerType: 8,
        Instances:       256,
        MailboxCapacity: 64,
    }),
    actorxdist.WithTransportLimits(actorxdist.TransportLimits{
        Requests:           256,
        Streams:            32,
        ContractCacheTypes: 1024,
    }),
)
```

| Field | Default | Scope |
| --- | ---: | --- |
| ExecutionLimits.Types | unlimited | Locally registered Actor types; independent of business instance capacity and the known-contract cache |
| ExecutionLimits.Creating | 16 | Concurrent instance creation/startup across this Runtime |
| ExecutionLimits.CreatingPerType | 4 | Concurrent instance creation/startup per Type within this Runtime |
| ExecutionLimits.Instances | 64 | Business instances including local children; also the independently counted distributed activation budget |
| ExecutionLimits.MailboxCapacity | 16 | Queued work plus reserved task completions per Actor |
| ExecutionLimits.Pending | 32 | Admitted, unsettled requests per Actor, including Send and Deferred requests |
| ExecutionLimits.Tasks | 4 | Tasks per Actor until result delivery or discard completes |
| TransportLimits.Requests | 128 | Outgoing and incoming unsettled network requests, bounded separately; also core business caller and shared reply association capacity |
| TransportLimits.Streams | 16 | Incoming and outgoing streams combined, including active callbacks |
| TransportLimits.ContractCacheTypes | 64 | Logical Type entries in this Runtime's caller contract cache; independent of local registration capacity |

Mailbox capacity is released when work leaves the queue; Pending remains occupied until the request settles. A Deferred handler can return with an empty mailbox while its reply still occupies Pending. StartTask reserves a mailbox completion slot as well as a task slot.

Type registration does not construct business instances. Types is an optional registration quota; it does not configure other Runtimes or change ContractCacheTypes. Creating and CreatingPerType bound creation/startup rather than running instances; both also obey Instances. Slots remain occupied until creation results are adopted or discarded with cleanup complete.

ContractCacheTypes bounds a Type LRU: hits refresh recency; a full cache evicts the oldest Type rather than returning ErrFull. Each uncached Type still reads its own Registry declaration, including after eviction; failed reads are not cached. Types with the same fingerprint share immutable canonical bytes only after full byte comparison; a conflict returns ErrContract. Sharing applies only within this Runtime's caller cache; registration declarations and Registry records remain independent per Type. Eviction neither stops Actors nor changes Addr generations or Registry records. In-flight calls keep valid snapshots without pinning cache entries; Close drains admitted work before clearing the cache and rejecting late fills.

At most ContractCacheTypes distinct contracts remain cached, each at most 64 KiB. Resident canonical bytes therefore total at most `ContractCacheTypes × 64 KiB`: 4 MiB by default, or 64 MiB for 1024 Types, often less with sharing. This bound excludes Type strings, map/list/object overhead, SDK read buffers and in-flight snapshots; it is not a Runtime memory limit. Capacity does not preallocate that memory, and there is no separate byte-capacity setting. Zero selects 64 entries, not an unlimited or disabled cache.

Instances configures both execution and activation capacity with one value. Each has its own accounting: failed or exited activations awaiting release can still occupy activation capacity, while local children occupy only execution capacity. Streams also use request capacity. Send ACK waits are capped at half Requests, rounded up; activation/control requests share reply associations with business requests.

Zero selects the default: Types has no count limit by default; all other fields have finite defaults. Negative values or resource arithmetic overflow return actorx.ErrSetup before a connection is opened. The last option of the same group replaces the whole group, then zero fields receive defaults. Configuration is fixed for the Runtime lifetime and does not change peers or server quotas.

Factory instance capacity follows Types. Factory task/completion capacity grows with the effective creation concurrency, retaining control/cleanup headroom independently of business Tasks. Request/reply buffers and dispatch queues grow with the configured transport limits; wire windows, payload sizes, dispatch worker counts and Registry I/O/storage limits stay independently bounded. These ceilings do not guarantee admission: another budget can return ErrFull first. See the [resource table](contract.md#time-and-development-resource-profile) for the default profile and derived capacities; increasing Instances requires checking storage headroom.

## Error classifications

Use errors.Is for framework and context errors rather than parsing diagnostics. Remote arbitrary business errors retain text, not custom Go types. Framework/context classifications survive downstream wrapping with at most four source records. A definite outer response does not erase downstream uncertainty.

| actorx-dist error | Meaning |
| --- | --- |
| ErrTypeUnavailable | No Factory responds to type activation; this does not authorize replacement of an existing generation |
| ErrNotActivated | Cold control found no current generation |
| ErrContract | Full contract, schema or mode mismatch; may also wrap core classifications |
| ErrProtocol | Unsupported wire version or protocol frame |
| ErrOwnerUnavailable | Original owner is unreachable; ownership is retained |
| ErrActivationUnknown | Uncertain activation or ownership update requiring original key/generation reconciliation |
| ErrOutcomeUnknown | Business request may have been sent without the operation's required completion evidence |
| ErrExitUnknown | Original terminal evidence is missing, expired or unverifiable |

Local setup, closure, stop, capacity, message, mode and result-type errors reuse actorx.ErrSetup/ErrClosed/ErrStopped/ErrFull/ErrUnhandled/ErrMode/ErrReplyType and related classifications. Uncertainty may include context.Canceled or DeadlineExceeded; timeout alone never proves non-execution. Applications define business queries, idempotency and compensation.
