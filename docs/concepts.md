# Concepts

English | [中文](concepts.zh.md)

Distinguish three things: **the logical object's identity, its current activation, and the generation an address targets**. Business code keeps actorx Actors, Bindings and serial handlers; dist adds addressing, on-demand activation and cross-process communication.

```text
ActorKey (Type + ID, unchanged across generations)
    └─ generation (one activation, never reused after exit)
           └─ Actor instance in a worker (memory state and resources)
Addr binds once; obtain a fresh Addr to select another generation.
```

generation is opaque, need not increase monotonically and differs from the revision used for conditional KV updates.

For example, a first call to `counter / orders-42` activates a generation and its count reaches 10. After complete exit, a fresh address for that key can activate another generation starting at zero. The old address still targets the old generation. Logical identity survives; in-memory state does not. Business code restores state from its own storage when needed.

## Declaration and startup

Workers declare creation capability with `rt.Register(ctx, spec)`. ActorSpec[T] supplies Type and New; T is a concrete struct pointer implementing actorx.Actor. Pure callers omit registration. Runtime owns the connection, local instances and management work. Successful NewRuntime means execution, connection and storage are ready. Register makes one Type's contract and activation routes ready without constructing business objects. Startup and later registration use the same API. Usually discard the registration handle and let Runtime.Close withdraw registrations and join their cleanup. See [registration and withdrawal](runtime.md#registration-and-withdrawal) when a Type needs an independent lifetime.

Only the ownership winner calls `New(ActorKey)`, returning a fresh non-nil object each time. New performs short local construction without goroutines or resources requiring cleanup; closures can capture dependencies. Put fallible checks, resource acquisition and state loading in Started, with application-defined initialization limits. Bindings must not depend on instance state; nil/live descriptions must match. Binding generation follows [actorx rules](https://github.com/go-aie/actorx/blob/main/docs/runtime.md); see [architecture](architecture.md#registration-and-context-integration) for registration and integration details.

## Addresses and activations

Addr retains identity and generation state, not a business object or local execution address. `rt.Addr(type, id)` validates identity and creates a handle without I/O or target activation. Omitting ID immediately generates a new identity; an explicit empty ID is invalid. Separate Addr calls allocate independent state; value copies share state and support concurrent use. `Key()` returns an identity copy and `IsZero()` only reports initialization. Complete naming rules live in the [wire contract](contract.md#names-and-canonical-identity).

The first Send/Call/Stream selects an existing generation or contends to create one, then waits for readiness and executes. All logical calls traverse NATS, including same-process targets. Selecting STARTING or FAILED does not imply business READY. Failure, exit or a replacement generation never rebinds a handle. Stop/Wait also target the original generation; cold control never creates. See [lifecycle](lifecycle.md).

Completing a call, dropping an address or closing the caller leaves remote objects alive. See [object lifetime](lifecycle.md#object-lifetime) for shared and temporary objects.

## Actor addressing and Self

Handlers, lifecycle hooks and serial task completions still receive actorx.Context:

- `actorxdist.MustFrom(ctx).Addr(type, id)` obtains a target in the owning Runtime without storing Runtime in business objects.
- `actorxdist.MustFrom(ctx).Self()` reads this activation's fixed reply address, which can travel in an ordinary message for later Send notifications.
- `dctx, ok := actorxdist.From(ctx)` supports optional local/distributed execution. Nil, typed-nil and pure core contexts return nil/false; MustFrom diagnoses missing capability with a panic. Addressing and communication errors still return normally.

Self is installed before Started and retains identity during shutdown. It performs no I/O and proves neither READY nor liveness. Old Self never follows a new generation. Pass ActorKey and obtain a fresh Addr at use time when current-generation selection is wanted. Do not synchronously Call/Stream/Wait on yourself from Started or a serial handler; use `ctx.Stop` for self-stop.

WithContext(parent) adopts the new parent's deadline, cancellation and values while preserving capability and Self. Do not retain contexts across requests. Obtain addresses before starting `actorx.StartTask(ctx, ...)`; workers communicate using their task context.

`ctx.Spawn` creates a local instance and returns actorx.Addr. Children and descendants have addressing capability but no logical identity: Self returns actorx.ErrSetup. They inherit neither a parent generation nor stop propagation. Address logical objects with `dctx.Addr`; see [Button → Counter](../examples/click/README.md).

## Address transfer

`json.Marshal(addr)` exports version/type/id and optional generation, omitted for unbound handles. `rt.DecodeAddr(data)` associates the receiving Runtime and creates independent state; a supplied generation binds immediately. It performs no lookup or activation. The codec imports nested Addr values in messages, results and stream chunks automatically. Cross-process transfer does not share mutable local state.

DecodeAddr rejects unknown/duplicate fields, invalid identities and explicit empty generations. Unsupported versions return ErrProtocol. Default json.Unmarshal has no receiving Runtime and returns actorx.ErrSetup. Closed Runtimes reject Addr/DecodeAddr with ErrClosed; existing addresses still expose Key and export identity.

Continue with [call semantics](runtime.md) and [lifecycle](lifecycle.md). Consult the [Registry protocol](registry.md) when you need the evidence behind unique ownership.
