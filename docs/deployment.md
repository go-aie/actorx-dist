# Connection and deployment environment

English | [中文](deployment.zh.md)

This guide covers server configuration, permissions, upgrades and fault handling. The default profile targets single-server development. See [call semantics](runtime.md) for invocation behavior and the [Registry protocol](registry.md) for storage rules.

## Server configuration

The local quick start uses [examples/nats.conf](../examples/nats.conf). Save the separate username-authenticated example below as nats.conf; its credentials differ from the quick start's anonymous URL. The development profile uses Go 1.27, nats.go v1.36.0 and nats-server v2.10.18. Storage must be writable; shared deployments also need TLS and scoped permissions.

```conf
host: "127.0.0.1"
port: 4222
max_payload: 131072
jetstream {
  store_dir: "./dist-data"
  max_file_store: 512MB
  max_memory_store: 64MB
}
accounts {
  APP {
    jetstream {
      max_file: 384MB
      max_memory: 32MB
      max_streams: 3
      max_consumers: 8
    }
    users: [{user: "dev", password: "change-me"}]
  }
}
```

Run `nats-server -c nats.conf`. Runtime defaults to `nats://127.0.0.1:4222`; override the URL or credentials:

```go
rt, err := actorxdist.NewRuntime(ctx,
    actorxdist.WithNATS("nats://127.0.0.1:4222", nats.UserInfo("dev", "change-me")),
    // After creation, register worker types with rt.Register(ctx, spec).
)
```

An empty URL also selects the default. The last WithNATS replaces the entire URL/options snapshot. Invalid URLs, nil SDK options, lifecycle/error callback overrides, custom dialers, NoEcho and implicit initial-connect retries are rejected. Authentication and TLS options remain available. Runtime owns the connection exclusively. Initialization defaults to a maximum of ten seconds; after success, Close owns the lifetime.

Register validates one Type's contract and waits for entry readiness. Installation failure starts automatic local cleanup and may return a handle for joining it through Unregister; already published immutable contracts remain. Pure callers have no Factory subscriptions or write maintenance. See [architecture](architecture.md#activation-and-business-paths) for initialization order.

## Storage and permissions

Runtimes with the same namespace in one NATS account share message routes and KV buckets. Use different `WithNamespace` values for independent deployments; the default is `default`. See the [NATS reference](nats.md) for subjects and JetStream API permission scope. Each namespace uses three streams; multiple deployments need correspondingly larger account stream and storage quotas. The example above fits one namespace.

Configure per-process execution and network capacity with [WithExecutionLimits and WithTransportLimits](runtime.md#resource-configuration). Increasing local capacity does not resize the following buckets or account quotas.

Runtime creates three KV buckets idempotently. Incompatible existing configuration fails without automatic repair. Types use 16 MiB with 64 KiB values; current ownership and exits each use 128 MiB with 16 KiB values. All use files, History=1, no TTL, DiscardNew and one replica. See the [Registry protocol](registry.md) for complete settings and conservative evidence rules. Any complete wire contract change requires a new Type. Stored type metadata does not establish that a Factory is online.

Grant JetStream account/stream information, the required KV reads and conditional writes, sequence-bounded maintenance purge, shared Factory entries, dedicated controls and temporary inboxes separately. `actorxdist.default.factory.<type-token>.<factory-token>.control.confirm` and `.control.stop` are distinct publish permissions. The receiver subscribes normally to `control.*`, without a queue group. Scope permissions to actual Type/token ranges. Send/Call/Stream callers publish to `actorxdist.default.actor.<type-token>.<id-token>`; workers subscribe under their declared Types. Both publish replies and subscribe to their own `_INBOX.>`. Call/Stream cancellation uses the original business subject without additional subscriptions or control permissions.

Exact connection and reserved control limits live in the [wire resource table](contract.md#time-and-development-resource-profile).

Disconnect suspends new network admission. Reconnect preserves factory-token and verifies buckets, contracts, held ownership and subscription Flush before resuming. Restart generates a new token without inheriting old ownership. There is no automatic takeover or business replay. `rt.Diagnostics()` drains up to 64 bounded events without blocking, returning the cumulative overflow count. SDK asynchronous errors, dispatch overflow and maintenance failures are observable without invoking user logic on SDK callbacks.

Worker Runtimes perform bounded maintenance of released metadata. Exits retain 24 hours plus a 500 ms guard; PREPARED/FAILED_HELD never age out. See the [Registry algorithm](registry.md#conditional-cleanup-and-physical-reclamation) for guarded deletion and physical reclamation.

Stream consumption acknowledgements publish to the owner inbox advertised in chunks. Both sides need the corresponding `_INBOX.>` publish permission. Two Runtime inboxes serve all calls and streams without per-stream subscriptions.

## Explicit recovery and storage repair

For ordinary shutdown, join the owner's Runtime.Close. After a crash or forced exit, first use your process/container platform to fence the old process, outstanding work and business subscriptions. Disconnection, suspension, TTL or a failed probe does not prove fencing. The command neither kills processes nor restores business state.

The following uses the README's local development service. Build from the actorx-dist repository root and inspect without mutation. The tool creates no Runtime, Factory or business subscriptions. For the authenticated profile above, use `nats://dev:change-me@127.0.0.1:4222` or supply a credentials file.

```sh
go build -o /tmp/dist-recover ./cmd/dist-recover
/tmp/dist-recover -nats nats://127.0.0.1:4222 -type counter -id orders-42
```

Review namespace, key, generation, factory, revision, current and terminal. `-namespace` defaults to `default`; custom deployments must supply the matching name for both inspection and apply. Snapshots and audit records include the namespace. After fencing, use the exact inspected values. `-creds` accepts a NATS credentials file; TLS can use a tls:// URL with system trust configuration:

```sh
/tmp/dist-recover -nats nats://127.0.0.1:4222 \
  -type counter -id orders-42 \
  -apply -generation '<generation>' -revision 123 \
  -confirm-stopped -reason 'incident-42: old container removed; work and subscriptions fenced' \
  -audit /durable/operator/dist-recovery.jsonl
```

Inspection is the default. Apply requires a fencing declaration, a 1–960 byte reason, exact generation/revision and writable durable audit path. Intent is written and fsynced before CAS; results are fsynced too. Retain the audit under your operations policy. The tool uses existing buckets without changing configuration or bulk cleanup. Stale key/generation/revision and conflicting evidence fail. After failure or ambiguity, inspect again rather than reuse the old revision or delete unconditionally.

Matching PREPARED or FAILED_HELD cleanup evidence permits conditional ownership release followed by conditional RELEASED commit. Matching INACTIVE release receipts permit resubmission. Already committed RELEASED is only verified and cannot affect a replacement. Without terminal evidence, only the exact ownership revision is conditionally deleted; audit records `released-without-exit` and no Exit is created. If replacement ownership overwrote an old PREPARED release receipt and the original process died, the new generation cannot establish the old Exit: ErrExitUnknown remains. Activate through a fresh Addr afterwards; old addresses stay fixed.

For capacity incidents, pause new activations and inspect bucket/account usage and pending generations. The 70% watermark and reserved control slots leave operating headroom but cannot guarantee submission under hard quota, disk failure or concurrent Runtime claims. Expand bucket/account capacity or repair storage. Reclaim only eligible, retained COMMITTED/RELEASED history conditionally; never delete PREPARED, FAILED_HELD or unreachable ownership to make room. Keep a live original owner's connection and management work for resubmission, then verify repeated Wait/Close. Do not apply explicit recovery against a live owner. A dead owner with missing evidence requires the fencing and exact recovery procedure above.

## Contract upgrades

The complete contract includes all ordinary messages, their package-qualified Go identities, schemas, results and modes. Registration reads nil-receiver Bindings without calling New or Started. Every worker for the same Type must match exactly; adding a handler is incompatible even when existing messages are unchanged. The registry retains the contract after workers and instances exit.

For an implementation-only rolling update with an identical complete contract, start the new worker, wait for its ready log, then gracefully Close the old worker. Existing generations remain with the original owner until full exit; the new process does not inherit them. A fresh Addr can select the next generation. No state transfer is implied.

For changed contracts, declare a new stable Type, such as example.counter.v2. Keep the old shared DTO package when its identity and schema are unchanged; use a distinct package or named type for changed DTOs. Deploy old and new Types together, direct upgraded callers to the new Type, then explicitly retire old owned objects and Close old workers after their clients drain. The same ID under different Types represents independent state. Preserve old terminal metadata for its retention period. Do not rewrite stored Type records or use deleting storage as an upgrade strategy.

## Multiple workers and machines

See [Counter](../examples/counter/README.md), [Click](../examples/click/README.md) and [Chat](../examples/chat/README.md) for local process setup against one service/account.

For actual separate hosts, provision the same pinned NATS service in a reachable network and bind its client listener to the intended interface instead of 127.0.0.1. Configure firewall access, authentication, TLS and the permissions above. Use a durable writable server storage directory and synchronized host clocks (±250 ms). Build the same worker/client code for each host; pass -nats with the reachable URL to every process. Start workers on worker hosts, wait for readiness and run the corresponding client roles from a caller host. Check outputs and repeated same-key accumulation as documented in each example. With the optional external-consumer tools retained, also start their fixture workers and use `consumer.py source --clients-only --nats <service-URL>` from the caller host for the complete consumer suite.

Record server configuration, host identities, versions, selected manifests, readiness and outputs. Source mode remains source evidence regardless of host count. A single server with Replicas=1 provides no replicated storage or automatic failover guarantee. Physical cross-host networking and replication remain unverified.
