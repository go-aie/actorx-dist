# NATS reference

English | [中文](nats.zh.md)

This reference explains how actorx-dist uses NATS for cross-process calls, on-demand activation and ownership coordination, followed by subjects, storage names and permissions. See [deployment](deployment.md) for connection and authentication settings, the [wire contract](contract.md) for frames and the [Registry protocol](registry.md) for storage algorithms.

## How NATS supports distributed Actors

actorx-dist uses **Core NATS for requests and replies** and **JetStream KV for coordination metadata**. Core NATS delivers messages to the process providing an Actor; the receiver then executes business handlers serially through an Actorx mailbox. KV holds contracts, current ownership and exit records so processes can identify the activation for a Type/ID and determine whether it has fully exited.

A Call brings communication and coordination together in this order:

1. The caller reads or retrieves the Type contract from its cache, checks message/result types and looks up current ownership.
2. An existing activation is confirmed by its original Factory. When creation is needed, the Factory queue group selects a Runtime. That Runtime checks capacity, claims ownership through a conditional KV write, creates the instance and publishes READY state.
3. The caller fixes the activation generation and sends a request to the Actor business subject. The receiver verifies identity before delivering it to the local mailbox.
4. When the handler completes, the result returns through the caller's reply inbox. Later calls reuse the running instance until that generation exits.

Features use the capabilities as follows:

| actorx-dist feature | How it uses NATS |
| --- | --- |
| Send / Call | Core NATS delivers requests; the reply inbox carries admission acknowledgements or final results |
| On-demand activation and unique ownership | A queue group distributes activation requests; conditional JetStream KV creation/updates establish ownership for one Type/ID |
| Contract registration and validation | KV stores an immutable contract per Type; registration compares complete declarations, and invocation checks operations, types and modes |
| Stream and cancellation | Core NATS carries chunks, termination and cancellation; consumers return window credit through a credit inbox |
| Stop / Wait | Dedicated Factory controls carry stop requests; KV retains exit evidence for Wait to read for the fixed generation |
| Explicit recovery and metadata maintenance | Conditional KV updates/deletes release ownership after process fencing; subject/sequence-bounded JetStream purge reclaims eligible records |

Three boundaries help interpret these capabilities:

- **Distribution and ownership:** a queue group selects a handler for an activation request; conditional KV writes establish unique ownership. A revision-based update (CAS) advances state only while the record still has the observed revision.
- **Transport and durable state:** business messages are not stored in JetStream, and Actor business state lives in provider memory. Disconnections do not buffer or replay business requests. Admission can resume after reconnect verification, without automatic takeover of unreachable Actors or resumption of old streams.
- **Streaming and consumption acknowledgement:** actorx-dist implements Stream sequence, termination and window rules. Credit returns over Core NATS; business streams do not use JetStream consumer ACKs or flow control.

Remote providers/consumers communicate with a Gateway over HTTP/SSE; its internal actorx-dist Runtime uses the NATS paths above. See [activation and business paths](architecture.md#activation-and-business-paths) for the full sequence and [call semantics](runtime.md) for completion, cancellation and unknown outcomes.

## Namespace and naming rules

`WithNamespace` configures one Runtime's namespace, defaulting to `default`, and scopes message subjects, Factory queue groups and all three KV buckets together. Runtimes with the same account and namespace share routes and the Registry. Cooperating Runtimes, Gateways and recovery tools must select the same namespace; Remote SDKs use the Gateway Runtime's namespace through the Gateway.

The inventory below uses `<namespace>` for this value. For `prod`, the message prefix is `actorxdist.prod` and the contract bucket is `actorxdist_prod_types_v1`. Actor Type/ID, random `_INBOX.*` subjects and the JetStream API prefix remain independent of the namespace. See [namespace configuration](runtime.md#namespace-and-deployment-isolation) for API usage and validation. Recovery uses `dist-recover -namespace prod ...`; see [explicit recovery](deployment.md#explicit-recovery-and-storage-repair).

## Subject reference

### Messages and queue groups

`T` and `I` are unpadded base64url encodings of the original Type and ID bytes. For example, `counter` → `Y291bnRlcg` and `orders-42` → `b3JkZXJzLTQy`. `F` is the registration's Factory token, `R` the request correlation ID and `N` the random Runtime inbox token. Random tokens are already encoded, not encoded twice. See the [wire identity rules](contract.md#names-and-canonical-identity).

| Publish subject | Purpose | Reception |
| --- | --- | --- |
| `actorxdist.<namespace>.factory.<T>` | Request activation by Type | Providing Runtimes subscribe using a queue group |
| `actorxdist.<namespace>.factory.<T>.<F>.control.confirm` | Confirm selected ownership and state | The selected registration subscribes normally to `actorxdist.<namespace>.factory.<T>.<F>.control.*` |
| `actorxdist.<namespace>.factory.<T>.<F>.control.stop` | Stop the selected generation | Shares the control subscription above |
| `actorxdist.<namespace>.actor.<T>.<I>` | Send/Call/Stream requests and Call/Stream cancellation | The current active instance subscribes normally |
| `_INBOX.<N>.<R>` | Activation/control replies, business replies, Stream chunks and termination | Runtime subscribes normally to `_INBOX.<N>.*` |
| `_INBOX.<N>.control.<R>` | Return window credit after Stream consumption | Runtime subscribes normally to `_INBOX.<N>.control.*` |
| `actorxdist.<namespace>.discovery.v1.query` | Internal discovery broadcast | Every Runtime subscribes normally, without a queue group |
| `actorxdist.<namespace>.discovery.v1.reply.<RuntimeID>` | One node's discovery snapshot or full/unavailable reply | The origin Runtime has one permanent ordinary subscription |

The Factory queue group is `actorxdist.<namespace>.factory.<T>.workers`, not a publish destination. Control and business entries use no queue group. Business operation names live in frames rather than per-handler subjects; cancellation uses the original business subject. Runtime reply and credit subscriptions are created lazily and shared, without per-call or per-stream application subscriptions.

The two discovery subscriptions are installed and flushed at initialization, including on Runtimes with no registered Types. RuntimeID identifies a Runtime lifetime. A discovery query's NATS Reply must exactly match the reply subject derived from its origin RuntimeID in the same namespace. Public DiscoveryCluster queries aggregate these snapshots in the local Discovery Actor over a bounded collection window. See the [discovery wire](contract.md#internal-discovery-transport) and [discovery_network.go](../discovery_network.go).

See [network.go](../network.go), [activation.go](../activation.go), [factory_rpc.go](../factory_rpc.go) and [stream.go](../stream.go) for implementation.

### KV and streams

Each bucket `B` has a backing stream `KV_<B>` capturing `$KV.<B>.>`. A record's complete KV subject is `$KV.<B>.<key>`; `G` denotes the activation generation.

| Stored content | Bucket (B) | Key |
| --- | --- | --- |
| Immutable Type contract | `actorxdist_<namespace>_types_v1` | `t.<T>` |
| Current activation ownership and state | `actorxdist_<namespace>_activations_v1` | `a.<T>.<I>` |
| Exit and release evidence | `actorxdist_<namespace>_exits_v1` | `e.<G>` |

For example, a `prod` contract record uses `$KV.actorxdist_prod_types_v1.t.<T>` in stream `KV_actorxdist_prod_types_v1`. The NATS SDK performs conditional writes and maintenance deletions; `dist-recover` follows the same naming rules. See the [Registry protocol](registry.md) for record states, retention and cleanup conditions, and [registry.go](../registry.go), [maintenance.go](../maintenance.go) and the [recovery tool](../internal/recovery/recovery.go) for implementation.

### JetStream API

The current nats.go v1.36.0 SDK generates the subjects below. `S` is a stream name from above; `K` is a complete KV subject (`$KV.<B>.<key>`).

| Subject | Purpose |
| --- | --- |
| `$JS.API.INFO` | Query account storage quota and usage |
| `$JS.API.STREAM.INFO.<S>` | Initialization, KV handle lookup, reconnect verification and capacity checks |
| `$JS.API.STREAM.CREATE.<S>` | Create missing KV streams during initialization |
| `$JS.API.DIRECT.GET.<S>.<K>` | Read the latest value for a key from a bucket with AllowDirect enabled |
| `$JS.API.STREAM.MSG.GET.<S>` | Maintenance scans and raw record reads; also KV reads when AllowDirect is disabled |
| `$JS.API.STREAM.PURGE.<S>` | Maintenance cleanup with a subject filter and exclusive sequence bound |

Runtime enables AllowDirect on newly created streams. The Registry uses point reads and bounded scans without KV watches or JetStream consumers. The SDK uses separate temporary `_INBOX.*` subjects for API responses and KV write acknowledgements.

`$JS.API.` and its operation paths belong to the NATS server API; the namespace affects only referenced resource names. See the [NATS JetStream API documentation](https://docs.nats.io/reference/jetstream/api/) for the full interface.

## Permissions and deployment considerations

Callers publish activation, confirmation, any required stop control and business requests. Providers subscribe to their offered Types, registration controls and instance business entries. Both sides publish replies and Stream credits and subscribe to their own inboxes. Treat confirm and stop as separate publish permissions.

Every Runtime queries streams during initialization and creates missing buckets; pure callers also read contracts, ownership and exit records. Contract publication, conditional ownership/exit writes and routine maintenance belong to provider paths. Use the name templates above to scope permissions to actual namespaces, Type/token ranges, KV keys and streams, with separate grants for each role.

Every Runtime also needs subscribe access to its namespace's discovery query subject and its own discovery reply subject, plus publish access to the query subject and peers' discovery reply subjects. These tokens constrain reply routing; they do not authenticate cluster members.

Each namespace uses three streams with separate bucket capacity, while account-wide storage quota and usage remain shared. Other deployments' usage also affects `$JS.API.INFO` capacity checks. Namespace naming isolates names; security isolation requires NATS accounts, authentication and permissions. See the [NATS account documentation](https://docs.nats.io/learn/security/accounts-and-multitenancy), and [deployment](deployment.md) for server configuration, capacity changes and fault handling.
