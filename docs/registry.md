# Registry protocol

English | [中文](registry.zh.md)

This reference describes current ownership, terminal and maintenance protocols against JetStream KV in nats.go v1.36.0 and nats-server v2.10.18. Versions and limits define a reproducible development profile.

## Configuration and request ownership

Use NATS Core for nonpersistent business messages and file-backed JetStream KV for metadata. No mirrors, sources, republish, compression or external domains. Development server max_payload=131072 bytes, file limit=512 MiB, memory limit=64 MiB; dedicated account storage limit=384 MiB, at least 3 streams and 8 consumers allowed. No unrelated users consume this account's headroom. Replicas=1 is the local baseline; replicated deployments require separate validation.

| KV bucket / backing stream | Key | MaxValueSize / MaxBytes | History / TTL / replicas |
| --- | --- | --- | --- |
| AD_TYPES_V1 / KV_AD_TYPES_V1 | t.<type-token> | 65536 /16 MiB | 1 /0 /1 |
| AD_ACTIVATIONS_V1 / KV_AD_ACTIVATIONS_V1 | a.<type-token>.<id-token> | 16384 /128 MiB | 1 /0 /1 |
| AD_EXITS_V1 / KV_AD_EXITS_V1 | e.<generation> | 16384 /128 MiB | 1 /0 /1 |

Validate KeyValueConfig and StreamInfo on startup: file storage, DiscardNew, MaxMsgsPerSubject=1, MaxAge=0, AllowRollup=true, DenyDelete=true, DenyPurge=false, expected subjects, sizes/replicas, no stream transforms or mirrors. Bucket creation is idempotent; inspect actual config instead of accepting a same-name bucket. Never silently mutate a mismatched deployment. Type contracts never expire. Exit retention is 24 hours after COMMITTED/RELEASED, using the server creation time of that committed revision; FAILED_HELD and PREPARED have no age eviction. Client/server clocks use the wire baseline's ±250ms assumption, and maintenance waits an extra 500ms before age eligibility. No bucket-wide TTL is used.

Connection options are snapshotted/applied once at initialization; framework normalization owns reconnect buffering disabled (SDK value -1; zero restores its default 8MiB), request style, subscription/readiness/error handling and close behavior after user authentication/TLS options. No borrowed connection. After connection loss, pause new admission, retain generation/factory-token, revalidate subscriptions/ownership before resuming. Exited subscriptions never rejoin. Init failure rolls back local resources; globally created matching type contracts remain.

Each Registry operation first reserves one of eight SDK slots (six general, two reserved for control/commit). Construct a per-operation `nc.JetStream(nats.Context(opCtx))` and obtain `js.KeyValue(bucket)` from it, because the legacy KV Create/Update/Delete signatures do not accept per-call context. Do not reuse a handle with a previous operation's context. The context covers lookup plus operation, ≤1s and the remaining phase budget. Force nats.UseOldRequestStyle so SDK requests use bounded transient inboxes, without the hidden asynchronous response mux; each ends with SDK unsubscribe. No KV Watch/Keys/History/PurgeDeletes bulk materialization. Use bounded sequence scans below.

A logical batch has ≤3 SDK operation attempts; Create can internally perform up to three RPCs around a deletion marker, so the maximum is nine underlying requests, sequentially within the same deadline/slot. An expired batch releases only its I/O slot. Claimed generation, cleanup evidence and deferred commit work remain owned by Runtime across batches. Reconnect alone is not authorization to claim an old key.

## Records and conditional operations

All records include version=1 and stable 22-character operation ID. Current record includes key, generation, factory-token, contract fingerprint and state. Terminal record includes key/generation/original factory-token, original cause envelope, created_instance bool, evidence (`no_instance`, `spawn_rollback`, `local_wait`), cleanup digest, intended disposition, prepared revision, release operation/revision when known, commit/disposition. Digest is SHA-256 of the canonical cleanup evidence excluding mutable commit fields; no application object or arbitrary error is stored.

An operation ID and its intended complete record are retained in the resource ledger before the write. A revision is the returned uint64 stream sequence, not the generation. Never refresh expected revision and retry an obsolete transition without rereading state and rechecking all guards. Mutation requires the current key/generation/factory and expected state; same key alone is insufficient.

| Transition | SDK call and guard | Success / lost acknowledgement verification |
| --- | --- | --- |
| publish type | types.Create(t,canonicalContract) | returned revision; existing canonical bytes/fingerprint must exactly match, otherwise ErrContract; no New is called |
| claim STARTING | current.Create(a,record) if absent/deleted; current.Update(a,record,r) only from observed INACTIVE | retain proposed generation/factory/op before send; read exact key after error; matching op/gen/factory proves claim; conflicting active generation loses; unknown stays ErrActivationUnknown |
| confirm candidate | current.Update(a,sameStateWithConfirmOp,r), only if generation/factory/state still valid | returned revision proves current ownership at that instant; lost response reads matching op or redoes guarded confirmation, ≤3 attempts; INACTIVE/RELEASED candidates cannot bind |
| publish READY | current.Update(a,ready,r), after Spawn success, subscription Flush and stop-boundary recheck | match returned revision or reread exact op/state; keep business gate closed until known; unknown publication cannot become an unverified FAILED |
| close admission / STOPPING | local gate first, then current.Update(a,stopping,r) under same generation/factory | ordinary Stop success proves local admission closed, not local Wait/terminal commit; retain local close fact and retry metadata; do not start construction after stop |
| prepare terminal | exits.Create(e,prepared) once full cleanup evidence exists | returned terminal revision p; if existing compare immutable key/gen/factory/cause/evidence/digest/intended disposition; conflicting evidence is protocol fault, never overwrite |
| publish FAILED | current.Update(a,failed,r) only from owned STARTING with cleanup complete and PREPARED present | returned revision or exact matching op; stop already accepted uses STOPPING/release; never roll back a later state |
| release current | current.Update(a,inactive,r), only owned STOPPING/FAILED, with PREPARED or FAILED_HELD plus cleanup evidence | INACTIVE stores release op, terminal key/digest and prepared revision; record returned inactive revision q in ledger; lost ack: same INACTIVE op/digest proves release, otherwise see crash table |
| commit terminal | exits.Update(e,committed,p) | only after verified FAILED hold or release; record cleanup digest and q/release op for RELEASED; return/update terminal revision c; lost ack reads exact generation matching immutable evidence and disposition, never just current state |
| release FAILED_HELD | current.Update(a,inactive,r), then exits.Update(e,released,p) | same original cause/digest; release op/q required as above; Stop cannot succeed until RELEASED confirmed; new retention period starts here |

Get/GetLastMsg/watch/caches alone offer candidates; successful conditional confirmation/claim establishes initial binding. Follow an existing candidate through its original factory-token, never a random Factory. Queue delivery cannot establish ownership. Factory retains STARTING work when the last waiter cancels. New nil/panic, descriptor mismatch and failed Spawn finish rollback before FAILED; a prior Stop/Close releases instead of retaining a new FAILED.

READY proves synchronous Started and ready subscription, not asynchronous business readiness. Ordinary handler errors end only that request; panic/self-stop/Stop/Close end the instance. Failed activation Wait may return Exit.Reason with nil observation error and still hold ownership: FAILED_HELD requires explicit Stop or owner Close. Pure caller closure and collaborator shutdown do not stop remote objects. Temporary exclusive objects use the same Addr for Stop→Wait with an independent bounded cleanup context.

## Crash and ambiguity review

| Durable facts at crash / lost response | What can be concluded |
| --- | --- |
| no confirmed claim, possible STARTING write | verify original proposed generation/op; never generate a replacement merely because client timed out |
| STARTING exists, constructor result only in dead process | ownership stays; no complete cleanup evidence, no successful Wait; explicit recovery after process fencing |
| PREPARED only, current still STOPPING | cleanup evidence exists but release is not committed; Wait cannot succeed; live original owner can guarded-release/commit |
| PREPARED plus matching INACTIVE release op/digest | original owner may verify and complete; operator recovery may use exact durable evidence after fencing; a restarted Runtime does not inherit the token |
| PREPARED; new generation has overwritten INACTIVE; no COMMITTED old exit | History=1 may already have removed q. New generation alone proves neither the release operation nor its cleanup journal linkage. Only a still-live original owner retaining successful q/release evidence can commit. If its acknowledgement was lost and evidence cannot be recovered, ErrExitUnknown persists; explicit recovery cannot invent a RELEASED Exit |
| COMMITTED/FAILED_HELD | old Wait observes failure; key still held until owner Stop/Close releases |
| COMMITTED/RELEASED with original evidence | old Wait succeeds independently of new generation/current-record removal; duplicate Stop succeeds without touching new generation |
| terminal missing/expired or contradictory | ErrExitUnknown; unreachable original manager may additionally yield ErrOwnerUnavailable. No synthesized Exit from absence, INACTIVE, new READY or operator assertions |

There is intentionally no multi-key transaction or promise of successful Wait at every crash point. New activation may claim INACTIVE before the old terminal commit. The above gap is part of the conservative failure contract: live-owner evidence can finish it; process loss plus missing persisted release evidence cannot. Runtime must retain unknown submissions and refuse successful Close; safe recovery may release/fence ownership, but cannot turn unknown old results into success. No infinite immediate retry loop substitutes for missing evidence.

## Conditional cleanup and physical reclamation

For an INACTIVE candidate read key/gen/revision r and its exact COMMITTED/RELEASED terminal first. Then `current.Delete(a,nats.LastRevision(r))`. Success writes a tombstone, not a physical purge. Conflict abandons the candidate. On lost acknowledgement, bounded Get of latest: same candidate retries with original r only; missing/new generation stops modification. For raw tombstone evidence use `js.GetLastMsg("KV_AD_ACTIVATIONS_V1","$KV.AD_ACTIVATIONS_V1."+a)` and inspect Sequence, Subject and KV-Operation header; KV.Get/GetRevision intentionally hide deletion markers.

A sequence scanner reads `js.StreamInfo(stream)` FirstSeq/LastSeq then at most 64 `js.GetMsg(stream,seq)` positions per second, with deadlines and skipped holes. It holds ≤32 candidates, wraps its cursor for another pass and does not accumulate all keys or create watch consumers. Validate raw subject, operation and record size before treating a frame as a candidate. There is one scanner per worker Runtime and none on pure callers. A candidate is always reread before mutation; restart may safely rescan.

For physical tombstone cleanup, read the raw current delete marker with sequence d after eligibility checks, then `js.PurgeStream(stream,&nats.StreamPurgeRequest{Subject:exactKVSubject,Sequence:d+1})`. Sequence is exclusive: purge only records ≤d. Concurrent new records have a larger sequence and survive. Never use Keep=0 without a sequence cutoff, unconditional key Purge, DeleteMsg (KV denies it), or SDK PurgeDeletes: that SDK method scans delete markers and later purges by subject without a sequence fence. A lost purge response permits retry with the same d+1, never a new unlimited purge. Data-plane logical deletion, server message count/bytes recovery and actual disk reuse are distinct observations; real storage behavior is covered by the Registry and lifecycle gates.

For terminal expiry, require COMMITTED/RELEASED and age≥24h+500ms. First verify current record is absent, a newer generation, or safely condition-deleted under the previous rule; PREPARED/FAILED_HELD cannot authorize cleanup. Only then `exits.Delete(e,nats.LastRevision(c))` with the observed terminal revision c, followed by marker sequence-fenced reclamation. A changed revision abandons the candidate. History=1 bounds old values, but tombstones still consume bytes until reclaimed. Type specs remain immutable/persistent and have no automatic deletion.

## Capacity and recovery boundary

Before new claim, read StreamInfo and AccountInfo under a ≤1s freshness window. Refuse admission at ≥70% of either current/exit bucket or account byte limit, ≥90% of type-bucket limit for new types, or when usage cannot be read. Definite pre-claim resource refusal returns ErrFull with bounded storage cause; a potentially accepted claim uses ErrActivationUnknown as well. A Runtime's default 64 generation slots and eight (16KiB+16KiB+1KiB) write allowances/generation leave about 16.5MiB sizing headroom, but separate buckets/account may fill first. Metadata payloads, NATS headers, record subjects, histories, tombstones, replicas and file-store overhead must be measured on the real server. Local token limits cannot reserve global storage atomically.

Control/commit slots remain available when new activation is refused. Hard bucket/account quota, disk loss or concurrent Runtime claims can still block final writes. Close admission, complete physical cleanup where possible, retain current ownership/PREPARED/FAILED_HELD and live-owner evidence, keep commit work tracked and report unsuccessful Wait/Close until required COMMITTED evidence exists. Already committed FAILED_HELD remains observable but unreleased. Never delete incomplete evidence to make room.

Operational order: pause new activation → inspect usage/owned pending generations → expand quota/repair storage, or conditionally reclaim only eligible committed history → original owner verifies/resubmits → verify Wait/Close and capacity before reopening. If owner died, first fence old process/subscriptions, then use exact key/generation/revision recovery; stale requests fail. Recovery cannot synthesize missing Exit or business outcomes.

Source references: [pinned KV SDK](https://github.com/nats-io/nats.go/blob/v1.36.0/kv.go), [pinned stream management SDK](https://github.com/nats-io/nats.go/blob/v1.36.0/jsm.go), [pinned server stream configuration](https://github.com/nats-io/nats-server/blob/v2.10.18/server/stream.go). The protocol, resource choices and conservative ambiguity rules above are dist design decisions.
