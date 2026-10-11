# Realtime V2 negotiation contract, revision 1

Status: pure state machines tested; Upcloud's opt-in transport is connected and tested with
synthetic callbacks/socketpairs. Cloud and the Python SDK opt-in adapters pass isolated TCP
tests, including actual Cloud/SDK interoperability with a synthetic upstream. No production
deployment or connection advertises this capability yet. Independent of History V2 and
session-rehome capabilities.

## Direction and activation barrier

| Link | OFFER | ACK | COMMIT | V2 data / SOURCE_STATUS |
| --- | --- | --- | --- | --- |
| Upcloud -> Cloud | Upcloud | Cloud | Upcloud | Upcloud |
| Cloud -> SDK | SDK | Cloud | SDK | Cloud |

The connection initiator sends OFFER and COMMIT on both links. Cloud always sends ACK.
For the upper link, Cloud installs its decoder before queueing ACK; Upcloud queues COMMIT
before V2 data. For the lower link, SDK installs its decoder before queueing COMMIT;
Cloud sends no V2 data until it receives COMMIT. Thus COMMIT is not always receiver-originated
(this explicitly refines section 5.2 of the original plan).

Control types are 0x60 OFFER, 0x61 ACK, 0x62 COMMIT, 0x64 SOURCE_STATUS. All use the
existing 10-byte !IIH outer header, symbol_id=0. 0x63 is the row type, symbol_id!=0.
No compression or batching of these control payloads. Process only on the authenticated,
current connection. An upstream ACK is NOT proof of authentication: Upcloud must first
observe the existing authenticated session confirmation (demand/heartbeat).

Transport adapters must serialize state transitions, ordered queue insertion, and queue
completion notifications on one connection executor (or under one lock). A successful
`sent` means the entire frame was atomically accepted into the ordered reliable output
queue, not that a single socket write succeeded. Notify the state machine before dispatching
any reply. A partially written older V1 frame must finish intact; never insert bytes into it.
Pre-existing complete V1 frames may precede the barrier. Cache selection and live forwarding
must consult the same state. A queue/write failure after enqueue closes the connection;
never silently downgrade an uncertain V2 session on the same socket.

## Capability payload (OFFER, ACK, COMMIT)

Little-endian header, exactly 56 bytes (`<HBBQIHHI32s`):

| Offset | Type | Meaning |
| --- | --- | --- |
| 0 | u16 | negotiation version=1 |
| 2 | u8 | sender role: Upcloud=1, Cloud=2, SDK=3 |
| 3 | u8 | flags=0 |
| 4 | u64 | initiator nonce, nonzero |
| 12 | u32 | maximum **row payload**, 64..16384 (outer header excluded) |
| 16 | u16 | schema count, 0..16 |
| 18 | u16 | reserved=0 |
| 20 | u32 | supported/selected source mask, bit(source_id-1), only bits 0..4 |
| 24 | 32 bytes | canonical registry SHA256, raw digest |

Follow with schema_count entries, 36 bytes each: u16 schema_id, u16 schema_version,
32 raw SHA256 bytes. IDs and versions are nonzero; IDs strictly increase. Exact length,
no trailing bytes. At most 632 bytes. Zero sources iff zero schemas (negative ACK).

OFFER lists locally supported source/schema combinations, not their current readiness.
ACK echoes the nonce, selects only offered AND locally supported exact schema/version/hash
tuples and sources, and takes min(max_payload). Registry hashes must match; otherwise
Cloud returns a zero-source ACK using the OFFER registry hash. Unmatched schemas simply
leave the intersection. Each selected source must have its registered schema selected;
no orphan schemas. Initiator validates ACK is a subset, never trusts added capability.
COMMIT is byte-identical to ACK except sender role is the initiator's role.
An empty ACK causes fallback without COMMIT. Default mode `legacy` emits no control frames.

Each new authenticated connection gets a strictly increasing local generation and a fresh
unpredictable nonzero nonce (nonce supplied by the adapter; it is NOT an auth credential).
Nonces are identities, not clocks. Ignore callbacks for old generations before parsing;
ignore ACK/COMMIT/STATUS with a different nonce. One negotiation per connection, no in-place
renegotiation. Malformed expected control frames fail closed; duplicate/out-of-phase
capability frames do not reset an active session.

Initiator ACK timeout: 1000ms after ordered OFFER enqueue; maintain V1 in auto mode,
report unavailable in required_v2, ignore late ACK. Responder COMMIT timeout: 1000ms
after ordered ACK enqueue; **close**, not local fallback (the initiator may already have
queued COMMIT/V2). Pending queue actions time out to close too. Tick at monotonic time;
an event exactly at the deadline is expired. After ACTIVE there is no capability timer.

## SOURCE_STATUS and source epochs

Only the data producer sends STATUS, after COMMIT is queued/received as appropriate,
before any row for a new source epoch. STATUS is a full view of exactly the negotiated
sources, not a delta. Header, 24 bytes (`<HHQQI`): version=1, entry_count (1..5), nonce,
revision (nonzero u64), reserved=0. Entries, 16 bytes each (`<HHIQ`): source_id,
state (0=unavailable, 1=ready), reason (u32), source_epoch (u64).

Reasons: 0=ready, 1=no_upstream, 2=legacy_upstream, 3=schema_mismatch,
4=suspended, 5=payload_limit. Ready requires reason=0 and nonzero epoch. Unavailable
requires nonzero reason and epoch=0. Source IDs strictly increase. Exact length <=104B.
STATUS revision strictly increases for the lifetime of the negotiated connection;
ignore duplicate/older revisions. Never numerically order source epochs. Changing readiness
or epoch returns an invalidation mask; the adapter MUST clear affected extended caches and
per-symbol sequence tracking before delivering more quotes. Reconnect clears all such state.

Support is not readiness: initially all sources are unavailable, even after COMMIT. Upcloud
may announce ready only after validating and binding its active IPC schema/epoch. Cloud
derives readiness exclusively from its current authenticated upstream's committed V2
state and status, never from compilation support or a V1 cache. Ready means the typed
source/epoch is admitted, NOT proof of a recent market tick. Subscription ACLs still apply.
Runtime drift, upstream loss, rollback or health suspension must publish unavailable before
any V1 replacement data. Publish ready with a new epoch before data from a replaced source.

Row admission requires ACTIVE, source negotiated, latest status ready, matching source_epoch,
payload <= negotiated limit, registry validation, authenticated symbol/code/source mapping,
and per-(source, symbol, epoch) increasing sequence. The state machine checks readiness/epoch;
transport owns symbol authorization, cache invalidation and sequence bounds. Conflation can
reorder symbols, so do not reject rows using a single global per-source sequence watermark.

`auto`: use V2 only for ready negotiated sources, otherwise V1 with an exposed reason.
`required_v2`: same V2 condition; otherwise explicitly unavailable, never deliver a V1 quote
as all-fields data. `legacy`: unchanged V1. Source status is not a subscription ACK and must
not alter the legacy ACK payload. The public subscription/error API is still pending.

## Upcloud transport implementation

`cloud_uplink.realtime_full_fields` is an optional boolean, default false. Enabled before
startup only; configured tables must match the existing registry and use `code`. It switches
the existing subscription's conversion callback, NOT the DDB table or publication configuration.
Per-node negotiation remains independent. Current main uses one rollout flag for all nodes;
per-node rollout configuration is not exposed yet. Do not enable production before Cloud/SDK
integration and the full version matrix have passed.

Upcloud starts OFFER only after demand/heartbeat confirmation, never because of a realtime ACK.
The callback copies owned field values synchronously, with a generation gate, fresh random epoch
after IPC replacement or schema failure, and source-local increasing sequence. Replaced callback
generations cannot update health/progress or emit data. Schema drift invalidates extended state;
bad individual rows are isolated. Source NULL remains NULL; it is not a schema failure.

Callbacks emit bounded chunks (256 rows or approximately 256KiB encoded payload) through the
existing demand filter, static symbol mapping, dispatcher and NodeManager. Unknown symbols remain
dropped by the existing mapper; full fields do not add dynamic dictionary entries or data sources.
Queue entries own V2 and V1 payloads, source/epoch/sequence. A connection executor lock serializes
status/format decisions and actual output enqueue/flush. Pending old-epoch snapshots are removed;
already enqueued bytes finish before unavailable/new-epoch status. Reconnect clears pending/cache
payloads, never schedules cached data for replay. Snapshot coalescing remains latest-by-symbol.

Latest payloads and outbound frame bytes each have a 16MiB budget plus the existing 10000-item cap;
these are payload budgets, not a total RSS bound. A row exceeding the negotiated limit marks that
table unavailable with `payload_limit` BEFORE V1 fallback. This limitation stays latched until the
table state/epoch changes or the connection renegotiates. V1-unrepresentable prices/volume are
not silently wrapped: their V1 view is omitted, while V2 remains available.

## Cloud transport implementation

`upstream.realtime_full_fields` is a startup-only boolean, default false. With it enabled,
Cloud negotiates independently with Upcloud and each authenticated SDK. The upstream decoder
validates schema/table identity, symbol/code dictionary equality, current epoch and per-symbol
sequence. The dictionary carries no table IDs: table identity is checked against the registry
and pinned from admitted V2 rows, never guessed from a code suffix. Full-field and legacy views
are cached together, bounded by 64MiB encoded/code bytes and 65536 entries (not an RSS bound).
NULL projections and unrepresentable legacy values are counted; an unrepresentable V1 view is
omitted, not wrapped. Disconnect clears both views; table state/epoch changes invalidate them.

Live delivery and subscription replay use one per-client selector. STATUS is reliable control;
V2 is lossy/coalesced realtime under the existing 256KiB/512-frame client queue limits. Partial
frames survive invalidation, ACK/STATUS remain ordered, and control enqueue failure closes the
connection. A smaller SDK payload limit publishes `payload_limit` before legacy fallback and
stays latched until table state/epoch changes or reconnect. Symbol-map changes clear cached and
unstarted queued quotes. HTTP discovery advertisement and detailed per-client rollout metrics
remain pending. Tests use the actual receiver/server over loopback TCP, not the production service.

## SDK transport implementation

`RtdataClient` and `API` expose `realtime_protocol='legacy'|'auto'|'required_v2'`, default legacy.
Authentication starts OFFER. A connection-object/generation gate and one state lock cover
control sends, COMMIT activation and row admission. A real timer handles ACK timeout without
requiring another incoming frame. Failed control sends shutdown the matching socket; they never
start synchronous reconnect under the state lock. Late ACK/STATUS and old-epoch rows are ignored.

V2 Quote exposes immutable original fields and compatibility aliases, preserving NULL, float64,
signed integers and actual book depth. Legacy Quote construction remains mutable and compatible.
`realtime_status`, `on_realtime_status`, `wait_for_realtime`, `wait_for_quote` and
`RealtimeUnavailableError` distinguish negotiation, table readiness and first-quote availability.
Required mode never admits V1 quotes. Cache invalidations clear affected quotes/watermarks;
callbacks check current cache identity before invocation (already-running callbacks cannot be
cancelled). V2 cache budgets are 16MiB wire bytes and 65536 symbols, not Python RSS limits.

## Remaining transport integration

Actual shared-memory subscription lifecycle and full-process network matrices remain unverified;
the Upcloud pipeline test injects synthetic tables through the real callback without calling
`start()` on the DDB reader. The vendor SDK and runtime pipeline have not been sanitizer tested.
The pure machines use explicit generations, monotonic time and send outcomes for reproducible
tests. Their sanitizer results must not be reported as runtime-pipeline sanitizer coverage.
