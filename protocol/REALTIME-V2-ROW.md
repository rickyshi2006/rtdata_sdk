# Realtime V2 row contract, revision 1

Status: codec implemented; Upcloud opt-in callback/transport and Cloud/SDK TCP adapters tested
in isolation. No production deployment or default protocol change.
This contract is independent of History V2. Existing snapshot types 0x01/0x02 keep their 32-byte rows.

## Registry

`realtime_v2_registry.json` is the canonical registry. Canonical hash input is UTF-8 JSON with
sorted keys, ASCII escapes, no whitespace (`separators=(',', ':')`). Hash each schema object
the same way for its schema hash. The registry hash is:

`0acc8d004af8a4a16e6a8dec97a21d222941d0877076cd1eff3c548f8efe0269`

Sources 1/2/4 (stock/HK/US) use schema 1 (35 columns); source 3 (futures) uses schema 2
(47 columns); source 5 (forex) uses schema 3 (15 columns). Schema field order is wire order.
Types are DDB types, not inferred from field names. Unknown source/schema/version is rejected.

## Framing

One V2 message contains exactly one complete snapshot for the header's nonzero symbol_id.
The existing outer header remains `!IIH`: payload_length, symbol_id, message_type.
Types 0x60/0x61/0x62 are capability OFFER/ACK/COMMIT; 0x63 is the V2 row;
0x64 is SOURCE_STATUS. Their offline contract/state machines are specified in
`REALTIME-V2-NEGOTIATION.md`. Runtime transport integration is not enabled yet.

Payload envelope is exactly 32 bytes, all little-endian:

| Offset | Type | Meaning |
| --- | --- | --- |
| 0 | uint16 | schema_version, must be 1 |
| 2 | uint16 | schema_id |
| 4 | uint16 | source_id |
| 6 | uint16 | field_count, exact registry count |
| 8 | uint64 | source_epoch, nonzero |
| 16 | uint64 | snapshot_seq, nonzero |
| 24 | uint16 | bitmap byte count, ceil(field_count/8) |
| 26 | uint16 | flags, must be zero |
| 28 | uint32 | values byte count |

The envelope is followed by the NULL bitmap (LSB first, 1=NULL) and values. Unused high bitmap
bits must be zero. NULL fields consume no value bytes. Values are concatenated in registry order:

- DOUBLE: IEEE754 binary64 little-endian; preserve signed zero and nonfinite bit patterns.
- LONG/TIMESTAMP: signed int64 little-endian; INT: signed int32; CHAR: signed int8.
- SYMBOL/STRING: uint16 little-endian byte length then strict UTF-8; no DDB symbol dictionary IDs.

Limits: payload <=16384 bytes, each string <=1024 bytes. Exactly consume the declared payload;
reject truncation/trailing data, wrong lengths, flags, types, invalid UTF-8 and overflow before
allocation. Empty text is allowed except code. Code and trade_time must be non-NULL; code must
be nonempty without embedded NUL. Other source NULLs are allowed. No timestamp timezone,
volume unit or price normalization is performed by the codec.

Epoch generation, replay protection, mapping code to symbol_id, source availability and connection
authorization belong to the transport layer and remain pending. A codec accepting a row is NOT
proof that it is authorized, fresh or valid for a particular client. Epoch IDs must not be numerically
ordered as timestamps; sequence comparison is meaningful only within the admitted epoch.

## V1 projection

The projection takes an explicit nonzero uint32 symbol_id, then encodes bid_price1/ask_price1/last
as float32 LE, volume as uint64 BE, trade_time as int64 BE, exactly matching the existing layout.
NULL price/volume maps to zero with an explicit null_fields count. Negative volume or nonfinite/
float32-overflow price is rejected for V1 only. V2 retains the original values. No silent unsigned wrap.
Finite float64-to-float32 uses the default round-to-nearest conversion; test goldens pin its result.

## Reproduction and distribution

Each repository contains its own checked-in header/registry/vector files and builds without a sibling
repository. During cross-project development, `scripts/realtime_schema_codegen.py --check` in the
integration workspace checks generated schema files and identical C++ codecs/tests; manual C++
edits are made in upcloud-realtime first, then explicitly synchronized by that generator.
SDK codec is independently implemented, not generated from C++.

`tests/fixtures/realtime_v2.tsv` contains source_id, full/null mode, expected payload hex and V1 hex.
The independent stdlib oracle never imports either production codec. Both C++ copies and Python
must match the same 10 vectors. Integration verification also compares mutated/truncated payloads.

Data-preservation codecs are tested offline. Upcloud additionally has opt-in callback-to-socket
tests with synthetic DDB tables; Cloud/SDK interoperability uses a synthetic upstream over TCP.
SDK connections default to legacy and require explicit opt-in to enable V2.
