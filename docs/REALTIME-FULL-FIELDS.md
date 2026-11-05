# Realtime quote fields

This document follows main source. `Quote/Kline.to_datetime()` is not included
in the published v0.3.7 wheel; see [installation](../README.md) and the
[market time guide](TIME-AND-MARKETS.md). Runnable examples with cleanup and
observation loops are indexed in [examples](../examples/README.md).

Both `API` and `RtdataClient` default to automatic V2 full-field negotiation.
Existing connection and subscription calls need no additional argument.

```python
import rtdata

api = rtdata.API(token="your_token")

@api.on_quote
def on_quote(quote):
    print(quote.symbol, quote.last)
    print(dict(quote.fields))

api.subscribe(["600519.SH"])
# Continue application work/waiting, then close the API in a finally block.
```

## Protocol selection

- `auto` (default): request extended quotes and fall back to the compatible
  quote format if negotiation is unavailable. Inspect `quote.wire_version`.
- `legacy`: explicitly retain the compatible quote format and mutable `Quote`
  construction. This remains an opt-out from automatic negotiation.
- `required_v2`: reject fallback. `connect()` waits for negotiation, not the
  first quote. `get_quote()` raises `RealtimeUnavailableError` if no current
  extended quote exists; `wait_for_quote()` waits until its timeout.

These options are accepted by both constructors. `subscribe()` is unchanged.
`wait_for_quote()` does not subscribe implicitly. A successful subscription or
negotiation does not mean a first quote has arrived.

## Additional markets

Since version 0.3.6, the SDK automatically includes every supported market
advertised by the connected server, including `overseas_futures` (schema 4)
and `crypto` (schema 5).
No market-enable or provider-selection argument is needed. Server-side routing
selects the data provider; client capability negotiation only selects the format.
The additional schemas support up to ten book levels, signed 64-bit quantities
and quantity precision fields. Actual available depth depends on the instrument.

| Public category | Schema | Defined book levels per side |
| --- | ---: | ---: |
| `cn_equity`, `hk_equity`, `us_equity` | 1 | 5 |
| `futures` | 2 | 5 |
| `fx` | 3 | 1 |
| `overseas_futures` | 4 | 10 |
| `crypto` | 5 | 10 |

These are schema capacities, not guaranteed available depth. US equities still
use the five-level public schema even if more levels are available elsewhere.
Null positions must not be removed and shifted to a different level.

The successful login response may advertise supported market IDs and frame
limits. These are re-evaluated on every connection. Without this metadata, the
SDK sends the original five-category offer understood by older strict parsers;
servers without V2 support retain six-field fallback. Existing category field
definitions and schema identities are unchanged. Since version 0.3.6, obsolete
provider-specific constructor keywords are no longer accepted. Remove them
from existing calls; no replacement argument is needed. Unsupported constructor
keywords raise `TypeError`. Explicit `realtime_protocol` modes remain supported.

`raw_bits` is a JSON string mapping source field-group numbers to hex-encoded
protobuf bytes. It preserves unnamed source fields without inventing names or
units. Missing named fields remain `None`; this includes fields unavailable
on a particular market. Book quantities preserve source integers; consult
`book_volume_precision` before displaying fractional units. The raw field is
bounded at 512 KiB, and the extended row at a negotiated maximum of 1 MiB;
oversized rows are rejected, not silently truncated.

Historical naive times for `.SGX`, `.HKFE`, `.OSE`, `.BMD`, `.CME`, `.CBOT`,
`.CBOE`, `.NYMEX` and `.COMEX` use their market timezone, including DST.
Confirmed crypto suffixes, `.CFBM` and `.FX` use UTC. An aware datetime or
UTC millisecond value always retains its original instant.

## Fields and compatibility

### Timestamp display

`quote.timestamp` (the `trade_time` alias) and `kline.timestamp` keep the
received integer milliseconds. On UTC-normalized servers these represent UTC
instants, regardless of market. Display conversion never adds an offset to the
stored timestamp:

```python
quote.to_datetime()                 # aware datetime in the symbol's market zone
quote.to_datetime("UTC")            # the same instant in UTC
quote.to_datetime("Asia/Shanghai")  # the same instant in Beijing time
kline.to_datetime()                 # same helper for historical bar labels
```

`tz` also accepts a `datetime.tzinfo`, for example `timezone.utc`. Omitting it
selects Shanghai for A shares/domestic futures, Hong Kong for Hong Kong shares,
New York (with DST) for US shares, the configured market zone for overseas
futures, and UTC for supported crypto/FX codes (including `EUR.USD`). Unknown
markets and a `Kline` without a symbol require an explicit timezone.
On systems without an IANA timezone database, install `rtdata[market-time]`.

The helper is display-only and cannot repair an old server's timezone-less
wall-time timestamp. Deploy the server-side normalization first. Repeated
trade timestamps can accompany order-book updates; display conversion does not
substitute receive time or round historical bar labels.

Extended quotes are read-only `Quote` instances. Available fields can be read
as attributes or through the immutable `quote.fields` mapping. Export with
`dict(quote.fields)`; legacy dataclass serialization is not a complete export.

- NULL values are `None`; real zero stays zero. Fields outside the schema are
  absent from `fields`. Use this mapping or `available_fields` to check support:
  inherited compatibility attributes can retain defaults even when not supplied.
- Floating-point values keep double precision; integer values remain Python
  integers. Negative values, NaN and infinity are not replaced with zero.
- Existing aliases remain: `bid=bid_price1`, `ask=ask_price1`,
  `timestamp=trade_time`, `prev_close=pre_close`, `turnover=amount`.
- `trade_time` preserves the received integer timestamp; historical input
  timezone conversion does not apply to it.
- `bid_prices`, `bid_volumes`, `ask_prices`, `ask_volumes` return independent
  lists with missing-value positions preserved. Available depth varies by market.
- `wire_version`, `schema_id`, `schema_version`, `source_id`, `source_epoch`
  and `sequence` describe quote format and freshness.
- The compatibility attribute `source_table` contains a public category:
  `cn_equity`, `hk_equity`, `futures`, `us_equity`, `fx`, `overseas_futures` or `crypto`.

Code that modifies a quote must keep its own copy. Code that exports quotes
should use `dict(quote.fields)` to include all available fields.

## Status and lifecycle

`realtime_status.tables` is keyed by the public categories above. Each status
provides `ready`, `reason` and `epoch`; negotiated support does not imply every
category is ready. Use `on_realtime_status` for changes. Do not block inside
callbacks or call synchronous query/wait methods from receive callbacks.

Disconnects, authentication failures and close clear cached quotes. Category
state changes invalidate affected quotes. A quote retained by application code
is a past immutable snapshot, not a live readiness indicator.

Quotes use latest-value-per-symbol semantics, not a lossless event stream.
Slow callbacks may skip superseded quotes. `realtime_statistics` reports
rejected rows, cached wire bytes and tracked symbols. Callback queues and the
quote cache are bounded; their wire-byte limits are not total Python memory limits.
