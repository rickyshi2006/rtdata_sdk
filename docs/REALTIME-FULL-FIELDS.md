# Realtime full-field API (automatic negotiation)

Since 0.3.5rc2, both `RtdataClient` and `API` default to `realtime_protocol='auto'`.
Existing construction and subscription calls need no new argument to receive full fields
from a capable server. Older servers fall back to V1. Explicit `legacy` and `required_v2`
remain available. This expands the fields from existing DDB stream tables, not data sources.

```python
from rtdata import RtdataClient, RealtimeUnavailableError

client = RtdataClient(
    token="test-token", host="127.0.0.1", port=9100,
)

@client.on_realtime_status
def status_changed(status):
    for name, table in status.tables.items():
        print(name, table.ready, table.reason, table.epoch)

client.connect()
client.subscribe(["600519.SH"])
try:
    quote = client.wait_for_quote("600519.SH", timeout=5)
    print(quote.fields, quote.bid_prices, quote.ask_volumes)
finally:
    client.close()
```

`API(token=...)` also negotiates automatically. Both constructors accept
`realtime_protocol='required_v2'` when fallback must be rejected, or `'legacy'` for old behavior.

## Modes and readiness

- `legacy`: send no realtime capability offer; keep six-field V1 decoding and mutable legacy
  `Quote` construction/defaults. V1 `available_fields` only includes `bid_price1`, `ask_price1`,
  `last`, `volume`, `trade_time`. The route ID and dictionary symbol are not claimed as DDB columns.
- `auto` (default): negotiate after authentication; ACK timeout falls back to V1. Each row is a new object;
  fallback never retains the previous V2 row's extra fields. Inspect `quote.wire_version`.
- `required_v2`: `connect()` waits for negotiation (not table readiness). No common schema/ACK
  timeout raises `RealtimeUnavailableError`. Incoming V1 rows never enter cache or callbacks.
  `get_quote()` raises if a current full-field row is absent; `wait_for_quote()` waits until its
  timeout and then raises, rather than returning incomplete data. A valid V2 subset does not
  imply all tables are ready: inspect `realtime_status.tables`.

`wait_for_realtime(timeout=2)` waits for negotiation only. `wait_for_quote(symbol, timeout=5)`
does not subscribe implicitly. A successful subscription still does not imply a first quote
has arrived. Table identity is the registered payload table, never guessed from code suffixes.
Discovery feature labels are informational; automatic negotiation attempts the authenticated TCP
handshake even when an older discovery response has no realtime feature labels.

`on_realtime_status` receives an immutable status snapshot; `on_error` receives a
`RealtimeUnavailableError` object for required-mode protocol fallback and reported table
unavailability. Other pre-existing error paths can still deliver strings. Status callbacks run
on receive/timer threads: do not block or call synchronous wait/query methods inside them.

## Quote contract

V2 quotes are `isinstance(quote, Quote)` and read-only. All DDB field names are available as
attributes and through `quote.fields`, an immutable mapping. Use `dict(quote.fields)` for export;
dataclass serialization of legacy compatibility members is not a full-field export.

- Source NULL is `None`; real zero stays zero; absent schema fields are absent from `fields`
  and raise `AttributeError`. `available_fields` separates absent from present-but-NULL.
- DOUBLE remains Python float64; signed LONG/INT/CHAR remain Python integers. Negative values,
  NaN and infinities are preserved, not converted to zero. JSON users must choose their own
  policy for non-finite values.
- `bid=bid_price1`, `ask=ask_price1`, `timestamp=trade_time`, `prev_close=pre_close`,
  `turnover=amount`. These are views of the same values, not independently stored copies.
- `code` retains the DDB string; `symbol` is the checked dictionary code. `trade_time` is the
  original signed integer. No historical timezone conversion or UTC assumption is applied.
- `bid_prices/bid_volumes/ask_prices/ask_volumes` return independent lists with NULL positions
  preserved. Forex has one level; the other schemas have five. Mutating a list does not change
  the quote.
- Metadata: `wire_version`, `schema_id`, `schema_version`, `source_id`, `source_table`,
  `source_epoch`, `sequence`. The table's field count is 35, 47 or 15, not 167 per row.

## Lifecycle and limits

Disconnect, authentication failure, terminal token status and explicit close clear cached
quotes; table state/epoch changes invalidate affected rows. Queued callbacks check current
cache identity before invocation. An already-running user callback cannot be cancelled; a
quote retained by application code is a historical immutable snapshot, not a live readiness flag.
Do not treat retained objects as fresh without checking current client state.

SDK coalescing remains latest-by-symbol, not a lossless tick stream. V2 cache has a 16MiB
wire-byte accounting budget and 65536 tracked-symbol limit, not a total Python RSS limit.
Async callbacks keep the existing item-count bound; backlog can skip superseded/invalidated
quotes. `realtime_statistics` exposes rejected rows, cached wire bytes and tracked symbols.

## Validation scope

Synthetic tests include the full DDB/shared-memory/Upcloud/Cloud/SDK functional chain,
fragmented loopback TCP, reconnect and legacy compatibility. Production throughput acceptance
is not complete. Version 0.3.5rc2 also retains the existing market-local history input and UTC
history-cache fixes; realtime trade_time remains the unmodified DDB value. A built candidate
wheel is not evidence of production activation or customer installation.
