# Realtime quote fields

Version 0.3.5 defaults to automatic negotiation in both `API` and `RtdataClient`.
Existing connection and subscription calls need no additional argument.

```python
import rtdata

api = rtdata.API(token="your_token")

@api.on_quote
def on_quote(quote):
    print(quote.symbol, quote.last)
    print(dict(quote.fields))

api.subscribe(["600519.SH"])
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

## Fields and compatibility

Extended quotes are read-only `Quote` instances. Available fields can be read
as attributes or through the immutable `quote.fields` mapping. Export with
`dict(quote.fields)`; legacy dataclass serialization is not a complete export.

- Missing values are `None`; real zero stays zero. Unavailable fields are absent
  from `fields` and raise `AttributeError`. Inspect `available_fields` first.
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
  `cn_equity`, `hk_equity`, `futures`, `us_equity` or `fx`.

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
