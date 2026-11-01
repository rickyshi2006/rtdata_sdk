import json
import tempfile
import unittest
from rtdata import API, RtdataClient
from rtdata import _realtime_capability as cap
from rtdata import _realtime_protocol as wire
from rtdata._realtime_schema import SCHEMAS


class ExtendedRealtimeTest(unittest.TestCase):
    def test_optional_sources_keep_default_offer_compatible(self):
        with tempfile.TemporaryDirectory() as directory:
            for factory in (API, RtdataClient):
                for enabled, mask in ((False, 31), (True, 127)):
                    client = factory('test', symbol_cache_dir=directory,
                                     history_cache_enabled=False, realtime_futu_markets=enabled)
                    try:
                        state = client._client if isinstance(client, API) else client
                        self.assertEqual(state._realtime.local.sources, mask)
                        self.assertEqual(cap.intersection(cap.supported(cap.Role.CLOUD), state._realtime.local).sources, 31)
                    finally:
                        client.close()

    def test_new_schema_full_depth_precision_and_nulls(self):
        for source, schema, code in ((6, 4, 'CNmain.SGX'), (7, 5, 'BTCUSDT.BINANCE')):
            fields = dict.fromkeys(name for name, _ in SCHEMAS[schema])
            fields.update(code=code, trade_time=1793497529283, last=100.0,
                          bid_price10=99.9, bid_volume10=5000000001,
                          stock_id='18446744073709551615', raw_bits=json.dumps({'3': 'ab' * 1000}))
            row = wire.Snapshot.from_fields(source, 10, 20, fields)
            encoded = wire.encode(row)
            self.assertEqual(wire.decode(encoded), row)
            self.assertEqual(row.fields['bid_volume10'], 5000000001)
            self.assertIsNone(row.fields['volume'])
            from rtdata import Quote
            quote = Quote.from_snapshot(row, 1, code)
            self.assertEqual(len(quote.bid_volumes), 10)
            self.assertEqual(quote.bid_volumes[-1], 5000000001)

    def test_extended_negotiation_status_and_schema_mismatch(self):
        local = cap.supported(cap.Role.CLOUD, 127)
        offer = cap.supported(cap.Role.SDK, 127)
        selected = cap.intersection(local, offer)
        self.assertEqual([s.id for s in selected.schemas], [1, 2, 3, 4, 5])
        states = tuple(cap.SourceState(i, True, cap.Reason.READY, 9) for i in range(1, 8))
        status = cap.Status(1, 1, states)
        self.assertEqual(cap.Status.decode(status.encode()), status)
        from dataclasses import replace
        broken = replace(offer, schemas=offer.schemas[:-1] + (replace(offer.schemas[-1], hash=bytes(32)),))
        self.assertEqual(cap.intersection(local, broken).sources, 63)

    def test_large_raw_field_boundaries(self):
        fields = dict.fromkeys(name for name, _ in SCHEMAS[5])
        fields.update(code='BTC.CRYPTO', trade_time=1, raw_bits='x' * wire.RAW_BITS_BYTES)
        row = wire.Snapshot.from_fields(7, 1, 1, fields)
        self.assertEqual(wire.decode(wire.encode(row)), row)
        fields['raw_bits'] += 'x'
        with self.assertRaises(ValueError):
            wire.Snapshot.from_fields(7, 1, 1, fields)
        encoded = wire.encode(row)
        for end in (0, 31, 33, len(encoded) - 1):
            with self.assertRaises(ValueError):
                wire.decode(encoded[:end])

    def test_new_market_wall_times(self):
        from datetime import datetime, timezone
        from rtdata._history_time import normalize_endpoint, _FUTURES_ZONES, _UTC_FEEDS
        from zoneinfo import ZoneInfo
        for suffix, zone in {**_FUTURES_ZONES, **dict.fromkeys(_UTC_FEEDS, 'UTC')}.items():
            for month in (7, 12):
                local = datetime(2026, month, 1, 9, 30)
                expected = int(local.replace(tzinfo=ZoneInfo(zone)).timestamp() * 1000)
                self.assertEqual(normalize_endpoint(local, symbol='TEST.' + suffix, is_end=False), expected)
                self.assertEqual(normalize_endpoint(local.replace(tzinfo=timezone.utc), symbol='TEST.' + suffix, is_end=False),
                                 int(local.replace(tzinfo=timezone.utc).timestamp() * 1000))
        self.assertEqual(normalize_endpoint('2026-10-30 09:00', symbol='CNmain.SGX', is_end=False),
                         int(datetime(2026, 10, 30, 1, tzinfo=timezone.utc).timestamp() * 1000))
        for suffix in ('CME', 'CBOT', 'CBOE', 'NYMEX', 'COMEX'):
            for text in ('2026-03-08 02:30', '2026-11-01 01:30'):
                with self.assertRaises(ValueError):
                    normalize_endpoint(text, symbol='TEST.' + suffix, is_end=False)


if __name__ == '__main__':
    unittest.main()
