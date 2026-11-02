import json
import inspect
import tempfile
import unittest
from rtdata import API, RtdataClient
from rtdata import _realtime_capability as cap
from rtdata import _realtime_protocol as wire
from rtdata._realtime_schema import SCHEMAS
from rtdata._realtime_client import _login_realtime_capability
from rtdata import _protocol as proto
from test_realtime_client import FakeConnection


def login_hint(markets=127, limit=1048576):
    return b'\x01' + json.dumps({'realtime_v2': {
        'version': 1, 'market_mask': markets, 'max_payload': limit}}).encode()


def extended_row(source, sequence=1):
    schema, code = (4, 'CNmain.SGX') if source == 6 else (5, 'BTCUSDT.BINANCE')
    fields = dict.fromkeys(name for name, _ in SCHEMAS[schema])
    fields.update(code=code, trade_time=1793497529283, last=78506.31,
                  bid_price1=78506.30, ask_price1=78506.31,
                  bid_volume1=176720000, ask_volume1=8638690000,
                  depth_levels=1, book_volume_precision=9,
                  raw_bits=json.dumps({'3': 'ab' * 12000}))
    return wire.Snapshot.from_fields(source, 91, sequence, fields)


class ExtendedRealtimeTest(unittest.TestCase):
    def test_deprecated_overrides_keep_existing_calls_compatible(self):
        with tempfile.TemporaryDirectory() as directory:
            for factory in (API, RtdataClient):
                for enabled, mask in ((False, 31), (True, 127)):
                    with self.assertWarns(DeprecationWarning):
                        client = factory('test', symbol_cache_dir=directory,
                                         history_cache_enabled=False, realtime_futu_markets=enabled)
                    try:
                        state = client._client if isinstance(client, API) else client
                        self.assertEqual(state._realtime.local.sources, mask)
                        self.assertEqual(cap.intersection(cap.supported(cap.Role.CLOUD), state._realtime.local).sources, 31)
                    finally:
                        client.close()

    def test_public_defaults_negotiate_all_markets_without_provider_option(self):
        with tempfile.TemporaryDirectory() as directory:
            for factory in (API, RtdataClient):
                self.assertNotIn('realtime_futu_markets', inspect.signature(factory).parameters)
                public = factory('test', symbol_cache_dir=directory,
                                 async_callbacks=False, history_cache_enabled=False)
                try:
                    client = public._client if isinstance(public, API) else public
                    client._conn = FakeConnection()
                    client._symbol_map.update_from_dict({i: extended_row(i).fields['code'] for i in (6, 7)})
                    client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, login_hint(), 1, client._conn)
                    client._authenticated = True
                    offer = cap.Capability.decode(client._conn.sent[-1][10:])
                    self.assertEqual((offer.sources, offer.max_payload), (127, 1048576))
                    ack = cap.intersection(cap.supported(cap.Role.CLOUD, 127, 1048576), offer)
                    client._dispatch_message(cap.ACK, 0, ack.encode(), 1, client._conn)
                    states = tuple(cap.SourceState(i, True, cap.Reason.READY, 91) for i in range(1, 8))
                    client._dispatch_message(cap.SOURCE_STATUS, 0, cap.Status(offer.nonce, 1, states).encode(), 1, client._conn)
                    for source in (6, 7):
                        row = extended_row(source)
                        client._dispatch_message(cap.SNAPSHOT, source, wire.encode(row), 1, client._conn)
                        quote = public.get_quote(row.fields['code'])
                        self.assertEqual(quote.wire_version, 2)
                        self.assertEqual(dict(quote.fields), dict(row.fields))
                        self.assertEqual(quote.bid_volumes[0], 176720000)
                        self.assertEqual(quote.ask_volumes[0], 8638690000)
                        self.assertEqual(quote.last, 78506.31)
                finally:
                    public.close()

    def test_login_hint_is_bounded_optional_and_connection_local(self):
        baseline = cap.supported(cap.Role.SDK)
        for payload in (b'\x01', b'\x00', b'\x01hello', b'\x01null', b'\x01[]', b'\x01\xff',
                        b'\x01' + b'[' * 1500 + b']' * 1500,
                        b'\x01' + b'x' * 4096, login_hint(0), login_hint(True),
                        login_hint(127, True), login_hint(127, 1048577), login_hint(127, 63),
                        b'\x01{"realtime_v2":{"version":true,"market_mask":127,"max_payload":1048576}}',
                        b'\x01{"realtime_v2":{"version":2}}'):
            self.assertEqual(_login_realtime_capability(payload), baseline)
        self.assertEqual(_login_realtime_capability(login_hint(255)).sources, 127)
        self.assertEqual(_login_realtime_capability(login_hint(31, 16384)), baseline)
        with tempfile.TemporaryDirectory() as directory:
            client = RtdataClient('test', symbol_cache_dir=directory, history_cache_enabled=False)
            try:
                client._conn = FakeConnection()
                for payload, mask in ((login_hint(), 127), (b'\x01', 31), (login_hint(), 127)):
                    client._reset_realtime('test_reconnect')
                    client._conn.generation += 1
                    client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, payload,
                                             client._conn.generation, client._conn)
                    offer = cap.Capability.decode(client._conn.sent[-1][10:])
                    self.assertEqual(offer.sources, mask)
            finally:
                client.close()

    def test_unknown_options_still_fail(self):
        for factory in (API, RtdataClient):
            with self.assertRaises(TypeError):
                factory('test', realtime_typo=True)
            with self.assertRaises(TypeError):
                factory('test', realtime_futu_markets='yes')

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
