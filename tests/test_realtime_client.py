import math
import struct
import tempfile
import time
import unittest
from pathlib import Path

from rtdata import API, Quote, RtdataClient, RealtimeUnavailableError
from rtdata import _protocol as proto
from rtdata import _realtime_capability as cap
from rtdata import _realtime_protocol as wire


def fixture(source=1, null=False, epoch=91, seq=1, code=None):
    path = Path(__file__).parent / 'fixtures/realtime_v2.tsv'
    for line in path.read_text().splitlines():
        if line.startswith('#') or not line:
            continue
        parts = line.split('\t')
        if int(parts[0]) == source and parts[1] == ('null' if null else 'full'):
            snap = wire.decode(bytes.fromhex(parts[2]))
            fields = dict(snap.fields)
            if code is not None:
                fields['code'] = code
            return wire.Snapshot.from_fields(source, epoch, seq, fields)
    raise AssertionError('fixture not found')


class FakeConnection:
    generation = 1
    connected = True
    reconnecting = False

    def __init__(self):
        self.sent = []
        self.fail_type = None
        self.before_send = None

    def send_for_generation(self, frame, generation):
        assert generation == self.generation
        _, _, kind = proto.decode_header(frame[:10])
        if self.before_send:
            self.before_send(kind)
        self.sent.append(frame)
        return kind != self.fail_type

    def abort_generation(self, generation):
        if generation == self.generation:
            self.connected = False

    def close(self):
        self.connected = False


class RealtimeClientTest(unittest.TestCase):
    def client(self, mode=None, **kwargs):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        if mode is not None:
            kwargs['realtime_protocol'] = mode
        client = RtdataClient(token='test', async_callbacks=False,
                              auto_reconnect=False, symbol_cache_dir=tmp.name,
                              history_cache_enabled=False, **kwargs)
        self.addCleanup(client.close)
        client._conn = FakeConnection()
        client._symbol_map.update_from_dict({sid: fixture(sid).fields['code'] for sid in range(1, 6)})
        return client

    def negotiate(self, client, sources=cap.SOURCE_MASK):
        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b'\x01', 1, client._conn)
        client._authenticated = True
        offer = cap.Capability.decode(client._conn.sent[-1][10:])
        ack = cap.intersection(cap.supported(cap.Role.CLOUD, sources), offer)
        client._dispatch_message(cap.ACK, 0, ack.encode(), 1, client._conn)
        return offer.nonce

    def status(self, client, nonce, revision=1, epoch=91, ready=True, ids=range(1, 6)):
        status = cap.Status(nonce, revision, tuple(cap.SourceState(sid, ready,
            cap.Reason.READY if ready else cap.Reason.SUSPENDED, epoch if ready else 0) for sid in ids))
        client._dispatch_message(cap.SOURCE_STATUS, 0, status.encode(), 1, client._conn)

    def quote(self, client, snap, sid=None, generation=1, conn=None):
        client._dispatch_message(cap.SNAPSHOT, sid or snap.source_id, wire.encode(snap),
                                 generation, conn or client._conn)

    def test_public_defaults_negotiate_without_protocol_argument(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        for factory in (RtdataClient, API):
            with self.subTest(factory=factory.__name__):
                public = factory('test', async_callbacks=False, symbol_cache_dir=tmp.name,
                                 history_cache_enabled=False)
                self.addCleanup(public.close)
                client = public._client if isinstance(public, API) else public
                client._conn = FakeConnection()
                client._symbol_map.update_from_dict({1: fixture().fields['code']})
                self.assertEqual(public.realtime_status.mode, 'auto')
                nonce = self.negotiate(client)
                self.status(client, nonce)
                self.quote(client, fixture())
                quote = public.get_quote(fixture().fields['code'])
                self.assertEqual(quote.wire_version, 2)
                self.assertEqual(dict(quote.fields), dict(fixture().fields))

    def test_explicit_protocol_modes_are_preserved(self):
        client = self.client('legacy')
        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b'\x01', 1, client._conn)
        self.assertEqual(client._conn.sent, [])
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        for factory in (RtdataClient, API):
            for mode in ('auto', 'legacy', 'required_v2'):
                with self.subTest(factory=factory.__name__, mode=mode):
                    public = factory('test', async_callbacks=False, realtime_protocol=mode,
                                     symbol_cache_dir=tmp.name, history_cache_enabled=False)
                    self.addCleanup(public.close)
                    self.assertEqual(public.realtime_status.mode, mode)
        with self.assertRaises(ValueError):
            self.client('wrong')

    def test_model_all_fields_aliases_null_books_precision(self):
        for sid in range(1, 6):
            for null in (False, True):
                snap = fixture(sid, null)
                quote = Quote.from_snapshot(snap, sid, snap.fields['code'])
                self.assertIsInstance(quote, Quote)
                self.assertEqual(dict(quote.fields), dict(snap.fields))
                self.assertEqual(quote.available_fields, frozenset(snap.fields))
                for name, value in snap.fields.items():
                    self.assertEqual(getattr(quote, name), value)
                self.assertEqual(quote.prev_close, quote.pre_close)
                self.assertEqual(quote.timestamp, quote.trade_time)
                self.assertEqual(quote.turnover, quote.amount)
                self.assertEqual(quote.bid, quote.bid_price1)
                self.assertEqual(len(quote.bid_prices), 1 if sid == 5 else 5)
                self.assertEqual(len(quote.ask_volumes), 1 if sid == 5 else 5)
                book = quote.bid_prices
                book[0] = 999
                self.assertNotEqual(quote.bid_prices[0], 999)
                with self.assertRaises(AttributeError):
                    quote.last = 999
                with self.assertRaises(TypeError):
                    quote.fields['last'] = 999
                self.assertIn('Quote(', str(quote))
                if sid == 5:
                    self.assertNotIn('bid_price2', quote.available_fields)
                    with self.assertRaises(AttributeError):
                        _ = quote.bid_price2
        legacy = Quote('A', 1, 1., 2., 3., 4, 5)
        legacy.last = 7
        self.assertEqual(legacy.fields['last'], 7)
        self.assertEqual(legacy.open, 0)
        self.assertNotIn('open', legacy.available_fields)
        self.assertNotIn('code', legacy.available_fields)

    def test_nonfinite_and_signed_values_are_preserved(self):
        fields = dict(fixture(3).fields)
        fields.update(last=float('nan'), bid_price1=float('-inf'), ask_price1=float('inf'),
                      volume=-9007199254740993, trade_time=-1700000000123, option_class=-128)
        quote = Quote.from_snapshot(wire.decode(wire.encode(wire.Snapshot.from_fields(3, 1, 1, fields))), 3, fields['code'])
        self.assertTrue(math.isnan(quote.last))
        self.assertEqual(quote.bid, float('-inf'))
        self.assertEqual(quote.volume, -9007199254740993)
        self.assertEqual(quote.timestamp, -1700000000123)
        self.assertEqual(quote.option_class, -128)
        self.assertIn('nan', str(quote))

    def test_commit_before_activation_and_status_before_quote(self):
        client = self.client()
        stages = []
        client._conn.before_send = lambda kind: stages.append((kind, client._realtime.phase))
        nonce = self.negotiate(client)
        self.assertEqual(stages, [(cap.OFFER, 'offer_pending'), (cap.COMMIT, 'commit_pending')])
        self.assertTrue(client.realtime_status.negotiated)
        snap = fixture()
        self.quote(client, snap)
        self.assertIsNone(client.get_quote(snap.fields['code']))
        self.status(client, nonce)
        self.quote(client, snap)
        self.assertEqual(client.get_quote(snap.fields['code']).fields, snap.fields)

    def test_identity_sequence_epoch_generation_and_status_invalidation(self):
        client = self.client()
        nonce = self.negotiate(client)
        self.status(client, nonce)
        first = fixture(seq=10)
        self.quote(client, first)
        saved = client.get_quote(first.fields['code'])
        for bad in (fixture(seq=9), fixture(epoch=90), fixture(code='WRONG')):
            self.quote(client, bad)
        self.quote(client, fixture(seq=11), generation=0)
        self.quote(client, fixture(seq=11), conn=FakeConnection())
        self.assertIs(client.get_quote(first.fields['code']), saved)
        client._symbol_map.update_from_dict({1: first.fields['code'], 6: 'B.SH'})
        self.quote(client, fixture(seq=1, code='B.SH'), sid=6)
        self.assertEqual(client.get_quote('B.SH').sequence, 1)
        self.status(client, nonce, revision=2, ready=False)
        self.assertIsNone(client.get_quote(first.fields['code']))
        self.status(client, nonce, revision=1)
        self.quote(client, first)
        self.assertIsNone(client.get_quote(first.fields['code']))
        self.status(client, nonce, revision=3, epoch=92)
        self.quote(client, fixture(epoch=92))
        self.assertEqual(client.get_quote(first.fields['code']).sequence, 1)

    def test_auto_fallback_replaces_full_object_and_clears_extra_fields(self):
        client = self.client()
        nonce = self.negotiate(client)
        self.status(client, nonce)
        snap = fixture()
        self.quote(client, snap)
        legacy, _ = wire.project_legacy(snap, 1)
        client._dispatch_message(1, 1, legacy, 1, client._conn)
        self.assertEqual(client.get_quote(snap.fields['code']).wire_version, 2)
        self.status(client, nonce, revision=2, ready=False)
        client._dispatch_message(1, 1, legacy, 1, client._conn)
        quote = client.get_quote(snap.fields['code'])
        self.assertEqual(quote.wire_version, 1)
        self.assertNotIn('open', quote.available_fields)
        self.assertEqual(quote.bid_prices, [])

    def test_required_v2_never_delivers_v1(self):
        client = self.client('required_v2')
        errors, quotes = [], []
        client.on_error(errors.append)
        client.on_quote(quotes.append)
        nonce = self.negotiate(client)
        legacy, _ = wire.project_legacy(fixture(), 1)
        client._dispatch_message(1, 1, legacy, 1, client._conn)
        self.assertEqual(quotes, [])
        self.assertIsInstance(errors[0], RealtimeUnavailableError)
        with self.assertRaises(RealtimeUnavailableError):
            client.get_quote(fixture().fields['code'])
        self.status(client, nonce)
        self.quote(client, fixture())
        self.assertEqual(client.wait_for_quote(fixture().fields['code'], .01).wire_version, 2)
        self.status(client, nonce, revision=2, ready=False)
        with self.assertRaises(RealtimeUnavailableError):
            client.wait_for_quote(fixture().fields['code'], .01)

    def test_timeout_without_more_input_and_late_ack(self):
        client = self.client()
        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b'\x01', 1, client._conn)
        offer = cap.Capability.decode(client._conn.sent[0][10:])
        deadline = time.monotonic() + 2
        while client.realtime_status.phase != 'fallback' and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertEqual(client.realtime_status.reason, 'ack_timeout')
        ack = cap.intersection(cap.supported(cap.Role.CLOUD), offer)
        client._dispatch_message(cap.ACK, 0, ack.encode(), 1, client._conn)
        self.assertEqual(len(client._conn.sent), 1)

    def test_send_failure_closes_and_auth_failure_never_offers(self):
        client = self.client()
        client._conn.fail_type = cap.COMMIT
        self.negotiate(client)
        self.assertEqual(client.realtime_status.phase, 'closed')
        self.assertFalse(client._conn.connected)
        other = self.client()
        other._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b'\x00bad', 1, other._conn)
        self.assertEqual(other._conn.sent, [])

    def test_stale_queued_callback_and_reconnect_nonce(self):
        client = self.client()
        nonce = self.negotiate(client)
        self.status(client, nonce)
        self.quote(client, fixture())
        quote = client.get_quote(fixture().fields['code'])
        seen = []
        client.on_quote(seen.append)
        old_conn = client._conn
        client._handle_disconnected('test')
        client._run_quote_callbacks(quote)
        self.assertEqual(seen, [])
        client._conn = FakeConnection()
        next_nonce = self.negotiate(client)
        self.assertNotEqual(next_nonce, nonce)
        self.status(client, nonce)
        self.quote(client, fixture(), conn=old_conn)
        self.assertIsNone(client.get_quote(fixture().fields['code']))
        self.status(client, next_nonce, epoch=92)
        self.quote(client, fixture(epoch=92))
        self.assertEqual(len(seen), 1)

    def test_schema_subset_and_malformed_status(self):
        client = self.client()
        nonce = self.negotiate(client, sources=1)
        self.status(client, nonce, ids=[1])
        self.assertTrue(client.realtime_status.tables['cn_equity'].ready)
        self.assertFalse(client.realtime_status.tables['fx'].ready)
        client._dispatch_message(cap.SOURCE_STATUS, 0, b'bad', 1, client._conn)
        self.assertEqual(client.realtime_status.phase, 'closed')
        self.assertFalse(client._conn.connected)

    def test_symbol_map_change_and_capacity_invalidate_cached_quote(self):
        client = self.client()
        nonce = self.negotiate(client)
        self.status(client, nonce)
        self.quote(client, fixture())
        client._realtime_cache_bytes = 16 * 1024 * 1024
        fields = dict(fixture(seq=2).fields)
        fields['trading_phase_code'] = 'X' * 1024
        self.quote(client, wire.Snapshot.from_fields(1, 91, 2, fields))
        self.assertIsNone(client.get_quote(fields['code']))
        self.assertEqual(client._realtime_watermarks[1][2], 2)
        client._reset_realtime('capacity_test')
        client._realtime_cache_bytes = 0
        nonce = self.negotiate(client)
        self.status(client, nonce)
        self.quote(client, fixture())
        self.assertIsNotNone(client.get_quote(fixture().fields['code']))
        code = b'NEW.SH'
        client._handle_symbol_map(struct.pack('!IIH', 1, 1, len(code)) + code)
        self.assertFalse(client._quote_cache)
        self.assertEqual(client.realtime_statistics['tracked_symbols'], 0)

    def test_close_and_replaced_disconnect_cannot_repopulate_cache(self):
        client = self.client('legacy')
        client._auth_success = True
        old = client._conn
        payload, _ = wire.project_legacy(fixture(), 1)
        client._dispatch_message(1, 1, payload, 1, old)
        self.assertIsNotNone(client.get_quote(fixture().fields['code']))
        client.close()
        client._handle_snapshot(1, payload, 1, old)
        self.assertIsNone(client.get_quote(fixture().fields['code']))
        client._conn = FakeConnection()
        client._authenticated = True
        client._handle_connection_disconnected(old, 'late disconnect')
        self.assertTrue(client._authenticated)


if __name__ == '__main__':
    unittest.main()
