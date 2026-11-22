import json
import queue
import tempfile
import threading
import unittest
from unittest.mock import patch

from rtdata import RtdataClient
from rtdata import _protocol as proto
from rtdata import _realtime_capability as cap
from rtdata import _realtime_protocol as wire
from rtdata import _realtime_block as block
from rtdata import _numeric_codec as numeric_codec
import test_realtime_client as helpers
from rtdata._market_subscription import RESPONSE, IDENTITIES


@unittest.skipUnless(block.available(), 'zstandard unavailable')
class RealtimeBlockClientTest(unittest.TestCase):
    def client(self, hint=True, numeric=False, numeric_hint=None):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        client = RtdataClient(token='test', async_callbacks=False, auto_reconnect=False,
                              symbol_cache_dir=tmp.name, history_cache_enabled=False,
                              history_v2_advertise=False, realtime_numeric=numeric)
        self.addCleanup(client.close)
        client._conn = helpers.FakeConnection()
        client._symbol_map.update_from_dict({1: helpers.fixture().fields['code']})
        auth = b'\x01' + json.dumps({'realtime_block_v1': hint, 'realtime_numeric_v1': numeric_hint}).encode()
        self.dispatch(client, proto.MsgType.AUTH_RESPONSE, auth)
        client._authenticated = True
        offer = cap.Capability.decode(client._conn.sent[-1][10:])
        ack = cap.intersection(cap.supported(cap.Role.CLOUD), offer)
        self.dispatch(client, cap.ACK, ack.encode())
        status = cap.Status(offer.nonce, 1, tuple(
            cap.SourceState(sid, True, cap.Reason.READY, 91) for sid in range(1, 6)))
        self.dispatch(client, cap.SOURCE_STATUS, status.encode())
        return client

    def dispatch(self, client, kind, payload, generation=1, connection=None):
        client._dispatch_message(kind, 0, payload, generation, connection or client._conn)

    def activate(self, client):
        client_nonce, zero = block.parse_negotiation(client._conn.sent[-1][10:], numeric=client._block_numeric)
        self.assertEqual(zero, 0)
        selected = block.negotiation(client_nonce, 987, numeric=client._block_numeric)
        self.dispatch(client, block.ACK, selected)
        self.assertEqual(proto.decode_header(client._conn.sent[-1][:10])[2], block.COMMIT)
        self.assertEqual(client._conn.sent[-1][10:], selected)
        self.dispatch(client, block.READY, selected)
        self.assertEqual(client.realtime_statistics['block_phase'], 'active')
        self.assertIsNone(client._block_timer)
        return selected

    def payload(self, *sequences):
        raw = b''.join(proto.build_message(cap.SNAPSHOT, 1, wire.encode(helpers.fixture(seq=n)))
                       for n in sequences)
        payload = block.encode(raw)
        self.assertTrue(payload)
        return payload

    def test_roundtrip_preserves_all_fields_order_and_wire_accounting(self):
        client = self.client()
        self.activate(client)
        rows = []
        client.on_quote(rows.append)
        payload = self.payload(1, 2, 3, 4)
        before = client._bytes_received
        self.dispatch(client, block.DATA, payload)
        self.assertEqual([row.sequence for row in rows], [1, 2, 3, 4])
        self.assertTrue(all(dict(row.fields) == dict(helpers.fixture().fields) for row in rows))
        self.assertEqual(client._bytes_received - before, 10 + len(payload))
        self.assertEqual(client.realtime_statistics['compressed_blocks'], 1)
        self.assertEqual(client.realtime_statistics['compressed_events'], 4)
        self.assertEqual(client.realtime_statistics['compressed_wire_bytes'], 10 + len(payload))

    def test_numeric_requires_explicit_option_native_and_strict_hint(self):
        for requested, hint, native, expected in (
                (False, True, True, False), (True, True, False, False),
                (True, None, True, False), (True, 1, True, False),
                (True, 'true', True, False), (True, True, True, True)):
            with self.subTest(requested=requested, hint=hint, native=native):
                with patch.object(numeric_codec, 'available', return_value=native):
                    client = self.client(numeric=requested, numeric_hint=hint)
                self.assertEqual(client._block_numeric, expected)
                self.activate(client)
                self.assertEqual(client.realtime_statistics['numeric_negotiated'], expected)
        with self.assertRaisesRegex(ValueError, 'must be bool'):
            self.client(numeric=1)

    def test_numeric_codec_is_bound_to_ack_and_ready(self):
        for numeric in (False, True):
            for stage in (block.ACK, block.READY):
                with self.subTest(numeric=numeric, stage=stage):
                    with patch.object(numeric_codec, 'available', return_value=True):
                        client = self.client(numeric=numeric, numeric_hint=True)
                    nonce = client._block_nonce[0]
                    if stage == block.READY:
                        self.dispatch(client, block.ACK, block.negotiation(nonce, 987, numeric=numeric))
                    self.dispatch(client, stage, block.negotiation(nonce, 987, numeric=not numeric))
                    self.assertFalse(client._conn.connected)

    @unittest.skipUnless(numeric_codec.available(), 'optional native codec unavailable')
    def test_numeric_roundtrip_fallback_fifo_corruption_and_reconnect(self):
        from test_numeric_codec import compact, packed
        client = self.client(numeric=True, numeric_hint=True)
        self.activate(client)
        seen = []
        client.on_quote(seen.append)
        transformed, raw = zip(*(compact(helpers.fixture(seq=n)) for n in (1, 2, 3)))
        offline = packed(b''.join(transformed), len(b''.join(raw)), 3)
        payload = b'RBN1\x01\x01' + offline[6:]
        self.dispatch(client, block.DATA, payload)
        self.dispatch(client, block.DATA, self.payload(4, 5))
        client._dispatch_message(cap.SNAPSHOT, 1, wire.encode(helpers.fixture(seq=6)), 1, client._conn)
        self.assertEqual([row.sequence for row in seen], [1, 2, 3, 4, 5, 6])
        self.assertTrue(all(dict(row.fields) == dict(helpers.fixture().fields) for row in seen))
        self.assertEqual(client.realtime_statistics['numeric_blocks'], 1)
        # Codec failures close the connection, not a silent lossy downgrade.
        for invalid in (offline, payload[:-1] + bytes([payload[-1] ^ 1])):
            peer = self.client(numeric=True, numeric_hint=True)
            self.activate(peer)
            rows = []
            peer.on_quote(rows.append)
            self.dispatch(peer, block.DATA, invalid)
            self.assertFalse(peer._conn.connected)
            self.assertEqual(rows, [])
        old = self.client()
        self.activate(old)
        self.dispatch(old, block.DATA, payload)
        self.assertFalse(old._conn.connected)
        client._reset_realtime('reconnect')
        self.assertFalse(client._block_numeric)
        self.assertFalse(client.realtime_statistics['numeric_negotiated'])

    @unittest.skipUnless(numeric_codec.available(), 'optional native codec unavailable')
    def test_numeric_corrupt_tail_never_delivers_prefix(self):
        from test_numeric_codec import compact, packed
        good, raw = compact(helpers.fixture())
        tail = bytearray(good)
        tail[10] = 0  # Bad inner schema version, in a valid checksummed Zstd block.
        offline = packed(good + tail, len(raw) * 2, 2)
        client = self.client(numeric=True, numeric_hint=True)
        self.activate(client)
        seen = []
        client.on_quote(seen.append)
        self.dispatch(client, block.DATA, b'RBN1\x01\x01' + offline[6:])
        self.assertFalse(client._conn.connected)
        self.assertEqual(seen, [])
        self.assertEqual(client.realtime_statistics['numeric_blocks'], 0)

    @unittest.skipUnless(numeric_codec.available(), 'optional native codec unavailable')
    def test_cpp_transport_sender_matches_sdk_native_inverse(self):
        import os
        import subprocess
        from pathlib import Path
        probe = os.environ.get('RTDATA_REALTIME_BLOCK_PROBE')
        if not probe:
            self.skipTest('native transport probe not configured')
        numeric, fallback = 0, 0
        for line in (Path(__file__).parent / 'fixtures/numeric_blocks_v1.jsonl').read_text().splitlines():
            raw = bytes.fromhex(json.loads(line)['raw'])
            packed = subprocess.run([probe, 'numeric-encode'], input=raw, capture_output=True,
                                    check=True, timeout=5).stdout
            if packed[:4] == b'RBN1':
                self.assertEqual(numeric_codec.decode(packed, transport=True), raw)
                numeric += 1
            elif packed:
                self.assertEqual(block.decode(packed), raw)
                fallback += 1
        self.assertGreater(numeric, 0)
        self.assertGreater(fallback, 0)

    def test_old_peer_nonboolean_hint_and_missing_codec_do_not_offer(self):
        for hint in (False, 1, 'true', None):
            with self.subTest(hint=hint):
                client = self.client(hint)
                self.assertEqual(client._block_phase, 'off')
                self.assertNotIn(block.OFFER, [proto.decode_header(f[:10])[2] for f in client._conn.sent])
        with patch.object(block, '_zstd', None):
            self.assertEqual(self.client()._block_phase, 'off')

    def test_unsolicited_data_bad_nonce_and_duplicate_ready_close(self):
        client = self.client(False)
        self.dispatch(client, block.DATA, self.payload(1, 2))
        self.assertFalse(client._conn.connected)
        self.assertEqual(client.realtime_status.phase, 'closed')
        client = self.client()
        self.dispatch(client, block.ACK, block.negotiation(client._block_nonce[0] ^ 2 or 1, 987))
        self.assertFalse(client._conn.connected)
        client = self.client()
        selected = self.activate(client)
        self.dispatch(client, block.READY, selected)
        self.assertFalse(client._conn.connected)

    def test_corrupt_checksum_and_schema_tail_deliver_no_prefix(self):
        for bad_schema in (False, True):
            with self.subTest(bad_schema=bad_schema):
                client = self.client()
                self.activate(client)
                rows = []
                client.on_quote(rows.append)
                if bad_schema:
                    raw = proto.build_message(cap.SNAPSHOT, 1, wire.encode(helpers.fixture()))
                    raw += proto.build_message(cap.SNAPSHOT, 1, b'\x00' * 64)
                    payload = block.encode(raw)
                else:
                    payload = bytearray(self.payload(1, 2))
                    payload[-1] ^= 1
                self.dispatch(client, block.DATA, payload)
                self.assertEqual(rows, [])
                self.assertFalse(client._conn.connected)
                self.assertEqual(client.realtime_statistics['compressed_events'], 0)

    def test_stale_generation_connection_and_timeout_are_ignored(self):
        client = self.client()
        self.activate(client)
        self.dispatch(client, block.DATA, b'bad', generation=0)
        self.dispatch(client, block.DATA, b'bad', connection=helpers.FakeConnection())
        client._block_timeout(client._realtime.generation - 1)
        self.assertTrue(client._conn.connected)
        client._reset_realtime('test reconnect')
        self.assertEqual(client._block_phase, 'off')
        self.assertEqual(client._block_nonce, (0, 0))
        self.assertIsNone(client._block_timer)

    def test_commit_failure_and_handshake_timeout_close(self):
        client = self.client()
        client._conn.fail_type = block.COMMIT
        self.dispatch(client, block.ACK, block.negotiation(client._block_nonce[0], 987))
        self.assertFalse(client._conn.connected)
        self.assertEqual(client.realtime_status.phase, 'closed')
        self.assertIsNone(client._block_timer)
        client = self.client()
        client._block_timeout(client._realtime.generation)
        self.assertFalse(client._conn.connected)
        self.assertIsNone(client._block_timer)

    def test_source_epoch_and_sequence_checks_are_not_bypassed(self):
        client = self.client()
        self.activate(client)
        rows = []
        client.on_quote(rows.append)
        self.dispatch(client, block.DATA, self.payload(2, 1, 3))
        self.assertEqual([row.sequence for row in rows], [2, 3])
        status = cap.Status(client._realtime.selected.nonce, 2,
                            tuple(cap.SourceState(sid, False, cap.Reason.SUSPENDED, 0)
                                  for sid in range(1, 6)))
        self.dispatch(client, cap.SOURCE_STATUS, status.encode())
        self.dispatch(client, block.DATA, self.payload(4, 5))
        self.assertEqual([row.sequence for row in rows], [2, 3])
        self.assertIsNone(client.get_quote(helpers.fixture().fields['code']))

    def queued_client(self, capacity=1000):
        client = self.client()
        self.activate(client)
        # Pause the worker deterministically; exercise the real queue/worker below.
        client._async_callbacks = True
        client._callback_queue = queue.Queue(maxsize=capacity)
        return client

    def drain_callbacks(self, client):
        done = threading.Event()
        client._callback_queue.put_nowait(None)
        run = client._run_quote_callbacks

        def until_sentinel(quote):
            if quote is None:
                client._callback_stop.set()
                done.set()
            else:
                run(quote)

        with patch.object(client, '_run_quote_callbacks', until_sentinel):
            worker = threading.Thread(target=client._callback_loop, daemon=True)
            worker.start()
            self.assertTrue(done.wait(2))
            worker.join(2)
            self.assertFalse(worker.is_alive())
        client._callback_stop.clear()

    def test_async_block_and_raw_fifo_keep_accepted_ticks_not_only_latest(self):
        client = self.queued_client()
        first, second = [], []
        client.on_quote(first.append)
        client.on_quote(second.append)
        self.dispatch(client, block.DATA, self.payload(1, 2, 3, 4))
        client._dispatch_message(cap.SNAPSHOT, 1, wire.encode(helpers.fixture(seq=5)), 1, client._conn)
        self.dispatch(client, block.DATA, self.payload(6, 6, 5, 7, 8))
        self.assertEqual(client._callback_queue.qsize(), 8)
        self.assertEqual(client.get_quote(helpers.fixture().fields['code']).sequence, 8)
        self.drain_callbacks(client)
        self.assertEqual([row.sequence for row in first], list(range(1, 9)))
        self.assertEqual(first, second)
        self.assertEqual(client._quotes_dropped, 0)
        self.assertEqual(client.realtime_statistics['rejected'], 2)

    def test_async_invalidation_still_discards_queued_and_inflight_quotes(self):
        for reason in ('source', 'permission', 'identity', 'reconnect'):
            with self.subTest(reason=reason):
                client = self.queued_client()
                seen = []
                client.on_quote(seen.append)
                self.dispatch(client, block.DATA, self.payload(1, 2, 3))
                inflight = client._callback_queue.get_nowait()
                if reason == 'source':
                    status = cap.Status(client._realtime.selected.nonce, 2, tuple(
                        cap.SourceState(sid, False, cap.Reason.SUSPENDED, 0) for sid in range(1, 6)))
                    self.dispatch(client, cap.SOURCE_STATUS, status.encode())
                elif reason == 'permission':
                    payload = json.dumps(dict(schema_version=1, sequence=1, status='revoked',
                        severity='critical', reason='test', server_time_ms=1, expires_ms=0,
                        message='test revocation')).encode()
                    with patch.object(client._conn, 'suspend_auto_reconnect', create=True) as suspend:
                        self.dispatch(client, proto.MsgType.TOKEN_STATUS, payload)
                        suspend.assert_called_once()
                    self.assertFalse(client._authenticated)
                elif reason == 'identity':
                    client._market_ready = True
                    self.addCleanup(setattr, client, '_market_ready', False)
                    with patch.object(client, '_market_code', return_value='OTHER.SH'):
                        client._run_quote_callbacks(inflight)
                        self.drain_callbacks(client)
                    self.assertEqual(seen, [])
                    continue
                else:
                    client._reset_realtime('test reconnect')
                client._run_quote_callbacks(inflight)
                self.drain_callbacks(client)
                self.assertEqual(seen, [])

    def test_async_overflow_remains_bounded_and_counts_discarded_ticks(self):
        client = self.queued_client(capacity=3)
        seen = []
        client.on_quote(seen.append)
        self.dispatch(client, block.DATA, self.payload(1, 2, 3, 4, 5, 6, 7, 8))
        self.assertEqual(client._callback_queue.qsize(), 3)
        self.assertEqual(client._quotes_dropped, 5)
        first = client._callback_queue.get_nowait()
        client._run_quote_callbacks(first)
        self.drain_callbacks(client)
        self.assertEqual([row.sequence for row in seen], [6, 7, 8])

    def test_retire_and_readmit_same_identity_never_revives_old_callbacks(self):
        client = self.queued_client()
        seen = []
        client.on_quote(seen.append)
        client._begin_markets(b'\x01{"market_subscription_v1":true}')

        def response(**body):
            self.dispatch(client, RESPONSE, json.dumps(dict(version=1, **body)).encode())

        def identities(generation, entries):
            self.dispatch(client, IDENTITIES, json.dumps(dict(version=1, generation=generation,
                request_id=0, catalog_revision='test', offset=0, next_offset=len(entries),
                total=len(entries), done=True, entries=entries)).encode())
            response(request_id=0, generation=generation, state='applied',
                     admitted_count=0 if entries[0]['action'] == 'remove' else 1)

        response(state='negotiated', request_id=1, generation=0)
        entry = dict(action='add', symbol_id=1, code=helpers.fixture().fields['code'], category='cn_equity')
        identities(1, [entry])
        self.dispatch(client, block.DATA, self.payload(1, 2))
        identities(2, [dict(action='remove', symbol_id=1)])
        self.assertNotIn(1, client._realtime_watermarks)
        identities(3, [entry])
        self.dispatch(client, block.DATA, self.payload(2, 3))
        self.drain_callbacks(client)
        self.assertEqual([row.sequence for row in seen], [2, 3])

    def test_source_resume_same_epoch_does_not_revive_old_queue(self):
        client = self.queued_client()
        seen = []
        client.on_quote(seen.append)
        self.dispatch(client, block.DATA, self.payload(1, 2))
        for revision, ready in ((2, False), (3, True)):
            status = cap.Status(client._realtime.selected.nonce, revision, tuple(
                cap.SourceState(sid, ready, cap.Reason.READY if ready else cap.Reason.SUSPENDED,
                                91 if ready else 0) for sid in range(1, 6)))
            self.dispatch(client, cap.SOURCE_STATUS, status.encode())
        self.dispatch(client, block.DATA, self.payload(2, 3))
        self.drain_callbacks(client)
        self.assertEqual([row.sequence for row in seen], [2, 3])

    def test_invalidation_inside_first_callback_fences_remaining_callbacks(self):
        client = self.queued_client()
        first, second = [], []

        def revoke(quote):
            first.append(quote.sequence)
            client._reset_realtime('revoked in callback')

        client.on_quote(revoke)
        client.on_quote(second.append)
        self.dispatch(client, block.DATA, self.payload(1, 2))
        client._run_quote_callbacks(client._callback_queue.get_nowait())
        self.assertEqual(first, [1])
        self.assertEqual(second, [])
        self.assertTrue(client._callback_queue.empty())
