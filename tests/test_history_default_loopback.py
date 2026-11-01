import contextlib
import inspect
import struct
import tempfile
import time
import unittest
from unittest.mock import patch

from rtdata import API, RtdataClient
from rtdata import _history_capabilities as cap
from rtdata import _history_v2_codec as codec
from rtdata import _history_v2_protocol as v2
from rtdata import _protocol as wire
from test_realtime_loopback import Peer, read_frame


ROWS = [(1793318400000 + i * 60000, 1.25, 1.5, 1.0, 1.375, 100 + i)
        for i in range(3)]


class HistoryDefaultLoopbackTest(unittest.TestCase):
    def run_query(self, *, api=False, peer_mode='v2', options=None,
                  missing_zstd=False, old_discovery=False):
        seen = {'offers': 0, 'requests': [], 'updates': 0}

        def scenario(peer):
            sock = peer.accept()
            while not peer.stop.is_set():
                kind, _, payload = read_frame(sock)
                if kind == wire.MsgType.CAPABILITY_OFFER:
                    seen['offers'] += 1
                    offer = cap.HistoryCapabilities.decode(payload)
                    seen['offer_v2'] = cap.v2_eligible(offer)
                    if peer_mode == 'silent':
                        continue
                    # Deliberately ACK after connect returns: the very first query
                    # must wait rather than pick V1 based on a pending exchange.
                    time.sleep(0.05)
                    if peer_mode == 'invalid':
                        ack = b'invalid'
                    elif peer_mode == 'v1' or not cap.v2_eligible(offer):
                        ack = cap.v1_capabilities(cap.CapabilityRole.CLOUD).encode()
                    else:
                        ack = cap.v2_capabilities(cap.CapabilityRole.CLOUD).encode()
                    frame = wire.build_message(wire.MsgType.CAPABILITY_ACK, 0, ack)
                    sock.sendall(frame[:7])
                    sock.sendall(frame[7:])
                elif kind == wire.MsgType.HISTORY_REQUEST:
                    request_id = struct.unpack_from('!I', payload)[0]
                    is_v2 = payload[-v2.REQUEST_OPTIONS_STRUCT.size:][:4] == b'H2V1'
                    seen['requests'].append(2 if is_v2 else 1)
                    if is_v2:
                        opts = v2.RequestOptions.decode(payload[-v2.REQUEST_OPTIONS_STRUCT.size:])
                        block = codec.encode_columnar_block(ROWS, opts.max_block_bytes)
                        begin = v2.HistoryBegin(
                            request_id=request_id, symbol_id=9001, period=1,
                            estimated_rows=len(ROWS), start_time_ms=ROWS[0][0],
                            end_time_ms=ROWS[-1][0], max_block_bytes=opts.max_block_bytes)
                        data = v2.HistoryDataHeader(
                            request_id=request_id, chunk_seq=0, row_count=len(ROWS),
                            uncompressed_size=len(block.uncompressed),
                            compressed_size=len(block.compressed),
                            first_timestamp_ms=ROWS[0][0], last_timestamp_ms=ROWS[-1][0])
                        end = v2.HistoryEnd(
                            request_id=request_id, actual_total_rows=len(ROWS),
                            actual_uncompressed_bytes=len(block.uncompressed),
                            actual_compressed_bytes=len(block.compressed),
                            chunk_count=1, last_chunk_seq=0)
                        frames = [(wire.MsgType.HISTORY_BEGIN, begin.encode()),
                                  (wire.MsgType.HISTORY_DATA,
                                   data.encode(opts.max_block_bytes) + block.compressed),
                                  (wire.MsgType.HISTORY_END, end.encode())]
                    else:
                        body = struct.pack('!IIBIHHH', request_id, 9001, 1, len(ROWS), 0, 1, len(ROWS))
                        for ts, o, h, l, c, volume in ROWS:
                            body += (struct.pack('!q', ts) + struct.pack('<ffff', o, h, l, c)
                                     + struct.pack('!Q', volume) + struct.pack('<d', 0)
                                     + struct.pack('!Q', 0))
                        frames = [(wire.MsgType.HISTORY_RESPONSE, body)]
                    for response_kind, body in frames:
                        sock.sendall(wire.build_message(response_kind, 0, body))
                elif kind == wire.MsgType.HISTORY_WINDOW_UPDATE:
                    seen['updates'] += 1

        peer = Peer(scenario)
        self.addCleanup(peer.close)
        with tempfile.TemporaryDirectory() as directory, contextlib.ExitStack() as stack:
            if missing_zstd:
                stack.enter_context(patch.object(codec, '_zstd', None))
            kwargs = dict(async_callbacks=False, history_cache_enabled=False,
                          symbol_cache_dir=directory)
            kwargs.update(options or {})
            if api:
                discovery = {'tcp_host': '127.0.0.1', 'tcp_port': peer.port,
                             'symbol_map_version': 1, 'protocol': {
                                 'features_supported': [] if old_discovery else ['history_capability_v1']}}
                stack.enter_context(patch('rtdata._discovery.discover_endpoint', return_value=discovery))
                stack.enter_context(patch('rtdata._discovery.fetch_symbol_map',
                                          return_value=({9001: 'TEST.US'}, 1)))
                public = API('test', api_url='https://discovery.invalid', **kwargs)
                client = public._client
            else:
                public = client = RtdataClient('test', host='127.0.0.1', port=peer.port,
                                               auto_reconnect=False, **kwargs)
            client._symbol_map.update_from_dict({9001: 'TEST.US'}, 1)
            try:
                if not api:
                    client.connect(timeout=2)
                rows = public.get_kline('TEST.US', period='1m', start=ROWS[0][0],
                                        end=ROWS[-1][0], timeout=3)
                self.assertEqual([(r.timestamp, r.open, r.high, r.low, r.close, r.volume)
                                  for r in rows], ROWS)
                self.assertTrue(all(r.turnover == r.open_interest == 0 for r in rows))
                seen['state'] = client.history_capability_state
                seen['reason'] = client.history_capability_fallback_reason
                seen['eligible'] = client.history_v2_eligible
                self.assertEqual(peer.errors, [])
            finally:
                # Mark expected EOF before closing the test client's socket.
                peer.stop.set()
                public.close()
        return seen

    def test_both_public_signatures_default_to_v2(self):
        for factory in (API, RtdataClient):
            for flag in ('history_v2_advertise', 'history_v2_default'):
                self.assertIs(inspect.signature(factory).parameters[flag].default, True)

    @unittest.skipUnless(codec.zstd_available(), 'Zstandard not installed')
    def test_both_public_entries_first_query_is_real_compressed_v2(self):
        for api in (False, True):
            with self.subTest(api=api):
                seen = self.run_query(api=api)
                self.assertEqual(seen['requests'], [2])
                self.assertEqual(seen['offers'], 1)
                self.assertTrue(seen['eligible'])

    def test_old_peer_acknowledges_v1(self):
        seen = self.run_query(peer_mode='v1')
        self.assertEqual(seen['requests'], [1])
        self.assertFalse(seen['eligible'])

    def test_old_peer_ignores_offer_and_query_falls_back(self):
        seen = self.run_query(peer_mode='silent', options={'history_capability_ack_timeout': 0.1})
        self.assertEqual(seen['requests'], [1])
        self.assertEqual(seen['reason'], 'ack_timeout')

    def test_old_discovery_peer_receives_no_offer(self):
        seen = self.run_query(api=True, old_discovery=True)
        self.assertEqual(seen['requests'], [1])
        self.assertEqual(seen['offers'], 0)
        self.assertIn('peer_does_not_advertise', seen['reason'])

    def test_invalid_ack_falls_back_without_disconnect(self):
        seen = self.run_query(peer_mode='invalid')
        self.assertEqual(seen['requests'], [1])
        self.assertIn('invalid_ack', seen['reason'])

    def test_missing_zstd_sends_only_v1_capability(self):
        seen = self.run_query(missing_zstd=True)
        self.assertEqual(seen['requests'], [1])
        self.assertFalse(seen['offer_v2'])

    def test_either_explicit_opt_out_preserves_v1(self):
        for options in ({'history_v2_advertise': False}, {'history_v2_default': False},
                        {'history_v2_advertise': False, 'history_v2_default': False}):
            for api in (False, True):
                with self.subTest(options=options, api=api):
                    seen = self.run_query(api=api, options=options)
                    self.assertEqual(seen['requests'], [1])
                    if options.get('history_v2_advertise') is False:
                        self.assertEqual(seen['offers'], 0)


if __name__ == '__main__':
    unittest.main()
