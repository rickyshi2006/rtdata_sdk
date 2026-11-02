import json
import struct
import tempfile
import unittest
from unittest.mock import Mock, patch

from rtdata import RtdataClient, Kline
from rtdata import _protocol as wire
from rtdata._history_limits import login_history_max_rows
from rtdata.exceptions import QueryError


class HistoryLimitsTest(unittest.TestCase):
    def test_login_limit_validation(self):
        for value, expected in [(0, 0), (5000, 5000), (100, 100), (True, 5000),
                                (-1, 5000), (5001, 5000), ('0', 5000), (None, 5000)]:
            with self.subTest(value=value):
                self.assertEqual(login_history_max_rows(
                    b'\x01' + json.dumps({'history_max_rows': value}).encode()), expected)
        for payload in (b'', b'\x01', b'\x01{}', b'\x01[]', b'\x01null',
                        b'\x00{"history_max_rows":0}', b'\x01' + b' ' * 4096,
                        b'\x01{"history_max_rows":5000,"history_max_rows":0}'):
            self.assertEqual(login_history_max_rows(payload), 5000)

    def test_limited_call_is_one_request_even_with_large_local_cache(self):
        for cache in (True, False):
            with self.subTest(cache=cache), tempfile.TemporaryDirectory() as directory:
                client = RtdataClient('offline', symbol_cache_dir=directory,
                                      history_cache_dir=directory, history_cache_enabled=cache,
                                      async_callbacks=False, history_v2_default=False)
                client._authenticated = True
                client._history_cache.load_range = Mock(return_value=[])
                client._history_cache.get_missing_ranges = Mock(return_value=[(1, 10001)])
                sent = []
                connection = Mock(connected=True, generation=1)
                client._conn = connection

                def send(message):
                    kind = wire.decode_header(message)[2]
                    if kind != wire.MsgType.HISTORY_REQUEST:
                        return True
                    payload = message[wire.HEADER_SIZE:]
                    sent.append(payload)
                    request_id = struct.unpack_from('!I', payload)[0]
                    self.assertEqual(struct.unpack_from('!I', payload, 25)[0], 5000)
                    self.assertEqual(payload[-1], 0x80)
                    entry = client._pending_queries[request_id]
                    entry['klines'] = [(t, 1, 1, 1, 1, 1, 1, 0) for t in range(5001, 10001)]
                    entry['event'].set()
                    return True

                connection.send.side_effect = send
                try:
                    for method in (client.get_kline, client.get_kline_range):
                        rows = method('TEST.US', '1m', start=1, end=10000)
                        self.assertEqual(len(rows), 5000)
                        self.assertEqual((rows[0].timestamp, rows[-1].timestamp), (5001, 10000))
                    self.assertEqual(len(sent), 2)
                finally:
                    client._conn = None
                    client.close()

    def test_cached_latest_suffix_is_bounded_and_gaps_do_not_trigger_page_downloads(self):
        with tempfile.TemporaryDirectory() as directory:
            client = RtdataClient('offline', symbol_cache_dir=directory,
                                  history_cache_dir=directory, async_callbacks=False)
            self.addCleanup(client.close)
            client._history_cache._tail_refresh_bars = 0
            rows = [(t, 1, 1, 1, 1, 1, 1, 0) for t in range(1, 10001)]
            client._history_cache.store_range('TEST.US', '1m', 'none', 1, 10001, rows)
            with patch.object(client, '_perform_history_query', side_effect=AssertionError('cache miss')):
                result = client.get_kline('TEST.US', '1m', start=1, end=10000)
                self.assertEqual([r.timestamp for r in result], list(range(5001, 10001)))
                result = client.get_kline('TEST.US', '1m', start=1, end=6000)
                self.assertEqual([r.timestamp for r in result], list(range(1001, 6001)))
            with patch.object(client, '_perform_history_query', return_value=[]) as query:
                self.assertEqual(client.get_kline('TEST.US', '1m', start=1, end=20000), [])
                query.assert_called_once()

    def test_short_empty_invalid_and_over_limit_results(self):
        with tempfile.TemporaryDirectory() as directory:
            client = RtdataClient('offline', symbol_cache_dir=directory,
                                  history_cache_enabled=False, async_callbacks=False)
            self.addCleanup(client.close)
            row = lambda t: Kline(t, 1, 1, 1, 1, 1, 1, 0, symbol='TEST.US')
            for rows in ([], [row(200)], [row(100), row(200)]):
                with patch.object(client, '_perform_history_query', return_value=rows) as query:
                    self.assertEqual(client.get_kline('TEST.US', start=100, end=200), rows)
                    query.assert_called_once()
            for rows in ([row(99)], [row(201)], [row(200), row(100)], [row(100)] * 5001):
                with patch.object(client, '_perform_history_query', return_value=rows):
                    with self.assertRaises(QueryError):
                        client.get_kline('TEST.US', start=100, end=200)

    def test_reauthentication_does_not_retain_unlimited_hint(self):
        with tempfile.TemporaryDirectory() as directory:
            client = RtdataClient('offline', symbol_cache_dir=directory,
                                  history_cache_enabled=False, async_callbacks=False)
            self.addCleanup(client.close)
            with patch.object(client, '_begin_realtime'), \
                    patch.object(client, '_begin_history_capability_negotiation'), \
                    patch.object(client, '_begin_session_capability_negotiation'):
                client._handle_auth_response(b'\x01{"history_max_rows":0}')
                self.assertEqual(client._history_max_rows, 0)
                client._handle_auth_response(b'\x01')
                self.assertEqual(client._history_max_rows, 5000)

    def test_latest_flag_preserves_adjustment_and_options(self):
        from rtdata._history_v2_protocol import RequestOptions
        for options in (b'', RequestOptions().encode()):
            for adjust, code in [('none', 0), ('forward', 1), ('backward', 2)]:
                message = wire.encode_history_request(1, 2, '1m', 100, 200, 5000,
                    'TEST.US', adjust, options, latest=True)
                payload = message[wire.HEADER_SIZE:]
                self.assertEqual(payload[38], 0x80 | code)
                self.assertEqual(payload[39:], options)

    def test_limited_session_cannot_enter_full_range_pager(self):
        import time
        with tempfile.TemporaryDirectory() as directory:
            client = RtdataClient('offline', symbol_cache_dir=directory,
                                  history_cache_enabled=False, async_callbacks=False)
            self.addCleanup(client.close)
            with patch.object(client, '_perform_history_query') as query:
                with self.assertRaises(QueryError):
                    list(client._iter_history_range_pages('TEST.US', '1m', 'none', 1, 100,
                                                         time.monotonic() + 1))
                query.assert_not_called()


if __name__ == '__main__':
    unittest.main()
