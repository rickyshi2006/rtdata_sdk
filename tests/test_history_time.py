import os
import struct
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from unittest import mock

from rtdata import RtdataClient
from rtdata import _protocol as wire
from rtdata._history_time import normalize_endpoint, market_zone, exclusive_end
from rtdata._history_segment_cache import HistorySegmentCache


def utc_ms(text):
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


class HistoryTimeTest(unittest.TestCase):
    def normalize(self, value, symbol='AAPL', is_end=False):
        return normalize_endpoint(value, symbol=symbol, is_end=is_end)

    def test_us_current_and_historical_dst(self):
        for day, hour in [('2025-07-01', '13'), ('2025-12-01', '14'),
                          ('2006-03-20', '14'), ('2006-04-03', '13')]:
            for symbol in ['AAPL', 'AAPL.US', 'BRK.B', 'BRK.B.US']:
                with self.subTest(day=day, symbol=symbol):
                    self.assertEqual(self.normalize(day + ' 09:30:00.123', symbol),
                                     utc_ms(day + 'T' + hour + ':30:00.123'))

    def test_asian_markets(self):
        for symbol in ['600519.SH', '000001.SZ', '00700.HK', 'rb2605.SHF',
                       'IF2605.CFE', 'MA605.CZC', 'a2605.DCE', 'sc2605.INE',
                       'SSE.600519', 'SHFE.rb2605', '10010000.SH']:
            with self.subTest(symbol=symbol):
                self.assertEqual(self.normalize('2025-07-01 09:30:00.123', symbol),
                                 utc_ms('2025-07-01T01:30:00.123'))

    def test_dates_expand_to_local_day_including_dst_length(self):
        for day, hours in [('2025-03-09', 23), ('2025-11-02', 25), ('2025-07-01', 24)]:
            self.assertEqual(self.normalize(day, is_end=True) + 1 - self.normalize(day),
                             hours * 3600000)
        self.assertEqual(self.normalize(date(2025, 7, 1)), utc_ms('2025-07-01T04:00:00'))
        self.assertEqual(self.normalize(datetime(2025, 7, 1, 9, 30)), utc_ms('2025-07-01T13:30:00'))

    def test_aware_strings_and_epoch_are_instants(self):
        expected = utc_ms('2025-07-01T13:30:00.123')
        for value in ['2025-07-01T09:30:00.123-04:00', '2025-07-01T13:30:00.123Z',
                      datetime.fromisoformat('2025-07-01T21:30:00.123+08:00'), expected]:
            self.assertEqual(self.normalize(value, 'unknown/symbol'), expected)
        self.assertEqual(self.normalize('1969-12-31T23:59:59.999Z'), -1)

    def test_naive_fold_gap_rejected_aware_fold_preserved(self):
        for value in ['2025-03-09 02:30', '2025-11-02 01:30']:
            with self.assertRaisesRegex(ValueError, 'Ambiguous or nonexistent'):
                self.normalize(value)
        self.assertEqual(self.normalize('2025-11-02T01:30:00-05:00') -
                         self.normalize('2025-11-02T01:30:00-04:00'), 3600000)

    def test_invalid_and_overflow(self):
        for value in [float('nan'), float('inf'), 1 << 63, -(1 << 63) - 1, 'nonsense']:
            with self.assertRaises(ValueError):
                self.normalize(value)
        with self.assertRaises(ValueError):
            exclusive_end((1 << 63) - 1)
        with self.assertRaises(TypeError):
            self.normalize(True)
        with self.assertRaises(ValueError):
            self.normalize('2025-07-01', 'unknown/symbol')

    def test_new_clock_uses_separate_cache_namespace(self):
        with tempfile.TemporaryDirectory() as directory:
            from pathlib import Path
            legacy = Path(directory) / 'history_v1'
            legacy.mkdir()
            cache = HistorySegmentCache(directory)
            self.assertEqual(cache._root_dir.name, 'history_utc_v1')
            self.assertTrue(legacy.exists())

    def test_host_timezone_does_not_change_inputs(self):
        code = ('import time; time.tzset(); from rtdata._history_time import normalize_endpoint; '
                'print(normalize_endpoint("2025-07-01 09:30", symbol="AAPL", is_end=False))')
        for zone in ['UTC', 'Asia/Shanghai', 'America/Los_Angeles']:
            result = subprocess.check_output([sys.executable, '-B', '-c', code],
                                             env=dict(os.environ, TZ=zone), text=True)
            self.assertEqual(int(result), utc_ms('2025-07-01T13:30:00'))

    def test_today_is_market_date(self):
        with tempfile.TemporaryDirectory() as directory:
            client = RtdataClient(token='test', symbol_cache_dir=directory, history_cache_enabled=False)
            with mock.patch('rtdata.client.datetime') as clock, mock.patch.object(client, 'get_kline') as query:
                clock.now.return_value.date.return_value = date(2025, 6, 30)
                client.get_kline_for_today('AAPL')
                clock.now.assert_called_once_with(market_zone('AAPL'))
                self.assertEqual(query.call_args.kwargs['start'], date(2025, 6, 30))
                self.assertEqual(query.call_args.kwargs['end'], date(2025, 6, 30))

    def test_actual_v1_request_has_utc_half_open_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            client = RtdataClient(token='test', symbol_cache_dir=directory,
                                  history_cache_enabled=False, history_v2_default=False)
            client._authenticated = True
            connection = mock.Mock(connected=True, generation=1)
            payloads = []

            def send(message):
                payload = message[wire.HEADER_SIZE:]
                payloads.append(payload)
                request_id = struct.unpack_from('!I', payload)[0]
                entry = client._pending_queries[request_id]
                entry['event'].set()
                return True

            connection.send.side_effect = send
            client._conn = connection
            client.get_kline('AAPL', '1m', start='2025-07-01 09:30', end='2025-07-01 16:00')
            self.assertEqual(struct.unpack_from('!qq', payloads[0], 9),
                             (utc_ms('2025-07-01T13:30'), utc_ms('2025-07-01T20:00') + 1))
            client._conn = None


if __name__ == '__main__':
    unittest.main()
