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

    def test_futu_new_suffixes(self):
        for symbol, expected in [
            ('ESmain.CME', '2025-07-01T14:30:00'),
            ('ZCmain.CBOT', '2025-07-01T14:30:00'),
            ('VXmain.CBOE', '2025-07-01T14:30:00'),
            ('CLmain.NYMEX', '2025-07-01T13:30:00'),
            ('GCmain.COMEX', '2025-07-01T13:30:00'),
            ('HSImain.HKFE', '2025-07-01T01:30:00'),
            ('CNmain.SGX', '2025-07-01T01:30:00'),
            ('JNImain.OSE', '2025-07-01T00:30:00'),
            ('FCPOmain.BMD', '2025-07-01T01:30:00'),
            ('BTCUSDT.BINANCE', '2025-07-01T09:30:00'),
            ('BTC.CRYPTO', '2025-07-01T09:30:00'),
            ('BRTI.CFBM', '2025-07-01T09:30:00'),
            ('XAUUSD.FX', '2025-07-01T09:30:00'),
        ]:
            with self.subTest(symbol=symbol):
                self.assertEqual(self.normalize('2025-07-01 09:30', symbol), utc_ms(expected))
        self.assertEqual(self.normalize('2025-12-01 09:30', 'ESmain.CME'), utc_ms('2025-12-01T15:30'))
        for symbol in ['BTC.CCDDEX', 'BTC.CCPT', 'BTC.CCPRINCA']:
            with self.assertRaises(ValueError):
                self.normalize('2025-07-01 09:30', symbol)

    def test_aware_strings_and_epoch_are_instants(self):
        expected = utc_ms('2025-07-01T13:30:00.123')
        for value in ['2025-07-01T09:30:00.123-04:00', '2025-07-01T13:30:00.123Z',
                      datetime.fromisoformat('2025-07-01T21:30:00.123+08:00'), expected]:
            self.assertEqual(self.normalize(value, 'unknown/symbol'), expected)
        self.assertEqual(self.normalize('1969-12-31T23:59:59.999Z'), -1)

    def test_sgx_local_time_matches_explicit_offset(self):
        for hour in (9, 10):
            with self.subTest(hour=hour):
                text = f'2026-10-30T{hour:02d}:00:00'
                expected = utc_ms(f'2026-10-30T{hour - 8:02d}:00:00')
                for value in (text, datetime.fromisoformat(text), text + '+08:00'):
                    self.assertEqual(self.normalize(value, 'CNmain.SGX'), expected)

    def test_all_futu_utc_suffixes(self):
        for suffix in ('CRYPTO', 'BITFLYER', 'BITFINEX', 'BITSTAMP', 'BTCTURK',
                       'GEMINI', 'HUOBI', 'KUCOIN', 'LBANK', 'OKX', 'BINANCE',
                       'BINANCEUS', 'HASHKEY', 'COINBASE', 'BITGO', 'CFBM', 'FX'):
            for day in ('2026-07-01', '2026-12-01'):
                with self.subTest(suffix=suffix, day=day):
                    self.assertEqual(self.normalize(day + ' 09:30', 'TEST.' + suffix),
                                     utc_ms(day + 'T09:30'))

    def test_futu_futures_winter_and_dst_boundaries(self):
        for symbol, winter_hour, summer_offset, winter_offset in (
            ('ESmain.CME', 15, '-05:00', '-06:00'),
            ('ZCmain.CBOT', 15, '-05:00', '-06:00'),
            ('VXmain.CBOE', 15, '-05:00', '-06:00'),
            ('CLmain.NYMEX', 14, '-04:00', '-05:00'),
            ('GCmain.COMEX', 14, '-04:00', '-05:00'),
        ):
            with self.subTest(symbol=symbol):
                self.assertEqual(self.normalize('2026-12-01 09:30', symbol),
                                 utc_ms(f'2026-12-01T{winter_hour}:30'))
                for value in ('2026-03-08 02:30', '2026-11-01 01:30'):
                    with self.assertRaisesRegex(ValueError, 'Ambiguous or nonexistent'):
                        self.normalize(value, symbol)
                self.assertEqual(
                    self.normalize('2026-11-01T01:30' + winter_offset, symbol) -
                    self.normalize('2026-11-01T01:30' + summer_offset, symbol),
                    3600000)

    def test_futu_date_only_bounds(self):
        for symbol, day, expected_start, hours in (
            ('CNmain.SGX', '2026-10-30', '2026-10-29T16:00', 24),
            ('BTCUSDT.BINANCE', '2026-10-30', '2026-10-30T00:00', 24),
            ('ESmain.CME', '2026-03-08', '2026-03-08T06:00', 23),
            ('ESmain.CME', '2026-11-01', '2026-11-01T05:00', 25),
        ):
            with self.subTest(symbol=symbol, day=day):
                start = self.normalize(day, symbol)
                self.assertEqual(start, utc_ms(expected_start))
                self.assertEqual(self.normalize(day, symbol, is_end=True) + 1 - start,
                                 hours * 3600000)

    def test_futu_explicit_instants_do_not_infer_market_zone(self):
        expected = utc_ms('2026-10-30T01:00')
        with mock.patch('rtdata._history_time.market_zone', side_effect=AssertionError('unexpected inference')):
            for symbol in ('CNmain.SGX', 'ESmain.CME', 'BTCUSDT.BINANCE', 'BTC.CCDDEX'):
                for value in (expected, '2026-10-30T01:00Z', '2026-10-30T09:00+08:00',
                              datetime.fromisoformat('2026-10-30T09:00+08:00')):
                    with self.subTest(symbol=symbol, value=value):
                        self.assertEqual(self.normalize(value, symbol), expected)

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
                'print(*[normalize_endpoint("2025-07-01 09:30", symbol=s, is_end=False) '
                'for s in ("AAPL", "CNmain.SGX", "ESmain.CME", "BTCUSDT.BINANCE")])')
        for zone in ['UTC', 'Asia/Shanghai', 'America/Los_Angeles']:
            result = subprocess.check_output([sys.executable, '-B', '-c', code],
                                             env=dict(os.environ, TZ=zone), text=True)
            self.assertEqual([int(value) for value in result.split()],
                             [utc_ms('2025-07-01T' + clock)
                              for clock in ('13:30', '01:30', '14:30', '09:30')])

    def test_today_is_market_date(self):
        with tempfile.TemporaryDirectory() as directory:
            client = RtdataClient(token='test', symbol_cache_dir=directory, history_cache_enabled=False)
            for symbol in ('AAPL', 'CNmain.SGX', 'ESmain.CME', 'BTCUSDT.BINANCE'):
                with self.subTest(symbol=symbol), mock.patch('rtdata.client.datetime') as clock, \
                        mock.patch.object(client, 'get_kline') as query:
                    clock.now.return_value.date.return_value = date(2025, 6, 30)
                    client.get_kline_for_today(symbol)
                    clock.now.assert_called_once_with(market_zone(symbol))
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
            try:
                for symbol, day, start, end, utc_start, utc_end in (
                    ('AAPL', '2025-07-01', '09:30', '16:00', '13:30', '20:00'),
                    ('CNmain.SGX', '2026-10-30', '09:00', '10:00', '01:00', '02:00'),
                    ('ESmain.CME', '2026-12-01', '09:00', '10:00', '15:00', '16:00'),
                    ('BTCUSDT.BINANCE', '2026-10-30', '09:00', '10:00', '09:00', '10:00'),
                ):
                    with self.subTest(symbol=symbol):
                        payloads.clear()
                        client.get_kline(symbol, '1m', start=day + ' ' + start, end=day + ' ' + end)
                        self.assertEqual(len(payloads), 1)
                        self.assertEqual(struct.unpack_from('!qq', payloads[0], 9),
                                         (utc_ms(day + 'T' + utc_start), utc_ms(day + 'T' + utc_end) + 1))
            finally:
                client._conn = None


if __name__ == '__main__':
    unittest.main()
