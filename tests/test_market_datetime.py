import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfoNotFoundError

from rtdata import Quote, Kline
from rtdata._history_time import market_zone, normalize_endpoint
from rtdata._realtime_protocol import Snapshot
from test_realtime_client import fixture


class MarketDatetimeTest(unittest.TestCase):
    stamp = 1793602801583  # 2026-11-02 07:00:01.583Z

    def quote(self, symbol):
        return Quote(symbol, 1, 1., 2., 1.5, 10, self.stamp)

    def test_market_display_keeps_timestamp(self):
        for symbol, hour in [('601919.SH', 15), ('au2612.SHF', 15), ('00700.HK', 15),
                             ('AAPL', 2), ('EUR.USD', 7), ('BTCUSDT.BINANCE', 7),
                             ('CNmain.SGX', 15), ('CLmain.NYMEX', 2)]:
            with self.subTest(symbol=symbol):
                q = self.quote(symbol)
                k = Kline(self.stamp, 1, 2, 1, 2, 3, 4, 0, symbol)
                for row in (q, k):
                    before = vars(row).copy()
                    dt = row.to_datetime()
                    self.assertEqual(dt.hour, hour)
                    self.assertEqual(dt.microsecond, 583000)
                    self.assertEqual(int(dt.timestamp() * 1000), self.stamp)
                    self.assertEqual(row.to_datetime(timezone.utc).hour, 7)
                    self.assertEqual(row.to_datetime('Asia/Shanghai').hour, 15)
                    self.assertEqual(vars(row), before)

    def test_full_quote_no_field_changes(self):
        original = fixture(1)
        fields = dict(original.fields, trade_time=self.stamp, code='601919.SH')
        q = Quote.from_snapshot(Snapshot.from_fields(1, 91, 1, fields), 1, '601919.SH')
        self.assertEqual(q.to_datetime().hour, 15)
        self.assertEqual(dict(q.fields), fields)
        self.assertEqual(q.timestamp, self.stamp)

    def test_dst_and_night_session(self):
        for date, utc_hour in [('2026-07-01', 13), ('2026-11-05', 14)]:
            q = self.quote('AAPL')
            q.timestamp = normalize_endpoint(date + 'T09:30:00.123', symbol='AAPL', is_end=False)
            self.assertEqual(q.to_datetime().hour, 9)
            self.assertEqual(q.to_datetime(timezone.utc).hour, utc_hour)
        q = self.quote('au2612.SHF')
        q.timestamp = normalize_endpoint('2026-11-06T21:00:00.500', symbol=q.symbol, is_end=False)
        self.assertEqual(q.to_datetime().isoformat(), '2026-11-06T21:00:00.500000+08:00')
        self.assertEqual(q.to_datetime(timezone.utc).hour, 13)

    def test_fx_input_and_dotted_equity(self):
        for symbol in ('EUR.USD', 'USD.JPY', 'XAU.USD', 'eur.usd', 'EURUSD.FX'):
            self.assertEqual(str(market_zone(symbol)), 'UTC')
            stamp = normalize_endpoint('2026-11-05T09:00:00', symbol=symbol, is_end=False)
            self.assertEqual(datetime.fromtimestamp(stamp / 1000, timezone.utc).hour, 9)
        self.assertEqual(str(market_zone('BRK.B')), 'America/New_York')

    def test_explicit_zone_does_not_infer(self):
        k = Kline(-1, 1, 1, 1, 1, 1, 1, 0)
        with patch('rtdata._history_time.market_zone', side_effect=AssertionError('inferred')):
            self.assertEqual(k.to_datetime('UTC').isoformat(), '1969-12-31T23:59:59.999000+00:00')
            self.assertEqual(k.to_datetime(timezone(timedelta(hours=8))).hour, 7)
        with self.assertRaises(ValueError):
            k.to_datetime()
        with self.assertRaises(TypeError):
            k.to_datetime(8)

    def test_bar_label_is_not_rounded(self):
        k = Kline(1793573999000, 1, 1, 1, 1, 1, 1, 0, 'BTCUSDT.OKX')
        self.assertEqual(k.to_datetime().second, 59)
        self.assertEqual(k.timestamp, 1793573999000)

    def test_invalid_timestamp_and_display_range(self):
        q = self.quote('601919.SH')
        for stamp in (None, True, 1.5, '1793602801583'):
            with self.subTest(stamp=stamp):
                q.timestamp = stamp
                with self.assertRaises(TypeError):
                    q.to_datetime(timezone.utc)
                self.assertEqual(q.timestamp, stamp)
        q.timestamp = (1 << 63) - 1
        with self.assertRaises(OverflowError):
            q.to_datetime(timezone.utc)
        self.assertEqual(q.timestamp, (1 << 63) - 1)

    def test_missing_timezone_database_is_not_host_fallback(self):
        q = self.quote('601919.SH')
        with patch('rtdata._history_time.ZoneInfo', side_effect=ZoneInfoNotFoundError('missing tzdata')):
            with self.assertRaises(ZoneInfoNotFoundError):
                q.to_datetime()
            with self.assertRaises(ZoneInfoNotFoundError):
                q.to_datetime('Asia/Shanghai')
            self.assertEqual(q.to_datetime(timezone.utc).hour, 7)
        self.assertEqual(q.timestamp, self.stamp)


if __name__ == '__main__':
    unittest.main()
