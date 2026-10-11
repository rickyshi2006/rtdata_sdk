import tempfile
import unittest
from unittest import mock

from rtdata import Kline, RtdataClient
from rtdata.exceptions import QueryError


def make_kline(timestamp: int) -> Kline:
    return Kline(
        timestamp=timestamp,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10,
        turnover=20.0,
        open_interest=0,
        symbol="TEST.US",
    )


class HistoryRangeCompletenessTest(unittest.TestCase):
    def test_inclusive_end_survives_half_open_upstream_and_cache(self):
        for cache in (False, True):
            for end in (100, 104):
                with self.subTest(cache=cache, end=end), tempfile.TemporaryDirectory() as directory:
                    client = RtdataClient(token='test', symbol_cache_dir=directory,
                                          history_cache_dir=directory, history_cache_enabled=cache)
                    calls = []

                    def fetch(_symbol, _period, start_ms, end_ms, max_count, _timeout, adjust='none'):
                        calls.append((start_ms, end_ms))
                        return [make_kline(t) for t in range(99, 106)
                                if start_ms <= t < end_ms][:max_count]

                    client._perform_history_query = fetch
                    with mock.patch('rtdata.client.HISTORY_V1_PAGE_ROWS', 2):
                        rows = client.get_kline('TEST.US', '1m', start=100, end=end)
                        self.assertEqual([row.timestamp for row in rows], list(range(100, end + 1)))
                        self.assertTrue(all(bound == end + 1 for _, bound in calls))
                        if cache:
                            client._perform_history_query = mock.Mock(side_effect=AssertionError('cache miss'))
                            self.assertEqual(client.get_kline('TEST.US', '1m', start=100, end=end), rows)

    def test_range_without_cache_pages_until_server_returns_empty(self):
        client = RtdataClient(
            token="test",
            history_cache_enabled=False,
            async_callbacks=False,
        )
        starts = []

        def fetch(_symbol, _period, start_ms, _end_ms, max_count,
                  _timeout, adjust="none"):
            self.assertEqual(max_count, 2)
            self.assertEqual(adjust, "none")
            starts.append(start_ms)
            pages = {
                100: [make_kline(100), make_kline(101)],
                102: [make_kline(102), make_kline(103)],
                104: [],
            }
            return pages[start_ms]

        client._perform_history_query = fetch
        with mock.patch("rtdata.client.HISTORY_V1_PAGE_ROWS", 2):
            rows = client.get_kline("TEST.US", "1m", start=100, end=109)

        self.assertEqual([row.timestamp for row in rows], [100, 101, 102, 103])
        self.assertEqual(starts, [100, 102, 104])

    def test_failed_later_page_does_not_cover_unfetched_cache_range(self):
        with tempfile.TemporaryDirectory() as cache_dir:
            client = RtdataClient(
                token="test",
                history_cache_dir=cache_dir,
                history_cache_enabled=True,
                async_callbacks=False,
            )

            def fetch(_symbol, _period, start_ms, _end_ms, _max_count,
                      _timeout, adjust="none"):
                if start_ms == 100:
                    return [make_kline(100), make_kline(101)]
                raise QueryError("injected second-page failure")

            client._perform_history_query = fetch
            with mock.patch("rtdata.client.HISTORY_V1_PAGE_ROWS", 2):
                with self.assertRaisesRegex(QueryError, "second-page failure"):
                    client.get_kline("TEST.US", "1m", start=100, end=109)

            cached = client._history_cache.load_range(
                "TEST.US", "1m", "none", 100, 109)
            missing = client._history_cache.get_missing_ranges(
                "TEST.US", "1m", "none", 100, 110)

            self.assertEqual([row[0] for row in cached], [100, 101])
            self.assertEqual(missing, [(102, 110)])

    def test_empty_tail_is_not_mislabeled_as_cached_data_coverage(self):
        with tempfile.TemporaryDirectory() as cache_dir:
            client = RtdataClient(
                token="test",
                history_cache_dir=cache_dir,
                history_cache_enabled=True,
                async_callbacks=False,
            )
            starts = []

            def fetch(_symbol, _period, start_ms, _end_ms, _max_count,
                      _timeout, adjust="none"):
                starts.append(start_ms)
                if start_ms == 100:
                    return [make_kline(100), make_kline(101)]
                return []

            client._perform_history_query = fetch
            with mock.patch("rtdata.client.HISTORY_V1_PAGE_ROWS", 2):
                rows = client.get_kline("TEST.US", "1m", start=100, end=109)

            missing = client._history_cache.get_missing_ranges(
                "TEST.US", "1m", "none", 100, 110)
            self.assertEqual([row.timestamp for row in rows], [100, 101])
            self.assertEqual(starts, [100, 102])
            self.assertEqual(missing, [(102, 110)])

    def test_non_increasing_page_is_rejected_before_cache_write(self):
        with tempfile.TemporaryDirectory() as cache_dir:
            client = RtdataClient(
                token="test",
                history_cache_dir=cache_dir,
                history_cache_enabled=True,
                async_callbacks=False,
            )
            client._perform_history_query = mock.Mock(return_value=[
                make_kline(101),
                make_kline(100),
            ])

            with mock.patch("rtdata.client.HISTORY_V1_PAGE_ROWS", 2):
                with self.assertRaisesRegex(QueryError, "non-increasing"):
                    client.get_kline("TEST.US", "1m", start=100, end=109)

            missing = client._history_cache.get_missing_ranges(
                "TEST.US", "1m", "none", 100, 110)
            self.assertEqual(missing, [(100, 110)])


if __name__ == "__main__":
    unittest.main()
