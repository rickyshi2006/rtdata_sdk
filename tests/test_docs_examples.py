import ast
import contextlib
import importlib
import inspect
import io
import os
from pathlib import Path
import re
import runpy
import sys
import unittest
from unittest.mock import create_autospec, patch
from datetime import datetime, timezone

import rtdata
from rtdata._history_segment_cache import HistorySegmentCache
from rtdata._realtime_protocol import Snapshot
from rtdata._realtime_schema import SCHEMAS, SOURCES
from rtdata._history_time import market_zone


ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = sorted((ROOT / "examples").glob("*.py"))
DOCS = sorted(ROOT.glob("*.md")) + sorted((ROOT / "docs").glob("*.md")) + [
    ROOT / "examples/README.md",
]


def full_quote(code="BTCUSDT.BINANCE", source=7, null=True):
    schema = SOURCES[source][1]
    fields = {name: None for name, _ in SCHEMAS[schema]}
    fields.update(code=code, trade_time=1793629800000)
    if not null:
        fields.update(last=1.120123, bid_price1=1.120122, ask_price1=1.120124,
                      volume=0, bid_volume1=0, ask_volume1=123)
    return rtdata.Quote.from_snapshot(Snapshot.from_fields(source, 1, 1, fields), 1, code)


class DocumentationTest(unittest.TestCase):
    def test_release_documents_use_current_package_version(self):
        version = rtdata.__version__
        notes = (ROOT / "docs/RELEASE-NOTES.md").read_text(encoding="utf-8")
        self.assertTrue(notes.startswith(f"## v{version}\n"))
        changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        self.assertIn(f"## {version} - ", changelog)
        for path in DOCS:
            if path.name.startswith("CHANGELOG"):
                continue
            for wheel_version in re.findall(r"rtdata-(\d+\.\d+\.\d+)-py3-none-any\.whl",
                                            path.read_text(encoding="utf-8")):
                with self.subTest(path=path.name):
                    self.assertEqual(wheel_version, version)

    def test_documentation_prose_is_chinese(self):
        for path in DOCS:
            text = path.read_text(encoding="utf-8")
            prose = re.sub(r"```.*?```", "", text, flags=re.S)
            prose = re.sub(r"`[^`]+`", "", prose)
            prose = re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", prose)
            for paragraph in re.split(r"\n\s*\n", prose):
                if len(re.findall(r"\b[A-Za-z]{2,}\b", paragraph)) >= 5:
                    with self.subTest(path=path.name, paragraph=paragraph[:80]):
                        self.assertRegex(paragraph, r"[\u4e00-\u9fff]")
            for heading in re.findall(r"^#+ (.+)$", prose, re.M):
                if re.fullmatch(r"v?\d+\.\d+\.\d+(?: - \d{4}-\d{2}-\d{2})?", heading):
                    continue
                with self.subTest(path=path.name, heading=heading):
                    self.assertRegex(heading, r"[\u4e00-\u9fff]")

    def test_market_guide_examples_match_timezone_rules(self):
        text = (ROOT / "docs/MARKET-SYMBOLS.md").read_text(encoding="utf-8")
        cases = {
            "Asia/Shanghai": ["601919.SH", "000001.SZ", "rb2701.SHF", "au2612.SHF",
                              "i2701.DCE", "MA701.CZC", "IF2612.CFE", "sc2612.INE",
                              "si2701.GFE", "y2605-P-9700.DCE", "fu2609C4800.SHF"],
            "Asia/Hong_Kong": ["00700.HK", "HSImain.HKFE", "A50main.HKFE"],
            "America/New_York": ["AAPL", "BRK.B", "CLmain.NYMEX", "GCmain.COMEX"],
            "America/Chicago": ["ZCmain.CBOT", "ES2612.CME", "VXmain.CBOE"],
            "Asia/Singapore": ["CNmain.SGX", "FEFmain.SGX"],
            "Asia/Tokyo": ["JNImain.OSE"],
            "Asia/Kuala_Lumpur": ["FCPOmain.BMD"],
            "UTC": ["EUR.USD", "USD.JPY", "XAUUSD.FX", "USDindex.FX",
                    "BTCUSDT.BINANCE", "BTCUSDT.OKX", "BTCUSD.COINBASE", "BRTI.CFBM"],
        }
        for zone, codes in cases.items():
            for code in codes:
                with self.subTest(code=code):
                    self.assertIn(f"`{code}`", text)
                    self.assertEqual(market_zone(code).key, zone)

    def test_market_guide_lists_exchange_suffixes(self):
        text = (ROOT / "docs/MARKET-SYMBOLS.md").read_text(encoding="utf-8")
        for suffix in ("SHF", "DCE", "CZC", "CFE", "INE", "GFE", "NYMEX", "COMEX",
                       "CBOT", "CME", "CBOE", "HKFE", "SGX", "OSE", "BMD",
                       "BITFLYER", "BITFINEX", "BITSTAMP", "BTCTURK", "GEMINI", "HUOBI",
                       "KUCOIN", "LBANK", "OKX", "BINANCE", "BINANCEUS", "HASHKEY",
                       "COINBASE", "BITGO", "CRYPTO", "CFBM"):
            self.assertIn(f"| `.{suffix}` |", text)

    def test_market_catalog_snippet_reads_only_directory(self):
        text = (ROOT / "docs/MARKET-SYMBOLS.md").read_text(encoding="utf-8")
        snippets = re.findall(r"```python\n(.*?)\n```", text, re.S)
        self.assertEqual(len(snippets), 1)
        api = create_autospec(rtdata.API, instance=True)
        api.__enter__.return_value = api
        api.symbols = {1: "601919.SH", 2: "BTCUSDT.BINANCE", 3: "CNmain.SGX"}
        with patch.dict(os.environ, {"RTDATA_TOKEN": "catalog-only"}), \
                patch.object(rtdata, "API", return_value=api), \
                patch("socket.socket") as socket, contextlib.redirect_stdout(io.StringIO()):
            namespace = {}
            exec(compile(snippets[0], "MARKET-SYMBOLS.md", "exec"), namespace)
        self.assertEqual(namespace["present"], ["601919.SH", "BTCUSDT.BINANCE", "CNmain.SGX"])
        self.assertEqual(namespace["missing"], ["AAPL", "EUR.USD", "BTCUSDT.OKX"])
        self.assertLessEqual(len(namespace["sample"]), 20)
        api.subscribe.assert_not_called()
        api.get_kline.assert_not_called()
        api.__exit__.assert_called_once()
        socket.assert_not_called()

    def test_constructor_tables_match_signatures(self):
        text = (ROOT / "docs/SDK_USAGE.md").read_text(encoding="utf-8")
        api_section = text.split("### API 构造参数\n", 1)[1].split("### RtdataClient", 1)[0]
        client_section = text.split("### RtdataClient 构造参数\n", 1)[1].split("## 2.", 1)[0]

        def defaults(section):
            return {name: ast.literal_eval(value) for name, value in re.findall(
                r"^\| `([a-z0-9_]+)` \| `([^`]+)` \|", section, re.M)}

        expected_api = defaults(api_section)
        expected_client = {**expected_api, **defaults(client_section)}
        for factory, expected in ((rtdata.API, expected_api), (rtdata.RtdataClient, expected_client)):
            signature = inspect.signature(factory)
            actual = {name: p.default for name, p in signature.parameters.items()
                      if p.default is not inspect.Parameter.empty}
            self.assertEqual(expected, actual, factory.__name__)
            self.assertEqual([n for n, p in signature.parameters.items()
                              if p.default is inspect.Parameter.empty], ["token"])

    def test_markdown_python_snippets_compile(self):
        for path in DOCS:
            text = path.read_text(encoding="utf-8")
            for index, snippet in enumerate(re.findall(r"```python\n(.*?)\n```", text, re.S)):
                with self.subTest(path=path.name, snippet=index):
                    compile(snippet, str(path), "exec")

    def test_local_markdown_links_resolve(self):
        for path in DOCS:
            for target in re.findall(r"\]\(([^)]+)\)", path.read_text(encoding="utf-8")):
                if "://" in target or target.startswith("#"):
                    continue
                with self.subTest(path=path.name, target=target):
                    self.assertTrue((path.parent / target.split("#", 1)[0]).exists())

    def test_cache_path_documentation_matches_implementation(self):
        cache = HistorySegmentCache(cache_dir="example-cache", enabled=False)
        for name in ("README.md", "docs/SDK_USAGE.md", "DELIVERY.md"):
            text = (ROOT / name).read_text(encoding="utf-8")
            self.assertIn(str(cache._root_dir.name), text)
            self.assertNotIn("~/.rtdata/history_v1/", text)

    def test_schema_depth_table_matches_registry(self):
        text = (ROOT / "docs/REALTIME-FULL-FIELDS.md").read_text(encoding="utf-8")
        for categories, schema, levels in re.findall(
                r"^\| (`[^\n]+`) \| (\d+) \| (\d+) \|$", text, re.M):
            schema, levels = int(schema), int(levels)
            for category in re.findall(r"`([^`]+)`", categories):
                self.assertIn((category, schema), SOURCES.values())
            for side in ("bid", "ask"):
                self.assertEqual(levels, sum(bool(re.fullmatch(side + r"_price\d+", name))
                                             for name, _ in SCHEMAS[schema]))
        self.assertEqual(len(re.findall(r"^\| `[^\n]+` \| \d+ \| \d+ \|$", text, re.M)), 5)

    def test_documented_same_instant_market_displays(self):
        instant = datetime(2026, 11, 2, 14, 30, tzinfo=timezone.utc)
        text = (ROOT / "docs/TIME-AND-MARKETS.md").read_text(encoding="utf-8")
        for code in ("601919.SH", "AAPL"):
            q = rtdata.Quote(code, 1, None, None, None, None, int(instant.timestamp() * 1000))
            self.assertIn(q.to_datetime().isoformat(timespec="milliseconds"), text)
            self.assertEqual(q.timestamp, int(instant.timestamp() * 1000))


class ExamplesTest(unittest.TestCase):
    def test_examples_compile_and_import_without_side_effects(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(rtdata, "API") as api, \
                patch("socket.socket") as socket, patch("threading.Thread.start") as start, \
                patch.object(sys, "path", [str(ROOT / "examples"), *sys.path]):
            for path in EXAMPLES:
                with self.subTest(path=path.name):
                    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                    runpy.run_path(str(path), run_name="example_import_check")
            api.assert_not_called()
            socket.assert_not_called()
            start.assert_not_called()

    def test_example_index_covers_entrypoints(self):
        text = (ROOT / "examples/README.md").read_text(encoding="utf-8")
        for path in EXAMPLES:
            if not path.name.startswith("_"):
                self.assertIn(f"]({path.name})", text)

    def test_missing_token_fails_before_network(self):
        common = importlib.import_module("examples._common")
        with patch.dict(os.environ, {}, clear=True), patch.object(rtdata, "API") as api:
            with self.assertRaisesRegex(SystemExit, "RTDATA_TOKEN"):
                common.connection_options()
            api.assert_not_called()

    def test_datetime_example_gate_accepts_release_and_rejects_old_api(self):
        common = importlib.import_module("examples._common")
        common.require_datetime()
        with patch.object(rtdata, "Quote", object):
            with self.assertRaisesRegex(SystemExit, "v0.3.8"):
                common.require_datetime()

    def test_quote_formatter_handles_null_zero_and_all_market_schemas(self):
        common = importlib.import_module("examples._common")
        cases = ((1, "601919.SH"), (2, "00700.HK"), (3, "rb2701.SHF"),
                 (4, "AAPL"), (5, "EUR.USD"), (6, "CNmain.SGX"), (7, "BTCUSDT.BINANCE"))
        for source, code in cases:
            with self.subTest(source=source):
                quote = full_quote(code, source)
                before = dict(quote.fields)
                rendered = common.format_quote(quote)
                self.assertIn("last=N/A", rendered)
                self.assertIn("volume_raw=N/A", rendered)
                self.assertEqual(before, dict(quote.fields))
                rendered = common.format_quote(full_quote(code, source, null=False))
                self.assertIn("volume_raw=0", rendered)
                self.assertIn("bid1=1.120122/0", rendered)
                self.assertIn("ask1=1.120124/123", rendered)
        legacy = rtdata.Quote("AAPL", 1, 1.0, 2.0, None, 0, 1793629800000)
        self.assertIn("bid1=1.000000/N/A", common.format_quote(legacy))

    def test_diagnostic_counts_repeated_trade_times_and_current_silence(self):
        module = importlib.import_module("examples.diagnose_realtime_stall")
        observations = module.Observations()
        with patch.object(module.time, "monotonic", side_effect=[10.0, 13.0]):
            observations.record(full_quote())
            observations.record(full_quote())
        count, last, gap, quote = observations.snapshot()["BTCUSDT.BINANCE"]
        self.assertEqual((count, last, gap), (2, 13.0, 3.0))
        api = create_autospec(rtdata.API, instance=True)
        api.realtime_statistics = {}
        with patch.object(module.time, "monotonic", return_value=20.0), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            module.report(api, observations, 10.0)
        self.assertIn("silence_s=7.0 max_callback_gap_s=7.0", output.getvalue())
        self.assertIn("event_age_ms=", output.getvalue())

    def test_all_example_main_flows_with_mock_api_and_cleanup(self):
        for path in EXAMPLES:
            if path.name.startswith("_"):
                continue
            with self.subTest(example=path.name):
                module = importlib.import_module("examples." + path.stem)
                api = create_autospec(rtdata.API, instance=True)
                api.__enter__.return_value = api
                api.__exit__.side_effect = lambda *args: api.close()
                api.realtime_statistics = {}
                api.token_status = None
                callbacks = []
                api.on_quote.side_effect = lambda fn: callbacks.append(fn) or fn
                api.subscribe.side_effect = lambda *args, **kwargs: [fn(full_quote()) for fn in callbacks]
                api.get_kline.return_value = [rtdata.Kline(
                    1793629800000, 1, 2, 1, 2, 0, 0, 0, symbol="000001.SZ")]
                for name in ("get_finance", "get_finance_ttm", "get_finance_ratios", "get_finance_pit"):
                    getattr(api, name).return_value = rtdata.FinanceData("600519.SH", "2025-12-31", {})
                with patch.dict(os.environ, {"RTDATA_TOKEN": "example-only", "RTDATA_API_URL": "https://discovery.example"}), \
                        patch.object(rtdata, "API", return_value=api) as factory, \
                        patch("socket.socket") as socket, \
                        patch("logging.basicConfig"), \
                        patch("time.sleep", side_effect=KeyboardInterrupt), \
                        contextlib.redirect_stdout(io.StringIO()) as output:
                    module.main()
                factory.assert_called_once()
                self.assertEqual(factory.call_args.kwargs["token"], "example-only")
                api.close.assert_called_once()
                self.assertNotIn("example-only", output.getvalue())
                socket.assert_not_called()

    def test_subscription_failure_also_closes_connection(self):
        for name in ("basic_usage", "subscribe_realtime", "diagnose_realtime_stall", "session_rehome"):
            module = importlib.import_module("examples." + name)
            api = create_autospec(rtdata.API, instance=True)
            api.subscribe.side_effect = rtdata.SymbolNotFoundError("example missing symbol")
            with self.subTest(example=name), patch.dict(os.environ, {"RTDATA_TOKEN": "example-only"}), \
                    patch.object(rtdata, "API", return_value=api), patch("logging.basicConfig"), \
                    contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(rtdata.SymbolNotFoundError):
                    module.main()
                api.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
