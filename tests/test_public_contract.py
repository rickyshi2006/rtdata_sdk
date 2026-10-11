import unittest

from rtdata import Quote
from rtdata import _history_capabilities as history
from rtdata import _history_v2_protocol as history_wire
from rtdata import _realtime_capability as realtime
from rtdata._realtime_schema import SOURCES, REGISTRY_SHA256, SCHEMA_HASHES
from test_realtime_client import fixture


class PublicContractTest(unittest.TestCase):
    def test_quote_categories_are_public_market_labels(self):
        expected = {1: ('cn_equity', 1), 2: ('hk_equity', 1), 3: ('futures', 2),
                    4: ('us_equity', 1), 5: ('fx', 3)}
        self.assertEqual(dict(SOURCES), expected)
        for sid, (label, _) in expected.items():
            quote = Quote.from_snapshot(fixture(sid), sid, fixture(sid).fields['code'])
            self.assertEqual(quote.source_table, label)

    def test_display_labels_do_not_change_wire_identity(self):
        self.assertEqual(REGISTRY_SHA256,
                         '0acc8d004af8a4a16e6a8dec97a21d222941d0877076cd1eff3c548f8efe0269')
        self.assertEqual(dict(SCHEMA_HASHES), {
            1: 'b626c7e17c707bf20db69c155a96cae88715dfb1e03427ce73d59b5291255a87',
            2: '5ea9dc55fbbcf038943ff8018373b29f5ae87544a07e8ce6942c42b180342de5',
            3: 'f34d842d61e9c60f7582ce36a18348a6139a9519a7dd7a7d1248103543d6deb5',
        })
        self.assertEqual(int(history.CapabilityRole.PRODUCER), 1)
        self.assertEqual(int(realtime.Role.PRODUCER), 1)
        self.assertEqual(int(history_wire.ErrorCode.QUERY_ERROR), 3)


if __name__ == '__main__':
    unittest.main()
