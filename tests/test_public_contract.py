import hashlib
import json
import unittest

from rtdata import Quote
from rtdata import _history_capabilities as history
from rtdata import _history_v2_protocol as history_wire
from rtdata import _realtime_capability as realtime
from rtdata._realtime_schema import SCHEMAS, SOURCES, REGISTRY_SHA256, SCHEMA_HASHES
from test_realtime_client import fixture


class PublicContractTest(unittest.TestCase):
    def test_quote_categories_are_public_market_labels(self):
        expected = {1: ('cn_equity', 1), 2: ('hk_equity', 1), 3: ('futures', 2),
                    4: ('us_equity', 1), 5: ('fx', 3)}
        self.assertEqual({sid: SOURCES[sid] for sid in range(1, 6)}, expected)
        self.assertEqual({sid: SOURCES[sid] for sid in (6, 7)},
                         {6: ('overseas_futures', 4), 7: ('crypto', 5)})
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
            4: 'cc48f9b7f689f968bac0ebcce63b64d852bafd6de2f73d62557fa97bc193e6c7',
            5: 'b5458ea9ee222bbc6ca65183a5a41ceafb07853e2433e96a38cf783b209a9d6c',
        })
        self.assertEqual(int(history.CapabilityRole.PRODUCER), 1)
        self.assertEqual(int(realtime.Role.PRODUCER), 1)
        self.assertEqual(int(history_wire.ErrorCode.QUERY_ERROR), 3)

    def test_schema_field_names_types_and_order_are_stable(self):
        self.assertEqual({sid: hashlib.sha256(json.dumps(fields, separators=(',', ':')).encode()).hexdigest()
                          for sid, fields in SCHEMAS.items()}, {
            1: '0fe973facf69c6ab5b3ab5f0d3d9e4305439d0cf299a9aee6b740a49d918c4f6',
            2: 'bb2e46caa8937adf5b4e10bd3937cc6ae9099271951e9528bc2b77005403179d',
            3: '41dcdc3261a85b6d284fe24fcdf43c509502c08b1f419ed2cc30191d05977e07',
            4: 'd73002193d38f49fa27f10b7003cfc234ec01bcf62caf380311679b54c9ac21a',
            5: 'ce853a90bd0b666e8000450c99d94cd64ba07b5d6809f06e7af98218f40a870a',
        })


if __name__ == '__main__':
    unittest.main()
