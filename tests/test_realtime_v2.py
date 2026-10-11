import math
from pathlib import Path
import struct
import unittest

from rtdata import _protocol as legacy
from rtdata import _realtime_protocol as rt
from rtdata._realtime_schema import SCHEMAS, SOURCES


def fixture(source_id, nulls=False):
    values = []
    for i, (name, kind) in enumerate(SCHEMAS[SOURCES[source_id][1]]):
        if name == 'code':
            value = 'TEST.01'
        elif name == 'trade_time':
            value = -1234567890123
        elif nulls:
            value = None
        elif kind == 'DOUBLE':
            value = 1.123456789012345 + i
        elif kind == 'LONG':
            value = (1 << 53) + i + 1
        elif kind == 'INT':
            value = -(1 << 31) if i % 2 else (1 << 31) - 1
        elif kind == 'CHAR':
            value = -128
        else:
            value = '\u6d4b\u8bd5' + str(i)
        values.append(value)
    return rt.Snapshot(source_id, (1 << 63) + 123, (1 << 64) - 1, tuple(values))


def replace(snap, **changes):
    fields = dict(snap.fields)
    fields.update(changes)
    return rt.Snapshot.from_fields(snap.source_id, snap.source_epoch, snap.sequence, fields)


class RealtimeV2Test(unittest.TestCase):
    def test_independent_goldens_all_sources(self):
        rows = Path(__file__).with_name('fixtures').joinpath('realtime_v2.tsv').read_text().splitlines()
        self.assertEqual(len(rows), 10)
        for row in rows:
            source, mode, payload_hex, legacy_hex = row.split('\t')
            with self.subTest(source=source, mode=mode):
                expected = bytes.fromhex(payload_hex)
                snap = fixture(int(source), mode == 'null')
                self.assertEqual(rt.encode(snap), expected)
                self.assertEqual(rt.decode(expected), snap)
                projected, nulls = rt.project_legacy(snap, 0x12345678)
                self.assertEqual(projected.hex(), legacy_hex)
                self.assertEqual(nulls, 4 if mode == 'null' else 0)
                decoded_legacy = legacy.decode_snapshot(projected, 0x12345678)
                self.assertEqual(len(decoded_legacy), 1)
                self.assertEqual(decoded_legacy[0][4], snap.fields['volume'] or 0)

    def test_every_truncation_and_trailing_bytes(self):
        for source in SOURCES:
            data = rt.encode(fixture(source))
            for length in range(len(data)):
                with self.subTest(source=source, length=length), self.assertRaises(ValueError):
                    rt.decode(data[:length])
            with self.assertRaises(ValueError):
                rt.decode(data + b'\0')

    def test_header_and_bitmap_rejection(self):
        data = rt.encode(fixture(1))
        for offset in [0, 2, 4, 6, 24, 26, 28]:
            broken = bytearray(data)
            broken[offset] = 255
            with self.subTest(offset=offset), self.assertRaises(ValueError):
                rt.decode(broken)
        broken = bytearray(data)
        broken[36] |= 128
        with self.assertRaises(ValueError):
            rt.decode(broken)
        with self.assertRaises(ValueError):
            rt.decode(bytes(rt.MAX_PAYLOAD_BYTES + 1))

    def test_field_type_and_integer_bounds(self):
        snap = fixture(3)
        for field, bad in [('last', 1), ('volume', True), ('volume', 1 << 63),
                           ('volume', -(1 << 63) - 1), ('bid_volume1', 1 << 31),
                           ('option_class', 128), ('code', b'test')]:
            with self.subTest(field=field, value=bad), self.assertRaises(ValueError):
                replace(snap, **{field: bad})
        snap = replace(snap, volume=-(1 << 63))
        self.assertEqual(rt.decode(rt.encode(snap)), snap)
        with self.assertRaises(ValueError):
            rt.project_legacy(snap, 1)

    def test_null_zero_absent_and_immutable_fields(self):
        snap = fixture(5, True)
        self.assertIsNone(snap.fields['volume'])
        self.assertNotIn('bid_price2', snap.fields)
        with self.assertRaises(TypeError):
            snap.fields['volume'] = 1
        snap = replace(snap, volume=0, last=0.0)
        self.assertEqual(rt.decode(rt.encode(snap)).fields['volume'], 0)
        with self.assertRaises(ValueError):
            replace(snap, bid_price2=None)

    def test_identity_required(self):
        snap = fixture(1)
        for name in ['code', 'trade_time']:
            with self.assertRaises(ValueError):
                replace(snap, **{name: None})
        for code in ['', 'abc\0def']:
            with self.assertRaises(ValueError):
                replace(snap, code=code)
        for source, epoch, sequence in [(0, 1, 1), (True, 1, 1), (1, 0, 1), (1, 1, 0), (1, 1, 1 << 64)]:
            with self.assertRaises(ValueError):
                rt.Snapshot(source, epoch, sequence, snap.values)

    def test_float_bits_and_projection_boundaries(self):
        snap = fixture(1)
        for bits in [0x8000000000000000, 0x7ff8000000001234, 0x7ff0000000000000, 0xfff0000000000000]:
            value = struct.unpack('<d', struct.pack('<Q', bits))[0]
            updated = replace(snap, last=value)
            result = rt.decode(rt.encode(updated)).fields['last']
            self.assertEqual(struct.pack('<d', result), struct.pack('<Q', bits))
            if not math.isfinite(value):
                with self.assertRaises(ValueError):
                    rt.project_legacy(updated, 1)
        with self.assertRaises(ValueError):
            rt.project_legacy(replace(snap, last=1e100), 1)
        self.assertNotEqual(snap.fields['last'], struct.unpack('<f', struct.pack('<f', snap.fields['last']))[0])

    def test_string_limits_and_utf8(self):
        snap = replace(fixture(1), code='x' * 1024)
        self.assertEqual(rt.decode(rt.encode(snap)), snap)
        for bad in ['x' * 1025, '\ud800']:
            with self.assertRaises(ValueError):
                replace(snap, code=bad)
        data = bytearray(rt.encode(fixture(1)))
        data[39] = 0xff
        with self.assertRaises(ValueError):
            rt.decode(data)
        data = bytearray(rt.encode(fixture(1)))
        struct.pack_into('<H', data, 37, 1025)
        with self.assertRaises(ValueError):
            rt.decode(data)


if __name__ == '__main__':
    unittest.main()
