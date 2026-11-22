import importlib.util
import concurrent.futures
import ctypes
import json
from pathlib import Path
import random
import struct
import sys
import unittest
from unittest.mock import patch

from rtdata import _numeric_codec as numeric
from rtdata import _realtime_protocol as wire
from rtdata import _realtime_block as block
import test_realtime_client as helpers


def compact(snapshot):
    body = wire.encode(snapshot)
    columns = wire.SCHEMAS[snapshot.schema_id]
    result = bytearray(block.FRAME.pack(len(body), 1, wire.SNAPSHOT) + body[:32 + (len(columns) + 7) // 8])
    for (name, kind), value in zip(columns, snapshot.values):
        if value is None:
            continue
        if kind == 'DOUBLE':
            result.extend(b'\0' + struct.pack('<d', value))
        elif kind in ('LONG', 'INT', 'TIMESTAMP'):
            number = 2 * value if value >= 0 else -2 * value - 1
            while number >= 128:
                result.append((number & 127) | 128)
                number >>= 7
            result.append(number)
        elif kind == 'CHAR':
            result.extend(struct.pack('<b', value))
        else:
            data = value.encode('utf-8')
            result.extend(len(data).to_bytes(4 if snapshot.schema_id >= 4 and name == 'raw_bits' else 2, 'little'))
            result.extend(data)
    return bytes(result), block.FRAME.pack(len(body), 1, wire.SNAPSHOT) + body


def packed(data, raw_size, count=1):
    params = block._zstd.ZstdCompressionParameters.from_level(1, window_log=16,
        write_checksum=1, write_content_size=1)
    zstd = block._zstd.ZstdCompressor(compression_params=params).compress(data)
    return numeric.HEADER.pack(b'RBLX', 1, 4, 0, raw_size, count, 0, len(zstd), len(data)) + zstd


class NumericOptionalTest(unittest.TestCase):
    def test_missing_package_does_not_change_original_codec(self):
        with patch.object(numeric, '_loaded', False), patch.object(numeric, '_decoder', None), \
                patch.dict(sys.modules, {'rtdata_numeric_codec': None}):
            self.assertFalse(numeric.available())
            raw = compact(helpers.fixture())[1] * 4
            self.assertEqual(block.decode(block.encode(raw)), raw)

    def test_schema_or_abi_failure_not_advertised(self):
        from types import SimpleNamespace
        for decoder in (lambda: (_ for _ in ()).throw(RuntimeError('ABI')),
                        lambda: SimpleNamespace(schema=lambda _: 'wrong')):
            with patch.object(numeric, '_loaded', False), patch.object(numeric, '_decoder', None), \
                    patch.dict(sys.modules, {'rtdata_numeric_codec': SimpleNamespace(Decoder=decoder)}):
                self.assertFalse(numeric.available())

    def test_selftest_detects_wrong_restoration(self):
        from types import SimpleNamespace
        with self.assertRaises(RuntimeError):
            numeric._selftest(SimpleNamespace(restore=lambda *args: b'bad'))


@unittest.skipUnless(importlib.util.find_spec('rtdata_numeric_codec'), 'optional native wheel not installed')
class NumericNativeTest(unittest.TestCase):
    def test_cpp_golden_blocks_byte_exact(self):
        records = [json.loads(line) for line in
                   (Path(__file__).parent / 'fixtures/numeric_blocks_v1.jsonl').read_text().splitlines()]
        self.assertEqual(len(records), 49)
        def check(record):
            raw = numeric.decode(bytes.fromhex(record['numeric']))
            self.assertEqual(raw, bytes.fromhex(record['raw']))
            rebuilt = b''.join(block.FRAME.pack(len(body), sid, wire.SNAPSHOT) + body
                               for sid, payload in block.frames(raw)
                               for body in (wire.encode(wire.decode(payload)),))
            self.assertEqual(rebuilt, raw)
        for record in records:
            check(record)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(check, records * 2))

    def test_schema_abi_and_exact_restore(self):
        self.assertTrue(numeric.available())
        data, raw = compact(helpers.fixture())
        self.assertEqual(numeric.decode(packed(data * 6, len(raw) * 6, 6)), raw * 6)

    def test_negative_envelope_checksum_and_atomic_tail(self):
        data, raw = compact(helpers.fixture())
        good = packed(data, len(raw))
        for payload in (good[:-1], b'x' + good[1:], good + b'x',
                        packed(data * 2 + b'x', len(raw) * 2, 2)):
            with self.assertRaises(ValueError):
                numeric.decode(payload)
        for offset in (4, 5, 6, 8, 12, 14, 16, 20, len(good) - 1):
            bad = bytearray(good)
            bad[offset] ^= 128
            with self.assertRaises(ValueError):
                numeric.decode(bytes(bad))

    def test_native_rejects_input_types_sizes(self):
        from rtdata_numeric_codec import Decoder
        decoder = Decoder()
        for data, size, count in ((b'', 1, 1), (b'x', 0, 1), (b'x', 65537, 1),
                                  (b'x', 1, 257), (bytearray(b'x'), 1, 1),
                                  (b'x', True, 1), (b'x', 1, True)):
            with self.assertRaises(ValueError):
                decoder.restore(data, size, count)

    def test_native_ffi_atomic_rejection_and_guard_bytes(self):
        from rtdata_numeric_codec import Decoder
        decoder = Decoder()
        data, raw = compact(helpers.fixture())
        output = ctypes.create_string_buffer(b'Q' * (65536 + 32), 65536 + 32)
        pointer = ctypes.c_void_p(ctypes.addressof(output) + 16)
        for item, size, count, capacity in ((data, len(raw) - 1, 1, 65536),
                (data[:-1], len(raw), 1, 65536), (data, len(raw), 1, len(raw) - 1),
                (data, len(raw), 257, 65536), (None, len(raw), 1, 65536),
                (data, len(raw), 1, 65537)):
            written = ctypes.c_size_t(999)
            self.assertNotEqual(decoder._call(item, len(item) if item else 0, size, count,
                pointer, capacity, ctypes.byref(written)), 0)
            self.assertEqual(written.value, 0)
            self.assertEqual(output.raw, b'Q' * (65536 + 32))
        self.assertNotEqual(decoder._call(data, len(data), len(raw), 1, pointer, 65536, None), 0)

    def test_mutated_inner_data_always_has_valid_schema_if_accepted(self):
        from rtdata_numeric_codec import Decoder
        decoder = Decoder()
        data, raw = compact(helpers.fixture())
        rng = random.Random(1711)
        for _ in range(512):
            bad = bytearray(data)
            bad[rng.randrange(len(bad))] ^= 1 << rng.randrange(8)
            try:
                restored = decoder.restore(bytes(bad), len(raw), 1)
            except ValueError:
                continue
            frames = block.frames(restored)
            self.assertEqual(len(frames), 1)
            self.assertEqual(wire.encode(wire.decode(frames[0][1])), bytes(frames[0][1]))


if __name__ == '__main__':
    unittest.main()
