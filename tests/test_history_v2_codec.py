import struct
import unittest

from rtdata import _history_v2_codec as codec
from rtdata import _history_v2_protocol as protocol


COLUMNAR_GOLDEN = bytes.fromhex(
    "01 02 00 3f 00 00 00 03 00 00 00 0c 00 00 00 00 "
    "00 00 01 8b cf e5 68 00 c0 a9 07 00 "
    "00 00 a0 3f 33 33 b3 3f 66 66 c6 3f "
    "00 00 c0 3f cd cc cc 3f 9a 99 d9 3f "
    "00 00 80 3f 66 66 a6 3f 00 00 c0 3f "
    "33 33 b3 3f 66 66 c6 3f 33 33 d3 3f "
    "64 00 00 00 00 00 00 00 c8 00 00 00 00 00 00 00 "
    "2c 01 00 00 00 00 00 00"
)


class HistoryV2CodecTest(unittest.TestCase):
    def setUp(self):
        self.rows = [
            (1700000000000, 1.25, 1.50, 1.00, 1.40, 100),
            (1700000060000, 1.40, 1.60, 1.30, 1.55, 200),
            (1700000120000, 1.55, 1.70, 1.50, 1.65, 300),
        ]

    def test_columnar_roundtrip_preserves_float32_bits(self):
        encoded = codec.encode_columnar_block(self.rows)
        self.assertEqual(encoded.uncompressed, COLUMNAR_GOLDEN)
        decoded = codec.decode_columnar_block(
            encoded.uncompressed, expected_rows=len(self.rows)
        )
        self.assertEqual([row[0] for row in decoded], [row[0] for row in self.rows])
        self.assertEqual([row[5] for row in decoded], [row[5] for row in self.rows])
        for expected, actual in zip(self.rows, decoded):
            for index in range(1, 5):
                self.assertEqual(
                    struct.pack("<f", expected[index]),
                    struct.pack("<f", actual[index]),
                )
            self.assertEqual(actual[6:], (0.0, 0))

    def test_rejects_order_row_count_and_trailing_data(self):
        with self.assertRaisesRegex(ValueError, "increasing"):
            codec.encode_columnar_block([self.rows[1], self.rows[0]])
        encoded = codec.encode_columnar_block(self.rows)
        with self.assertRaisesRegex(ValueError, "row count"):
            codec.decode_columnar_block(encoded.uncompressed, expected_rows=2)
        with self.assertRaisesRegex(ValueError, "length mismatch"):
            codec.decode_columnar_block(
                encoded.uncompressed + b"x", expected_rows=len(self.rows)
            )

    def test_zstd_checksum_when_optional_dependency_is_available(self):
        if not codec.zstd_available():
            self.skipTest("optional zstandard package is not installed")
        raw = b"x" * 4096
        compressed = codec.compress_zstd(raw)
        self.assertEqual(codec.decompress_zstd(compressed, len(raw)), raw)
        corrupted = bytearray(compressed)
        corrupted[len(corrupted) // 2] ^= 0x40
        with self.assertRaisesRegex(ValueError, "Zstd decompression failed"):
            codec.decompress_zstd(bytes(corrupted), len(raw))

    def test_optional_dependency_absence_keeps_raw_codec_available(self):
        encoded = codec.encode_columnar_block(self.rows)
        self.assertTrue(encoded.uncompressed)
        if not codec.zstd_available():
            self.assertEqual(encoded.compressed, b"")
            with self.assertRaisesRegex(RuntimeError, "not available"):
                codec.compress_zstd(encoded.uncompressed)


if __name__ == "__main__":
    unittest.main()
