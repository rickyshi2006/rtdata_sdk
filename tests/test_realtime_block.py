import os
import random
import subprocess
import unittest

from rtdata import _realtime_block as block
from rtdata import _realtime_protocol as realtime


def inner(payload=b'A' * 256, symbol=1, kind=0x63):
    return block.FRAME.pack(len(payload), symbol, kind) + payload


def envelope(raw, compressed, count=1):
    return block.HEADER.pack(b'RBLK', 1, 1, 0, len(raw), count, 0, len(compressed)) + compressed


def oversized_window(compressed):
    # A 266-byte single-segment frame has a two-byte content size. Keep that
    # exact size while advertising an excessive non-single-segment window.
    if compressed[4] >> 6 != 1 or not compressed[4] & 32:
        raise AssertionError('unexpected test frame header')
    return compressed[:4] + bytes((compressed[4] & ~32, 0x88)) + compressed[5:]


@unittest.skipUnless(block.available(), 'Zstd dependency unavailable')
class RealtimeBlockTests(unittest.TestCase):
    def test_roundtrip_limits_and_raw_fallback(self):
        for count in (1, 32, 256):
            raw = b''.join(inner(b'A' * 32, n + 1) for n in range(count))
            packed = block.encode(raw)
            if packed:
                self.assertEqual(block.decode(packed), raw)
                self.assertEqual(len(block.frames(raw)), count)
        raw = inner(b'A' * (block.MAX_RAW_BYTES - block.FRAME.size))
        self.assertEqual(block.decode(block.encode(raw)), raw)
        generator = random.Random(19)
        noise = bytes(generator.getrandbits(8) for _ in range(4096))
        self.assertEqual(block.encode(inner(noise)), b'')
        for raw in (b'', inner(b'A' * block.MAX_RAW_BYTES), inner() * 257):
            with self.assertRaises(ValueError):
                block.encode(raw)

    def test_envelope_and_zstd_rejections(self):
        packed = block.encode(inner() * 32)
        for n in range(len(packed)):
            with self.assertRaises(ValueError):
                block.decode(packed[:n])
        for offset in (0, 4, 5, 6, 8, 12, 14, 16, len(packed) - 1):
            bad = bytearray(packed)
            bad[offset] ^= 128
            with self.assertRaises(ValueError):
                block.decode(bytes(bad))
        raw = inner()
        zstd = block._zstd
        for compressor in (
                zstd.ZstdCompressor(write_checksum=False),
                zstd.ZstdCompressor(write_checksum=True, write_content_size=False)):
            with self.assertRaises(ValueError):
                block.decode(envelope(raw, compressor.compress(raw)))
        compressed = zstd.ZstdCompressor(write_checksum=True).compress(raw)
        for extra in (b'\0', compressed):
            with self.assertRaises(ValueError):
                block.decode(envelope(raw, compressed + extra))
        # A tiny content size must not permit a large advertised window.
        changed = oversized_window(compressed)
        self.assertEqual(zstd.get_frame_parameters(changed).content_size, len(raw))
        self.assertGreater(zstd.get_frame_parameters(changed).window_size, block.MAX_RAW_BYTES)
        with self.assertRaises(ValueError):
            block.decode(envelope(raw, changed))

    def test_control_nested_and_malformed_inner(self):
        zstd = block._zstd.ZstdCompressor(write_checksum=True)
        for raw in (inner(kind=3), inner(symbol=0), inner(b'A'), inner() + b'A',
                    inner(kind=0x65), inner() * 257):
            with self.assertRaises(ValueError):
                block.decode(envelope(raw, zstd.compress(raw)))
        with self.assertRaises(ValueError):
            block.decode(envelope(inner(), zstd.compress(inner()), count=2))


@unittest.skipUnless(os.environ.get('RTDATA_REALTIME_BLOCK_PROBE'), 'native probe not configured')
class RealtimeBlockCrossLanguageTests(unittest.TestCase):
    def probe(self, mode, data=b'', ok=True):
        result = subprocess.run([os.environ['RTDATA_REALTIME_BLOCK_PROBE'], mode], input=data,
                                capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0 if ok else 2, result.stderr)
        return result.stdout

    def test_seven_source_full_fields_both_directions(self):
        raw = self.probe('sample')
        packed = self.probe('sample-block')
        self.assertEqual(block.decode(packed), raw)
        decoded = [realtime.decode(payload) for _, payload in block.frames(raw)]
        self.assertEqual({row.source_id for row in decoded}, set(range(1, 8)))
        self.assertEqual(self.probe('decode', block.encode(raw)), raw)
        self.assertEqual(block.decode(self.probe('encode', raw)), raw)

    def test_native_rejects_foreign_corrupt_frames(self):
        raw = inner() * 8
        packed = block.encode(raw)
        compressed = packed[block.HEADER.size:]
        self.probe('decode', envelope(raw, compressed + compressed, 8), ok=False)
        bad = bytearray(packed)
        bad[-1] ^= 1
        self.probe('decode', bad, ok=False)
        no_checksum = block._zstd.ZstdCompressor(write_checksum=False).compress(raw)
        self.probe('decode', envelope(raw, no_checksum, 8), ok=False)
        one = inner()
        compressed = block._zstd.ZstdCompressor(write_checksum=True).compress(one)
        self.probe('decode', envelope(one, oversized_window(compressed)), ok=False)

    def test_mutated_envelopes_agree(self):
        packed = block.encode(inner() * 8)
        generator = random.Random(7041)
        for _ in range(64):
            changed = bytearray(packed)
            index = generator.randrange(len(changed))
            changed[index] ^= 1 << generator.randrange(8)
            with self.assertRaises(ValueError):
                block.decode(bytes(changed))
            self.probe('decode', changed, ok=False)
