import unittest
from dataclasses import replace

from rtdata import _history_capabilities as capabilities


GOLDEN_PAYLOAD = bytes.fromhex(
    "01 01 00 03 00 03 00 03 00 00 00 0f 00 04 00 00 00 00 00 03"
)


def full_capabilities(role, max_block_bytes=256 * 1024):
    return capabilities.HistoryCapabilities(
        role=role,
        history_protocol_mask=capabilities.PROTOCOL_KNOWN_MASK,
        codec_mask=capabilities.CODEC_KNOWN_MASK,
        compression_mask=capabilities.COMPRESSION_KNOWN_MASK,
        feature_mask=capabilities.FEATURE_KNOWN_MASK,
        max_block_bytes=max_block_bytes,
        column_schema_mask=capabilities.COLUMN_SCHEMA_KNOWN_MASK,
    )


class HistoryCapabilitiesTest(unittest.TestCase):
    def test_golden_fixture(self):
        value = full_capabilities(capabilities.CapabilityRole.UPCLOUD)
        value = replace(
            value,
            compression_mask=(
                capabilities.COMPRESSION_NONE | capabilities.COMPRESSION_ZSTD
            ),
        )

        self.assertEqual(value.encode(), GOLDEN_PAYLOAD)
        decoded = capabilities.HistoryCapabilities.decode(GOLDEN_PAYLOAD)
        self.assertEqual(decoded, value)
        self.assertTrue(capabilities.v2_eligible(decoded))

    def test_rejects_truncated_unknown_and_invalid_block_payloads(self):
        with self.assertRaisesRegex(ValueError, "exactly 20 bytes"):
            capabilities.HistoryCapabilities.decode(GOLDEN_PAYLOAD[:-1])

        invalid_version = bytes([2]) + GOLDEN_PAYLOAD[1:]
        with self.assertRaisesRegex(ValueError, "schema version"):
            capabilities.HistoryCapabilities.decode(invalid_version)

        invalid_role = GOLDEN_PAYLOAD[:1] + bytes([0]) + GOLDEN_PAYLOAD[2:]
        with self.assertRaisesRegex(ValueError, "capability role"):
            capabilities.HistoryCapabilities.decode(invalid_role)

        unknown_feature = bytearray(GOLDEN_PAYLOAD)
        unknown_feature[9] = 0x80
        with self.assertRaisesRegex(ValueError, "feature mask"):
            capabilities.HistoryCapabilities.decode(bytes(unknown_feature))

        invalid_block = replace(
            full_capabilities(capabilities.CapabilityRole.UPCLOUD),
            max_block_bytes=1024,
        )
        with self.assertRaisesRegex(ValueError, "block size"):
            invalid_block.encode()

    def test_intersection_and_v1_fallback(self):
        upstream = full_capabilities(
            capabilities.CapabilityRole.UPCLOUD,
            max_block_bytes=512 * 1024,
        )
        cloud = replace(
            full_capabilities(capabilities.CapabilityRole.CLOUD),
            compression_mask=(
                capabilities.COMPRESSION_NONE | capabilities.COMPRESSION_ZSTD
            ),
            feature_mask=(
                capabilities.FEATURE_WINDOW_UPDATE
                | capabilities.FEATURE_CANCEL
                | capabilities.FEATURE_OPTIONAL_COLUMNS
            ),
        )

        intersection = capabilities.intersect_capabilities(
            upstream, cloud, capabilities.CapabilityRole.CLOUD)
        self.assertEqual(intersection.max_block_bytes, 256 * 1024)
        self.assertEqual(intersection.compression_mask, cloud.compression_mask)
        self.assertEqual(intersection.feature_mask, cloud.feature_mask)
        self.assertTrue(capabilities.v2_eligible(intersection))

        missing_required_feature = replace(
            intersection,
            feature_mask=(
                capabilities.FEATURE_WINDOW_UPDATE
                | capabilities.FEATURE_CANCEL
            ),
        )
        self.assertFalse(capabilities.v2_eligible(missing_required_feature))

        fallback = capabilities.intersect_capabilities(
            intersection,
            capabilities.v1_capabilities(capabilities.CapabilityRole.RTDATA),
            capabilities.CapabilityRole.CLOUD,
        )
        self.assertEqual(fallback.history_protocol_mask, capabilities.PROTOCOL_V1)
        self.assertEqual(fallback.max_block_bytes, 0)
        self.assertFalse(capabilities.v2_eligible(fallback))


if __name__ == "__main__":
    unittest.main()
