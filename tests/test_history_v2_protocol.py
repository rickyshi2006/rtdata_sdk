import unittest
from dataclasses import replace

from rtdata import _history_v2_protocol as v2


REQUEST_GOLDEN = bytes.fromhex(
    "48 32 56 31 01 02 01 01 00 00 00 0b "
    "00 01 00 00 00 08 00 00 00 18 00 00"
)
BEGIN_GOLDEN = bytes.fromhex(
    "01 02 03 04 01 01 01 05 11 22 33 44 01 00 00 00 "
    "00 00 00 3f 00 01 00 00 00 00 00 00 00 00 00 03 "
    "00 00 01 8b cf e5 68 00 00 00 01 8b cf e7 3c c0"
)
DATA_HEADER_GOLDEN = bytes.fromhex(
    "01 02 03 04 00 00 00 02 00 00 00 03 00 00 00 64 "
    "00 00 00 32 00 00 01 8b cf e5 68 00 00 00 01 8b "
    "cf e7 3c c0 00 00 00 00 00 00 00 00"
)
END_GOLDEN = bytes.fromhex(
    "01 02 03 04 00 00 00 00 00 00 00 03 "
    "00 00 00 00 00 00 00 64 00 00 00 00 00 00 00 32 "
    "00 00 00 01 00 00 00 00 00 00 00 00"
)
ERROR_GOLDEN = bytes.fromhex(
    "01 02 03 04 00 03 00 01 00 00 00 00 "
    "00 00 00 00 00 00 00 02 00 03 00 00 64 64 62"
)
CANCEL_GOLDEN = bytes.fromhex(
    "01 02 03 04 00 03 00 00 00 00 00 02"
)
WINDOW_GOLDEN = bytes.fromhex(
    "01 02 03 04 00 02 00 00 00 00 00 02"
)
COLUMNS_HEADER_GOLDEN = bytes.fromhex(
    "01 02 00 3f 00 00 00 03 00 00 00 0a 00 00 00 00"
)


class HistoryV2ProtocolTest(unittest.TestCase):
    def test_request_options_golden_and_bounds(self):
        options = v2.RequestOptions()
        self.assertEqual(options.encode(), REQUEST_GOLDEN)
        self.assertEqual(v2.RequestOptions.decode(REQUEST_GOLDEN), options)
        self.assertTrue(v2.has_request_magic(REQUEST_GOLDEN))

        malformed = bytearray(REQUEST_GOLDEN)
        malformed[-1] = 1
        with self.assertRaisesRegex(ValueError, "options header"):
            v2.RequestOptions.decode(bytes(malformed))
        with self.assertRaisesRegex(ValueError, "initial window"):
            replace(options, initial_window_bytes=64 * 1024).encode()

    def test_response_envelope_golden(self):
        begin = v2.HistoryBegin(
            request_id=0x01020304,
            symbol_id=0x11223344,
            period=1,
            estimated_rows=3,
            start_time_ms=1700000000000,
            end_time_ms=1700000120000,
        )
        self.assertEqual(begin.encode(), BEGIN_GOLDEN)
        self.assertEqual(v2.HistoryBegin.decode(BEGIN_GOLDEN), begin)

        data = v2.HistoryDataHeader(
            request_id=0x01020304,
            chunk_seq=2,
            row_count=3,
            uncompressed_size=100,
            compressed_size=50,
            first_timestamp_ms=1700000000000,
            last_timestamp_ms=1700000120000,
        )
        self.assertEqual(data.encode(), DATA_HEADER_GOLDEN)
        decoded, compressed = v2.HistoryDataHeader.decode(
            DATA_HEADER_GOLDEN + b"z" * 50,
            max_block_bytes=v2.DEFAULT_BLOCK_BYTES,
        )
        self.assertEqual(decoded, data)
        self.assertEqual(bytes(compressed), b"z" * 50)
        with self.assertRaisesRegex(ValueError, "compressed size"):
            v2.HistoryDataHeader.decode(DATA_HEADER_GOLDEN + b"z" * 49)

        finish = v2.HistoryEnd(
            request_id=0x01020304,
            actual_total_rows=3,
            actual_uncompressed_bytes=100,
            actual_compressed_bytes=50,
            chunk_count=1,
            last_chunk_seq=0,
        )
        self.assertEqual(finish.encode(), END_GOLDEN)
        self.assertEqual(v2.HistoryEnd.decode(END_GOLDEN), finish)

    def test_control_envelope_golden(self):
        failure = v2.HistoryError(
            request_id=0x01020304,
            error_code=v2.ErrorCode.DDB_ERROR,
            flags=1,
            last_chunk_seq=0,
            delivered_rows=2,
            message="ddb",
        )
        self.assertEqual(failure.encode(), ERROR_GOLDEN)
        self.assertEqual(v2.HistoryError.decode(ERROR_GOLDEN), failure)

        cancel = v2.HistoryCancel(
            request_id=0x01020304,
            reason=v2.CancelReason.TIMEOUT,
            last_seen_seq=2,
        )
        self.assertEqual(cancel.encode(), CANCEL_GOLDEN)
        self.assertEqual(v2.HistoryCancel.decode(CANCEL_GOLDEN), cancel)

        window = v2.HistoryWindowUpdate(
            request_id=0x01020304,
            grant_bytes=128 * 1024,
            received_through_seq=2,
        )
        self.assertEqual(window.encode(), WINDOW_GOLDEN)
        self.assertEqual(v2.HistoryWindowUpdate.decode(WINDOW_GOLDEN), window)

    def test_columns_header_golden_and_exact_length(self):
        header = v2.HistoryColumnsHeader(row_count=3, timestamp_bytes=10)
        self.assertEqual(header.encode(), COLUMNS_HEADER_GOLDEN)
        payload = COLUMNS_HEADER_GOLDEN + bytes(10 + 3 * 24)
        self.assertEqual(v2.HistoryColumnsHeader.decode(payload), header)
        with self.assertRaisesRegex(ValueError, "length mismatch"):
            v2.HistoryColumnsHeader.decode(payload + b"x")

    def test_rejects_request_scoped_malformed_data(self):
        corrupted = bytearray(BEGIN_GOLDEN)
        corrupted[13] = 1
        with self.assertRaises(ValueError):
            v2.HistoryBegin.decode(bytes(corrupted))
        with self.assertRaises(ValueError):
            v2.HistoryError.decode(ERROR_GOLDEN[:-1])
        with self.assertRaises(ValueError):
            v2.HistoryWindowUpdate.decode(WINDOW_GOLDEN[:-1])


if __name__ == "__main__":
    unittest.main()
