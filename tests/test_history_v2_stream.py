import unittest
from unittest import mock

from rtdata import _history_v2_codec as codec
from rtdata import _history_v2_protocol as protocol
from rtdata._history_v2_stream import HistoryV2RequestState


class HistoryV2RequestStateTest(unittest.TestCase):
    def setUp(self):
        self.request_id = 101
        self.rows = [
            (1700000000000, 1.25, 1.50, 1.00, 1.40, 100),
            (1700000060000, 1.40, 1.60, 1.30, 1.55, 200),
            (1700000120000, 1.55, 1.70, 1.50, 1.65, 300),
        ]
        self.raw = codec.encode_columnar_block(self.rows).uncompressed
        self.compressed = b"zstd-frame"
        self.options = protocol.RequestOptions()

    def make_state(self):
        return HistoryV2RequestState(
            request_id=self.request_id,
            options=self.options,
            capability_generation=7,
            expected_symbol_id=9001,
            expected_period=1,
        )

    def begin_payload(self):
        return protocol.HistoryBegin(
            request_id=self.request_id,
            symbol_id=9001,
            period=1,
            estimated_rows=len(self.rows),
            start_time_ms=self.rows[0][0],
            end_time_ms=self.rows[-1][0],
        ).encode()

    def data_payload(self, chunk_seq=0):
        header = protocol.HistoryDataHeader(
            request_id=self.request_id,
            chunk_seq=chunk_seq,
            row_count=len(self.rows),
            uncompressed_size=len(self.raw),
            compressed_size=len(self.compressed),
            first_timestamp_ms=self.rows[0][0],
            last_timestamp_ms=self.rows[-1][0],
        )
        return header.encode(self.options.max_block_bytes) + self.compressed

    def test_success_replenishes_exact_wire_bytes_and_finishes(self):
        state = self.make_state()
        state.handle_frame(protocol.MSG_HISTORY_BEGIN, self.begin_payload())
        data_payload = self.data_payload()
        with mock.patch.object(
            codec, "decompress_zstd", return_value=self.raw
        ) as decompress:
            result = state.handle_frame(
                protocol.MSG_HISTORY_DATA, data_payload
            )
        decompress.assert_called_once_with(
            self.compressed, len(self.raw)
        )
        self.assertEqual(
            result.window_grant_bytes,
            protocol.OUTER_HEADER_SIZE + len(data_payload),
        )
        self.assertEqual(result.received_through_seq, 0)

        finish = protocol.HistoryEnd(
            request_id=self.request_id,
            actual_total_rows=len(self.rows),
            actual_uncompressed_bytes=len(self.raw),
            actual_compressed_bytes=len(self.compressed),
            chunk_count=1,
            last_chunk_seq=0,
        )
        result = state.handle_frame(
            protocol.MSG_HISTORY_END, finish.encode()
        )
        self.assertTrue(result.terminal)
        decoded = state.take_rows()
        self.assertEqual([row[0] for row in decoded], [row[0] for row in self.rows])
        self.assertFalse(state.snapshot().active)

    def test_sequence_failure_cancels_once(self):
        state = self.make_state()
        state.handle_frame(protocol.MSG_HISTORY_BEGIN, self.begin_payload())
        with self.assertRaisesRegex(ValueError, "sequence"):
            state.handle_frame(
                protocol.MSG_HISTORY_DATA, self.data_payload(chunk_seq=1)
            )
        cancel = state.cancel(protocol.CancelReason.BACKPRESSURE)
        self.assertIsNotNone(cancel)
        self.assertEqual(cancel.request_id, self.request_id)
        self.assertEqual(cancel.last_seen_seq, 0xFFFFFFFF)
        self.assertIsNone(state.cancel(protocol.CancelReason.SHUTDOWN))

    def test_pre_begin_error_is_terminal(self):
        state = self.make_state()
        failure = protocol.HistoryError(
            request_id=self.request_id,
            error_code=protocol.ErrorCode.DDB_ERROR,
            message="ddb failed",
        )
        result = state.handle_frame(
            protocol.MSG_HISTORY_ERROR, failure.encode()
        )
        self.assertTrue(result.terminal)
        self.assertIn("ddb_error", result.error)
        self.assertIn("ddb failed", result.error)

    def test_begin_must_match_fixed_request_options(self):
        state = self.make_state()
        incompatible = protocol.HistoryBegin(
            request_id=self.request_id,
            symbol_id=9001,
            period=1,
            estimated_rows=0,
            start_time_ms=0,
            end_time_ms=0,
            max_block_bytes=128 * 1024,
        )
        with self.assertRaisesRegex(ValueError, "block size mismatch"):
            state.handle_frame(
                protocol.MSG_HISTORY_BEGIN, incompatible.encode()
            )


if __name__ == "__main__":
    unittest.main()
