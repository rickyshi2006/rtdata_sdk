import struct
import threading
import unittest
from unittest import mock

from rtdata import _history_capabilities as capabilities
from rtdata import _history_capability_runtime as capability_runtime
from rtdata import _history_v2_codec as codec
from rtdata import _history_v2_protocol as protocol
from rtdata import _protocol as wire
from rtdata.client import HISTORY_V2_PAGE_ROWS, RtdataClient
from rtdata.exceptions import QueryTimeoutError


class FakeConnection:
    def __init__(self, client, rows, raw, *, respond=True):
        self.client = client
        self.rows = rows
        self.raw = raw
        self.respond = respond
        self.connected = True
        self.generation = 1
        self.sent = []
        self._lock = threading.Lock()

    def send(self, message):
        payload_length, _, msg_type = wire.decode_header(message)
        payload = message[wire.HEADER_SIZE :]
        assert payload_length == len(payload)
        with self._lock:
            self.sent.append((msg_type, payload))
        if msg_type == wire.MsgType.HISTORY_REQUEST and self.respond:
            self._respond_to_history(payload)
        return self.connected

    def _respond_to_history(self, request_payload):
        request_id = struct.unpack_from("!I", request_payload)[0]
        options = protocol.RequestOptions.decode(
            request_payload[-protocol.REQUEST_OPTIONS_STRUCT.size :]
        )
        begin = protocol.HistoryBegin(
            request_id=request_id,
            symbol_id=9001,
            period=1,
            estimated_rows=len(self.rows),
            start_time_ms=self.rows[0][0],
            end_time_ms=self.rows[-1][0],
            max_block_bytes=options.max_block_bytes,
        )
        compressed = b"zstd-frame"
        data_header = protocol.HistoryDataHeader(
            request_id=request_id,
            chunk_seq=0,
            row_count=len(self.rows),
            uncompressed_size=len(self.raw),
            compressed_size=len(compressed),
            first_timestamp_ms=self.rows[0][0],
            last_timestamp_ms=self.rows[-1][0],
        )
        finish = protocol.HistoryEnd(
            request_id=request_id,
            actual_total_rows=len(self.rows),
            actual_uncompressed_bytes=len(self.raw),
            actual_compressed_bytes=len(compressed),
            chunk_count=1,
            last_chunk_seq=0,
        )
        for msg_type, payload in (
            (wire.MsgType.HISTORY_BEGIN, begin.encode()),
            (
                wire.MsgType.HISTORY_DATA,
                data_header.encode(options.max_block_bytes) + compressed,
            ),
            (wire.MsgType.HISTORY_END, finish.encode()),
        ):
            self.client._dispatch_message(
                msg_type,
                0,
                payload,
                self.generation,
                self,
            )

    def close(self):
        self.connected = False

    def sent_frames(self, msg_type):
        with self._lock:
            return [payload for current, payload in self.sent if current == msg_type]


class HistoryV2ClientFlowTest(unittest.TestCase):
    def setUp(self):
        self.rows = [
            (1700000000000, 1.25, 1.50, 1.00, 1.40, 100),
            (1700000060000, 1.40, 1.60, 1.30, 1.55, 200),
            (1700000120000, 1.55, 1.70, 1.50, 1.65, 300),
        ]
        self.raw = codec.encode_columnar_block(self.rows).uncompressed

    def make_client(self, *, default_enabled=True, respond=True):
        with mock.patch.object(capability_runtime.codec, "_zstd", object()):
            client = RtdataClient(
                token="test",
                async_callbacks=False,
                history_cache_enabled=False,
                history_v2_advertise=True,
                history_v2_default=default_enabled,
                history_v2_max_block_bytes=256 * 1024,
            )
        self.assertTrue(client._history_capability.begin_offer(lambda _: True))
        acknowledgement = capabilities.v2_capabilities(
            capabilities.CapabilityRole.CLOUD,
            max_block_bytes=256 * 1024,
        )
        self.assertTrue(
            client._history_capability.handle_ack(acknowledgement.encode())
        )
        connection = FakeConnection(
            client, self.rows, self.raw, respond=respond
        )
        client._conn = connection
        client._authenticated = True
        return client, connection

    def test_v2_request_decodes_off_thread_and_replenishes_window(self):
        client, connection = self.make_client()
        with mock.patch.object(
            client._symbol_map, "code_to_id", return_value=9001
        ), mock.patch.object(
            codec, "decompress_zstd", return_value=self.raw
        ):
            result = client._perform_history_query(
                "TEST.US",
                "1m",
                1700000000000,
                1700000120000,
                5000,
                2.0,
            )

        self.assertEqual([row.timestamp for row in result], [row[0] for row in self.rows])
        requests = connection.sent_frames(wire.MsgType.HISTORY_REQUEST)
        self.assertEqual(len(requests), 1)
        self.assertEqual(struct.unpack_from("!I", requests[0], 25)[0], HISTORY_V2_PAGE_ROWS)
        options = protocol.RequestOptions.decode(
            requests[0][-protocol.REQUEST_OPTIONS_STRUCT.size :]
        )
        self.assertEqual(options.max_block_bytes, 256 * 1024)

        updates = connection.sent_frames(wire.MsgType.HISTORY_WINDOW_UPDATE)
        self.assertEqual(len(updates), 1)
        update = protocol.HistoryWindowUpdate.decode(updates[0])
        self.assertEqual(update.received_through_seq, 0)
        self.assertGreater(update.grant_bytes, protocol.OUTER_HEADER_SIZE)
        self.assertFalse(connection.sent_frames(wire.MsgType.HISTORY_CANCEL))
        client.close()

    def test_timeout_sends_one_idempotent_cancel(self):
        client, connection = self.make_client(respond=False)
        with mock.patch.object(
            client._symbol_map, "code_to_id", return_value=9001
        ):
            with self.assertRaises(QueryTimeoutError):
                client._perform_history_query(
                    "TEST.US", "1m", 0, 0, 5000, 0.05
                )
        cancels = connection.sent_frames(wire.MsgType.HISTORY_CANCEL)
        self.assertEqual(len(cancels), 1)
        cancel = protocol.HistoryCancel.decode(cancels[0])
        self.assertEqual(cancel.reason, protocol.CancelReason.TIMEOUT)
        client.close()

    def test_negotiated_v2_stays_v1_when_default_switch_is_off(self):
        client, _ = self.make_client(default_enabled=False)
        options, _ = client._select_history_v2_request()
        self.assertIsNone(options)
        client.close()


if __name__ == "__main__":
    unittest.main()
