import time
import unittest
from unittest.mock import patch

from rtdata import RtdataClient
from rtdata import _history_capabilities as capabilities
from rtdata import _protocol as proto


class FakeConnection:
    def __init__(self, generation=1, send_result=True):
        self.connected = True
        self.generation = generation
        self.sent = []
        self._send_result = send_result

    def send(self, data):
        self.sent.append(data)
        return self._send_result


def decode_message(message):
    payload_len, symbol_id, msg_type = proto.decode_header(
        message[:proto.HEADER_SIZE]
    )
    payload = message[proto.HEADER_SIZE:]
    if len(payload) != payload_len:
        raise AssertionError("invalid test message length")
    return msg_type, symbol_id, payload


class HistoryCapabilityRuntimeTest(unittest.TestCase):
    def make_client(self, connection=None, zstd_available=True, **kwargs):
        with patch(
            "rtdata._history_capability_runtime.codec.zstd_available",
            return_value=zstd_available,
        ):
            client = RtdataClient(
                token="test",
                async_callbacks=False,
                **kwargs,
            )
        client._conn = connection or FakeConnection()
        return client

    def test_default_disabled_sends_no_extra_message(self):
        connection = FakeConnection()
        client = self.make_client(connection)

        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1)

        self.assertEqual(connection.sent, [])
        self.assertEqual(client.history_capability_state, "disabled")
        self.assertFalse(client.history_v2_eligible)

    def test_auth_success_sends_offer_and_accepts_valid_ack(self):
        connection = FakeConnection()
        client = self.make_client(
            connection,
            history_v2_advertise=True,
            history_capability_ack_timeout=0.5,
        )
        self.assertEqual(connection.sent, [])

        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1)

        self.assertEqual(len(connection.sent), 1)
        msg_type, symbol_id, payload = decode_message(connection.sent[0])
        self.assertEqual(msg_type, proto.MsgType.CAPABILITY_OFFER)
        self.assertEqual(symbol_id, 0)
        offer = capabilities.HistoryCapabilities.decode(payload)
        self.assertEqual(offer.role, capabilities.CapabilityRole.RTDATA)
        self.assertTrue(capabilities.v2_eligible(offer))
        self.assertEqual(client.history_capability_state, "waiting_ack")

        ack = capabilities.v2_capabilities(
            capabilities.CapabilityRole.CLOUD,
            max_block_bytes=128 * 1024,
        ).encode()
        client._dispatch_message(proto.MsgType.CAPABILITY_ACK, 0, ack, 1)

        self.assertEqual(client.history_capability_state, "negotiated")
        self.assertTrue(client.history_v2_eligible)
        self.assertEqual(client.history_capabilities.max_block_bytes, 128 * 1024)

    def test_missing_zstd_advertises_v1_without_breaking_connection(self):
        connection = FakeConnection()
        client = self.make_client(
            connection,
            zstd_available=False,
            history_v2_advertise=True,
            history_capability_ack_timeout=0.5,
        )

        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1)

        self.assertEqual(len(connection.sent), 1)
        offer = capabilities.HistoryCapabilities.decode(
            decode_message(connection.sent[0])[2]
        )
        self.assertFalse(capabilities.v2_eligible(offer))
        self.assertEqual(offer.history_protocol_mask, capabilities.PROTOCOL_V1)
        self.assertTrue(connection.connected)

    def test_malformed_ack_falls_back_without_disconnect(self):
        connection = FakeConnection()
        client = self.make_client(
            connection,
            history_v2_advertise=True,
            history_capability_ack_timeout=0.5,
        )
        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1)

        client._dispatch_message(proto.MsgType.CAPABILITY_ACK, 0, b"short", 1)

        self.assertTrue(connection.connected)
        self.assertEqual(client.history_capability_state, "fallback")
        self.assertFalse(client.history_v2_eligible)
        self.assertIn("invalid_ack", client.history_capability_fallback_reason)

    def test_ack_with_wrong_role_falls_back_without_disconnect(self):
        connection = FakeConnection()
        client = self.make_client(
            connection,
            history_v2_advertise=True,
            history_capability_ack_timeout=0.5,
        )
        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1)
        wrong_role_ack = capabilities.v2_capabilities(
            capabilities.CapabilityRole.RTDATA
        ).encode()

        client._dispatch_message(
            proto.MsgType.CAPABILITY_ACK, 0, wrong_role_ack, 1
        )

        self.assertTrue(connection.connected)
        self.assertEqual(client.history_capability_state, "fallback")
        self.assertIn(
            "unexpected capability role",
            client.history_capability_fallback_reason,
        )

    def test_ack_timeout_falls_back_without_disconnect(self):
        connection = FakeConnection()
        client = self.make_client(
            connection,
            history_v2_advertise=True,
            history_capability_ack_timeout=0.02,
        )
        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1)

        deadline = time.monotonic() + 0.3
        while (
            client.history_capability_state != "fallback"
            and time.monotonic() < deadline
        ):
            time.sleep(0.005)

        self.assertTrue(connection.connected)
        self.assertEqual(client.history_capability_state, "fallback")
        self.assertEqual(
            client.history_capability_fallback_reason,
            "ack_timeout",
        )

    def test_reconnect_resets_resends_and_ignores_old_generation_ack(self):
        first_connection = FakeConnection(generation=1)
        client = self.make_client(
            first_connection,
            history_v2_advertise=True,
            history_capability_ack_timeout=0.5,
        )
        ack = capabilities.v2_capabilities(
            capabilities.CapabilityRole.CLOUD
        ).encode()

        client._dispatch_message(
            proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1, first_connection
        )
        client._dispatch_message(
            proto.MsgType.CAPABILITY_ACK, 0, ack, 1, first_connection
        )
        self.assertTrue(client.history_v2_eligible)

        client._handle_disconnected("test disconnect")
        self.assertEqual(client.history_capability_state, "idle")
        self.assertFalse(client.history_v2_eligible)

        second_connection = FakeConnection(generation=1)
        client._conn = second_connection
        client._dispatch_message(
            proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1, second_connection
        )
        self.assertEqual(len(second_connection.sent), 1)
        self.assertEqual(client.history_capability_state, "waiting_ack")

        client._dispatch_message(
            proto.MsgType.CAPABILITY_ACK, 0, ack, 1, first_connection
        )
        self.assertEqual(client.history_capability_state, "waiting_ack")
        client._dispatch_message(
            proto.MsgType.CAPABILITY_ACK, 0, ack, 0, second_connection
        )
        self.assertEqual(client.history_capability_state, "waiting_ack")
        client._dispatch_message(
            proto.MsgType.CAPABILITY_ACK, 0, ack, 1, second_connection
        )
        self.assertTrue(client.history_v2_eligible)

    def test_old_discovery_peer_does_not_receive_offer(self):
        connection = FakeConnection()
        client = self.make_client(
            connection,
            api_url="https://discovery.invalid",
            history_v2_advertise=True,
        )

        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1)

        self.assertEqual(connection.sent, [])
        self.assertEqual(client.history_capability_state, "fallback")
        self.assertEqual(
            client.history_capability_fallback_reason,
            "peer_does_not_advertise_history_capability_v1",
        )

    def test_discovery_supported_peer_receives_offer(self):
        connection = FakeConnection()
        client = self.make_client(
            connection,
            api_url="https://discovery.invalid",
            history_v2_advertise=True,
            history_capability_ack_timeout=0.5,
        )
        discovery_response = {
            "tcp_host": "127.0.0.1",
            "tcp_port": 9100,
            "protocol": {
                "features_supported": ["history_capability_v1"],
                "features_enabled": ["history_capability_v1"],
            },
            "symbol_map_version": 0,
        }
        with patch(
            "rtdata._discovery.discover_endpoint",
            return_value=discovery_response,
        ), patch(
            "rtdata._discovery.fetch_symbol_map",
            return_value=(None, 0),
        ):
            client._do_discovery(timeout=0.1)

        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1)

        self.assertIn(
            "history_capability_v1",
            client.protocol_features_supported,
        )
        self.assertEqual(len(connection.sent), 1)
        self.assertEqual(
            decode_message(connection.sent[0])[0],
            proto.MsgType.CAPABILITY_OFFER,
        )

    def test_invalid_runtime_configuration_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "history_v2_max_block_bytes"):
            self.make_client(
                history_v2_max_block_bytes=capabilities.MIN_BLOCK_BYTES - 1
            )
        with self.assertRaisesRegex(ValueError, "ack_timeout"):
            self.make_client(history_capability_ack_timeout=0)


if __name__ == "__main__":
    unittest.main()
