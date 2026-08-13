import threading
import time
import unittest
import socket
from dataclasses import replace
from unittest.mock import Mock, patch

from rtdata import RtdataClient
from rtdata import _protocol as proto
from rtdata import _session_capabilities as capabilities
from rtdata import _session_rehome_protocol as rehome_protocol
from rtdata._connection import Connection


def decode_message(message):
    payload_len, symbol_id, msg_type = proto.decode_header(
        message[:proto.HEADER_SIZE]
    )
    payload = message[proto.HEADER_SIZE:]
    if len(payload) != payload_len:
        raise AssertionError("invalid test message length")
    return msg_type, symbol_id, payload


class FakeConnection:
    def __init__(self):
        self.connected = True
        self.generation = 1
        self.sent = []
        self.reconnect_requests = []
        self.reconnect_event = threading.Event()
        self._host = "old.invalid"
        self._port = 9100

    def send(self, data):
        self.sent.append(data)
        return True

    def request_reconnect(self, reason, *, require_pre_reconnect_success=False):
        self.reconnect_requests.append(
            (reason, require_pre_reconnect_success)
        )
        self.reconnect_event.set()
        return True


class SessionProtocolTest(unittest.TestCase):
    def test_capability_golden_round_trip(self):
        value = capabilities.rehome_capabilities()
        self.assertEqual(
            value.encode(),
            bytes.fromhex("01 03 00 01 00 00 00 01 00 00 00 00"),
        )
        self.assertEqual(
            capabilities.SessionCapabilities.decode(value.encode()), value
        )

    def test_capability_rejects_unknown_and_reserved_bits(self):
        encoded = bytearray(capabilities.rehome_capabilities().encode())
        encoded[2] = 0x80
        with self.assertRaisesRegex(ValueError, "flags"):
            capabilities.SessionCapabilities.decode(bytes(encoded))

        encoded = bytearray(capabilities.rehome_capabilities().encode())
        encoded[7] = 0x02
        with self.assertRaisesRegex(ValueError, "feature"):
            capabilities.SessionCapabilities.decode(bytes(encoded))

        encoded = bytearray(capabilities.rehome_capabilities().encode())
        encoded[11] = 1
        with self.assertRaisesRegex(ValueError, "reserved"):
            capabilities.SessionCapabilities.decode(bytes(encoded))

    def test_rehome_round_trip_and_validation(self):
        request = rehome_protocol.RehomeRequest(
            migration_id=0x0102030405060708,
            target_node_id="node_aliyun",
            reason=rehome_protocol.RehomeReason.NODE_DEGRADED,
        )
        self.assertEqual(
            rehome_protocol.RehomeRequest.decode(request.encode()), request
        )
        with self.assertRaisesRegex(ValueError, "non-zero"):
            replace(request, migration_id=0).encode()
        with self.assertRaisesRegex(ValueError, "target node"):
            replace(request, target_node_id="").encode()
        with self.assertRaisesRegex(ValueError, "target node length"):
            rehome_protocol.RehomeRequest.decode(request.encode()[:-1])


class SessionRehomeClientTest(unittest.TestCase):
    def make_client(
        self,
        *,
        api_url="https://discovery.invalid",
        advertise=True,
        auto_reconnect=True,
    ):
        client = RtdataClient(
            token="test",
            api_url=api_url,
            auto_reconnect=auto_reconnect,
            session_rehome_advertise=advertise,
            session_capability_ack_timeout=0.5,
            async_callbacks=False,
        )
        connection = FakeConnection()
        client._conn = connection
        client._protocol_features_supported = ["session_rehome_v1"]
        return client, connection

    def negotiate(self, client, connection):
        client._dispatch_message(
            proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1, connection
        )
        offers = [
            message for message in connection.sent
            if decode_message(message)[0]
            == proto.MsgType.SESSION_CAPABILITY_OFFER
        ]
        self.assertEqual(len(offers), 1)
        offer = capabilities.SessionCapabilities.decode(
            decode_message(offers[0])[2]
        )
        self.assertEqual(offer.role, capabilities.Role.RTDATA)

        ack = capabilities.rehome_capabilities(
            capabilities.Role.CLOUD
        ).encode()
        client._dispatch_message(
            proto.MsgType.SESSION_CAPABILITY_ACK,
            0,
            ack,
            1,
            connection,
        )
        self.assertTrue(client.session_rehome_negotiated)

    def test_requires_explicit_flag_and_api_url(self):
        for api_url, advertise, auto_reconnect in (
            (None, True, True),
            ("https://discovery.invalid", False, True),
            ("https://discovery.invalid", True, False),
        ):
            client, connection = self.make_client(
                api_url=api_url,
                advertise=advertise,
                auto_reconnect=auto_reconnect,
            )
            client._dispatch_message(
                proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1, connection
            )
            self.assertEqual(connection.sent, [])
            self.assertFalse(client.session_rehome_negotiated)

    def test_old_cloud_discovery_does_not_receive_offer(self):
        client, connection = self.make_client()
        client._protocol_features_supported = []
        client._dispatch_message(
            proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1, connection
        )
        self.assertEqual(connection.sent, [])
        self.assertEqual(client.session_capability_state, "fallback")

    def test_malformed_ack_falls_back_without_disconnect(self):
        client, connection = self.make_client()
        client._dispatch_message(
            proto.MsgType.AUTH_RESPONSE, 0, b"\x01", 1, connection
        )
        client._dispatch_message(
            proto.MsgType.SESSION_CAPABILITY_ACK,
            0,
            b"short",
            1,
            connection,
        )
        self.assertTrue(connection.connected)
        self.assertEqual(client.session_capability_state, "fallback")

    def test_rehome_requires_negotiation_and_deduplicates(self):
        client, connection = self.make_client()
        payload = rehome_protocol.RehomeRequest(
            migration_id=7, target_node_id="node_aliyun"
        ).encode()
        client._dispatch_message(
            proto.MsgType.SESSION_REHOME, 0, payload, 1, connection
        )
        self.assertFalse(connection.reconnect_event.wait(0.05))

        self.negotiate(client, connection)
        client._dispatch_message(
            proto.MsgType.SESSION_REHOME, 0, payload, 1, connection
        )
        self.assertTrue(connection.reconnect_event.wait(1))
        self.assertEqual(len(connection.reconnect_requests), 1)
        self.assertTrue(connection.reconnect_requests[0][1])

        client._dispatch_message(
            proto.MsgType.SESSION_REHOME, 0, payload, 1, connection
        )
        time.sleep(0.05)
        self.assertEqual(len(connection.reconnect_requests), 1)

    def test_rehome_discovery_failure_never_reuses_old_endpoint(self):
        client, connection = self.make_client()
        self.negotiate(client, connection)
        client._session_rehome_requested = True
        client._session_rehome_target = "node_aliyun"

        with patch.object(
            client, "_do_discovery", side_effect=RuntimeError("unavailable")
        ):
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                client._before_reconnect()

        self.assertEqual(connection._host, "old.invalid")
        self.assertEqual(connection._port, 9100)

    def test_rehome_requires_discovery_to_return_target_node(self):
        client, connection = self.make_client()
        self.negotiate(client, connection)
        client._session_rehome_requested = True
        client._session_rehome_target = "node_aliyun"

        def wrong_node(timeout):
            del timeout
            client._host = "oracle.invalid"
            client._port = 9100
            client._current_node_id = "node_oracle"

        with patch.object(client, "_do_discovery", side_effect=wrong_node):
            with self.assertRaisesRegex(Exception, "expected node_aliyun"):
                client._before_reconnect()

    def test_connection_rehome_keeps_auto_reconnect_enabled(self):
        connection = Connection(
            "127.0.0.1",
            1,
            lambda *_args: None,
            lambda _reason: None,
            auto_reconnect=True,
        )
        first, second = socket.socketpair()
        connection._sock = first
        connection._connected = True
        with patch.object(connection, "_reconnect_loop") as reconnect_loop:
            self.assertTrue(connection.request_reconnect(
                "session rehome requested",
                require_pre_reconnect_success=True,
            ))
        self.assertTrue(connection._auto_reconnect)
        self.assertTrue(connection._require_pre_reconnect_success)
        reconnect_loop.assert_called_once_with()
        second.close()

    def test_required_discovery_failure_never_calls_connect(self):
        connection = Connection(
            "old.invalid",
            9100,
            lambda *_args: None,
            lambda _reason: None,
            auto_reconnect=True,
        )
        connection._require_pre_reconnect_success = True
        connection._on_before_reconnect = Mock(side_effect=RuntimeError("down"))
        connection._stop_event.wait = Mock(side_effect=[False, True])
        with patch.object(connection, "_do_connect") as do_connect, patch(
            "rtdata._connection.random.uniform", return_value=0
        ):
            connection._do_reconnect_loop()
        do_connect.assert_not_called()

    def test_rehome_flag_clears_only_after_reconnect_completes(self):
        client, _connection = self.make_client()
        client._session_rehome_requested = True
        client._session_rehome_target = "node_aliyun"
        client._handle_reconnect_completed()
        self.assertFalse(client._session_rehome_requested)
        self.assertEqual(client._session_rehome_target, "")

    def test_rejected_rehome_does_not_leave_client_migrating(self):
        client, connection = self.make_client()
        self.negotiate(client, connection)
        connection.request_reconnect = Mock(return_value=False)
        payload = rehome_protocol.RehomeRequest(
            migration_id=99,
            target_node_id="node_aliyun",
        ).encode()

        client._dispatch_message(
            proto.MsgType.SESSION_REHOME, 0, payload, 1, connection
        )

        self.assertFalse(client._session_rehome_requested)
        self.assertEqual(client._session_rehome_target, "")


if __name__ == "__main__":
    unittest.main()
