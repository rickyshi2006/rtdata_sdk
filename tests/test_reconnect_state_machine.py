import socket
import threading
import time
import unittest
from unittest.mock import patch

from rtdata import API, RtdataClient
from rtdata._connection import Connection
from rtdata import _protocol as proto


class ReconnectServer:
    def __init__(self):
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(4)
        self.port = self._listener.getsockname()[1]
        self.accepted = 0
        self.third_connected = threading.Event()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def close(self):
        self.stop.set()
        try:
            self._listener.close()
        except OSError:
            pass
        self.thread.join(timeout=2)

    def _run(self):
        while not self.stop.is_set():
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            self.accepted += 1
            if self.accepted < 3:
                time.sleep(0.05)
                client.close()
                continue
            self.third_connected.set()
            self.stop.wait()
            client.close()
            return


class FakeConnection:
    def __init__(self):
        self.connected = True
        self.reconnecting = False
        self.on_auth = None

    def send(self, _data):
        if self.on_auth is not None:
            callback = self.on_auth
            self.on_auth = None
            callback()
        return self.connected


class DisconnectingEvent:
    def __init__(self, connection):
        self.connection = connection

    def clear(self):
        pass

    def wait(self, timeout=None):
        self.connection.connected = False
        return False


class ReconnectStateMachineTest(unittest.TestCase):
    def test_session_rehome_protocol_round_trip(self):
        target = b"node_oracle"
        ticket = b"signed-handoff-ticket"
        payload = (
            bytes((1, 1)) +
            (1).to_bytes(2, "big") +
            (42).to_bytes(8, "big") +
            len(target).to_bytes(2, "big") + target +
            len(ticket).to_bytes(4, "big") + ticket
        )

        decoded = proto.decode_session_rehome(payload)

        self.assertEqual(decoded['migration_id'], 42)
        self.assertEqual(decoded['target_node_id'], 'node_oracle')
        self.assertEqual(decoded['handoff_ticket'], ticket)
        offer = proto.encode_session_handoff_offer(ticket)
        payload_len, symbol_id, msg_type = proto.decode_header(offer)
        self.assertEqual(payload_len, len(ticket))
        self.assertEqual(symbol_id, 0)
        self.assertEqual(msg_type, proto.MsgType.SESSION_HANDOFF_OFFER)

    def test_rehome_request_is_deduplicated(self):
        class FakeSocket:
            def __init__(self):
                self.shutdowns = 0

            def shutdown(self, _how):
                self.shutdowns += 1

        client = RtdataClient(token="test", api_url="https://gateway")
        fake_socket = FakeSocket()
        client._conn = type("FakeConn", (), {"_sock": fake_socket})()
        target = b"node_oracle"
        ticket = b"ticket"
        payload = (bytes((1, 1)) + (1).to_bytes(2, "big") +
                   (7).to_bytes(8, "big") + len(target).to_bytes(2, "big") +
                   target + len(ticket).to_bytes(4, "big") + ticket)

        client._handle_session_rehome(payload)
        client._handle_session_rehome(payload)

        self.assertEqual(client._pending_rehome['target_node_id'], 'node_oracle')
        self.assertEqual(fake_socket.shutdowns, 1)

    def test_rehome_reconnect_uses_targeted_discovery(self):
        client = RtdataClient(token="test", api_url="https://gateway")
        client._conn = type("FakeConn", (), {"_host": "old", "_port": 1})()
        client._pending_rehome = {
            'migration_id': 9,
            'target_node_id': 'node_oracle',
            'handoff_ticket': b'ticket',
            'deadline': time.monotonic() + 10,
        }

        def targeted_discovery(timeout, target_node_id=None):
            self.assertEqual(timeout, 10.0)
            self.assertEqual(target_node_id, 'node_oracle')
            client._host = '151.145.72.82'
            client._port = 9100
            client._current_node_id = target_node_id

        client._do_discovery = targeted_discovery
        client._before_reconnect()

        self.assertEqual(client._conn._host, '151.145.72.82')
        self.assertEqual(client._conn._port, 9100)
        self.assertEqual(client.current_node_id, 'node_oracle')

    def test_targeted_discovery_failure_does_not_reuse_source_endpoint(self):
        client = RtdataClient(token="test", api_url="https://gateway")
        client._conn = type("FakeConn", (), {"_host": "source", "_port": 9100})()
        client._pending_rehome = {
            'migration_id': 10,
            'target_node_id': 'node_aliyun',
            'handoff_ticket': b'ticket',
            'deadline': time.monotonic() + 10,
        }
        client._do_discovery = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError('target unavailable'))

        with self.assertRaisesRegex(RuntimeError, 'target unavailable'):
            client._before_reconnect()

        self.assertEqual(client._conn._host, 'source')

    def test_second_disconnect_stays_in_reconnect_loop(self):
        server = ReconnectServer()
        server.start()
        disconnected = []

        connection = Connection(
            "127.0.0.1",
            server.port,
            lambda *_: None,
            disconnected.append,
            heartbeat_interval=60,
            auto_reconnect=True,
        )

        def restore_state():
            time.sleep(0.15)

        connection._on_reconnected = restore_state
        try:
            with patch("rtdata._connection.random.uniform", return_value=0):
                connection.connect()
                connection.start_recv_loop()
                self.assertTrue(server.third_connected.wait(timeout=3))
                deadline = time.time() + 1
                while time.time() < deadline and not connection.connected:
                    time.sleep(0.01)

            self.assertGreaterEqual(server.accepted, 3)
            self.assertTrue(connection.connected)
            self.assertGreaterEqual(len(disconnected), 2)
        finally:
            connection.close()
            server.close()

    def test_dead_socket_does_not_emit_connect_callback(self):
        client = RtdataClient(
            token="test",
            auto_reconnect=True,
            async_callbacks=False,
        )
        connection = FakeConnection()
        connection.on_auth = lambda: client._handle_auth_response(b"\x01")
        client._conn = connection
        client._symbol_map._id_to_code = {1: "TEST"}
        client._symbol_map._code_to_id = {"TEST": 1}
        client._symbol_map_event = DisconnectingEvent(connection)
        client._subscribed_codes = ["TEST"]
        connected = []
        client._connect_callbacks.append(lambda: connected.append(True))

        with self.assertRaisesRegex(
                RuntimeError, "Connection lost while waiting for symbol map"):
            client._handle_reconnected()

        self.assertFalse(client.is_connected)
        self.assertFalse(client._authenticated)
        self.assertEqual(connected, [])

    def test_api_status_uses_authenticated_transport(self):
        api = API(token="test")
        api._connected = True
        api._client._authenticated = False
        api._client._conn = FakeConnection()

        self.assertFalse(api.is_connected)


if __name__ == "__main__":
    unittest.main()
