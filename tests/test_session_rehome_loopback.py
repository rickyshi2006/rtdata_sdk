import json
import socketserver
import struct
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from rtdata import RtdataClient
from rtdata import _protocol as proto
from rtdata import _session_capabilities as capabilities
from rtdata import _session_rehome_protocol as rehome_protocol


def wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


def recv_exact(sock, size):
    chunks = []
    received = 0
    while received < size:
        chunk = sock.recv(size - received)
        if not chunk:
            return None
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


class GatewayState:
    def __init__(self, node_id, send_rehome=False):
        self.node_id = node_id
        self.send_rehome = send_rehome
        self.auth_tokens = []
        self.capability_offers = []
        self.subscriptions = []
        self.errors = []
        self.subscribed = threading.Event()
        self._lock = threading.Lock()
        self._rehome_sent = False

    def record_auth(self, token):
        with self._lock:
            self.auth_tokens.append(token)

    def record_offer(self, offer):
        with self._lock:
            self.capability_offers.append(offer)

    def record_subscription(self, symbol_ids):
        with self._lock:
            self.subscriptions.append(symbol_ids)
        self.subscribed.set()

    def claim_rehome(self):
        with self._lock:
            if not self.send_rehome or self._rehome_sent:
                return False
            self._rehome_sent = True
            return True


class GatewayHandler(socketserver.BaseRequestHandler):
    def handle(self):
        state = self.server.state
        try:
            while True:
                header = recv_exact(self.request, proto.HEADER_SIZE)
                if header is None:
                    return
                payload_len, _symbol_id, msg_type = proto.decode_header(header)
                payload = recv_exact(self.request, payload_len)
                if payload is None:
                    return
                if msg_type == proto.MsgType.AUTH:
                    state.record_auth(payload.decode("utf-8"))
                    self.request.sendall(
                        proto.build_message(
                            proto.MsgType.AUTH_RESPONSE, 0, b"\x01"
                        )
                    )
                elif msg_type == proto.MsgType.SESSION_CAPABILITY_OFFER:
                    offer = capabilities.SessionCapabilities.decode(payload)
                    state.record_offer(offer)
                    ack = capabilities.rehome_capabilities(
                        capabilities.Role.CLOUD
                    ).encode()
                    self.request.sendall(
                        proto.build_message(
                            proto.MsgType.SESSION_CAPABILITY_ACK, 0, ack
                        )
                    )
                elif msg_type == proto.MsgType.SUBSCRIBE_REQUEST:
                    count = struct.unpack("!I", payload[:4])[0]
                    symbol_ids = list(
                        struct.unpack(f"!{count}I", payload[4:])
                    ) if count else []
                    state.record_subscription(symbol_ids)
                    response = struct.pack("!I", count)
                    if symbol_ids:
                        response += struct.pack(f"!{count}I", *symbol_ids)
                    messages = [
                        proto.build_message(
                            proto.MsgType.SUBSCRIBE_RESPONSE, 0, response
                        )
                    ]
                    if state.claim_rehome():
                        rehome = rehome_protocol.RehomeRequest(
                            migration_id=42,
                            target_node_id="node_aliyun",
                            reason=rehome_protocol.RehomeReason.NODE_DEGRADED,
                        ).encode()
                        messages.append(
                            proto.build_message(
                                proto.MsgType.SESSION_REHOME, 0, rehome
                            )
                        )
                    self.request.sendall(b"".join(messages))
        except (ConnectionError, OSError):
            return
        except Exception as exc:
            state.errors.append(repr(exc))


class GatewayServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, state):
        super().__init__(("127.0.0.1", 0), GatewayHandler)
        self.state = state


class DiscoveryState:
    def __init__(self, oracle_port, aliyun_port):
        self.oracle_port = oracle_port
        self.aliyun_port = aliyun_port
        self.connect_requests = 0
        self._lock = threading.Lock()

    def next_endpoint(self):
        with self._lock:
            self.connect_requests += 1
            first = self.connect_requests == 1
        if first:
            return "node_oracle", self.oracle_port
        return "node_aliyun", self.aliyun_port


class DiscoveryHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/api/v1/connect":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        json.loads(self.rfile.read(length).decode("utf-8"))
        node_id, port = self.server.state.next_endpoint()
        self.send_json({
            "tcp_host": "127.0.0.1",
            "tcp_port": port,
            "node_id": node_id,
            "gateway_version": "loopback",
            "symbol_map_version": 1,
            "symbol_count": 1,
            "protocol": {
                "features_supported": ["session_rehome_v1"],
                "features_enabled": [],
            },
        })

    def do_GET(self):
        if not self.path.startswith("/api/v1/symbol_map"):
            self.send_error(404)
            return
        self.send_json({"version": 1, "symbols": {"7": "TEST.LOCAL"}})

    def send_json(self, value):
        payload = json.dumps(value).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format, *_args):
        pass


class SessionRehomeLoopbackTest(unittest.TestCase):
    def test_discovery_reauth_capability_and_subscription_restore(self):
        oracle_state = GatewayState("node_oracle", send_rehome=True)
        aliyun_state = GatewayState("node_aliyun")
        oracle = GatewayServer(oracle_state)
        aliyun = GatewayServer(aliyun_state)
        discovery_state = DiscoveryState(
            oracle.server_address[1], aliyun.server_address[1]
        )
        discovery = ThreadingHTTPServer(
            ("127.0.0.1", 0), DiscoveryHandler
        )
        discovery.state = discovery_state
        servers = (oracle, aliyun, discovery)
        threads = [
            threading.Thread(target=server.serve_forever, daemon=True)
            for server in servers
        ]
        for thread in threads:
            thread.start()

        client = None
        try:
            with tempfile.TemporaryDirectory() as cache_dir:
                client = RtdataClient(
                    token="loopback-token",
                    api_url=(
                        f"http://127.0.0.1:{discovery.server_address[1]}"
                    ),
                    heartbeat_interval=60,
                    symbol_cache_dir=cache_dir,
                    history_cache_enabled=False,
                    async_callbacks=False,
                    session_rehome_advertise=True,
                    session_capability_ack_timeout=0.5,
                )
                with patch(
                    "rtdata._connection.random.uniform", return_value=0
                ):
                    client.connect(timeout=3)
                    self.assertTrue(wait_until(
                        lambda: client.session_rehome_negotiated
                    ))
                    client.subscribe(["TEST.LOCAL"])
                    self.assertTrue(oracle_state.subscribed.wait(2))
                    self.assertTrue(wait_until(
                        lambda: (
                            client.current_node_id == "node_aliyun"
                            and client.is_connected
                            and client.session_rehome_negotiated
                            and aliyun_state.subscribed.is_set()
                        ),
                        timeout=5,
                    ))

                self.assertGreaterEqual(discovery_state.connect_requests, 2)
                self.assertEqual(oracle_state.auth_tokens, ["loopback-token"])
                self.assertEqual(aliyun_state.auth_tokens, ["loopback-token"])
                self.assertEqual(oracle_state.subscriptions, [[7]])
                self.assertEqual(aliyun_state.subscriptions, [[7]])
                self.assertEqual(len(oracle_state.capability_offers), 1)
                self.assertEqual(len(aliyun_state.capability_offers), 1)
                self.assertEqual(oracle_state.errors, [])
                self.assertEqual(aliyun_state.errors, [])
        finally:
            if client is not None:
                client.close()
            for server in servers:
                server.shutdown()
                server.server_close()
            for thread in threads:
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
