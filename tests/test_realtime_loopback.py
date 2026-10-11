import socket
import struct
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from unittest.mock import patch

from rtdata import RtdataClient, RealtimeUnavailableError
from rtdata import _protocol as proto
from rtdata import _realtime_capability as cap
from rtdata import _realtime_protocol as wire
from test_realtime_client import fixture


def read_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError
        data.extend(part)
    return bytes(data)


def read_frame(sock):
    size, sid, kind = proto.decode_header(read_exact(sock, 10))
    return kind, sid, read_exact(sock, size)


class Peer:
    def __init__(self, scenario):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(4)
        self.listener.settimeout(4)
        self.port = self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.errors = []
        self.sockets = []
        self.pending = {}
        def run():
            try:
                scenario(self)
            except Exception as exc:
                if not self.stop.is_set():
                    self.errors.append(exc)
        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def accept(self):
        sock, _ = self.listener.accept()
        self.sockets.append(sock)
        sock.settimeout(4)
        kind, _, _ = read_frame(sock)
        assert kind == proto.MsgType.AUTH
        sock.sendall(proto.build_message(proto.MsgType.AUTH_RESPONSE, 0, b'\x01'))
        return sock

    def until(self, sock, expected):
        pending = self.pending.setdefault(sock, [])
        for i, frame in enumerate(pending):
            if frame[0] == expected:
                return pending.pop(i)
        for _ in range(32):
            frame = read_frame(sock)
            if frame[0] == expected:
                return frame
            pending.append(frame)
        raise AssertionError('missing message')

    def negotiate(self, sock, epoch=91):
        _, _, payload = self.until(sock, cap.OFFER)
        offer = cap.Capability.decode(payload)
        ack = cap.intersection(cap.supported(cap.Role.CLOUD), offer)
        frame = proto.build_message(cap.ACK, 0, ack.encode())
        sock.sendall(frame[:7])
        sock.sendall(frame[7:])
        _, _, commit = self.until(sock, cap.COMMIT)
        assert commit == replace(ack, role=cap.Role.SDK).encode()
        self.status(sock, ack.nonce, epoch=epoch)
        return ack.nonce

    def status(self, sock, nonce, revision=1, epoch=91, ready=True):
        status = cap.Status(nonce, revision, tuple(cap.SourceState(sid, ready,
            cap.Reason.READY if ready else cap.Reason.SUSPENDED, epoch if ready else 0) for sid in range(1, 6)))
        sock.sendall(proto.build_message(cap.SOURCE_STATUS, 0, status.encode()))

    def subscribe(self, sock):
        _, _, payload = self.until(sock, proto.MsgType.SUBSCRIBE_REQUEST)
        sock.sendall(proto.build_message(proto.MsgType.SUBSCRIBE_RESPONSE, 0, payload))

    def close(self):
        self.stop.set()
        for sock in self.sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        self.listener.close()
        self.thread.join(5)


class RealtimeLoopbackTest(unittest.TestCase):
    def make(self, scenario, mode='auto', auto_reconnect=False, **kwargs):
        peer = Peer(scenario)
        self.addCleanup(peer.close)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        client = RtdataClient('test', host='127.0.0.1', port=peer.port,
            realtime_protocol=mode, auto_reconnect=auto_reconnect,
            symbol_cache_dir=tmp.name, history_cache_enabled=False, **kwargs)
        client._symbol_map.update_from_dict({sid: fixture(sid).fields['code'] for sid in range(1, 6)})
        self.addCleanup(client.close)
        return peer, client

    def test_actual_recv_five_tables_nulls_and_callback_close(self):
        def scenario(peer):
            sock = peer.accept()
            peer.negotiate(sock)
            peer.subscribe(sock)
            for sid in range(1, 6):
                frame = proto.build_message(cap.SNAPSHOT, sid, wire.encode(fixture(sid, null=True)))
                sock.sendall(frame[:9])
                sock.sendall(frame[9:])
            peer.stop.wait(5)
        peer, client = self.make(scenario, mode='required_v2', async_callbacks=False)
        seen, done = [], threading.Event()
        def callback(quote):
            seen.append(quote)
            if len(seen) == 5:
                client.close()
                done.set()
        client.on_quote(callback)
        client.connect(2)
        client.subscribe(list(client.symbols.values()))
        self.assertTrue(done.wait(3))
        self.assertEqual(len(seen), 5)
        for q in seen:
            self.assertEqual(dict(q.fields), dict(fixture(q.source_id, null=True).fields))
        self.assertFalse(client.is_connected)
        self.assertEqual(peer.errors, [])

    def test_old_server_auto_fallback_and_required_error(self):
        def scenario(peer):
            sock = peer.accept()
            while not peer.stop.is_set():
                kind, _, payload = read_frame(sock)
                if kind == proto.MsgType.SUBSCRIBE_REQUEST:
                    sock.sendall(proto.build_message(proto.MsgType.SUBSCRIBE_RESPONSE, 0, payload))
                    legacy, _ = wire.project_legacy(fixture(), 1)
                    sock.sendall(proto.build_message(1, 1, legacy))
        peer, client = self.make(scenario, async_callbacks=False)
        client.connect(2)
        client.subscribe([fixture().fields['code']])
        self.assertEqual(client.wait_for_quote(fixture().fields['code'], 2).wire_version, 1)
        with self.assertRaisesRegex(RealtimeUnavailableError, 'ack_timeout'):
            client.wait_for_realtime(2)
        self.assertEqual(peer.errors, [])
        _, required = self.make(scenario, mode='required_v2', async_callbacks=False)
        with self.assertRaisesRegex(RealtimeUnavailableError, 'ack_timeout'):
            required.connect(2)
        self.assertFalse(required.is_connected)

    def test_async_pending_quote_dropped_after_status_invalidation(self):
        first_entered = threading.Event()
        release = threading.Event()
        invalidated = threading.Event()
        sent = threading.Event()
        def scenario(peer):
            sock = peer.accept()
            nonce = peer.negotiate(sock)
            peer.subscribe(sock)
            sock.sendall(proto.build_message(cap.SNAPSHOT, 1, wire.encode(fixture())))
            assert first_entered.wait(2)
            sock.sendall(proto.build_message(cap.SNAPSHOT, 1, wire.encode(fixture(seq=2))))
            peer.status(sock, nonce, revision=2, ready=False)
            sent.set()
            peer.stop.wait(5)
        peer, client = self.make(scenario, async_callbacks=True)
        seen = []
        def quote_callback(quote):
            seen.append(quote.sequence)
            first_entered.set()
            release.wait(3)
        client.on_quote(quote_callback)
        client.on_realtime_status(lambda status: invalidated.set() if status.revision == 2 else None)
        client.connect(2)
        client.subscribe([fixture().fields['code']])
        try:
            self.assertTrue(sent.wait(2))
            self.assertTrue(invalidated.wait(2))
            self.assertIsNone(client.get_quote(fixture().fields['code']))
        finally:
            release.set()
        time.sleep(.1)
        self.assertEqual(seen, [1])
        self.assertEqual(peer.errors, [])

    def test_reconnect_old_nonce_and_buffered_frame_are_ignored(self):
        first = threading.Event()
        second = threading.Event()
        def scenario(peer):
            sock = peer.accept()
            nonce = peer.negotiate(sock)
            peer.subscribe(sock)
            sock.sendall(proto.build_message(cap.SNAPSHOT, 1, wire.encode(fixture())))
            assert first.wait(2)
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            new = peer.accept()
            peer.negotiate(new, epoch=92)
            peer.subscribe(new)
            peer.status(new, nonce, revision=999, epoch=91)
            new.sendall(proto.build_message(cap.SNAPSHOT, 1, wire.encode(fixture())))
            new.sendall(proto.build_message(cap.SNAPSHOT, 1, wire.encode(fixture(epoch=92))))
            peer.stop.wait(5)
        peer, client = self.make(scenario, auto_reconnect=True, async_callbacks=False)
        seen = []
        def callback(quote):
            seen.append(quote.source_epoch)
            (first if quote.source_epoch == 91 else second).set()
        client.on_quote(callback)
        with patch('rtdata._connection.random.uniform', return_value=0):
            client.connect(2)
            client.subscribe([fixture().fields['code']])
            self.assertTrue(first.wait(2))
            self.assertTrue(second.wait(4))
        self.assertEqual(seen, [91, 92])
        self.assertEqual(client.get_quote(fixture().fields['code']).source_epoch, 92)
        self.assertEqual(peer.errors, [])

    def test_oversized_header_closes_without_waiting_for_body(self):
        def scenario(peer):
            sock = peer.accept()
            peer.negotiate(sock)
            peer.subscribe(sock)
            sock.sendall(struct.pack('!IIH', 16385, 1, cap.SNAPSHOT))
            peer.stop.wait(5)
        peer, client = self.make(scenario, async_callbacks=False)
        disconnected = threading.Event()
        client.on_disconnect(lambda _: disconnected.set())
        client.connect(2)
        client.subscribe([fixture().fields['code']])
        self.assertTrue(disconnected.wait(2))
        self.assertFalse(client.is_connected)
        self.assertEqual(peer.errors, [])


if __name__ == '__main__':
    unittest.main()
