import socket
import struct
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from rtdata import API, RtdataClient
from rtdata import _protocol as proto
from rtdata.exceptions import ConnectionError, SymbolNotFoundError
from test_realtime_loopback import Peer


class Transport:
    def __init__(self, client):
        self.client = client
        self.connected = True
        self.reconnecting = False
        self.generation = 1
        self.frames = []
        self.fail_send = False

    def send(self, frame):
        _, _, kind = proto.decode_header(frame[:proto.HEADER_SIZE])
        if kind == proto.MsgType.AUTH:
            self.client._handle_auth_response(b'\x01')
        if self.fail_send or not self.connected:
            return False
        self.frames.append(frame)
        return True

    def send_for_generation(self, frame, generation):
        return generation == self.generation and self.send(frame)

    def close(self):
        self.connected = False

    def requests(self, kind=proto.MsgType.SUBSCRIBE_REQUEST):
        return [proto.decode_subscribe_response(frame[proto.HEADER_SIZE:])
                for frame in self.frames
                if proto.decode_header(frame[:proto.HEADER_SIZE])[2] == kind]


class SubscriptionRecoveryTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.client = RtdataClient(token='test', symbol_cache_dir=temp.name,
                                   async_callbacks=False, history_cache_enabled=False,
                                   realtime_protocol='legacy', history_v2_advertise=False)
        self.conn = Transport(self.client)
        self.client._conn = self.conn
        self.client._authenticated = True
        self.client._auth_success = True
        self.addCleanup(self.client.close)
        self.catalog = {i: 'SYMBOL%d' % i for i in range(1, 10)}
        self.update(self.catalog)

    def update(self, mapping):
        with patch.object(proto, 'decode_symbol_map', return_value=mapping):
            self.client._handle_symbol_map(b'catalog')

    def ack(self, ids, generation=None, connection=None):
        payload = proto.encode_subscribe(ids)[proto.HEADER_SIZE:]
        self.client._dispatch_message(proto.MsgType.SUBSCRIBE_RESPONSE, 0, payload,
                                      generation or self.conn.generation,
                                      connection or self.conn)

    def reconnect(self):
        self.client._handle_disconnected('test disconnect')
        self.conn.generation += 1
        self.conn.connected = True
        self.client._handle_reconnected()

    def test_reconnect_partial_catalog_recovers_all_nine(self):
        self.client.subscribe(list(self.catalog.values()))
        self.ack(list(self.catalog))
        self.update(dict(list(self.catalog.items())[:7]))
        self.reconnect()
        self.assertEqual(self.conn.requests()[-1], list(range(1, 8)))
        self.ack(list(range(1, 8)))
        self.assertEqual(len(self.client.get_subscribed_symbols()), 7)
        self.assertIn('SYMBOL8', self.client.last_subscribe_warning)
        self.assertEqual(self.client.last_subscribe_rejected, [])
        self.update(self.catalog)
        self.assertEqual(self.conn.requests()[-1], [8, 9])
        self.ack(list(self.catalog))
        self.assertEqual(self.client.get_subscribed_symbols(), list(self.catalog.values()))
        self.assertIsNone(self.client.last_subscribe_warning)

    def test_partial_initial_request_keeps_unknown_pending(self):
        self.update({1: 'SYMBOL1'})
        self.client.subscribe(['SYMBOL1', 'SYMBOL2'])
        self.ack([1])
        self.assertEqual(self.client.last_subscribe_rejected, [])
        self.assertIn('SYMBOL2', self.client.last_subscribe_warning)
        self.update(self.catalog)
        self.assertEqual(self.conn.requests(), [[1], [2]])
        self.ack([1, 2])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1', 'SYMBOL2'])

    def test_all_unknown_raises_without_subscribing_everything(self):
        with self.assertRaises(SymbolNotFoundError):
            self.client.subscribe(['MISSING'])
        self.assertEqual(self.conn.requests(), [])

    def test_pending_intent_survives_disconnect_before_ack(self):
        self.client.subscribe(['SYMBOL1'])
        self.reconnect()
        self.assertEqual(self.conn.requests(), [[1], [1]])
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])

    def test_overlapping_subscribe_requests_are_serialized(self):
        self.client.subscribe(['SYMBOL1'])
        self.client.subscribe(['SYMBOL2'])
        self.assertEqual(self.conn.requests(), [[1]])
        self.ack([1])
        self.assertEqual(self.conn.requests(), [[1], [2]])
        self.ack([1, 2])
        self.assertEqual(self.client.last_subscribe_rejected, [])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1', 'SYMBOL2'])

    def test_concurrent_requests_are_merged_until_ack(self):
        barrier = threading.Barrier(9)
        errors = []

        def subscribe(code):
            try:
                barrier.wait(2)
                self.client.subscribe([code])
            except Exception as exc:
                errors.append(exc)

        workers = [threading.Thread(target=subscribe, args=(code,), daemon=True)
                   for code in self.catalog.values()]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(3)
            self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(self.conn.requests()), 1)
        self.ack(self.conn.requests()[0])
        self.assertEqual(len(self.conn.requests()), 2)
        self.ack(list(self.catalog))
        self.assertEqual(set(self.client.get_subscribed_symbols()), set(self.catalog.values()))
        self.assertEqual(self.client.last_subscribe_rejected, [])

    def test_old_ack_cannot_satisfy_new_request_after_guard_race(self):
        self.client.subscribe(['SYMBOL1'])
        old_generation = self.conn.generation
        self.reconnect()
        self.client._set_subscribed_codes_from_ids([], old_generation, self.conn)
        self.assertEqual(self.client.last_subscribe_rejected, [])
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])

    def test_queued_send_failure_does_not_erase_queued_intent(self):
        self.client.subscribe(['SYMBOL1'])
        self.client.subscribe(['SYMBOL2'])
        self.conn.fail_send = True
        with self.assertRaises(ConnectionError):
            self.ack([1])
        self.conn.fail_send = False
        self.reconnect()
        self.assertEqual(self.conn.requests()[-1], [1, 2])
        self.ack([1, 2])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1', 'SYMBOL2'])

    def test_rejection_is_visible_and_not_retried_on_catalog_updates(self):
        self.client.subscribe(['SYMBOL1', 'SYMBOL2'])
        self.ack([1])
        for _ in range(3):
            self.update(self.catalog)
        self.assertEqual(self.conn.requests(), [[1, 2]])
        self.assertEqual(self.client.last_subscribe_rejected, ['SYMBOL2'])
        self.client.subscribe(['SYMBOL2'])
        self.ack([1, 2])
        self.assertEqual(self.client.last_subscribe_rejected, [])

    def test_unsubscribe_while_disconnected_removes_restore_intent(self):
        self.client.subscribe(['SYMBOL1', 'SYMBOL2'])
        self.ack([1, 2])
        self.client._handle_disconnected('test disconnect')
        self.client.unsubscribe(['SYMBOL2'])
        self.reconnect()
        self.assertEqual(self.conn.requests()[-1], [1])
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])

    def test_unsubscribe_all_while_disconnected_cancels_restore(self):
        self.client.subscribe(['SYMBOL1'])
        self.client._handle_disconnected('test disconnect')
        self.client.unsubscribe()
        count = len(self.conn.requests())
        self.reconnect()
        self.assertEqual(len(self.conn.requests()), count)

    def test_late_ack_does_not_revive_cancelled_subscription(self):
        self.client.subscribe(['SYMBOL1', 'SYMBOL2'])
        self.client.unsubscribe(['SYMBOL2'])
        self.ack([1, 2])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])
        self.assertEqual(self.client.last_subscribe_rejected, [])

    def test_cancel_then_subscribe_requires_new_ack(self):
        self.client.subscribe(['SYMBOL1'])
        self.client.unsubscribe(['SYMBOL1'])
        self.client.subscribe(['SYMBOL1'])
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), [])
        self.assertEqual(self.conn.requests(), [[1], [1]])
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])

    def test_cancel_unknown_before_catalog_recovers(self):
        self.update({1: 'SYMBOL1'})
        self.client.subscribe(['SYMBOL1', 'SYMBOL2'])
        self.client.unsubscribe(['SYMBOL2'])
        self.ack([1])
        self.update(self.catalog)
        self.assertEqual(self.conn.requests(), [[1]])
        self.assertIsNone(self.client.last_subscribe_warning)

    def test_ack_uses_sent_mapping_not_replaced_catalog(self):
        self.client.subscribe(['SYMBOL1'])
        self.update({1: 'OTHER', 100: 'SYMBOL1'})
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])
        self.client.unsubscribe(['SYMBOL1'])
        self.assertEqual(self.conn.requests(proto.MsgType.UNSUBSCRIBE_REQUEST), [[1]])

    def test_stale_generation_and_connection_ack_are_ignored(self):
        self.client.subscribe(['SYMBOL1'])
        self.reconnect()
        self.ack([], generation=1)
        self.ack([], connection=Transport(self.client))
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])
        self.assertEqual(self.client.last_subscribe_rejected, [])

    def test_send_failure_is_reported_and_intent_restored(self):
        self.conn.fail_send = True
        with self.assertRaises(ConnectionError):
            self.client.subscribe(['SYMBOL1'])
        self.conn.fail_send = False
        self.reconnect()
        self.assertEqual(self.conn.requests(), [[1]])
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])

    def test_duplicate_subscribe_does_not_consume_extra_ack(self):
        self.client.subscribe(['SYMBOL1', 'SYMBOL1'])
        self.client.subscribe(['SYMBOL1'])
        self.ack([1])
        self.client.subscribe(['SYMBOL1'])
        self.assertEqual(self.conn.requests(), [[1]])

    def test_warning_callback_can_cancel_without_deadlock(self):
        self.update({1: 'SYMBOL1'})
        self.client._error_callbacks.append(lambda _: self.client.unsubscribe(['SYMBOL2']))
        worker = threading.Thread(target=self.client.subscribe, args=(['SYMBOL1', 'SYMBOL2'],),
                                  daemon=True)
        worker.start()
        worker.join(1)
        self.assertFalse(worker.is_alive())
        self.ack([1])
        self.update(self.catalog)
        self.assertEqual(self.conn.requests(), [[1]])

    def test_public_api_unsubscribe_during_reconnect_does_not_connect(self):
        public = API(token='test', async_callbacks=False)
        public._client.close()
        public._client = self.client
        self.client.subscribe(['SYMBOL1'])
        self.client._handle_disconnected('test disconnect')
        self.conn.reconnecting = True
        with patch.object(public, '_ensure_connected', side_effect=AssertionError('must not connect')):
            public.unsubscribe()
        self.reconnect()
        self.assertEqual(self.conn.requests(), [[1]])

    def test_all_desired_symbols_missing_on_reconnect_are_retained(self):
        self.client.subscribe(['SYMBOL1'])
        self.ack([1])
        self.update({2: 'SYMBOL2'})
        self.reconnect()
        self.assertEqual(self.conn.requests(), [[1]])
        self.assertIn('SYMBOL1', self.client.last_subscribe_warning)
        self.update(self.catalog)
        self.assertEqual(self.conn.requests(), [[1], [1]])

    def test_discovery_update_releases_pending_symbols(self):
        self.update({1: 'SYMBOL1'})
        self.client.subscribe(['SYMBOL1', 'SYMBOL2'])
        self.ack([1])
        self.client._api_url = 'https://example.invalid'
        with patch('rtdata._discovery.discover_endpoint', return_value={
                'tcp_host': '127.0.0.1', 'tcp_port': 1234, 'symbol_map_version': 2}), \
                patch('rtdata._discovery.fetch_symbol_map', return_value=(self.catalog, 2)) as fetch:
            self.client._do_discovery(1)
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.conn.requests(), [[1], [2]])

    def start_refresh(self, fetch):
        self.update({1: 'SYMBOL1'})
        self.client._api_url = 'https://example.invalid'
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def blocked(*args, **kwargs):
            entered.set()
            if not release.wait(3):
                raise AssertionError('refresh not released')
            return fetch(*args, **kwargs)

        mocked = patch('rtdata._discovery.fetch_symbol_map', side_effect=blocked)
        mock = mocked.start()
        self.addCleanup(mocked.stop)
        self.client.subscribe(['SYMBOL1', 'SYMBOL2'])
        self.assertTrue(entered.wait(1))
        with self.client._subscribed_lock:
            worker = self.client._subscription_refresh_thread
        return mock, release, worker

    def test_missing_catalog_refresh_is_nonblocking_and_recovers(self):
        mock, release, worker = self.start_refresh(lambda *a, **k: (self.catalog, 2))
        self.ack([1])
        self.assertEqual(self.client.get_subscribed_symbols(), ['SYMBOL1'])
        release.set()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        mock.assert_called_once_with('https://example.invalid', 'test', timeout=5.0)
        self.assertEqual(self.conn.requests(), [[1], [2]])
        self.ack([1, 2])
        self.assertIsNone(self.client.last_subscribe_warning)

    def test_incomplete_refresh_runs_only_once_per_generation(self):
        mock, release, worker = self.start_refresh(lambda *a, **k: ({1: 'SYMBOL1'}, 2))
        release.set()
        worker.join(2)
        self.ack([1])
        for _ in range(3):
            self.update({1: 'SYMBOL1'})
        self.assertEqual(mock.call_count, 1)
        self.assertIn('SYMBOL2', self.client.last_subscribe_warning)
        self.assertEqual(self.client.last_subscribe_rejected, [])

    def test_refresh_failure_does_not_retry_forever(self):
        def fail(*args, **kwargs):
            raise RuntimeError('offline')
        mock, release, worker = self.start_refresh(fail)
        release.set()
        worker.join(2)
        self.ack([1])
        self.update({1: 'SYMBOL1'})
        self.assertEqual(mock.call_count, 1)
        self.assertIn('SYMBOL2', self.client.last_subscribe_warning)

    def test_refresh_cannot_overwrite_newer_stream_catalog(self):
        _, release, worker = self.start_refresh(lambda *a, **k: ({1: 'SYMBOL1', 99: 'SYMBOL2'}, 2))
        self.update(self.catalog)
        release.set()
        worker.join(2)
        self.assertEqual(self.client.symbols, self.catalog)
        self.ack([1])
        self.assertEqual(self.conn.requests(), [[1], [2]])

    def test_refresh_does_not_revive_cancelled_intent(self):
        _, release, worker = self.start_refresh(lambda *a, **k: (self.catalog, 2))
        self.client.unsubscribe(['SYMBOL2'])
        release.set()
        worker.join(2)
        self.ack([1])
        self.assertEqual(self.conn.requests(), [[1]])

    def test_refresh_after_close_does_not_mutate_catalog(self):
        _, release, worker = self.start_refresh(lambda *a, **k: (self.catalog, 2))
        self.client.close()
        release.set()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.client.symbols, {1: 'SYMBOL1'})
        self.assertEqual(self.conn.requests(), [[1]])

    def test_refresh_after_generation_change_does_not_mutate_catalog(self):
        _, release, worker = self.start_refresh(lambda *a, **k: (self.catalog, 2))
        self.client._handle_disconnected('test disconnect')
        self.conn.generation += 1
        release.set()
        worker.join(2)
        self.assertEqual(self.client.symbols, {1: 'SYMBOL1'})


class SubscriptionRecoveryLoopbackTest(unittest.TestCase):
    def test_nine_to_seven_to_nine_over_real_tcp_reconnect(self):
        full = {i: 'SYMBOL%d' % i for i in range(1, 10)}
        partial = {i: full[i] for i in range(1, 8)}
        first, dropped, restored, finish = [threading.Event() for _ in range(4)]
        received = []

        def send_catalog(sock, mapping):
            payload = struct.pack('!I', len(mapping))
            for sid, code in mapping.items():
                raw = code.encode()
                payload += struct.pack('!IH', sid, len(raw)) + raw
            sock.sendall(proto.build_message(proto.MsgType.SYMBOL_MAP, 0, payload))

        def request(peer, sock):
            _, _, payload = peer.until(sock, proto.MsgType.SUBSCRIBE_REQUEST)
            return proto.decode_subscribe_response(payload)

        def ack(sock, ids):
            payload = proto.encode_subscribe(ids)[proto.HEADER_SIZE:]
            sock.sendall(proto.build_message(proto.MsgType.SUBSCRIBE_RESPONSE, 0, payload))

        def scenario(peer):
            sock = peer.accept()
            self.assertEqual(request(peer, sock), list(full))
            ack(sock, list(full))
            self.assertTrue(first.wait(3))
            send_catalog(sock, partial)
            self.assertTrue(dropped.wait(3))
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            new = peer.accept()
            self.assertEqual(request(peer, new), list(partial))
            ack(new, list(partial))
            self.assertTrue(restored.wait(3))
            send_catalog(new, full)
            self.assertEqual(request(peer, new), [8, 9])
            ack(new, list(full))
            for sid in (8, 9):
                quote = (struct.pack('!I', sid) + struct.pack('<fff', 10, 11, 10.5)
                         + struct.pack('!Qq', 123, 1700000000000))
                new.sendall(proto.build_message(proto.MsgType.SNAPSHOT_FULL, sid, quote))
            finish.set()
            peer.stop.wait(5)

        def wait_for(predicate):
            deadline = time.monotonic() + 4
            while not predicate() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(predicate())

        with tempfile.TemporaryDirectory() as temp, \
                patch('rtdata._connection.random.uniform', return_value=0):
            peer = Peer(scenario)
            client = RtdataClient(token='test', host='127.0.0.1', port=peer.port,
                                  symbol_cache_dir=temp, history_cache_enabled=False,
                                  history_v2_advertise=False, realtime_protocol='legacy',
                                  async_callbacks=False, heartbeat_interval=60)
            try:
                client._quote_callbacks.append(lambda quote: received.append(quote.symbol))
                client._symbol_map.update_from_dict(full, 1)
                client.connect()
                client.subscribe(list(full.values()))
                wait_for(lambda: len(client.get_subscribed_symbols()) == 9)
                first.set()
                wait_for(lambda: client.symbols == partial)
                dropped.set()
                wait_for(lambda: len(client.get_subscribed_symbols()) == 7)
                self.assertIn('SYMBOL8', client.last_subscribe_warning)
                restored.set()
                self.assertTrue(finish.wait(4))
                wait_for(lambda: len(client.get_subscribed_symbols()) == 9)
                wait_for(lambda: received == ['SYMBOL8', 'SYMBOL9'])
                self.assertIsNone(client.last_subscribe_warning)
            finally:
                first.set()
                dropped.set()
                restored.set()
                client.close()
                peer.close()
            self.assertEqual(peer.errors, [])


if __name__ == '__main__':
    unittest.main()
