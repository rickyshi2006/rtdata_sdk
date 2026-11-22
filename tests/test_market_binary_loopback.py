"""显式启用的本机限速验收，不连接远端节点。"""

import os
import socket
import threading
import time
import unittest
from unittest.mock import patch

from rtdata import _market_identity_wire as wire
import test_market_subscription as market_tests


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_BINARY_SLOW_TEST') == '1' and
                     os.environ.get('RTDATA_MARKET_TEST_SERVER'), '需要显式启用隔离限速验收')
class BinaryBandwidthTests(unittest.TestCase):
    full_fields = False
    def setUp(self):
        identity_only = os.environ.get('RTDATA_IDENTITY_ONLY_TEST', '1')
        env = dict(os.environ)
        if identity_only == '1':
            env['RTDATA_MARKET_TEST_IDENTITY_ONLY'] = '1'
        else:
            env.pop('RTDATA_MARKET_TEST_IDENTITY_ONLY', None)
        if getattr(self, 'blocks', False):
            env['RTDATA_MARKET_TEST_BLOCK'] = '1'
        with patch.dict(os.environ, env, clear=True):
            market_tests.MarketLoopbackTests.setUp(self)
    write_registry = market_tests.MarketLoopbackTests.write_registry
    stop_server = market_tests.MarketLoopbackTests.stop_server
    client = market_tests.MarketLoopbackTests.client

    def tearDown(self):
        self.stop_server()
        self.log.seek(0)
        lines = self.log.read().splitlines()
        for line in lines:
            if '关闭' in line or 'close' in line.lower() or 'stall' in line or 'timeout' in line:
                print('identity_test_server:', line, flush=True)
        if os.environ.get('RTDATA_IDENTITY_ONLY_TEST') == '0':
            print('identity_test_server_tail:', '\n'.join(lines[-20:]), flush=True)

    def initial_rows(self):
        return [dict(symbol=f'S{i:06}.SH', symbol_id=i, stock_id=str(10000 + i),
                     source_market=30, instrument_type=3, active=True, legacy=False)
                for i in range(1, 12382)]

    def proxy(self, rate):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        upstream = self.port
        self.port = listener.getsockname()[1]
        stopped, accepted = threading.Event(), threading.Event()
        sockets, workers, errors = [listener], [], []
        totals = [0]

        def relay(source, target, throttled):
            try:
                while not stopped.is_set():
                    data = source.recv(512 if throttled else 65536)
                    if not data:
                        return
                    if throttled:
                        if stopped.wait(len(data) / rate):
                            return
                        totals[0] += len(data)
                    elif stopped.wait(getattr(self, 'upstream_delay', 0)):
                        return
                    target.sendall(data)
            except OSError as error:
                if not stopped.is_set():
                    errors.append(error)

        def run():
            try:
                downstream, _ = listener.accept()
                remote = socket.create_connection(('127.0.0.1', upstream), timeout=5)
                remote.settimeout(None)
                sockets.extend((downstream, remote))
                worker = threading.Thread(target=relay, args=(downstream, remote, False))
                workers.append(worker)
                worker.start()
                accepted.set()
                relay(remote, downstream, True)
            except OSError as error:
                if not stopped.is_set():
                    errors.append(error)

        main = threading.Thread(target=run)
        main.start()

        def close():
            stopped.set()
            for sock in sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
            main.join(5)
            for worker in workers:
                worker.join(5)
            self.assertFalse(main.is_alive())

        self.addCleanup(close)
        return totals, errors

    def exercise(self, compressed):
        totals, errors = self.proxy(12000)
        with patch.object(wire, 'available', return_value=compressed):
            from rtdata import RtdataClient
            client = RtdataClient('isolated-market-test', host='127.0.0.1', port=self.port,
                                  auto_reconnect=False,
                                  realtime_protocol='required_v2' if self.full_fields else 'legacy',
                                  history_v2_advertise=False, history_cache_enabled=False,
                                  heartbeat_interval=.2, symbol_cache_dir=self.folder.name)
            self.addCleanup(client.close)
            client._symbol_map.update_from_dict({1: 'S000001.SH'})
            client.connect(timeout=3)
            deadline = time.monotonic() + 5
            while not client._market_ready and time.monotonic() < deadline:
                time.sleep(.01)
        self.assertTrue(client._market_binary)
        self.assertTrue(client._market_replay_credit)
        self.assertEqual(client._market_zstd, compressed)
        before = totals[0]
        start = time.monotonic()
        result = client.subscribe_market('cn_equity').wait_applied()
        first_time, first_bytes = time.monotonic() - start, totals[0] - before
        self.assertEqual(result['admitted_count'], 12381)
        self.assertEqual(len(client._market_cache), 12381)
        unsubscribe_start = time.monotonic()
        try:
            client.unsubscribe_market('cn_equity').wait_applied()
        except Exception:
            print('identity_test_client_status:', client.market_subscription_status,
                  'first_seconds=', first_time, 'first_bytes=', first_bytes,
                  'received_bytes=', totals[0], flush=True)
            raise
        self.assertFalse(client._market_identities)
        unsubscribe_time = time.monotonic() - unsubscribe_start
        self.assertLess(unsubscribe_time, 20)
        before = totals[0]
        start = time.monotonic()
        self.assertEqual(client.subscribe_market('cn_equity').wait_applied()['admitted_count'], 12381)
        repeat_time, repeat_bytes = time.monotonic() - start, totals[0] - before
        self.assertLess(repeat_bytes, first_bytes)
        self.assertFalse(errors, errors)
        if not compressed:
            self.assertGreater(first_time, 30)
        print(f'identity_bandwidth zstd={compressed} rate=12000 symbols=12381 '
              f'first_bytes={first_bytes} first_seconds={first_time:.3f} '
              f'unsubscribe_seconds={unsubscribe_time:.3f} full={self.full_fields} '
              f'repeat_bytes={repeat_bytes} repeat_seconds={repeat_time:.3f}', flush=True)

    def test_raw_progress_beyond_30_seconds(self):
        self.exercise(False)

    @unittest.skipUnless(wire.available(), '需要可选压缩依赖')
    def test_compressed(self):
        self.exercise(True)


class FullFieldsBandwidthTests(BinaryBandwidthTests):
    full_fields = True


class BlockBandwidthTests(FullFieldsBandwidthTests):
    blocks = True


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER'), '需要隔离行情进程')
class ReplayCompletenessTests(unittest.TestCase):
    full_fields = False
    setUp = market_tests.MarketLoopbackTests.setUp
    tearDown = market_tests.MarketLoopbackTests.tearDown
    write_registry = market_tests.MarketLoopbackTests.write_registry
    stop_server = market_tests.MarketLoopbackTests.stop_server
    client = market_tests.MarketLoopbackTests.client
    def initial_rows(self):
        return BinaryBandwidthTests.initial_rows(self)

    def test_complete_replay_without_credit_loss(self):
        client = self.client()
        seen = []
        client.on_quote(lambda quote: seen.append(quote.symbol))
        self.assertEqual(client.subscribe_market('cn_equity').wait_applied(15)['admitted_count'], 12381)
        deadline = time.monotonic() + 15
        while len(seen) < 12381 and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertEqual(len(seen), 12381)
        self.assertEqual(len(set(seen)), 12381)
        self.assertGreater(client._market_replay_checkpoint, 4)


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER'), '需要隔离行情进程')
class RolloutCompatibilityTests(unittest.TestCase):
    full_fields = False
    initial_rows = market_tests.MarketLoopbackTests.initial_rows
    write_registry = market_tests.MarketLoopbackTests.write_registry
    stop_server = market_tests.MarketLoopbackTests.stop_server
    client = market_tests.MarketLoopbackTests.client

    def setUp(self):
        with patch.dict(os.environ, {'RTDATA_MARKET_TEST_NO_BINARY': '1'}):
            market_tests.MarketLoopbackTests.setUp(self)

    def test_disabled_server_uses_legacy_identity_without_credit(self):
        client = self.client()
        self.assertEqual(client.subscribe_market('us_equity').wait_applied(10)['admitted_count'], 1501)
        self.assertFalse(client._market_binary)
        self.assertFalse(client._market_replay_credit)
        self.assertEqual(client.unsubscribe_market('us_equity').wait_applied(10)['admitted_count'], 0)


class FullFieldsReplayCompletenessTests(ReplayCompletenessTests):
    full_fields = True


class DelayedAckBandwidthTests(BinaryBandwidthTests):
    upstream_delay = .1
