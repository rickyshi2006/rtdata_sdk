import json
import multiprocessing
import os
from pathlib import Path
import time
import unittest

from rtdata import RtdataClient
from rtdata._market_subscription import MARKETS
import test_market_subscription as _market_tests


def _peer_main(channel, port, folder, token):
    client = None
    latest = 0
    received = 0
    measure_since = None
    histogram = [0] * 2001
    max_latency = 0
    worst = []
    over_300ms = 0
    try:
        client = RtdataClient(token, host='127.0.0.1', port=port,
                              async_callbacks=False, auto_reconnect=False,
                              realtime_protocol='required_v2', history_v2_advertise=False,
                              symbol_cache_dir=folder, history_cache_enabled=False)
        client._symbol_map.update_from_dict({1: 'TEST1'})

        def quote_seen(quote):
            nonlocal latest, received, max_latency, over_300ms
            latest = max(latest, quote.trade_time or 0)
            received += 1
            if measure_since is not None and quote.trade_time >= measure_since:
                latency = max(0, int(time.time() * 1000) - quote.trade_time)
                histogram[min(latency, 2000)] += 1
                max_latency = max(max_latency, latency)
                over_300ms += latency > 300
                if latency > 100:
                    worst.append(dict(ms=latency, symbol=quote.symbol, sequence=quote.sequence,
                                      elapsed_ms=quote.trade_time - measure_since))
                    worst.sort(key=lambda item: item['ms'], reverse=True)
                    del worst[8:]

        client.on_quote(quote_seen)
        client.connect(timeout=5)
        channel.send({'connected': True})
        deadline = time.monotonic() + 1800
        while time.monotonic() < deadline:
            if not channel.poll(.2):
                continue
            command = channel.recv()
            if command == 'stop':
                break
            result = None
            if command == 'subscribe':
                result = client.subscribe_markets(MARKETS).wait_applied(25)
            elif command == 'subscribe-us':
                result = client.subscribe_markets(['us_equity']).wait_applied(25)
            elif command == 'clear':
                result = client.unsubscribe().wait_applied(15)
            elif command == 'measure':
                measure_since = int(time.time() * 1000)
                histogram = [0] * 2001
                max_latency = 0
                worst = []
                over_300ms = 0
            elif command != 'status':
                raise ValueError('unknown process peer command')
            samples = sum(histogram)
            percentiles = {}
            for percentile in (50, 95, 99):
                cumulative = 0
                for latency, count in enumerate(histogram):
                    cumulative += count
                    if samples and cumulative * 100 >= samples * percentile:
                        percentiles[f'p{percentile}_ms'] = latency
                        break
            channel.send({'connected': client.is_connected, 'result': result,
                          'feed': client.market_feed_status, 'latest': latest, 'received': received,
                          'latency': dict(percentiles, samples=samples, max_ms=max_latency,
                                          overflow_2000ms=histogram[-1], over_300ms=over_300ms, worst=worst)})
    except Exception as error:
        channel.send({'error': repr(error)})
    finally:
        if client is not None:
            client.close()
        channel.close()


class _Peer:
    def __init__(self, case, index):
        context = multiprocessing.get_context('spawn')
        self.channel, child = context.Pipe()
        folder = Path(case.folder.name) / f'peer-{index}'
        folder.mkdir()
        self.process = context.Process(target=_peer_main, args=(
            child, case.port, str(folder), f'isolated-market-peer-{index}'))
        self.case = case
        self.process.start()
        child.close()
        case.addCleanup(self.close)
        self.receive()

    def send(self, command):
        self.channel.send(command)

    def receive(self, timeout=30):
        self.case.assertTrue(self.channel.poll(timeout), 'process peer response timed out')
        result = self.channel.recv()
        self.case.assertNotIn('error', result, result)
        return result

    def close(self):
        if self.process.is_alive():
            try:
                self.channel.send('stop')
            except (EOFError, BrokenPipeError):
                pass
            self.process.join(8)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(5)
        self.channel.close()


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER') and
                     os.environ.get('RTDATA_MARKET_PROCESS_TEST') == '1', '需显式启用独立进程负载测试')
class MarketProcessLoadTests(unittest.TestCase):
    full_fields = True
    setUp = _market_tests.MarketLoopbackTests.setUp
    write_registry = _market_tests.MarketLoopbackTests.write_registry
    stop_server = _market_tests.MarketLoopbackTests.stop_server
    command = _market_tests.MarketLoopbackTests.command
    stats = _market_tests.MarketLoopbackTests.stats
    assert_clean = _market_tests.MarketLoopbackTests.assert_clean

    def initial_rows(self):
        self.count = int(os.environ.get('RTDATA_MARKET_PROCESS_SYMBOLS', '50000'))
        self.cycles = int(os.environ.get('RTDATA_MARKET_PROCESS_CYCLES', '6'))
        self.assertIn(self.count, (10000, 50000))
        self.assertTrue(1 <= self.cycles <= 30)
        rows = _market_tests.MarketLoopbackTests.initial_rows(self)
        rows.extend(dict(symbol=f'US{i:05}', symbol_id=i, stock_id=str(10000 + i),
                         source_market=11, instrument_type=3, active=True, legacy=False)
                    for i in range(1508, self.count + 1))
        return rows

    def statuses(self, peers):
        for peer in peers:
            peer.send('status')
        return [peer.receive(5) for peer in peers]

    def wait_states(self, peers, expected):
        started = time.monotonic()
        while time.monotonic() - started < 8:
            statuses = self.statuses(peers)
            self.assertTrue(all(status['connected'] for status in statuses), statuses)
            summaries = [{row['category']: row['state'] for row in status['feed']['markets']}
                         for status in statuses]
            if all(summary == expected for summary in summaries):
                return time.monotonic() - started
            time.sleep(.02)
        debug = self.command('feed-debug', 'MARKET_TEST_FEED_DEBUG=')
        self.log.seek(0)
        self.fail(f'process feed timeout: {statuses}; server={debug}; log={self.log.read()[-8000:]}')

    def test_independent_clients_supply_churn(self):
        peers = [_Peer(self, i) for i in range(3)]
        for peer in peers:
            peer.send('subscribe')
        for peer in peers:
            self.assertEqual(peer.receive()['result']['admitted_count'], self.count)
        normal = dict.fromkeys(MARKETS, 'normal')
        self.wait_states(peers, normal)
        baseline = self.stats()
        started = time.monotonic()
        delays, applied, samples = [], [], []
        for _ in range(self.cycles):
            self.command('supply-backup', 'MARKET_TEST_BACKUP')
            self.command('supply-down', 'MARKET_TEST_DOWN')
            expected = dict(normal, us_equity='partial', fx='paused', overseas_futures='paused', crypto='paused')
            delays.append(self.wait_states(peers, expected))
            self.command('quotes', 'MARKET_TEST_QUOTES')
            time.sleep(.6)
            self.command('quotes', 'MARKET_TEST_QUOTES')
            delays.append(self.wait_states(peers, normal))
            peers[2].send('clear')
            self.assertEqual(peers[2].receive()['result']['admitted_count'], 0)
            tick = time.monotonic()
            peers[2].send('subscribe')
            self.assertEqual(peers[2].receive()['result']['admitted_count'], self.count)
            applied.append(time.monotonic() - tick)
            delays.append(self.wait_states(peers, normal))
            sent_ms = int(time.time() * 1000)
            self.command('quotes', 'MARKET_TEST_QUOTES')
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                statuses = self.statuses(peers)
                if all(status['latest'] >= sent_ms for status in statuses):
                    break
                time.sleep(.02)
            self.assertTrue(all(status['latest'] >= sent_ms for status in statuses), statuses)
            stats = self.stats()
            self.assertEqual(stats['subscriptions'], self.count * 3)
            self.assertLessEqual(stats['subscription_state_reserved_bytes'], 64 * 1024 * 1024)
            samples.append(stats['rss_kib'])
        elapsed = time.monotonic() - started
        end = self.stats()
        if len(samples) >= 6:
            self.assertLess(max(samples[-3:]) - max(samples[:3]), 48 * 1024)
        for peer in peers:
            peer.send('clear')
        for peer in peers:
            self.assertEqual(peer.receive()['result']['admitted_count'], 0)
            peer.close()
        cleanup = self.assert_clean()
        print('market_process_load=' + json.dumps(dict(
            processes=3, symbols_per_client=self.count, cycles=self.cycles, seconds=round(elapsed, 3),
            max_feed_seconds=round(max(delays), 3), max_applied_seconds=round(max(applied), 3),
            rss_samples_kib=samples,
            cloud_cpu_percent=round(100 * (end['cpu_seconds'] - baseline['cpu_seconds']) / elapsed, 2),
            cleanup=cleanup), sort_keys=True))


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER') and
                     os.environ.get('RTDATA_MARKET_RATE_TEST') == '1', '需显式启用定速输入测试')
class MarketRateLoadTests(unittest.TestCase):
    full_fields = True
    setUp = _market_tests.MarketLoopbackTests.setUp
    write_registry = _market_tests.MarketLoopbackTests.write_registry
    stop_server = _market_tests.MarketLoopbackTests.stop_server
    command = _market_tests.MarketLoopbackTests.command
    stats = _market_tests.MarketLoopbackTests.stats
    assert_clean = _market_tests.MarketLoopbackTests.assert_clean
    statuses = MarketProcessLoadTests.statuses
    wait_states = MarketProcessLoadTests.wait_states

    def initial_rows(self):
        self.rate = int(os.environ.get('RTDATA_MARKET_RATE', '1000'))
        self.seconds = int(os.environ.get('RTDATA_MARKET_RATE_SECONDS', '20'))
        self.assertIn(self.rate, (500, 1000, 2000, 5000))
        self.assertTrue(5 <= self.seconds <= 60)
        rows = _market_tests.MarketLoopbackTests.initial_rows(self)
        rows.extend(dict(symbol=f'US{i:05}', symbol_id=i, stock_id=str(10000 + i),
                         source_market=11, instrument_type=3, active=True, legacy=False)
                    for i in range(1508, 10001))
        return rows

    def test_paced_dense_snapshots(self):
        peers = [_Peer(self, i) for i in range(3)]
        for peer in peers:
            peer.send('subscribe')
        for peer in peers:
            self.assertEqual(peer.receive()['result']['admitted_count'], len(self.rows))
        self.wait_states(peers, dict.fromkeys(MARKETS, 'normal'))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            warmup = self.statuses(peers)
            if all(status['received'] >= len(self.rows) for status in warmup):
                break
            time.sleep(.02)
        self.assertTrue(all(status['received'] == len(self.rows) for status in warmup), warmup)
        baseline = self.stats()
        for peer in peers:
            peer.send('measure')
        for peer in peers:
            peer.receive()
        self.log.seek(0, 2)
        offset = self.log.tell()
        self.server.stdin.write(f'stream {self.rate} {self.seconds}\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + self.seconds + 15
        source = None
        while time.monotonic() < deadline:
            self.log.seek(offset)
            for line in self.log.read().splitlines():
                if line.startswith('MARKET_TEST_STREAM='):
                    source = json.loads(line.split('=', 1)[1])
                    break
            if source is not None or self.server.poll() is not None:
                break
            time.sleep(.1)
        self.assertIsNotNone(source, 'paced producer did not complete')
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            statuses = self.statuses(peers)
            if all(status['latency']['samples'] >= source['accepted'] for status in statuses):
                break
            time.sleep(.05)
        self.assertTrue(all(status['connected'] for status in statuses), statuses)
        self.assertTrue(all(status['latency']['samples'] > 0 for status in statuses), statuses)
        self.assertEqual(source['accepted'], source['generated'])
        end = self.stats()
        for peer in peers:
            peer.send('clear')
        for peer in peers:
            self.assertEqual(peer.receive()['result']['admitted_count'], 0)
            peer.close()
        cleanup = self.assert_clean()
        print('market_rate_load=' + json.dumps(dict(
            source=source, actual_rate=round(source['generated'] / source['seconds'], 2),
            replay_drained=True,
            mean_wire_bytes=round(source['wire_bytes'] / source['accepted'], 1),
            clients=[dict(latency=status['latency'],
                          delivery_ratio=round(status['latency']['samples'] / source['accepted'], 6))
                     for status in statuses],
            cloud_cpu_percent=round(100 * (end['cpu_seconds'] - baseline['cpu_seconds']) / source['seconds'], 2),
            cleanup=cleanup), sort_keys=True))


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER') and
                     os.environ.get('RTDATA_MARKET_MIXED_TEST') == '1', '需显式启用混合持续压力测试')
class MarketMixedLoadTests(unittest.TestCase):
    full_fields = True
    setUp = _market_tests.MarketLoopbackTests.setUp
    write_registry = _market_tests.MarketLoopbackTests.write_registry
    stop_server = _market_tests.MarketLoopbackTests.stop_server
    command = _market_tests.MarketLoopbackTests.command
    stats = _market_tests.MarketLoopbackTests.stats
    assert_clean = _market_tests.MarketLoopbackTests.assert_clean
    client = _market_tests.MarketLoopbackTests.client
    statuses = MarketProcessLoadTests.statuses

    def initial_rows(self):
        self.duration = int(os.environ.get('RTDATA_MARKET_MIXED_SECONDS', '180'))
        self.rate = int(os.environ.get('RTDATA_MARKET_MIXED_RATE', '100'))
        self.assertTrue(150 <= self.duration <= 600)
        self.assertIn(self.rate, (100, 1000))
        rows = _market_tests.MarketLoopbackTests.initial_rows(self)
        rows.extend(dict(symbol=f'US{i:05}', symbol_id=i, stock_id=str(10000 + i),
                         source_market=11, instrument_type=3, active=True, legacy=False)
                    for i in range(1508, 10001))
        return rows

    def test_slow_reader_shared_category_and_live_source_switching(self):
        import threading
        release, blocked = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        peers = [_Peer(self, index) for index in range(3)]
        for index, peer in enumerate(peers):
            peer.send('subscribe-us' if index < 2 else 'subscribe')
        for index, peer in enumerate(peers):
            self.assertEqual(peer.receive()['result']['admitted_count'], 9994 if index < 2 else 10000)
        slow = self.client('isolated-market-slow')
        self.assertEqual(slow.subscribe_market('us_equity').wait_applied(25)['admitted_count'], 9994)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            states = self.statuses(peers)
            if all(state['received'] >= (9994 if i < 2 else 10000) for i, state in enumerate(states)):
                break
            time.sleep(.05)
        self.assertTrue(all(state['received'] >= (9994 if i < 2 else 10000) for i, state in enumerate(states)))
        self.command('bound-slow-socket', 'MARKET_TEST_SLOW_BOUNDED')
        slow._conn._sock.setsockopt(_market_tests.socket.SOL_SOCKET, _market_tests.socket.SO_RCVBUF, 4096)

        def hold_reader(quote):
            blocked.set()
            release.wait(self.duration + 30)

        slow.on_quote(hold_reader)
        for peer in peers:
            peer.send('measure')
        for peer in peers:
            peer.receive()
        self.log.seek(0, 2)
        offset = self.log.tell()
        started = time.monotonic()
        self.server.stdin.write(f'stream {self.rate} {self.duration} mixed\n')
        self.server.stdin.flush()
        self.assertTrue(blocked.wait(5))
        source = None
        saw_backup = saw_paused = saw_recovered = False
        latest = [0] * 3
        last_progress = [time.monotonic()] * 3
        max_gap = 0
        while time.monotonic() - started < self.duration + 15:
            states = self.statuses(peers)
            now = time.monotonic()
            self.assertTrue(all(state['connected'] for state in states), states)
            for index, state in enumerate(states):
                if state['received'] > latest[index]:
                    max_gap = max(max_gap, now - last_progress[index])
                    last_progress[index] = now
                    latest[index] = state['received']
                self.assertLess(now - last_progress[index], 5, 'healthy SDK made no progress')
            shared = [row for state in states[:2] for row in state['feed']['markets']]
            saw_backup |= any(row.get('backup_count', 0) > 0 for row in shared)
            other = {row['category']: row for row in states[2]['feed']['markets']}
            paused = all(other.get(category, {}).get('state') == 'paused'
                         for category in ('fx', 'overseas_futures', 'crypto'))
            saw_paused |= paused
            if saw_paused and all(other.get(category, {}).get('state') == 'normal'
                                  for category in ('fx', 'overseas_futures', 'crypto')):
                saw_recovered = True
            self.log.seek(offset)
            lines = self.log.read().splitlines()
            for line in lines:
                if line.startswith('MARKET_TEST_STREAM='):
                    source = json.loads(line.split('=', 1)[1])
            if source is not None:
                break
            self.assertIsNone(self.server.poll())
            time.sleep(.5)
        self.assertIsNotNone(source)
        time.sleep(.5)
        states = self.statuses(peers)
        end = self.stats()
        print('market_mixed_measurement=' + json.dumps(dict(source=source,
            clients=[state['latency'] for state in states], end=end,
            backup_seen=saw_backup, paused_seen=saw_paused, recovered_seen=saw_recovered), sort_keys=True))
        self.assertTrue(saw_backup and saw_paused and saw_recovered)
        self.assertGreaterEqual(source['source_switches'], 2)
        self.assertGreaterEqual(source['generated'] / source['seconds'], self.rate * .99)
        for state in states:
            self.assertGreater(state['latency']['samples'], self.rate * self.duration * .5)
            self.assertLessEqual(state['latency']['max_ms'], 300)
        self.assertTrue(source['observed_stall'], 'slow SDK never caused real socket backpressure')
        self.assertLessEqual(source['peak_subscription_state_bytes'], 64 * 1024 * 1024)
        self.assertLessEqual(source['peak_queue_metadata_bytes'], 16 * 1024 * 1024)
        self.assertLessEqual(end['subscription_state_reserved_bytes'], 64 * 1024 * 1024)
        self.assertLessEqual(end['queue_metadata_reserved_bytes'], 16 * 1024 * 1024)
        release.set()
        slow.close()
        for peer in peers:
            peer.send('clear')
        for peer in peers:
            self.assertEqual(peer.receive()['result']['admitted_count'], 0)
            peer.close()
        cleanup = self.assert_clean()
        print('market_mixed_load=' + json.dumps(dict(source=source,
            healthy_sdk_processes=3, stalled_sdk=1, max_progress_gap_seconds=max_gap,
            backup_seen=saw_backup, paused_seen=saw_paused, recovered_seen=saw_recovered,
            clients=[state['latency'] for state in states], end=end, cleanup=cleanup), sort_keys=True))
