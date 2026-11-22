"""独立原生负载进程中的真实 SDK 探针，只使用临时本机服务。"""
import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import rtdata
from rtdata import RtdataClient


@unittest.skipUnless(os.environ.get('RTDATA_FANOUT_BINARY') and
                     os.environ.get('RTDATA_FANOUT_SDK_TEST') == '1', '需显式启用千连接 SDK 探针')
class MarketFanoutSdkTests(unittest.TestCase):
    def test_full_category_sdk_callback_under_native_fanout(self):
        clients = int(os.environ.get('RTDATA_FANOUT_CLIENTS', '1000'))
        symbols = int(os.environ.get('RTDATA_FANOUT_SYMBOLS', '10000'))
        rate = int(os.environ.get('RTDATA_FANOUT_RATE', '100'))
        rounds = int(os.environ.get('RTDATA_FANOUT_ROUNDS', '1'))
        duration_text = os.environ.get('RTDATA_FANOUT_MAX_SECONDS', '120')
        self.assertIn(duration_text, ('120', '300', '600', '1800'))
        duration_limit = int(duration_text)
        if os.environ.get('RTDATA_FANOUT_INSTALLED_SDK') == '1':
            self.assertTrue(sys.flags.isolated)
            self.assertTrue(Path(rtdata.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()))
        async_callbacks = os.environ.get('RTDATA_FANOUT_ASYNC') == '1'
        self.assertTrue(3 <= clients <= 1000)
        self.assertIn(symbols, (128, 10000))
        self.assertIn(rate, (100, 1000, 3000))
        self.assertTrue(1 <= rounds <= 15000 and rounds * symbols <= rate * duration_limit)
        expected = rounds * symbols
        heterogeneous = os.environ.get('RTDATA_FANOUT_HETEROGENEOUS') == '1'
        cohort = int(os.environ.get('RTDATA_FANOUT_SDK_COHORT', '0')) if heterogeneous else 0
        self.assertIn(cohort, (0, 1, 2))
        selected_symbols = (symbols if cohort == 0 else symbols - symbols // 3 if cohort == 1 else symbols // 3)
        expected_delivery = rounds * selected_symbols
        sequence_offset = 2 if heterogeneous and os.environ.get('RTDATA_FANOUT_PRIME_SOURCES') == '1' else 0
        markets = (['cn_equity', 'hk_equity'] if heterogeneous and cohort == 0 else
                   ['hk_equity'] if cohort == 2 else ['cn_equity'])
        with tempfile.TemporaryDirectory() as folder:
            controller, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            self.addCleanup(controller.close)
            env = dict(os.environ, RTDATA_FANOUT_CONTROL_FD=str(child.fileno()))
            with (Path(folder) / 'native.log').open('w+') as log:
                process = subprocess.Popen([os.environ['RTDATA_FANOUT_BINARY'], str(rounds), str(clients),
                    'v2', 'market', str(rate), 'full', str(symbols), 'decode', 'split', 'sdk'],
                    env=env, pass_fds=(child.fileno(),), stdout=log, stderr=subprocess.STDOUT,
                    cwd=folder, start_new_session=True)
                child.close()
                if os.environ.get('RTDATA_FANOUT_PIN'):
                    allowed = os.sched_getaffinity(0)
                    self.assertEqual(len(allowed), 3)
                    sdk_cpus = (set(sorted(allowed)[:2]) if os.environ.get('RTDATA_FANOUT_SDK_ON_CLOUD')
                                else {max(allowed)})
                    os.sched_setaffinity(0, sdk_cpus)
                    self.addCleanup(os.sched_setaffinity, 0, allowed)
                client = None
                try:
                    def receive(timeout):
                        controller.settimeout(timeout)
                        raw = controller.recv(65537)
                        self.assertTrue(raw and len(raw) <= 65536, 'native control closed or oversized')
                        return json.loads(raw)

                    def send(value):
                        controller.sendall(json.dumps(value).encode())

                    ready = receive(15)
                    self.assertEqual(ready['symbols'], symbols)
                    self.assertEqual(ready['input_frames'], expected)
                    client = RtdataClient(ready['token'], host='127.0.0.1', port=ready['port'],
                        async_callbacks=async_callbacks, auto_reconnect=False, realtime_protocol='required_v2',
                        realtime_numeric=os.environ.get('RTDATA_FANOUT_NUMERIC') == '1',
                        history_v2_advertise=False, symbol_cache_dir=folder, history_cache_enabled=False)
                    counts, latest, histogram, failures = {}, {}, [0] * 2001, []
                    max_us = 0
                    lock = threading.Lock()
                    sdk_memory = []

                    def sample_memory(started):
                        fields = Path('/proc/self/statm').read_text().split()
                        sdk_memory.append(dict(seconds=time.monotonic() - started,
                            rss_kib=int(fields[1]) * os.sysconf('SC_PAGE_SIZE') // 1024))
                        self.assertLessEqual(len(sdk_memory), duration_limit // 5 + 2)

                    def observed(quote):
                        nonlocal max_us
                        delay = time.monotonic_ns() // 1000 - quote.trade_time
                        with lock:
                            sid = quote.symbol_id
                            suffix = ('SH' if not heterogeneous or sid % 3 == 1 else 'SZ' if sid % 3 == 2 else 'HK')
                            permitted = cohort == 0 or (cohort == 1 and sid % 3 != 0) or (cohort == 2 and sid % 3 == 0)
                            if not (1 <= sid <= symbols and quote.symbol == f'{sid}.{suffix}' and permitted
                                    and quote.wire_version == 2 and quote.source_id == (2 if suffix == 'HK' else 1)):
                                if len(failures) < 8:
                                    failures.append('identity_or_format')
                            counts[quote.symbol_id] = counts.get(quote.symbol_id, 0) + 1
                            latest[quote.symbol_id] = quote.sequence
                            if quote.sequence != (counts[quote.symbol_id] - 1) * symbols + quote.symbol_id + sequence_offset:
                                if len(failures) < 8:
                                    failures.append('sequence_gap_or_duplicate')
                            if delay < 0 and len(failures) < 8:
                                failures.append('negative_clock_delta')
                            histogram[min(max(0, delay) // 1000, 2000)] += 1
                            max_us = max(max_us, delay)

                    client.on_quote(observed)
                    # 不预先植入目录；映射由 Cloud 的协议下发。
                    client.connect(timeout=5)
                    status = client.subscribe_markets(markets).wait_applied(25)
                    self.assertEqual(status['admitted_count'], selected_symbols)
                    send('ready')
                    if os.environ.get('RTDATA_FANOUT_MEMORY_TRACE'):
                        started = time.monotonic()
                        deadline = started + duration_limit + 60
                        sample_memory(started)
                        while True:
                            self.assertLess(time.monotonic(), deadline, 'publisher timeout')
                            try:
                                published = receive(min(5, deadline - time.monotonic()))
                                break
                            except socket.timeout:
                                sample_memory(started)
                        self.assertEqual(published, {'state': 'published'})
                    else:
                        self.assertEqual(receive(duration_limit + 60), {'state': 'published'})
                    deadline = time.monotonic() + 8
                    while time.monotonic() < deadline:
                        with lock:
                            if sum(histogram) >= expected_delivery:
                                break
                        time.sleep(.01)
                    with lock:
                        samples = sum(histogram)
                        percentiles = {}
                        for percentile in (50, 95, 99):
                            cumulative = 0
                            for bucket, count in enumerate(histogram):
                                cumulative += count
                                if samples and cumulative * 100 >= samples * percentile:
                                    percentiles[f'p{percentile}_ms'] = bucket
                                    break
                        result = dict(percentiles, received=samples, distinct_symbols=len(counts),
                            max_ms=max_us / 1000, errors=failures,
                            delivery_complete=len(counts) == selected_symbols and all(n == rounds for n in counts.values()),
                            connected=client.is_connected,
                            latest_complete=len(latest) == selected_symbols and all(
                                sequence == (rounds - 1) * symbols + sid + sequence_offset for sid, sequence in latest.items()),
                            async_callbacks=async_callbacks, callback_dropped=client._quotes_dropped,
                            sdk_module=str(Path(rtdata.__file__).resolve()), sdk_python=sys.executable,
                            sdk_allowed_cpus=sorted(os.sched_getaffinity(0)),
                            block_statistics=dict(client.realtime_statistics),
                            selected_symbols=selected_symbols, cohort=cohort, memory_samples=sdk_memory,
                            sdk_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                        result['acceptance_met'] = (result['delivery_complete'] and not failures and
                                                   result['connected'] and not result['callback_dropped']
                                                   and max_us <= 300000)
                    send(result)
                    self.assertEqual(receive(30), {'state': 'close'})
                    client.close()
                    send('closed')
                    code = process.wait(15)
                    log.seek(0)
                    self.assertLessEqual(log.seek(0, 2), 4 * 1024 * 1024, 'native test log exceeds bounded read')
                    log.seek(0)
                    output = log.read(4 * 1024 * 1024)
                    memory_rows = [line for line in output.splitlines() if line.startswith('fanout_memory=')]
                    self.assertLessEqual(len(memory_rows), duration_limit // 5 + 12)
                    for line in memory_rows:
                        print(line)
                    reports = [json.loads(line.split('=', 1)[1]) for line in output.splitlines()
                               if line.startswith('fanout_latency=')]
                    self.assertEqual(len(reports), 1, output[-8000:])
                    report = reports[0]
                    print('market_fanout_sdk=' + json.dumps(report, sort_keys=True))
                    self.assertEqual(report['clients'], clients)
                    self.assertEqual(report['native_clients'], clients - 1)
                    self.assertTrue(report['cleanup_budgets_zero'])
                    self.assertEqual(code, 0, report)
                    self.assertTrue(report['acceptance_met'], report)
                    if heterogeneous:
                        self.assertTrue(report['heterogeneous'])
                        self.assertEqual(report['permission_rejections'], sum(c % 4 in (2, 3) for c in range(clients - 1)))
                        wanted = [symbols if c % 4 == 0 else symbols - symbols // 3 if c % 4 == 1 else
                                  symbols // 3 if c % 4 == 2 else 0 for c in range(clients - 1)]
                        self.assertEqual(report['expected_symbols_per_native_client'], wanted)
                        self.assertEqual(report['received_per_native_client'], [rounds * n for n in wanted])
                    if os.environ.get('RTDATA_FANOUT_BLOCK'):
                        self.assertGreater(report['accounting']['compressed_blocks'], 0)
                        if not heterogeneous or cohort in (0, 1, 2):
                            self.assertGreater(result['block_statistics']['compressed_blocks'], 0)
                        if heterogeneous:
                            for c, (numeric, plain) in enumerate(zip(report['numeric_blocks_per_native_client'],
                                                                     report['plain_blocks_per_native_client'])):
                                if c % 4 in (0, 1):
                                    self.assertGreater(numeric + plain, 0)
                                elif c % 4 == 3:
                                    self.assertEqual(numeric + plain, 0)
                    if os.environ.get('RTDATA_FANOUT_NUMERIC') == '1':
                        expected_numeric = os.environ.get('RTDATA_FANOUT_EXPECT_SDK_NUMERIC', '1') == '1'
                        self.assertEqual(result['block_statistics']['numeric_negotiated'], expected_numeric)
                        if expected_numeric:
                            self.assertGreater(result['block_statistics']['numeric_blocks'], 0)
                        elif not expected_numeric:
                            self.assertEqual(result['block_statistics']['numeric_blocks'], 0)
                        self.assertGreater(report['native_numeric_blocks'], 0)
                        if os.environ.get('RTDATA_FANOUT_NUMERIC_MIXED'):
                            self.assertGreater(report['native_plain_blocks'], 0)
                finally:
                    if client is not None:
                        client.close()
                    if process.poll() is None:
                        # 仅终止本测试创建的新会话，包含原生 reader 子进程。
                        import signal
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(10)
                    if process.returncode:
                        log.seek(0)
                        remaining, found = 4 * 1024 * 1024, 0
                        while remaining > 0 and found < 16:
                            line = log.readline(min(remaining, 16384))
                            if not line:
                                break
                            remaining -= len(line)
                            if any(marker in line for marker in ('fanout_direct_failure=', 'fanout_trace=',
                                    'fanout_sequence_failure=', 'Assertion ', 'terminate called',
                                    'what():', 'reason=shared_stream_drain_failed')):
                                print('fanout_first_failure=' + line.rstrip())
                                found += 1
                        log.seek(0, 2)
                        size = log.tell()
                        log.seek(max(0, size - 16000))
                        print('fanout_native_failure_tail=' + log.read())


if __name__ == '__main__':
    unittest.main()
