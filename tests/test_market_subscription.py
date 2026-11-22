import json
import os
from pathlib import Path
import subprocess
import socket
import struct
import threading
import tempfile
import time
import unittest

from rtdata import RtdataClient
from rtdata.exceptions import RtdataError
from rtdata import _protocol as proto
from rtdata._market_subscription import MARKETS, REQUEST, RESPONSE, IDENTITIES, IDENTITY_ACK
from rtdata._market_subscription import MAX_CONTROL_BYTES, MAX_QUEUED_BYTES
from rtdata.exceptions import AuthenticationError, ConnectionError
from test_realtime_client import FakeConnection


class RawMarketPeer:
    def __init__(self, case, token='isolated-market-test'):
        self.case = case
        self.socket = socket.create_connection(('127.0.0.1', case.port), timeout=5)
        case.addCleanup(self.socket.close)
        self.socket.sendall(proto.encode_auth(token))
        kind, payload = self.receive()
        case.assertEqual(kind, proto.MsgType.AUTH_RESPONSE)
        case.assertEqual(payload[0], 1)
        self.request(1, 'hello', symbol_capacity=250000)
        case.assertEqual(self.receive_control()['state'], 'negotiated')

    def exact(self, size):
        data = bytearray()
        while len(data) < size:
            chunk = self.socket.recv(size - len(data))
            if not chunk:
                raise EOFError('market peer closed')
            data.extend(chunk)
        return bytes(data)

    def receive(self):
        length, symbol, kind = proto.decode_header(self.exact(10))
        self.case.assertLessEqual(length, 65536)
        return kind, self.exact(length)

    def receive_control(self):
        kind, payload = self.receive()
        self.case.assertEqual(kind, RESPONSE)
        return json.loads(payload)

    def request(self, rid, op, **fields):
        fields.update(version=1, request_id=rid, op=op)
        self.socket.sendall(proto.build_message(REQUEST, 0, json.dumps(fields).encode()))

    def ack(self, page):
        body = {key: page[key] for key in ('version', 'generation', 'catalog_revision', 'next_offset')}
        self.socket.sendall(proto.build_message(IDENTITY_ACK, 0, json.dumps(body).encode()))

    def final_identity(self, rid=2):
        self.request(rid, 'subscribe', markets=['us_equity'])
        for _ in range(2000):
            kind, payload = self.receive()
            self.case.assertIn(kind, (RESPONSE, IDENTITIES))
            body = json.loads(payload)
            if kind == IDENTITIES:
                if body['done']:
                    return body
                self.ack(body)
            else:
                self.case.assertNotEqual(body['state'], 'applied')
                self.case.assertNotEqual(body['state'], 'rejected', body)
        self.case.fail('identity page limit exceeded')


class MarketSubscriptionTests(unittest.TestCase):
    def client(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        client = RtdataClient('test', async_callbacks=False, auto_reconnect=False,
                              realtime_protocol='legacy', symbol_cache_dir=folder.name,
                              history_cache_enabled=False, history_v2_advertise=False)
        self.addCleanup(client.close)
        client._conn = FakeConnection()
        client._authenticated = True
        client._begin_markets(b'\x01{"market_subscription_v1":true}')
        self.respond(client, state='negotiated', request_id=1, generation=0)
        return client

    def respond(self, client, **body):
        body['version'] = 1
        client._handle_market_control(RESPONSE, 0, json.dumps(body).encode(), 1, client._conn)

    def identities(self, client, generation, rid, entries, offset=0, total=None):
        total = len(entries) if total is None else total
        body = dict(version=1, request_id=rid, generation=generation, catalog_revision='r1',
                    offset=offset, next_offset=offset + len(entries), total=total,
                    done=offset + len(entries) == total, entries=entries)
        client._handle_market_control(IDENTITIES, 0, json.dumps(body).encode(), 1, client._conn)

    def test_identity_barrier_and_overlap(self):
        client = self.client()
        handle = client.subscribe_market('us_equity')
        self.assertEqual(json.loads(client._conn.sent[-1][10:])['markets'], ['us_equity'])
        self.identities(client, 1, 2, [dict(action='add', symbol_id=8, code='BRK.B')])
        self.assertIsNone(client._market_code(8))
        self.assertEqual(proto.decode_header(client._conn.sent[-1][:10])[2], IDENTITY_ACK)
        self.respond(client, request_id=2, generation=1, state='applied', admitted_count=1)
        self.assertEqual(handle.wait_applied(.1)['admitted_count'], 1)
        self.assertEqual(client._market_code(8), 'BRK.B')
        self.assertEqual(client._desired_markets, {'us_equity'})
        client.subscribe(['BRK.B'])
        self.assertEqual(proto.decode_header(client._conn.sent[-1][:10])[2], REQUEST)
        self.identities(client, 2, 3, [])
        self.respond(client, request_id=3, generation=2, state='applied', admitted_count=1)
        self.assertEqual(set(client._desired_subscriptions), {'BRK.B'})
        client.unsubscribe_market('us_equity')
        self.identities(client, 3, 4, [])
        self.respond(client, request_id=4, generation=3, state='applied', admitted_count=1)
        self.assertFalse(client._desired_markets)
        self.assertEqual(client._market_code(8), 'BRK.B')

    def test_feed_status_is_optional_generation_bound_and_separate(self):
        client = self.client()
        self.assertEqual(client.market_feed_status['state'], 'unsupported')
        client._market_feed_supported = True
        client._desired_markets = {'fx'}
        client._market_committed_generation = 3
        events = []
        client.on_market_feed_status(events.append)
        body = dict(version=1, request_id=0, state='feed_status', subscription_generation=3,
                    revision=1, sample_duration_ms=2, atomic=False, markets=[dict(
                        category='fx', state='paused', event='updated', reason='no_usable_source',
                        admitted_count=1, available_count=0, primary_count=0, backup_count=0,
                        unavailable_count=1, unknown_count=0)])
        old = client.market_subscription_status
        self.respond(client, **body)
        self.assertEqual(client.market_feed_status['markets'][0]['state'], 'paused')
        self.assertEqual(client.market_subscription_status, old)
        self.respond(client, **body)
        self.assertEqual(len(events), 1)
        body['revision'] = 2
        body['subscription_generation'] = 4
        self.respond(client, **body)
        self.assertEqual(len(events), 1)
        body['subscription_generation'] = 3
        body['markets'][0]['available_count'] = 1
        self.respond(client, **body)
        self.assertTrue(client._conn.connected)
        self.assertEqual(len(events), 1)
        body['markets'][0].update(primary_count=1, unavailable_count=0, state='normal', event='recovered')
        self.respond(client, **body)
        self.assertEqual(len(events), 2)
        client._reset_markets()
        self.assertEqual(client.market_feed_status['state'], 'disconnected')

    def test_reject_keeps_intent_and_generation_isolation(self):
        client = self.client()
        handle = client.subscribe_market('us_equity')
        self.respond(client, request_id=2, generation=1, state='rejected', reason='symbol_limit')
        with self.assertRaisesRegex(RtdataError, 'symbol_limit'):
            handle.wait_applied(.1)
        self.assertFalse(client._desired_markets)
        conn = FakeConnection()
        client._handle_market_control(RESPONSE, 0, b'bad', 1, conn)
        self.assertTrue(client._conn.connected)
        client.subscribe_market('fx')
        self.identities(client, 2, 3, [dict(action='add', symbol_id=9, code='EUR.USD')], offset=1)
        self.assertFalse(client._conn.connected)

    def test_reconnect_restores_small_intent(self):
        client = self.client()
        client.subscribe_market('us_equity')
        self.identities(client, 1, 2, [dict(action='add', symbol_id=8, code='AAPL')])
        self.respond(client, request_id=2, generation=1, state='applied', admitted_count=1)
        client._conn = FakeConnection()
        client._begin_markets(b'\x01{"market_subscription_v1":true}')
        self.respond(client, request_id=1, generation=0, state='negotiated')
        restore = json.loads(client._conn.sent[-1][10:])
        self.assertEqual(restore['markets'], ['us_equity'])
        self.assertNotIn('symbols', restore)
        self.assertFalse(client._market_identities)

    def test_old_endpoint_and_input_validation(self):
        client = self.client()
        client._begin_markets(b'\x01')
        with self.assertRaisesRegex(RtdataError, 'unavailable'):
            client.subscribe_market('us_equity')
        for value in ('US', '', None):
            with self.assertRaises(ValueError):
                client.subscribe_market(value)

    def test_restore_is_lazy_and_byte_bounded(self):
        client = self.client()
        client._desired_subscriptions = {f'S{i:06}' + 'X' * 248: 0 for i in range(40000)}
        client._conn = FakeConnection()
        client._begin_markets(b'\x01{"market_subscription_v1":true}')
        self.respond(client, request_id=1, generation=0, state='negotiated')
        self.assertFalse(client._market_queue)
        frame = client._conn.sent[-1]
        self.assertLessEqual(len(frame) - 10, MAX_CONTROL_BYTES)
        self.assertLess(len(json.loads(frame[10:])['symbols']), 512)
        self.assertLessEqual(client._market_queued_bytes, MAX_QUEUED_BYTES)
        first = client._market_inflight
        self.respond(client, request_id=2, state='rejected', reason='subscription_busy')
        client._market_retry_at = 0
        client._poll_markets(client._conn, 1)
        self.assertIs(client._market_inflight, first)
        self.assertEqual(json.loads(frame[10:])['symbols'], json.loads(client._conn.sent[-1][10:])['symbols'])

    def test_queue_limits_and_control_deadline(self):
        client = self.client()
        client.subscribe_market('us_equity')
        handles = []
        for i in range(40):
            try:
                handles.append(client.subscribe([f'S{i:03}{j:03}' + 'X' * 248 for j in range(200)]))
            except RtdataError:
                break
        self.assertGreater(len(handles), 1)
        self.assertLess(len(handles), 40)
        self.assertLessEqual(client._market_queued_bytes, MAX_QUEUED_BYTES)
        client._market_inflight._deadline = time.monotonic() - 1
        client._poll_markets(client._conn, 1)
        self.assertFalse(client._conn.connected)
        self.assertTrue(all(h.status()['state'] == 'disconnected' for h in handles))
        self.assertEqual(client._market_queued_bytes, 0)

    def test_send_failure_rejects_all_pending_handles(self):
        client = self.client()
        first = client.subscribe_market('us_equity')
        queued = client.subscribe_market('fx')
        client._conn.fail_type = REQUEST
        self.respond(client, request_id=2, state='rejected', reason='symbol_limit')
        self.assertEqual(first.status()['state'], 'rejected')
        self.assertEqual(queued.status()['state'], 'disconnected')
        self.assertFalse(client._conn.connected)
        client = self.client()
        client._conn.fail_type = REQUEST
        with self.assertRaises(ConnectionError):
            client.subscribe_market('fx')
        self.assertIsNone(client._market_inflight)

    def test_negotiation_timeout_and_rejection(self):
        for rejected in (False, True):
            client = self.client()
            client._conn = FakeConnection()
            client._begin_markets(b'\x01{"market_subscription_v1":true}')
            waiting = client.subscribe_market('fx')
            if rejected:
                self.respond(client, request_id=1, state='rejected', reason='not_supported')
            else:
                client._market_inflight._deadline = 0
                client._poll_markets(client._conn, 1)
            self.assertEqual(waiting.status()['state'], 'disconnected')
            self.assertFalse(client._conn.connected)


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER'), '需要隔离测试服务程序')
class MarketLoopbackTests(unittest.TestCase):
    full_fields = False

    def initial_rows(self):
        sources = [(30, 3), (1, 3), (9001, 10), (11, 3), (120, 11), (90, 10), (374, 12)]
        rows = [dict(symbol=f'TEST{i}', symbol_id=i, stock_id=str(10000 + i),
                     source_market=market, instrument_type=kind, active=True, legacy=False)
                for i, (market, kind) in enumerate(sources, 1)]
        rows += [dict(symbol=f'US{i:05}', symbol_id=i, stock_id=str(10000 + i),
                      source_market=11, instrument_type=3, active=True, legacy=False)
                 for i in range(8, 1508)]
        return rows

    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        root = Path(self.folder.name)
        self.registry = root / 'registry.jsonl'
        self.rows = self.initial_rows()
        self.write_registry()
        self.log = (root / 'server.log').open('w+')
        self.addCleanup(self.log.close)
        args = [os.environ['RTDATA_MARKET_TEST_SERVER'], str(self.registry)]
        if self.full_fields:
            args.append('--full')
        self.server = subprocess.Popen(args,
                                       stdin=subprocess.PIPE, stdout=self.log, stderr=subprocess.STDOUT,
                                       text=True, cwd=root)
        self.addCleanup(self.stop_server)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            self.log.seek(0)
            for line in self.log.read().splitlines():
                if line.startswith('MARKET_TEST_PORT='):
                    self.port = int(line.split('=')[1])
                    return
            if self.server.poll() is not None:
                break
            time.sleep(.02)
        self.log.seek(0)
        self.fail('isolated server did not start: ' + self.log.read()[-4000:])

    def write_registry(self):
        with self.registry.open('w') as output:
            output.write(json.dumps({'contract': 'futu-symbol-map-v1'}) + '\n')
            for row in self.rows:
                output.write(json.dumps(row) + '\n')

    def stop_server(self):
        if self.server.poll() is None:
            self.server.communicate('stop\n', timeout=10)

    def client(self, token='isolated-market-test'):
        client = RtdataClient(token, host='127.0.0.1', port=self.port,
                              async_callbacks=False, auto_reconnect=False,
                              realtime_protocol='required_v2' if self.full_fields else 'legacy',
                              history_v2_advertise=False, symbol_cache_dir=self.folder.name,
                              history_cache_enabled=False)
        self.addCleanup(client.close)
        # The tested identity pages, not this discovery cache, authorize data.
        client._symbol_map.update_from_dict({1: 'TEST1'})
        client.connect(timeout=3)
        return client

    def command(self, command, marker):
        self.log.seek(0, 2)
        offset = self.log.tell()
        self.server.stdin.write(command + '\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            self.log.seek(offset)
            for line in self.log.read().splitlines():
                if line.startswith(marker):
                    return line[len(marker):]
            if self.server.poll() is not None:
                break
            time.sleep(.01)
        self.log.seek(offset)
        self.fail(f'fixture command failed: {command}, exit={self.server.poll()}: ' + self.log.read()[-3000:])

    def stats(self):
        return json.loads(self.command('stats', 'MARKET_TEST_STATS='))

    def assert_clean(self):
        keys = ('subscriptions', 'subscription_state_reserved_bytes', 'receive_reserved_bytes',
                'queue_metadata_reserved_bytes')
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            stats = self.stats()
            if all(stats[key] == 0 for key in keys):
                return stats
            time.sleep(.05)
        self.fail(f'fixture did not release resources: {stats}')

    def test_seven_categories_manual_union_and_retirement(self):
        client = self.client()
        seen = []
        client.on_quote(seen.append)
        catalog = client.get_markets()
        self.assertEqual([row['category'] for row in catalog['markets']], list(MARKETS))
        handle = client.subscribe_markets(MARKETS)
        self.assertEqual(handle.wait_applied(10)['admitted_count'], 1507)
        deadline = time.monotonic() + 5
        while not seen and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(seen)
        upstream = handle.list_issues(scope='upstream', limit=20)
        self.assertEqual(len(upstream['entries']), 20)
        self.assertTrue(all(row['state'] == 'unknown' for row in upstream['entries']))
        self.assertFalse(upstream['acceptance_is_data'])
        with self.assertRaisesRegex(RtdataError, 'upstream_changed'):
            handle.list_issues(scope='upstream', upstream_revision=123)
        with self.assertRaisesRegex(RtdataError, 'subscription_changed'):
            handle.list_issues(scope='upstream', subscription_generation=123)
        self.assertFalse(any(quote.symbol.startswith('UNKNOWN_') for quote in seen))
        client.subscribe(['TEST4']).wait_applied(5)
        status = client.unsubscribe_market('us_equity').wait_applied(5)
        self.assertEqual(status['admitted_count'], 7)
        self.assertEqual(client._market_code(4), 'TEST4')
        self.assertIsNone(client._market_code(8))
        client.subscribe_market('us_equity').wait_applied(5)
        self.rows[-1]['active'] = False
        self.write_registry()
        self.server.stdin.write('reload\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + 5
        while client.market_subscription_status.get('admitted_count') != 1506 and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertEqual(client.market_subscription_status['admitted_count'], 1506)
        self.assertTrue(handle.list_issues()['done'] is False)
        self.assertEqual(client.unsubscribe().wait_applied(5)['admitted_count'], 0)

    def test_quota_rejects_entire_category(self):
        client = self.client('isolated-market-limited')
        handle = client.subscribe_market('us_equity')
        with self.assertRaisesRegex(RtdataError, 'symbol_limit'):
            handle.wait_applied(5)
        self.assertFalse(client._market_identities)
        self.assertFalse(client._desired_markets)

    def test_hot_addition_and_invalid_publication_keep_connection(self):
        client = self.client()
        client.subscribe_market('us_equity').wait_applied(5)
        connection = client._conn
        generation = connection.generation
        previous = client.market_subscription_status['catalog_revision']
        new = dict(symbol='NEWUS', symbol_id=2000, stock_id='9007199254740993',
                   source_market=11, instrument_type=3, active=True, legacy=False)
        self.rows.extend([new, self.rows[0]])
        self.write_registry()
        self.server.stdin.write('reload\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            self.log.seek(0)
            if 'MARKET_TEST_REJECTED=' in self.log.read():
                break
            time.sleep(.01)
        else:
            self.fail('invalid catalog was not rejected')
        self.assertIsNone(client._market_code(2000))
        self.assertEqual(client.market_subscription_status['catalog_revision'], previous)
        self.rows.pop()
        self.write_registry()
        self.server.stdin.write('reload\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + 5
        while client._market_code(2000) is None and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertEqual(client._market_code(2000), 'NEWUS')
        self.assertTrue(client.is_connected)
        self.assertIs(client._conn, connection)
        self.assertEqual(connection.generation, generation)
        self.assertEqual(client.market_subscription_status['admitted_count'], 1502)

    def test_policy_revocation_closes_market_session(self):
        client = self.client()
        client.subscribe_market('us_equity').wait_applied(5)
        self.server.stdin.write('revoke\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + 5
        while (client.is_connected or client.market_subscription_status.get('state') != 'disconnected') and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertFalse(client.is_connected)
        self.assertFalse(client._market_identities)

    def test_combination_hot_add_manual_overlap_retire_and_reactivate(self):
        client = self.client()
        seen = []
        client.on_quote(seen.append)
        self.assertEqual(client.subscribe_market('futures').wait_applied(5)['admitted_count'], 1)
        connection = client._conn
        generation = connection.generation
        combination = dict(symbol='SP m2701&m2705.DCE', symbol_id=2000, stock_id='92000',
                           source_market=9002, instrument_type=10, active=True, legacy=False)

        def reload_count(count):
            previous = client.market_subscription_status['catalog_revision']
            self.write_registry()
            self.command('reload', 'MARKET_TEST_RELOADED')
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                status = client.market_subscription_status
                if (status.get('catalog_revision') != previous and status.get('state') == 'applied'
                        and status.get('admitted_count') == count):
                    break
                time.sleep(.01)
            else:
                self.fail(f'combination membership did not converge: {client.market_subscription_status}')
            self.assertTrue(client.is_connected)
            self.assertIs(client._conn, connection)
            self.assertEqual(connection.generation, generation)

        self.rows.append(combination)
        reload_count(2)
        self.assertEqual(client._market_code(2000), combination['symbol'])
        if self.full_fields:
            self.command('quotes', 'MARKET_TEST_QUOTES')
            deadline = time.monotonic() + 5
            while not any(q.symbol == combination['symbol'] for q in seen) and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(any(q.symbol == combination['symbol'] for q in seen))
        self.assertEqual(client.subscribe([combination['symbol']]).wait_applied(5)['admitted_count'], 2)
        self.assertEqual(client.unsubscribe_market('futures').wait_applied(5)['admitted_count'], 1)
        self.assertEqual(client._market_code(2000), combination['symbol'])
        self.assertEqual(client.subscribe_market('futures').wait_applied(5)['admitted_count'], 2)
        combination['active'] = False
        reload_count(1)
        self.assertIsNone(client._market_code(2000))
        combination['active'] = True
        reload_count(2)
        self.rows.pop()
        reload_count(1)
        previous = client.market_subscription_status['catalog_revision']
        self.rows.append({**combination, 'stock_id': '92001'})
        self.write_registry()
        self.assertIn('market_source_identity_changed', self.command('reload', 'MARKET_TEST_REJECTED='))
        self.assertEqual(client.market_subscription_status['catalog_revision'], previous)
        self.rows[-1] = combination
        reload_count(2)
        self.assertEqual(client._market_code(2000), combination['symbol'])
        self.assertEqual(client.unsubscribe().wait_applied(5)['admitted_count'], 0)


class MarketFullFieldsLoopbackTests(MarketLoopbackTests):
    full_fields = True

    def test_shared_category_repeat_manual_transition_and_source_recovery(self):
        clients = [self.client(), self.client('isolated-market-peer-0')]
        for client in clients:
            self.assertEqual(client.subscribe_market('us_equity').wait_applied(10)['admitted_count'], 1501)
        seen = []
        clients[0].on_quote(lambda quote: seen.append(quote.trade_time))
        for _ in range(2):
            self.assertEqual(clients[0].subscribe(['TEST4']).wait_applied(10)['admitted_count'], 1501)
            self.assertEqual(clients[0].unsubscribe(['TEST4']).wait_applied(10)['admitted_count'], 1501)
        self.assertEqual(clients[0].subscribe_market('us_equity').wait_applied(10)['admitted_count'], 1501)

        def wait_supply(state, backup_count=None):
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                rows = [client.market_feed_status['markets'] for client in clients]
                if all(len(row) == 1 and row[0]['state'] == state and
                       (backup_count is None or row[0]['backup_count'] == backup_count) for row in rows):
                    return rows
                time.sleep(.02)
            self.fail(f'shared category supply mismatch: {rows}')

        wait_supply('normal')
        self.command('supply-backup', 'MARKET_TEST_BACKUP')
        self.command('supply-down', 'MARKET_TEST_DOWN')
        for row in wait_supply('partial', backup_count=751):
            self.assertEqual(row[0]['backup_count'], 751)
            self.assertEqual(row[0]['admitted_count'], 1501)
        self.command('quotes', 'MARKET_TEST_QUOTES')
        time.sleep(.6)
        self.command('quotes', 'MARKET_TEST_QUOTES')
        wait_supply('normal')
        self.assertTrue(seen)
        for client in clients:
            self.assertTrue(client.is_connected)
            client.unsubscribe().wait_applied(10)
            client.close()
        self.assert_clean()

    def test_category_supply_loss_backup_and_recovery(self):
        client = self.client()
        events = []
        client.on_market_feed_status(events.append)
        handle = client.subscribe_markets(MARKETS)
        handle.wait_applied(10)

        def wait_states(expected):
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                status = client.market_feed_status
                rows = {row['category']: row for row in status['markets']}
                if all(rows.get(key, {}).get('state') == value for key, value in expected.items()):
                    return rows
                time.sleep(.02)
            self.fail(f'feed status mismatch: {status}')

        wait_states(dict.fromkeys(MARKETS, 'normal'))
        self.command('supply-backup', 'MARKET_TEST_BACKUP')
        self.command('supply-down', 'MARKET_TEST_DOWN')
        rows = wait_states(dict(cn_equity='normal', hk_equity='normal', futures='normal',
                                us_equity='partial', fx='paused', overseas_futures='paused', crypto='paused'))
        self.assertEqual(rows['cn_equity']['backup_count'], 1)
        self.assertEqual(rows['us_equity']['backup_count'], 751)
        self.assertEqual(rows['us_equity']['unknown_count'], 750)
        self.assertEqual(handle.status()['state'], 'applied')
        self.assertTrue(client.is_connected)
        self.assertEqual(client._desired_markets, set(MARKETS))
        # Recovery requires fresh primary progress, not transport reconnection alone.
        self.command('quotes', 'MARKET_TEST_QUOTES')
        time.sleep(.6)
        self.command('quotes', 'MARKET_TEST_QUOTES')
        wait_states(dict.fromkeys(MARKETS, 'normal'))
        self.assertTrue(any(row['event'] == 'recovered' and row['category'] == 'fx'
                            for event in events for row in event['markets']))
        client.unsubscribe().wait_applied(10)
        deadline = time.monotonic() + 5
        while client.market_feed_status['state'] != 'feed_status' and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertEqual(client.market_feed_status['markets'], [])
        client.close()
        self.assertEqual(client.market_feed_status['state'], 'disconnected')

    def test_new_subscription_while_source_down_is_paused_not_rejected(self):
        self.command('supply-down', 'MARKET_TEST_DOWN')
        client = self.client()
        handle = client.subscribe_markets(['fx', 'overseas_futures', 'crypto'])
        self.assertEqual(handle.wait_applied(10)['admitted_count'], 3)
        deadline = time.monotonic() + 5
        while client.market_feed_status['state'] != 'feed_status' and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertEqual([row['state'] for row in client.market_feed_status['markets']], ['paused'] * 3)

    def test_upstream_batch_outcomes_reach_sdk(self):
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.registry) + '.unused-socket')
        listener.listen(1)
        listener.settimeout(.1)
        stopped = threading.Event()
        errors = []

        def serve():
            peer = None
            try:
                while not stopped.is_set():
                    try:
                        peer, _ = listener.accept()
                        break
                    except socket.timeout:
                        continue
                if peer is None:
                    return
                peer.settimeout(.1)
                buffer = bytearray()
                while not stopped.is_set():
                    try:
                        data = peer.recv(65536)
                    except socket.timeout:
                        continue
                    if not data:
                        return
                    buffer.extend(data)
                    while len(buffer) >= 5:
                        size = struct.unpack('>I', buffer[:4])[0]
                        if not 1 <= size <= 1024 * 1024:
                            raise ValueError('invalid fixture frame')
                        if len(buffer) < size + 4:
                            break
                        kind, raw = buffer[4], bytes(buffer[5:size + 4])
                        del buffer[:size + 4]
                        if kind == 1:
                            kind, body = 0x14, dict(proto=3, profile='snapshot-v1', payload='RDP3')
                        elif kind in (2, 3):
                            codes = json.loads(raw)['symbols']
                            bindings = [dict(symbol=code, stock_id=str(10000 + i))
                                        for i, code in enumerate(sorted(row['symbol'] for row in self.rows))
                                        if code in codes and code != 'TEST4'] if kind == 2 else []
                            unavailable = [dict(symbol='TEST4', reason='not_planned')] if kind == 2 and 'TEST4' in codes else []
                            kind, body = 0x10, dict(mode='depth', bindings=bindings,
                                accepted=dict(symbols=len(bindings)), unavailable=unavailable,
                                unavailable_total=len(unavailable))
                        else:
                            continue
                        payload = json.dumps(body).encode()
                        peer.sendall(struct.pack('>IB', len(payload) + 1, kind) + payload)
            except Exception as exc:
                if not stopped.is_set():
                    errors.append(exc)
            finally:
                if peer is not None:
                    peer.close()

        worker = threading.Thread(target=serve, daemon=True)
        worker.start()

        def stop_feed():
            stopped.set()
            worker.join(2)
            listener.close()
            self.assertFalse(worker.is_alive())
            self.assertFalse(errors)

        self.addCleanup(stop_feed)
        client = self.client()
        handle = client.subscribe_market('us_equity')
        handle.wait_applied(5)
        deadline = time.monotonic() + 5
        entries = []
        while time.monotonic() < deadline:
            entries, offset, revision = [], 0, None
            try:
                while True:
                    page = handle.list_issues(scope='upstream', offset=offset, limit=512,
                                              upstream_revision=revision)
                    revision = page['upstream_revision']
                    entries.extend(page['entries'])
                    if page['done']:
                        break
                    offset = page['next_offset']
                if all(row['state'] in ('accepted', 'unavailable') for row in entries):
                    break
            except RtdataError as exc:
                if 'upstream_changed' not in str(exc):
                    raise
            time.sleep(.02)
        self.assertEqual(sum(row['state'] == 'accepted' for row in entries), 1500)
        self.assertEqual([row['reason'] for row in entries if row['state'] == 'unavailable'], ['not_planned'])
        self.assertFalse(page['acceptance_is_data'])
        client.subscribe_market('fx').wait_applied(5)

        def wait_feed(state):
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                rows = client.market_feed_status['markets']
                if len(rows) == 2 and all(row['state'] == state for row in rows):
                    return
                time.sleep(.02)
            self.fail(f'actual source transport state mismatch: {client.market_feed_status}')

        wait_feed('normal')
        # Exercise the real UDS disconnect callback, not only cache injection.
        stopped.set()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        wait_feed('paused')
        self.assertTrue(client.is_connected)
        self.assertEqual(handle.status()['state'], 'applied')
        stopped.clear()
        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        wait_feed('pending')  # Negotiated/accepted again is not first data.
        self.command('quotes', 'MARKET_TEST_QUOTES')
        wait_feed('normal')

    def test_seven_full_field_sources_and_hot_addition(self):
        client = self.client()
        seen = {}
        client.on_quote(lambda quote: seen.update({quote.symbol: quote}))
        client.subscribe_markets(MARKETS).wait_applied(5)
        self.server.stdin.write('quotes\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + 5
        while not all(f'TEST{i}' in seen for i in range(1, 8)) and time.monotonic() < deadline:
            time.sleep(.01)
        for source in range(1, 8):
            quote = seen[f'TEST{source}']
            self.assertEqual(quote.source_id, source)
            self.assertTrue(quote.fields)
        self.rows.append(dict(symbol='NEWUS', symbol_id=2000, stock_id='9007199254740993',
                              source_market=11, instrument_type=3, active=True, legacy=False))
        self.write_registry()
        self.server.stdin.write('reload\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + 5
        while client._market_code(2000) is None and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertEqual(client._market_code(2000), 'NEWUS')
        self.server.stdin.write('quotes\n')
        self.server.stdin.flush()
        deadline = time.monotonic() + 5
        while 'NEWUS' not in seen and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertEqual(seen['NEWUS'].source_id, 4)


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER'), '需要隔离测试服务程序')
@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER'), '需隔离测试服务')
class MarketPermissionLoopbackTests(unittest.TestCase):
    full_fields = True
    setUp = MarketLoopbackTests.setUp
    write_registry = MarketLoopbackTests.write_registry
    stop_server = MarketLoopbackTests.stop_server
    client = MarketLoopbackTests.client
    command = MarketLoopbackTests.command
    stats = MarketLoopbackTests.stats
    assert_clean = MarketLoopbackTests.assert_clean

    def initial_rows(self):
        rows = MarketLoopbackTests.initial_rows(self)
        rows.extend(dict(symbol=f'CN{market}', symbol_id=2000 + market, stock_id=str(20000 + market),
                         source_market=market, instrument_type=3, active=True, legacy=False)
                    for market in (31, 38))
        return rows

    def test_seven_permission_sets_preserve_committed_membership_on_rejection(self):
        for category in MARKETS:
            with self.subTest(category=category):
                client = self.client('isolated-market-' + category)
                seen = set()
                client.on_quote(lambda quote: seen.add(quote.symbol))
                result = client.subscribe_market(category).wait_applied(10)
                expected = 1501 if category == 'us_equity' else 3 if category == 'cn_equity' else 1
                self.assertEqual(result['admitted_count'], expected)
                committed = client.get_subscribed_symbols()
                deadline = time.monotonic() + 5
                while not seen and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(seen, 'authorized category did not deliver data')
                for denied in set(MARKETS) - {category}:
                    with self.assertRaisesRegex(RtdataError, 'market_permission'):
                        client.subscribe_market(denied).wait_applied(5)
                    self.assertEqual(set(client.get_subscribed_symbols()), set(committed))
                    self.assertEqual(client._desired_markets, {category})
                    self.assertTrue(client.is_connected)
                self.assertLessEqual(seen, set(committed))
                client.close()
                self.assert_clean()

    def test_partial_exchange_grant_does_not_silently_truncate_category(self):
        client = self.client('isolated-market-cn-partial')
        client.subscribe(['TEST1']).wait_applied(5)
        with self.assertRaisesRegex(RtdataError, 'market_permission'):
            client.subscribe_market('cn_equity').wait_applied(5)
        self.assertEqual(set(client.get_subscribed_symbols()), {'TEST1'})
        self.assertFalse(client._desired_markets)

    def test_empty_permission_set_cannot_subscribe(self):
        client = self.client('isolated-market-none')
        for category in MARKETS:
            with self.assertRaisesRegex(RtdataError, 'market_permission'):
                client.subscribe_market(category).wait_applied(5)
        self.assertFalse(client.get_subscribed_symbols())

    def test_invalid_disabled_expired_and_duplicate_login(self):
        for name, reason in [('missing', 'Invalid token'), ('disabled', 'disabled'), ('expired', 'expired')]:
            with self.subTest(name=name), self.assertRaisesRegex(AuthenticationError, reason):
                self.client('isolated-market-' + name)
        first = self.client()
        first.subscribe_market('fx').wait_applied(5)
        with self.assertRaisesRegex(AuthenticationError, 'already in use'):
            self.client()
        self.assertTrue(first.is_connected)
        self.assertEqual(set(first.get_subscribed_symbols()), {'TEST5'})

    def test_tiers_obey_quota_and_failed_add_preserves_previous(self):
        for name in ('web', 'limited', 'vip'):
            with self.subTest(tier=name):
                client = self.client('isolated-market-' + name)
                client.subscribe_market('fx').wait_applied(5)
                with self.assertRaisesRegex(RtdataError, 'symbol_limit'):
                    client.subscribe_market('us_equity').wait_applied(5)
                self.assertEqual(set(client.get_subscribed_symbols()), {'TEST5'})
                self.assertEqual(client._desired_markets, {'fx'})
                client.close()
                self.assert_clean()

    def assert_policy_closes(self, command):
        client = self.client()
        client.subscribe_market('us_equity').wait_applied(5)
        self.command(command, 'MARKET_TEST_POLICY_CHANGED')
        deadline = time.monotonic() + 5
        while client.market_feed_status['state'] != 'disconnected' and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertFalse(client.is_connected)
        self.assertFalse(client._market_identities)
        self.assertEqual(client.market_feed_status['state'], 'disconnected')
        self.assert_clean()

    def test_runtime_disable(self):
        self.assert_policy_closes('disable')

    def test_runtime_expire(self):
        self.assert_policy_closes('expire')

    def test_runtime_remove(self):
        self.assert_policy_closes('remove')

    def test_runtime_quota_reduction(self):
        self.assert_policy_closes('limit')


class MarketRaceLoopbackTests(unittest.TestCase):
    full_fields = False
    setUp = MarketLoopbackTests.setUp
    initial_rows = MarketLoopbackTests.initial_rows
    write_registry = MarketLoopbackTests.write_registry
    stop_server = MarketLoopbackTests.stop_server
    client = MarketLoopbackTests.client
    command = MarketLoopbackTests.command
    stats = MarketLoopbackTests.stats
    assert_clean = MarketLoopbackTests.assert_clean

    def test_final_ack_is_route_barrier(self):
        peer = RawMarketPeer(self)
        final = peer.final_identity()
        self.assertEqual(self.stats()['subscriptions'], 0)
        peer.ack(final)
        self.assertEqual(peer.receive_control()['admitted_count'], 1501)
        self.assertEqual(self.stats()['subscriptions'], 1501)
        peer.socket.close()
        self.assert_clean()

    def test_catalog_changes_before_final_ack(self):
        peer = RawMarketPeer(self)
        final = peer.final_identity()
        self.assertEqual(self.stats()['subscriptions'], 0)
        self.rows[-1]['active'] = False
        self.write_registry()
        self.command('reload', 'MARKET_TEST_RELOADED')
        # Let the rejected transaction settle before delivering its late ACK.
        rejected = peer.receive_control()
        self.assertEqual(rejected['reason'], 'catalog_changed')
        peer.ack(final)
        self.assertEqual(peer.receive_control()['reason'], 'invalid_identity_ack')
        self.assertEqual(self.stats()['subscriptions'], 0)
        final = peer.final_identity(3)
        peer.ack(final)
        self.assertEqual(peer.receive_control()['admitted_count'], 1500)
        peer.socket.close()
        self.assert_clean()

    def test_policy_revocation_before_final_ack(self):
        peer = RawMarketPeer(self)
        peer.final_identity()
        self.assertEqual(self.stats()['subscriptions'], 0)
        self.command('revoke', 'MARKET_TEST_REVOKED')
        with self.assertRaises((EOFError, ConnectionResetError)):
            peer.receive()
        self.assert_clean()

    def test_identity_replacement_before_final_ack(self):
        peer = RawMarketPeer(self)
        peer.final_identity()
        self.command('replace-identities', 'MARKET_TEST_IDENTITIES_REPLACED')
        with self.assertRaises((EOFError, ConnectionResetError)):
            peer.receive()
        self.assert_clean()

    def test_disconnect_before_final_ack(self):
        peer = RawMarketPeer(self)
        peer.final_identity()
        self.assertEqual(self.stats()['subscriptions'], 0)
        peer.socket.close()
        self.assert_clean()

    def test_repeated_multi_client_subscribe_and_clear(self):
        slow = RawMarketPeer(self, 'isolated-market-slow')
        slow.final_identity()
        clients = [self.client(f'isolated-market-peer-{i}') for i in range(3)]
        latencies = []
        for _ in range(8):
            started = time.monotonic()
            handles = [client.subscribe_market('us_equity') for client in clients]
            for handle in handles:
                self.assertEqual(handle.wait_applied(10)['admitted_count'], 1501)
                latencies.append(time.monotonic() - started)
            self.assertEqual(self.stats()['subscriptions'], 4503)
            handles = [client.unsubscribe() for client in clients]
            for handle in handles:
                self.assertEqual(handle.wait_applied(10)['admitted_count'], 0)
            self.assertEqual(self.stats()['subscriptions'], 0)
        for client in clients:
            client.close()
        slow.socket.close()
        stats = self.assert_clean()
        print(f'market_multi clients=3 stalled_peers=1 cycles=8 max_applied_seconds={max(latencies):.3f}'
              f' cleanup={json.dumps(stats, sort_keys=True)}')


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER') and
                     os.environ.get('RTDATA_MARKET_SCALE_TEST') == '1', '需显式启用有界容量测试')
class MarketSlowConsumerLoopbackTests(unittest.TestCase):
    full_fields = True
    setUp = MarketLoopbackTests.setUp
    initial_rows = MarketLoopbackTests.initial_rows
    write_registry = MarketLoopbackTests.write_registry
    stop_server = MarketLoopbackTests.stop_server
    client = MarketLoopbackTests.client
    command = MarketLoopbackTests.command
    stats = MarketLoopbackTests.stats
    assert_clean = MarketLoopbackTests.assert_clean

    def test_full_fields_slow_reader_does_not_block_healthy_peer(self):
        slow = self.client('isolated-market-slow')
        healthy = self.client()
        slow.subscribe_market('us_equity').wait_applied(10)
        healthy.subscribe_market('us_equity').wait_applied(10)
        self.command('bound-slow-socket', 'MARKET_TEST_SLOW_BOUNDED')
        blocked, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def block_reader(quote):
            blocked.set()
            release.wait(20)

        slow._conn._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        slow.on_quote(block_reader)
        seen = []
        healthy.on_quote(lambda quote: seen.append(quote.trade_time))
        self.command('quotes', 'MARKET_TEST_QUOTES')
        self.assertTrue(blocked.wait(3))
        peak_queued = 0
        observed_stall = False
        for batch in range(80):
            self.command('quotes', 'MARKET_TEST_QUOTES')
            stats = self.stats()
            peak_queued = max(peak_queued, stats['queued_bytes'])
            observed_stall |= stats['stalled_flushes'] > 0
            self.assertLessEqual(stats['subscription_state_reserved_bytes'], 64 * 1024 * 1024)
            self.assertLessEqual(stats['queue_metadata_reserved_bytes'], 16 * 1024 * 1024)
            if batch >= 15 and observed_stall:
                break
        self.assertTrue(observed_stall, 'slow reader never reached socket backpressure')
        self.command('quotes', 'MARKET_TEST_QUOTES')
        expected_time = 1794290082000 + batch + 4
        deadline = time.monotonic() + 5
        while (not seen or max(seen) < expected_time) and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(seen)
        self.assertEqual(max(seen), expected_time)
        self.assertLessEqual(peak_queued, 2 * 256 * 1024)
        self.assertTrue(healthy.is_connected)
        self.assertEqual(healthy.unsubscribe().wait_applied(10)['admitted_count'], 0)
        release.set()
        slow.close()
        healthy.close()
        cleanup = self.assert_clean()
        print(f'market_slow_reader full_fields=1 symbols=1501 batches={batch + 3}'
              f' peak_queued_bytes={peak_queued} cleanup={json.dumps(cleanup, sort_keys=True)}')


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER') and
                     os.environ.get('RTDATA_MARKET_SOAK_TEST') == '1', '需显式启用有界持续测试')
class MarketSoakLoopbackTests(unittest.TestCase):
    full_fields = True
    setUp = MarketLoopbackTests.setUp
    write_registry = MarketLoopbackTests.write_registry
    stop_server = MarketLoopbackTests.stop_server
    client = MarketLoopbackTests.client
    command = MarketLoopbackTests.command
    stats = MarketLoopbackTests.stats
    assert_clean = MarketLoopbackTests.assert_clean

    def initial_rows(self):
        self.duration = int(os.environ.get('RTDATA_MARKET_SOAK_SECONDS', '180'))
        self.assertTrue(30 <= self.duration <= 1800)
        count = int(os.environ.get('RTDATA_MARKET_SOAK_SYMBOLS', '10000'))
        self.assertIn(count, (1507, 10000, 50000))
        rows = MarketLoopbackTests.initial_rows(self)
        rows.extend(dict(symbol=f'US{i:05}', symbol_id=i, stock_id=str(10000 + i),
                         source_market=11, instrument_type=3, active=True, legacy=False)
                    for i in range(1508, count + 1))
        return rows

    def test_three_clients_supply_churn_and_resource_plateau(self):
        clients = [self.client(f'isolated-market-peer-{i}') for i in range(3)]
        latest = [0] * len(clients)
        for index, client in enumerate(clients):
            def observe(quote, index=index):
                latest[index] = max(latest[index], quote.trade_time or 0)
            client.on_quote(observe)

        def wait_states(expected):
            start = time.monotonic()
            while time.monotonic() - start < 8:
                summaries = [{row['category']: row['state'] for row in client.market_feed_status['markets']}
                             for client in clients]
                if all(summary == expected for summary in summaries):
                    return time.monotonic() - start
                self.assertTrue(all(client.is_connected for client in clients))
                time.sleep(.02)
            debug = self.command('feed-debug', 'MARKET_TEST_FEED_DEBUG=')
            self.log.seek(0)
            log_tail = self.log.read()[-10000:]
            self.fail(f'supply summaries did not converge: {summaries}; server={debug}; log={log_tail}')

        applied, feed_delays, delivery_delays = [], [], []
        count = len(self.rows)
        handles = [client.subscribe_markets(MARKETS) for client in clients]
        for handle in handles:
            self.assertEqual(handle.wait_applied(20)['admitted_count'], count)
        wait_states(dict.fromkeys(MARKETS, 'normal'))
        baseline = self.stats()
        started = time.monotonic()
        samples, cycles, batches = [], 0, 0
        while time.monotonic() - started < self.duration:
            self.command('supply-backup', 'MARKET_TEST_BACKUP')
            self.command('supply-down', 'MARKET_TEST_DOWN')
            expected = dict.fromkeys(MARKETS, 'normal')
            expected.update(us_equity='partial', fx='paused', overseas_futures='paused', crypto='paused')
            feed_delays.append(wait_states(expected))
            self.command('quotes', 'MARKET_TEST_QUOTES')
            time.sleep(.6)
            self.command('quotes', 'MARKET_TEST_QUOTES')
            feed_delays.append(wait_states(dict.fromkeys(MARKETS, 'normal')))
            # Two clients keep receiving while the third changes membership.
            self.assertEqual(clients[2].unsubscribe().wait_applied(10)['admitted_count'], 0)
            tick = time.monotonic()
            self.assertEqual(clients[2].subscribe_markets(MARKETS).wait_applied(20)['admitted_count'], count)
            applied.append(time.monotonic() - tick)
            feed_delays.append(wait_states(dict.fromkeys(MARKETS, 'normal')))
            for _ in range(5):
                sent_ms = int(time.time() * 1000)
                tick = time.monotonic()
                self.command('quotes', 'MARKET_TEST_QUOTES')
                deadline = tick + 5
                while min(latest) < sent_ms and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertGreaterEqual(min(latest), sent_ms, 'a healthy peer stopped receiving current data')
                delivery_delays.append(time.monotonic() - tick)
                batches += 1
                time.sleep(.2)
            stats = self.stats()
            self.assertEqual(stats['subscriptions'], count * len(clients))
            self.assertLessEqual(stats['subscription_state_reserved_bytes'], 64 * 1024 * 1024)
            self.assertLessEqual(stats['queue_metadata_reserved_bytes'], 16 * 1024 * 1024)
            samples.append(stats['rss_kib'])
            cycles += 1
        elapsed = time.monotonic() - started
        end = self.stats()
        self.assertGreaterEqual(cycles, 2)
        # A bounded regression gate, not a proof against all possible leaks.
        growth = max(samples[-3:]) - max(samples[:3]) if cycles >= 6 else None
        if growth is not None:
            self.assertLess(growth, 48 * 1024, f'post-warmup RSS growth: {samples}')
        for client in clients:
            client.unsubscribe().wait_applied(10)
            client.close()
        cleanup = self.assert_clean()
        print('market_soak=' + json.dumps(dict(
            seconds=round(elapsed, 3), clients=3, symbols_per_client=count, cycles=cycles, batches=batches,
            max_applied_seconds=round(max(applied), 3), max_feed_seconds=round(max(feed_delays), 3),
            max_delivery_seconds=round(max(delivery_delays), 3), rss_samples_kib=samples,
            rss_growth_kib=growth, cloud_cpu_percent=round(100 * (end['cpu_seconds'] - baseline['cpu_seconds']) / elapsed, 2),
            cleanup=cleanup), sort_keys=True))


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER') and
                     os.environ.get('RTDATA_MARKET_COMBINATION_TEST') == '1', 'explicit bounded combination soak')
class MarketCombinationSoakTests(unittest.TestCase):
    full_fields = True
    setUp = MarketLoopbackTests.setUp
    write_registry = MarketLoopbackTests.write_registry
    stop_server = MarketLoopbackTests.stop_server
    command = MarketLoopbackTests.command
    stats = MarketLoopbackTests.stats
    assert_clean = MarketLoopbackTests.assert_clean

    def initial_rows(self):
        self.duration = int(os.environ.get('RTDATA_MARKET_COMBINATION_SECONDS', '120'))
        self.assertIn(self.duration, (30, 120, 300, 600))
        rows = MarketLoopbackTests.initial_rows(self)
        rows.extend(dict(symbol=f'US{i:05}', symbol_id=i, stock_id=str(10000 + i),
                         source_market=11, instrument_type=3, active=True, legacy=False)
                    for i in range(1508, 10001))
        return rows

    def test_overlapping_combinations_identity_barrier_and_delivery(self):
        clients = []
        for index in range(3):
            client = RtdataClient(f'isolated-market-peer-{index}', host='127.0.0.1', port=self.port,
                async_callbacks=False, auto_reconnect=False, realtime_protocol='required_v2',
                realtime_numeric=os.environ.get('RTDATA_MARKET_TEST_NUMERIC') == '1',
                history_v2_advertise=False, symbol_cache_dir=self.folder.name, history_cache_enabled=False)
            self.addCleanup(client.close)
            client.connect(timeout=5)
            clients.append(client)
        ids = {market: ({index + 1} if market != 'us_equity' else {4, *range(8, 10001)})
               for index, market in enumerate(MARKETS)}

        def members(markets):
            return set().union(*(ids[market] for market in markets))

        all_ids = members(MARKETS)
        for client in clients:
            self.assertEqual(client.subscribe_markets(MARKETS).wait_applied(20)['admitted_count'], 10000)
        lock = threading.Lock()
        expected_time, counts, failures = [1794290082001], [{}, {}, {}], []
        codes = {row['symbol_id']: row['symbol'] for row in self.rows}
        sources = {sid: index + 1 for index, market in enumerate(MARKETS) for sid in ids[market]}

        def observe(index, quote):
            with lock:
                if quote.trade_time != expected_time[0]:
                    if quote.trade_time > expected_time[0] and len(failures) < 8:
                        failures.append('unexpected_publication_time')
                    return  # Cache replay during a membership barrier is not a new publication.
                seen = counts[index]
                seen[quote.symbol_id] = seen.get(quote.symbol_id, 0) + 1
                if (quote.wire_version != 2 or quote.symbol != codes.get(quote.symbol_id)
                        or quote.source_id != sources.get(quote.symbol_id)) and len(failures) < 8:
                    failures.append('identity_or_wire_version')

        for index, client in enumerate(clients):
            client.on_quote(lambda quote, index=index: observe(index, quote))
        combinations = [(market,) for market in MARKETS] + [
            ('us_equity', 'cn_equity'), ('cn_equity', 'hk_equity'),
            ('us_equity', 'fx', 'crypto'), ('futures', 'overseas_futures'), tuple(MARKETS)]
        selected = set(MARKETS)
        cycles, batches, applied, delivery, rss = 0, 0, [], [], []
        started = time.monotonic()
        while time.monotonic() - started < self.duration or cycles < len(combinations):
            target = set(combinations[cycles % len(combinations)])
            tick = time.monotonic()
            # Add before removing to exercise overlapping old/new group ownership.
            self.assertEqual(clients[2].subscribe_markets(reversed(sorted(target))).wait_applied(20)
                             ['admitted_count'], len(members(selected | target)))
            for market in sorted(selected - target):
                selected.remove(market)
                self.assertEqual(clients[2].unsubscribe_market(market).wait_applied(20)['admitted_count'],
                                 len(members(selected | target)))
            selected = target
            applied.append(time.monotonic() - tick)
            wanted = [all_ids, all_ids, members(target)]
            self.assertEqual(clients[2]._desired_markets, target)
            self.assertEqual(set(clients[2]._market_identities), wanted[2])
            for _ in range(2):
                batches += 1
                with lock:
                    expected_time[0] = 1794290082001 + batches
                    counts[:] = [{}, {}, {}]
                tick = time.monotonic()
                self.command('quotes-paced', 'MARKET_TEST_QUOTES')
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    with lock:
                        complete = all(set(seen) == desired for seen, desired in zip(counts, wanted))
                    if complete:
                        break
                    self.assertTrue(all(client.is_connected for client in clients))
                    time.sleep(.01)
                time.sleep(.03)
                with lock:
                    self.assertFalse(failures)
                    for index, (seen, desired) in enumerate(zip(counts, wanted)):
                        missing, extra = desired - set(seen), set(seen) - desired
                        if missing or extra:
                            print('market_combination_failure=' + json.dumps(dict(cycle=cycles, batch=batches,
                                client=index, target=sorted(target), expected=len(desired), received=len(seen),
                                missing=sorted(missing)[:16], extra=sorted(extra)[:16], stats=self.stats(),
                                realtime=[dict(client.realtime_statistics) for client in clients],
                                feed=[client.market_feed_status for client in clients]), sort_keys=True))
                        self.assertFalse(bool(missing or extra), f'client={index} batch={batches} '
                                         f'missing={len(missing)} extra={len(extra)}')
                        self.assertTrue(all(n == 1 for n in seen.values()), 'duplicate publication')
                delivery.append(time.monotonic() - tick)
            stats = self.stats()
            self.assertEqual(stats['subscriptions'], 20000 + len(wanted[2]))
            self.assertLessEqual(stats['subscription_state_reserved_bytes'], 64 * 1024 * 1024)
            self.assertLessEqual(stats['queue_metadata_reserved_bytes'], 16 * 1024 * 1024)
            self.assertLessEqual(os.fstat(self.log.fileno()).st_size, 4 * 1024 * 1024)
            for line in stats['realtime_accounting'].splitlines():
                self.assertIn('stream_gap=0 ', line)
                self.assertIn('stream_stale=0 ', line)
            rss.append(stats['rss_kib'])
            cycles += 1
            time.sleep(.1)
        elapsed = time.monotonic() - started
        growth = max(rss[-3:]) - max(rss[3:6]) if cycles >= 9 else None
        if growth is not None:
            self.assertLess(growth, 48 * 1024)
        block_statistics = [dict(client.realtime_statistics) for client in clients]
        if os.environ.get('RTDATA_MARKET_TEST_NUMERIC') == '1':
            self.assertTrue(all(row['numeric_blocks'] > 0 for row in block_statistics))
        for client in clients:
            client.unsubscribe().wait_applied(20)
            client.close()
        cleanup = self.assert_clean()
        print('market_combination_soak=' + json.dumps(dict(seconds=round(elapsed, 3), clients=3,
            symbols=10000, combinations=len(combinations), cycles=cycles, batches=batches,
            max_applied_seconds=round(max(applied), 3), max_batch_delivery_seconds=round(max(delivery), 3),
            rss_growth_kib=growth, rss_samples_kib=rss, block_statistics=block_statistics,
            cleanup=cleanup, initial_cache_replay=True, publication_rate=3000), sort_keys=True))


@unittest.skipUnless(os.environ.get('RTDATA_MARKET_TEST_SERVER') and
                     os.environ.get('RTDATA_MARKET_SCALE_TEST') == '1', '需显式启用有界容量测试')
class MarketScaleLoopbackTests(unittest.TestCase):
    full_fields = False
    setUp = MarketLoopbackTests.setUp
    write_registry = MarketLoopbackTests.write_registry
    stop_server = MarketLoopbackTests.stop_server
    client = MarketLoopbackTests.client
    command = MarketLoopbackTests.command
    stats = MarketLoopbackTests.stats
    assert_clean = MarketLoopbackTests.assert_clean

    def initial_rows(self):
        self.scale_count = int(os.environ.get('RTDATA_MARKET_SCALE_COUNT', '70000'))
        self.assertIn(self.scale_count, (70000, 100000, 200000, 250000))
        # Clean startup avoids consuming IDs with an unrelated small registry.
        return [dict(symbol=f'US{i:06}', symbol_id=100000 + i, stock_id=str(900000 + i),
                     source_market=11, instrument_type=3, active=True, legacy=False)
                for i in range(self.scale_count)]

    def stalled_peer(self):
        slow = socket.create_connection(('127.0.0.1', self.port), timeout=5)
        self.addCleanup(slow.close)
        slow.sendall(proto.encode_auth('isolated-market-slow'))

        def exact(size):
            data = bytearray()
            while len(data) < size:
                chunk = slow.recv(size - len(data))
                self.assertTrue(chunk)
                data.extend(chunk)
            return bytes(data)

        def receive():
            length, symbol, kind = proto.decode_header(exact(10))
            self.assertLessEqual(length, 65536)
            return kind, exact(length)

        receive()
        slow.sendall(proto.build_message(REQUEST, 0, json.dumps(dict(
            version=1, request_id=1, op='hello', symbol_capacity=250000)).encode()))
        self.assertEqual(json.loads(receive()[1])['state'], 'negotiated')
        slow.sendall(proto.build_message(REQUEST, 0, json.dumps(dict(
            version=1, request_id=2, op='subscribe', markets=['us_equity'])).encode()))
        while receive()[0] != IDENTITIES:
            pass
        # This peer deliberately never acknowledges its first identity page.
        return slow

    def test_large_market_with_stalled_identity_peer(self):
        import resource
        stalled = os.environ.get('RTDATA_MARKET_STALLED_PEER', '1')
        self.assertIn(stalled, ('0', '1'))
        slow = self.stalled_peer() if stalled == '1' else None
        client = self.client()
        started = time.monotonic()
        handle = client.subscribe_market('us_equity')
        # The default shared 64 MiB budget cannot reserve 250k routing entries
        # plus transactional state. Rejection is a measured boundary, not success.
        if self.scale_count == 250000:
            with self.assertRaisesRegex(RtdataError, 'subscription_state_memory_budget'):
                handle.wait_applied(25)
            self.assertFalse(client._market_identities)
            self.assertFalse(client._desired_markets)
            outcome = 'resource_rejected'
        else:
            result = handle.wait_applied(25)
            self.assertEqual(result['admitted_count'], self.scale_count)
            self.assertEqual(len(client.get_subscribed_symbols()), self.scale_count)
            outcome = 'applied'
        elapsed = time.monotonic() - started
        if outcome == 'applied':
            deadline = time.monotonic() + 10
            while client.market_feed_status['state'] != 'feed_status' and time.monotonic() < deadline:
                time.sleep(.02)
            rows = client.market_feed_status['markets']
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['admitted_count'], self.scale_count)
            # This fixture is legacy-only: do not invent proven supply from ACKs.
            self.assertEqual(rows[0]['state'], 'pending')
            self.assertEqual(rows[0]['unknown_count'], self.scale_count)
        print(f'market_tcp symbols={self.scale_count} stalled_peers={stalled} outcome={outcome}'
              f' operation_seconds={elapsed:.3f}'
              f' sdk_peak_rss_kib={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}')
        client.unsubscribe().wait_applied(10)
        self.assertEqual(self.stats()['subscriptions'], 0)
        if outcome == 'applied':
            # Full clear must free retained routing capacity, so the same market
            # can be admitted again without closing the connection.
            self.assertEqual(client.subscribe_market('us_equity').wait_applied(25)['admitted_count'], self.scale_count)
            self.assertEqual(client.subscribe_market('us_equity').wait_applied(10)['admitted_count'], self.scale_count)
            self.assertEqual(client.unsubscribe().wait_applied(10)['admitted_count'], 0)
        client.close()
        if slow:
            slow.close()
        stats = self.assert_clean()
        print('market_tcp_cleanup=' + json.dumps(stats, sort_keys=True))
