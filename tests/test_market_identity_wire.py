import os
from pathlib import Path
import struct
import unittest
from unittest.mock import patch

from rtdata import _market_identity_wire as wire
from rtdata._market_subscription import BINARY_IDENTITIES
import test_market_subscription as market_tests


def frame(rows, *, rid=2, generation=1, offset=0, total=None, codec=0):
    total = offset + len(rows) if total is None else total
    body = b'r1'
    for action, sid in rows:
        body += struct.pack('<BI', action, sid)
        if action == 1:
            body += struct.pack('<BBIIHHH', 1, 1, 1, 3, 9, 2, 0) + b'600000.SHSH'
    raw_size = len(body)
    if codec:
        import zstandard
        body = zstandard.ZstdCompressor(level=1, write_checksum=True).compress(body)
    return wire.HEADER.pack(b'MID1', codec, int(offset + len(rows) == total), 64,
                            raw_size, len(body), rid, generation, 7, offset,
                            offset + len(rows), total, len(rows), 2, 0) + body


class IdentityWireTests(unittest.TestCase):
    def test_raw_and_bounds(self):
        data = frame([(1, 42)])
        decoded = wire.decode(data)
        self.assertEqual(len(data), 98)
        self.assertEqual(decoded['entries'][0]['code'], '600000.SH')
        for size in range(len(data)):
            with self.assertRaises(ValueError):
                wire.decode(data[:size])
        for offset in (0, 4, 5, 6, 56, 66, 71, 72):
            bad = bytearray(data)
            bad[offset] = 255
            with self.assertRaises(ValueError):
                wire.decode(bad)
        with self.assertRaises(ValueError):
            wire.decode(data + b'x')

    @unittest.skipUnless(wire.available(), 'optional zstandard unavailable')
    def test_zstd_integrity_and_negotiation(self):
        data = frame([(1, 42)], codec=1)
        self.assertEqual(wire.decode(data, True)['entries'][0]['symbol_id'], 42)
        with self.assertRaises(ValueError):
            wire.decode(data)
        corrupt = bytearray(data)
        corrupt[-1] ^= 1
        with self.assertRaises(ValueError):
            wire.decode(corrupt, True)
        duplicate = bytearray(data + data[64:])
        struct.pack_into('<I', duplicate, 12, len(duplicate) - 64)
        with self.assertRaises(ValueError):
            wire.decode(duplicate, True)

    @unittest.skipUnless(os.environ.get('RTDATA_IDENTITY_GOLDEN'), 'C++ golden path not provided')
    def test_cpp_golden(self):
        self.assertEqual(Path(os.environ['RTDATA_IDENTITY_GOLDEN']).read_bytes(), frame([(1, 42)]))


class BinaryClientTests(unittest.TestCase):
    client = market_tests.MarketSubscriptionTests.client
    respond = market_tests.MarketSubscriptionTests.respond
    def binary_client(self):
        client = self.client()
        client._market_binary = True
        client._market_epoch = 7
        return client

    def deliver(self, client, data):
        client._handle_market_control(BINARY_IDENTITIES, 0, data, 1, client._conn)

    def applied(self, client, rid, gen, count, cache=1):
        self.respond(client, request_id=rid, generation=gen, state='applied',
                     admitted_count=count, catalog_revision='r1', cache_generation=cache, cache_epoch=7, cache_revision='r1')

    def test_cache_is_not_membership(self):
        client = self.binary_client()
        first = client.subscribe_market('cn_equity')
        self.deliver(client, frame([(1, 42)]))
        self.assertIsNone(client._market_code(42))
        self.applied(client, 2, 1, 1)
        first.wait_applied(.1)
        clear = client.unsubscribe_market('cn_equity')
        self.deliver(client, frame([(0, 42)], rid=3, generation=2))
        self.applied(client, 3, 2, 0)
        clear.wait_applied(.1)
        self.assertIsNone(client._market_code(42))
        self.assertIn(42, client._market_cache)
        again = client.subscribe_market('cn_equity')
        self.deliver(client, frame([(2, 42)], rid=4, generation=3))
        self.assertIsNone(client._market_code(42))
        self.applied(client, 4, 3, 1, cache=3)
        again.wait_applied(.1)
        self.assertEqual(client._market_code(42), '600000.SH')
        client._reset_markets()
        self.assertFalse(client._market_cache)

    def test_missing_reference_is_not_acked(self):
        client = self.binary_client()
        client.subscribe_market('cn_equity')
        before = len(client._conn.sent)
        self.deliver(client, frame([(2, 42)]))
        self.assertEqual(len(client._conn.sent), before)
        self.assertEqual(client.market_subscription_status['state'], 'disconnected')

    def test_replay_credit_is_ordered_and_does_not_extend_transaction(self):
        client = self.binary_client()
        client._market_replay_credit = True
        handle = client.subscribe_market('cn_equity')
        deadline = handle._deadline
        self.respond(client, state='replay_checkpoint', request_id=0, checkpoint=1)
        self.assertEqual(client._market_replay_checkpoint, 1)
        self.assertEqual(handle._deadline, deadline)
        before = len(client._conn.sent)
        self.respond(client, state='replay_checkpoint', request_id=0, checkpoint=1)
        self.assertEqual(len(client._conn.sent), before)
        self.assertEqual(client.market_subscription_status['state'], 'disconnected')

    def test_replay_credit_requires_negotiation(self):
        client = self.binary_client()
        before = len(client._conn.sent)
        self.respond(client, state='replay_checkpoint', request_id=0, checkpoint=1)
        self.assertEqual(len(client._conn.sent), before)
        self.assertEqual(client.market_subscription_status['state'], 'disconnected')

    def test_progress_has_fixed_total_limit(self):
        client = self.binary_client()
        with patch('rtdata._market_subscription.time.monotonic', return_value=100):
            handle = client.subscribe_market('cn_equity')
        with patch('rtdata._market_subscription.time.monotonic', return_value=120):
            self.deliver(client, frame([(1, 42)], total=2))
        self.assertEqual(handle._deadline, 155)
        self.assertEqual(client._market_hard_deadline, 225)
        with patch('rtdata._market_subscription.time.monotonic', return_value=154):
            self.deliver(client, frame([(1, 43)], offset=1, total=2))
        # Duplicate code is invalid; receiving bytes is not verified progress.
        self.assertEqual(client.market_subscription_status['state'], 'disconnected')
