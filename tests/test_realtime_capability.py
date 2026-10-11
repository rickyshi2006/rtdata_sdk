import struct
import unittest
from dataclasses import replace

from rtdata import _realtime_capability as c
from rtdata import _protocol as framing


class RealtimeCapabilityTests(unittest.TestCase):
    def test_message_registration_and_unchanged_outer_frame(self):
        names = ('REALTIME_CAPABILITY_OFFER', 'REALTIME_CAPABILITY_ACK', 'REALTIME_CAPABILITY_COMMIT',
                 'REALTIME_SNAPSHOT_V2', 'REALTIME_SOURCE_STATUS')
        for kind, name in enumerate(names, c.OFFER):
            self.assertEqual(getattr(framing.MsgType, name), kind)
            symbol = 123 if kind == c.SNAPSHOT else 0
            self.assertEqual(framing.build_message(kind, symbol, b'abc'), struct.pack('!IIH', 3, symbol, kind) + b'abc')

    def pair(self, role=c.Role.SDK, mode='required_v2', mask=31):
        a = c.Session(role, c.Role.CLOUD, mode, mask)
        b = c.Session(c.Role.CLOUD, role, 'auto', mask)
        a.reset(1, True)
        b.reset(1, True)
        return a, b

    def transfer(self, a, b, action, now=1):
        self.assertIsNotNone(action)
        a.sent(1, action, True, now)
        return b.receive(1, action.type, action.payload, now)

    def activate(self, a, b):
        offer = a.begin(1, 9, 0)
        ack = self.transfer(a, b, offer)
        self.assertEqual(b.phase, 'ack_pending')
        commit = self.transfer(b, a, ack)
        self.assertEqual(a.phase, 'commit_pending')
        self.assertNotEqual(a.format(1), 'v2')
        self.assertNotEqual(b.format(1), 'v2')
        self.transfer(a, b, commit)
        self.assertEqual((a.phase, b.phase), ('active', 'active'))
        self.assertNotEqual(a.format(1), 'v2')
        return offer, ack, commit

    def test_both_directions_and_readiness(self):
        for role in (c.Role.PRODUCER, c.Role.SDK):
            with self.subTest(role=role):
                a, b = self.pair(role)
                self.activate(a, b)
                producer, consumer = (a, b) if a.producer else (b, a)
                self.assertEqual(a.take_invalidations(), 31)
                self.assertEqual(b.take_invalidations(), 31)
                ready = tuple(c.SourceState(i, True, c.Reason.READY, (1 << 64) - 1) for i in range(1, 6))
                status = producer.publish(1, ready, 2)
                self.assertFalse(producer.admits(1, (1 << 64) - 1, 500))
                self.transfer(producer, consumer, status, 3)
                for session in (producer, consumer):
                    self.assertTrue(session.admits(1, (1 << 64) - 1, 500))
                    self.assertFalse(session.admits(1, 1, 500))
                    self.assertFalse(session.admits(1, (1 << 64) - 1, 16385))
                    self.assertEqual(session.take_invalidations(), 31)
                consumer.receive(1, c.SOURCE_STATUS, status.payload, 4)
                self.assertEqual(consumer.take_invalidations(), 0)
                changed = (replace(ready[0], epoch=1),) + ready[1:]
                self.transfer(producer, consumer, producer.publish(1, changed, 5), 6)
                self.assertTrue(consumer.admits(1, 1, 500))
                self.assertEqual(consumer.take_invalidations(), 1)
                consumer.receive(1, c.SOURCE_STATUS, status.payload, 7)
                self.assertTrue(consumer.admits(1, 1, 500))
                unavailable = (c.SourceState(1, reason=c.Reason.LEGACY_UPSTREAM),) + changed[1:]
                self.transfer(producer, consumer, producer.publish(1, unavailable, 8), 9)
                self.assertEqual(consumer.format(1), 'unavailable' if role == c.Role.SDK else 'v1')
                self.assertEqual(consumer.take_invalidations(), 1)

    def test_old_peers_timeout_and_legacy_default(self):
        legacy = c.Session(c.Role.SDK, c.Role.CLOUD)
        legacy.reset(1, True)
        self.assertIsNone(legacy.begin(1, 1, 0))
        self.assertEqual(legacy.format(1), 'v1')
        for mode in ('auto', 'required_v2'):
            a, b = self.pair(mode=mode)
            ack = self.transfer(a, b, a.begin(1, 1, 0), 0)
            b.sent(1, ack, True, 0)
            self.assertIsNone(a.receive(1, c.ACK, ack.payload, 1000))
            self.assertEqual(a.phase, 'fallback')
            self.assertEqual(a.reason, 'ack_timeout')
            self.assertEqual(a.format(1), 'v1' if mode == 'auto' else 'unavailable')
            b.tick(1, 1000)
            self.assertEqual(b.phase, 'closed')

    def test_generation_nonce_and_auth_gates(self):
        a, b = self.pair()
        offer = a.begin(1, 1, 0)
        ack = self.transfer(a, b, offer, 0)
        b.sent(1, ack, True, 0)
        wrong = replace(c.Capability.decode(ack.payload), nonce=2).encode()
        self.assertIsNone(a.receive(1, c.ACK, wrong, 1))
        self.assertEqual(a.phase, 'waiting_ack')
        a.reset(2, False)
        self.assertIsNone(a.begin(2, 2, 2))
        a.reset(3, True)
        self.assertIsNone(a.receive(1, c.ACK, b'', 3))
        new_offer = a.begin(3, 3, 3)
        a.sent(1, new_offer, False, 3)
        self.assertEqual(a.phase, 'offer_pending')
        with self.assertRaises(ValueError):
            a.reset(3, True)

    def test_intersection_and_negative_ack(self):
        local = c.supported(c.Role.CLOUD)
        offer = c.supported(c.Role.SDK)
        self.assertEqual(c.intersection(local, replace(offer, registry=bytes(32))).sources, 0)
        broken = replace(offer.schemas[1], hash=bytes(32))
        self.assertEqual(c.intersection(local, replace(offer, schemas=(offer.schemas[0], broken,
                                                                      offer.schemas[2]))).sources, 27)
        self.assertEqual(c.intersection(local, c.supported(c.Role.SDK, 4, 512)).sources, 4)
        self.assertEqual(c.intersection(local, c.supported(c.Role.SDK, 4, 512)).max_payload, 512)
        a, b = self.pair()
        a.local = replace(a.local, registry=bytes(32))
        ack = self.transfer(a, b, a.begin(1, 1, 0))
        self.assertIsNone(self.transfer(b, a, ack))
        self.assertEqual((a.phase, b.phase), ('fallback', 'fallback'))

    def test_ack_cannot_expand_offer(self):
        for field in ('sources', 'max_payload', 'registry', 'schemas', 'role'):
            a = c.Session(c.Role.SDK, c.Role.CLOUD, 'auto', 1, 512)
            a.reset(1, True)
            offer = a.begin(1, 1, 0)
            a.sent(1, offer, True, 0)
            ack = c.supported(c.Role.CLOUD, 1, 512)
            changes = {'sources': 3, 'max_payload': 513, 'registry': bytes(32),
                       'schemas': (replace(ack.schemas[0], hash=bytes(32)),), 'role': c.Role.PRODUCER}
            a.receive(1, c.ACK, replace(ack, **{field: changes[field]}).encode(), 1)
            self.assertEqual(a.phase, 'closed', field)

    def test_commit_barrier_and_send_failures(self):
        for failure in ('offer', 'ack', 'commit', 'status'):
            a, b = self.pair()
            offer = a.begin(1, 9, 0)
            if failure == 'offer':
                a.sent(1, offer, False, 1)
                self.assertEqual(a.phase, 'closed')
                continue
            ack = self.transfer(a, b, offer)
            if failure == 'ack':
                b.sent(1, ack, False, 1)
                self.assertEqual(b.phase, 'closed')
                continue
            commit = self.transfer(b, a, ack)
            if failure == 'commit':
                a.sent(1, commit, False, 1)
                self.assertEqual(a.phase, 'closed')
                self.assertEqual(b.phase, 'waiting_commit')
                continue
            self.transfer(a, b, commit)
            status = b.publish(1, tuple(c.SourceState(i) for i in range(1, 6)), 2)
            b.sent(1, status, False, 2)
            self.assertEqual(b.phase, 'closed')
        a, b = self.pair()
        offer = a.begin(1, 9, 0)
        ack = self.transfer(a, b, offer)
        commit = self.transfer(b, a, ack)
        changed = replace(c.Capability.decode(commit.payload), max_payload=1024).encode()
        b.receive(1, c.COMMIT, changed, 2)
        self.assertEqual(b.phase, 'closed')

    def test_status_scope_direction_and_revision(self):
        for fault in ('scope', 'direction', 'malformed'):
            a, b = self.pair()
            self.activate(a, b)
            status = c.Status(9, 1, tuple(c.SourceState(i) for i in range(1, 6)))
            if fault == 'scope':
                status = replace(status, entries=status.entries[:1])
            payload = status.encode() if fault != 'malformed' else b''
            target = b if fault == 'direction' else a
            target.receive(1, c.SOURCE_STATUS, payload, 2)
            self.assertEqual(target.phase, 'closed')
        a, b = self.pair()
        self.activate(a, b)
        a.take_invalidations()
        a.receive(1, c.SOURCE_STATUS, c.Status(10, 1, (c.SourceState(1),)).encode(), 2)
        self.assertEqual(a.phase, 'active')
        self.assertEqual(a.take_invalidations(), 0)
        b.revision = (1 << 64) - 1
        self.assertIsNone(b.publish(1, tuple(c.SourceState(i) for i in range(1, 6)), 3))
        self.assertEqual(b.phase, 'closed')

    def test_wire_bounds_and_round_trip(self):
        cap = c.supported(c.Role.SDK).encode()
        status = c.Status(1, 1, (c.SourceState(1, True, c.Reason.READY, 99),)).encode()
        for decoder, payload in ((c.Capability.decode, cap), (c.Status.decode, status)):
            self.assertEqual(decoder(payload).encode(), payload)
            for size in range(len(payload)):
                with self.assertRaises(ValueError):
                    decoder(payload[:size])
            with self.assertRaises(ValueError):
                decoder(payload + b'\0')
        for offset, value in ((0, 2), (2, 9), (3, 1), (16, 17), (18, 1), (20, 32), (92, 1)):
            bad = bytearray(cap)
            bad[offset] = value
            with self.assertRaises(ValueError):
                c.Capability.decode(bad)
        for offset, value in ((0, 2), (2, 0), (20, 1), (24, 6), (26, 2), (28, 6)):
            bad = bytearray(status)
            bad[offset] = value
            with self.assertRaises(ValueError):
                c.Status.decode(bad)
        self.assertEqual(len(cap), 164)
        self.assertEqual(struct.unpack_from('<Q', status, 4)[0], 1)
        self.assertEqual(struct.unpack_from('<Q', status, 12)[0], 1)
        self.assertEqual(struct.unpack_from('<Q', status, 32)[0], 99)

    def test_duplicate_controls_and_partial_intersection(self):
        a, b = self.pair(mask=1)
        offer, ack, commit = self.activate(a, b)
        for session, action in ((a, ack), (b, offer), (b, commit)):
            self.assertIsNone(session.receive(1, action.type, action.payload, 2))
            self.assertEqual(session.phase, 'active')
        ready = (c.SourceState(1, True, c.Reason.READY, 1),)
        self.transfer(b, a, b.publish(1, ready, 3), 4)
        self.assertEqual(a.format(1), 'v2')
        self.assertEqual(a.format(2), 'unavailable')


if __name__ == '__main__':
    unittest.main()
