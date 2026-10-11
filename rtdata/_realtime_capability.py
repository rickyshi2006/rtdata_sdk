"""Pure realtime negotiation machine; transport serializes calls and ordered sends."""

import struct
from dataclasses import dataclass, replace
from enum import IntEnum
from typing import Optional, Tuple

from ._realtime_schema import REGISTRY_SHA256, SCHEMA_HASHES, SOURCES

OFFER, ACK, COMMIT, SNAPSHOT, SOURCE_STATUS = range(0x60, 0x65)
TIMEOUT_MS = 1000
MAX_PAYLOAD = 16384
SOURCE_MASK = 0x1F
_CAP = struct.Struct('<HBBQIHHI32s')
_SCHEMA = struct.Struct('<HH32s')
_STATUS = struct.Struct('<HHQQI')
_SOURCE = struct.Struct('<HHIQ')


class Role(IntEnum):
    PRODUCER = 1
    CLOUD = 2
    SDK = 3


class Reason(IntEnum):
    READY = 0
    NO_UPSTREAM = 1
    LEGACY_UPSTREAM = 2
    SCHEMA_MISMATCH = 3
    SUSPENDED = 4
    PAYLOAD_LIMIT = 5


def _uint(value, bits):
    return type(value) is int and 0 <= value < (1 << bits)


def source_bit(source_id):
    if type(source_id) is not int or source_id not in SOURCES:
        raise ValueError('unknown realtime source')
    return 1 << (source_id - 1)


@dataclass(frozen=True)
class SchemaRef:
    id: int
    version: int
    hash: bytes


@dataclass(frozen=True)
class Capability:
    role: Role
    nonce: int
    max_payload: int
    sources: int
    registry: bytes
    schemas: Tuple[SchemaRef, ...]

    def encode(self):
        if (self.role not in tuple(Role) or not _uint(self.nonce, 64) or not self.nonce
                or not _uint(self.max_payload, 32) or not 64 <= self.max_payload <= MAX_PAYLOAD
                or not _uint(self.sources, 32) or self.sources & ~SOURCE_MASK
                or type(self.registry) is not bytes or len(self.registry) != 32
                or type(self.schemas) is not tuple or len(self.schemas) > 16
                or (self.sources == 0) != (not self.schemas)):
            raise ValueError('invalid realtime capability')
        previous = 0
        for s in self.schemas:
            if (not _uint(s.id, 16) or s.id <= previous or not _uint(s.version, 16)
                    or not s.version or type(s.hash) is not bytes or len(s.hash) != 32):
                raise ValueError('invalid realtime schema reference')
            previous = s.id
        return _CAP.pack(1, self.role, 0, self.nonce, self.max_payload,
                         len(self.schemas), 0, self.sources, self.registry) + b''.join(
                             _SCHEMA.pack(s.id, s.version, s.hash) for s in self.schemas)

    @classmethod
    def decode(cls, payload):
        if not 56 <= len(payload) <= 632:
            raise ValueError('invalid capability length')
        version, role, flags, nonce, limit, count, reserved, sources, registry = _CAP.unpack_from(payload)
        if version != 1 or flags or reserved or count > 16 or len(payload) != 56 + 36 * count:
            raise ValueError('invalid capability header')
        c = cls(Role(role), nonce, limit, sources, registry, tuple(
            SchemaRef(*_SCHEMA.unpack_from(payload, 56 + 36 * i)) for i in range(count)))
        c.encode()
        return c


def supported(role, sources=SOURCE_MASK, max_payload=MAX_PAYLOAD):
    ids = {SOURCES[sid][1] for sid in SOURCES if sources & source_bit(sid)}
    c = Capability(Role(role), 1, max_payload, sources, bytes.fromhex(REGISTRY_SHA256), tuple(
        SchemaRef(sid, 1, bytes.fromhex(SCHEMA_HASHES[sid])) for sid in sorted(ids)))
    c.encode()
    return c


def intersection(local, offer):
    local.encode()
    offer.encode()
    sources = 0
    schemas = []
    if local.registry == offer.registry:
        for s in local.schemas:
            if s not in offer.schemas:
                continue
            selected = sum(source_bit(sid) for sid in SOURCES
                           if local.sources & offer.sources & source_bit(sid)
                           and SOURCES[sid][1] == s.id)
            if selected:
                sources |= selected
                schemas.append(s)
    return Capability(local.role, offer.nonce, min(local.max_payload, offer.max_payload),
                      sources, offer.registry, tuple(schemas))


@dataclass(frozen=True)
class SourceState:
    id: int
    ready: bool = False
    reason: Reason = Reason.NO_UPSTREAM
    epoch: int = 0


@dataclass(frozen=True)
class Status:
    nonce: int
    revision: int
    entries: Tuple[SourceState, ...]

    def mask(self):
        if (not _uint(self.nonce, 64) or not self.nonce or not _uint(self.revision, 64)
                or not self.revision or type(self.entries) is not tuple or not 1 <= len(self.entries) <= 5):
            raise ValueError('invalid realtime source status')
        mask, previous = 0, 0
        for s in self.entries:
            if (s.id <= previous or type(s.ready) is not bool or s.reason not in tuple(Reason)
                    or not _uint(s.epoch, 64) or
                    (s.reason != Reason.READY or not s.epoch if s.ready
                     else s.reason == Reason.READY or s.epoch != 0)):
                raise ValueError('invalid realtime source state')
            mask |= source_bit(s.id)
            previous = s.id
        return mask

    def encode(self):
        self.mask()
        return _STATUS.pack(1, len(self.entries), self.nonce, self.revision, 0) + b''.join(
            _SOURCE.pack(s.id, s.ready, s.reason, s.epoch) for s in self.entries)

    @classmethod
    def decode(cls, payload):
        if not 40 <= len(payload) <= 104:
            raise ValueError('invalid status length')
        version, count, nonce, revision, reserved = _STATUS.unpack_from(payload)
        if version != 1 or reserved or not 1 <= count <= 5 or len(payload) != 24 + 16 * count:
            raise ValueError('invalid status header')
        entries = []
        for i in range(count):
            sid, ready, reason, epoch = _SOURCE.unpack_from(payload, 24 + 16 * i)
            if ready not in (0, 1):
                raise ValueError('unknown source state')
            entries.append(SourceState(sid, bool(ready), Reason(reason), epoch))
        status = cls(nonce, revision, tuple(entries))
        status.mask()
        return status


@dataclass(frozen=True)
class Action:
    type: int
    payload: bytes


class Session:
    def __init__(self, role, peer, mode='legacy', sources=SOURCE_MASK, max_payload=MAX_PAYLOAD):
        self.role, self.peer = Role(role), Role(peer)
        if (self.role, self.peer) not in ((Role.PRODUCER, Role.CLOUD), (Role.SDK, Role.CLOUD),
                                         (Role.CLOUD, Role.PRODUCER), (Role.CLOUD, Role.SDK)):
            raise ValueError('invalid realtime role pair')
        if mode not in ('legacy', 'auto', 'required_v2'):
            raise ValueError('invalid realtime mode')
        self.mode = mode
        self.local = supported(self.role, sources, max_payload)
        self.generation = 0
        self.authenticated = False
        self.phase = 'idle'
        self.reason = ''
        self.offered: Optional[Capability] = None
        self.selected: Optional[Capability] = None
        self.pending: Optional[Action] = None
        self.states: Tuple[SourceState, ...] = ()
        self.revision = 0
        self.deadline = 0
        self.invalidations = 0

    @property
    def producer(self):
        return self.role == Role.PRODUCER or (self.role == Role.CLOUD and self.peer == Role.SDK)

    def reset(self, generation, authenticated):
        if not _uint(generation, 64) or generation <= self.generation:
            raise ValueError('stale connection generation')
        self.invalidations |= self.selected.sources if self.selected else 0
        self.generation, self.authenticated = generation, bool(authenticated)
        self.phase, self.reason = 'idle', ''
        self.pending = self.selected = self.offered = None
        self.states, self.revision, self.deadline = (), 0, 0

    def take_invalidations(self):
        mask, self.invalidations = self.invalidations, 0
        return mask

    def _current(self, generation):
        return bool(generation and generation == self.generation and self.authenticated)

    def _arm(self, now):
        if not _uint(now, 64) or now > (1 << 64) - 1 - TIMEOUT_MS:
            raise ValueError('invalid monotonic time')
        self.deadline = now + TIMEOUT_MS

    def _queue(self, msg_type, payload, now):
        self._arm(now)
        self.pending = Action(msg_type, payload)
        return self.pending

    def _finish(self, phase, reason):
        self.invalidations |= self.selected.sources if self.selected else 0
        self.phase, self.reason = phase, reason
        self.pending, self.deadline, self.states = None, 0, ()

    def _activate(self):
        self.phase, self.deadline, self.revision = 'active', 0, 0
        self.invalidations |= self.selected.sources
        self.states = tuple(SourceState(sid) for sid in SOURCES if self.selected.sources & source_bit(sid))

    def _apply(self, status):
        old = {s.id: s for s in self.states}
        for s in status.entries:
            if s.id not in old or (s.ready, s.epoch) != (old[s.id].ready, old[s.id].epoch):
                self.invalidations |= source_bit(s.id)
        self.states, self.revision = status.entries, status.revision

    def begin(self, generation, nonce, now):
        if (not self._current(generation) or self.role == Role.CLOUD or self.mode == 'legacy'
                or self.phase != 'idle'):
            return None
        self.offered = replace(self.local, nonce=nonce)
        payload = self.offered.encode()
        self.phase = 'offer_pending'
        return self._queue(OFFER, payload, now)

    def tick(self, generation, now):
        if not self._current(generation) or not self.deadline or now < self.deadline:
            return
        if self.phase == 'waiting_ack':
            self._finish('fallback', 'ack_timeout')
        else:
            self._finish('closed', 'commit_or_queue_timeout')

    def sent(self, generation, action, success, now):
        self.tick(generation, now)
        if not self._current(generation) or self.pending is None or self.pending != action:
            return
        self.pending = None
        if not success:
            self._finish('closed', 'control_send_failed')
        elif action.type == OFFER:
            self.phase = 'waiting_ack'
            self._arm(now)
        elif action.type == ACK:
            if self.selected.sources:
                self.phase = 'waiting_commit'
                self._arm(now)
            else:
                self._finish('fallback', 'no_common_schema')
        elif action.type == COMMIT:
            self._activate()
        elif action.type == SOURCE_STATUS:
            self._apply(Status.decode(action.payload))
            self.deadline = 0
        else:
            raise RuntimeError('unexpected pending control type')

    def receive(self, generation, msg_type, payload, now):
        self.tick(generation, now)
        if (not self._current(generation) or self.mode == 'legacy'
                or self.phase in ('closed', 'fallback')):
            return None
        expected = ((msg_type == OFFER and self.role == Role.CLOUD and self.phase == 'idle')
                    or (msg_type == ACK and self.role != Role.CLOUD and self.phase == 'waiting_ack')
                    or (msg_type == COMMIT and self.role == Role.CLOUD and self.phase == 'waiting_commit')
                    or (msg_type == SOURCE_STATUS and self.phase == 'active'))
        if not expected:
            return None
        try:
            if msg_type == SOURCE_STATUS:
                status = Status.decode(payload)
                if status.nonce != self.selected.nonce:
                    return None
                if self.producer or status.mask() != self.selected.sources:
                    raise ValueError('unexpected source status')
                if status.revision > self.revision:
                    self._apply(status)
                return None
            c = Capability.decode(payload)
            if msg_type != OFFER and c.nonce != (self.offered if msg_type == ACK else self.selected).nonce:
                return None
            if c.role != self.peer:
                raise ValueError('unexpected capability role')
            if msg_type == OFFER:
                self.selected = intersection(self.local, c)
                self.phase = 'ack_pending'
                return self._queue(ACK, self.selected.encode(), now)
            if msg_type == ACK:
                if c.registry != self.offered.registry:
                    raise ValueError('ACK registry differs from OFFER')
                allowed = intersection(intersection(self.local, self.offered), c)
                if replace(allowed, role=self.peer).encode() != payload:
                    raise ValueError('ACK outside OFFER intersection')
                self.selected = c
                if not c.sources:
                    self._finish('fallback', 'no_common_schema')
                    return None
                self.phase = 'commit_pending'
                return self._queue(COMMIT, replace(c, role=self.role).encode(), now)
            if replace(self.selected, role=self.peer).encode() != payload:
                raise ValueError('COMMIT differs from ACK')
            self._activate()
        except ValueError as exc:
            self._finish('closed', str(exc))
        return None

    def publish(self, generation, states, now):
        self.tick(generation, now)
        if not self._current(generation) or self.phase != 'active' or not self.producer or self.pending:
            return None
        if self.revision == (1 << 64) - 1:
            self._finish('closed', 'status_revision_exhausted')
            return None
        status = Status(self.selected.nonce, self.revision + 1, tuple(states))
        if status.mask() != self.selected.sources:
            raise ValueError('incomplete source status')
        return self._queue(SOURCE_STATUS, status.encode(), now)

    def format(self, source_id):
        bit = source_bit(source_id)
        if self.phase == 'closed':
            return 'unavailable'
        if self.phase == 'active' and self.selected.sources & bit:
            if any(s.id == source_id and s.ready for s in self.states):
                return 'v2'
        return 'unavailable' if self.mode == 'required_v2' else 'v1'

    def admits(self, source_id, epoch, payload_size):
        return (self.format(source_id) == 'v2'
                and 32 <= payload_size <= self.selected.max_payload
                and any(s.id == source_id and s.epoch == epoch for s in self.states))
