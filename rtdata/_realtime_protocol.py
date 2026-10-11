"""Bounded, schema-typed realtime V2 row codec; connections require explicit opt-in."""
import math
import struct
from dataclasses import dataclass
from types import MappingProxyType

from ._realtime_schema import SCHEMAS, SOURCES

OFFER, ACK, COMMIT, SNAPSHOT = 0x60, 0x61, 0x62, 0x63
MAX_PAYLOAD_BYTES = 16384
MAX_STRING_BYTES = 1024
ENVELOPE = struct.Struct('<4H2Q2HI')
NUMBERS = {name: struct.Struct(fmt) for name, fmt in {
    'CHAR': '<b', 'INT': '<i', 'LONG': '<q', 'TIMESTAMP': '<q', 'DOUBLE': '<d',
}.items()}


def _schema(source_id):
    if type(source_id) is not int or source_id not in SOURCES:
        raise ValueError('unknown realtime source')
    return SCHEMAS[SOURCES[source_id][1]]


def _validate_value(kind, value):
    if value is None:
        return
    if kind in ('SYMBOL', 'STRING'):
        if type(value) is not str or len(value.encode('utf-8')) > MAX_STRING_BYTES:
            raise ValueError('invalid realtime string')
    elif kind == 'DOUBLE':
        if type(value) is not float:
            raise ValueError('DOUBLE must be float, not an integer coercion')
    else:
        bits = NUMBERS[kind].size * 8
        if type(value) is not int or not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
            raise ValueError('realtime signed integer out of range')


@dataclass(frozen=True)
class Snapshot:
    source_id: int
    source_epoch: int
    sequence: int
    values: tuple

    def __post_init__(self):
        fields = _schema(self.source_id)
        for value in (self.source_epoch, self.sequence):
            if type(value) is not int or not 0 < value < 1 << 64:
                raise ValueError('invalid realtime epoch/sequence')
        values = tuple(self.values)
        if len(values) != len(fields):
            raise ValueError('realtime field count mismatch')
        for (_, kind), value in zip(fields, values):
            _validate_value(kind, value)
        code = values[0]
        if not isinstance(code, str) or not code or '\x00' in code or values[1] is None:
            raise ValueError('missing realtime code/time')
        object.__setattr__(self, 'values', values)

    @property
    def fields(self):
        return MappingProxyType(dict(zip((name for name, _ in _schema(self.source_id)), self.values)))

    @property
    def source_table(self):
        return SOURCES[self.source_id][0]

    @property
    def schema_id(self):
        return SOURCES[self.source_id][1]

    @classmethod
    def from_fields(cls, source_id, source_epoch, sequence, fields):
        columns = _schema(source_id)
        if set(fields) != {name for name, _ in columns}:
            raise ValueError('realtime fields missing or unknown')
        return cls(source_id, source_epoch, sequence, tuple(fields[name] for name, _ in columns))


def encode(snapshot):
    columns = _schema(snapshot.source_id)
    bitmap = bytearray((len(columns) + 7) // 8)
    body = bytearray()
    for i, ((_, kind), value) in enumerate(zip(columns, snapshot.values)):
        if value is None:
            bitmap[i // 8] |= 1 << (i % 8)
        elif kind in ('SYMBOL', 'STRING'):
            raw = value.encode('utf-8')
            body.extend(struct.pack('<H', len(raw)))
            body.extend(raw)
        else:
            body.extend(NUMBERS[kind].pack(value))
    payload = ENVELOPE.pack(1, snapshot.schema_id, snapshot.source_id, len(columns),
                            snapshot.source_epoch, snapshot.sequence, len(bitmap), 0, len(body)) + bitmap + body
    if len(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError('realtime payload too long')
    return bytes(payload)


def decode(payload):
    if not ENVELOPE.size <= len(payload) <= MAX_PAYLOAD_BYTES:
        raise ValueError('invalid realtime payload size')
    version, schema_id, source_id, count, epoch, seq, bitmap_size, flags, body_size = ENVELOPE.unpack_from(payload)
    columns = _schema(source_id)
    if version != 1 or schema_id != SOURCES[source_id][1] or count != len(columns):
        raise ValueError('realtime schema mismatch')
    if flags or bitmap_size != (count + 7) // 8 or len(payload) != ENVELOPE.size + bitmap_size + body_size:
        raise ValueError('realtime lengths/flags mismatch')
    bitmap = payload[ENVELOPE.size:ENVELOPE.size + bitmap_size]
    if count % 8 and bitmap[-1] >> (count % 8):
        raise ValueError('realtime bitmap padding')
    offset = ENVELOPE.size + bitmap_size
    values = []
    for i, (_, kind) in enumerate(columns):
        if bitmap[i // 8] & (1 << (i % 8)):
            values.append(None)
            continue
        size = 2 if kind in ('SYMBOL', 'STRING') else NUMBERS[kind].size
        if offset + size > len(payload):
            raise ValueError('truncated realtime value')
        if kind in ('SYMBOL', 'STRING'):
            size = struct.unpack_from('<H', payload, offset)[0]
            offset += 2
            if size > MAX_STRING_BYTES or offset + size > len(payload):
                raise ValueError('invalid realtime string length')
            value = bytes(payload[offset:offset + size]).decode('utf-8', errors='strict')
        else:
            value = NUMBERS[kind].unpack_from(payload, offset)[0]
        offset += size
        values.append(value)
    if offset != len(payload):
        raise ValueError('trailing realtime values')
    return Snapshot(source_id, epoch, seq, tuple(values))


def project_legacy(snapshot, symbol_id):
    if type(symbol_id) is not int or not 0 < symbol_id < 1 << 32:
        raise ValueError('invalid legacy symbol id')
    fields = snapshot.fields
    prices = [fields[name] for name in ('bid_price1', 'ask_price1', 'last')]
    volume = fields['volume']
    null_fields = sum(value is None for value in prices + [volume])
    prices = [0.0 if value is None else value for value in prices]
    volume = 0 if volume is None else volume
    if volume < 0 or any(not math.isfinite(v) or abs(v) > 3.4028234663852886e38 for v in prices):
        raise ValueError('value cannot be projected to V1')
    return (struct.pack('!I', symbol_id) + struct.pack('<3f', *prices) +
            struct.pack('!Qq', volume, fields['trade_time'])), null_fields
