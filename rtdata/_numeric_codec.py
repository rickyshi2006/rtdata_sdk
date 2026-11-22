"""Optional native numeric codec; offline RBLX and negotiated RBN1 stay distinct."""
import struct
import threading

from . import _realtime_block as block
from . import _realtime_protocol as realtime

HEADER = struct.Struct('<4sBBHIHHII')
_lock = threading.Lock()
_loaded = False
_decoder = None


def _load():
    global _loaded, _decoder
    with _lock:
        if not _loaded:
            try:
                from rtdata_numeric_codec import Decoder
                candidate = Decoder()
                kinds = {'DOUBLE': 'd', 'INT': 'i', 'CHAR': 'c', 'LONG': 'q',
                         'TIMESTAMP': 'q', 'STRING': 's', 'SYMBOL': 's'}
                for schema, columns in realtime.SCHEMAS.items():
                    expected = ''.join('r' if schema >= 4 and name == 'raw_bits' else kinds[kind]
                                       for name, kind in columns)
                    if candidate.schema(schema) != expected:
                        raise RuntimeError('numeric codec schema mismatch')
                _selftest(candidate)
                _decoder = candidate
            except (ImportError, OSError, AttributeError, ValueError, RuntimeError):
                _decoder = None
            _loaded = True
        return _decoder


def _selftest(candidate):
    values = ('TEST', 1, 0.1) + (None,) * (len(realtime.SCHEMAS[3]) - 3)
    payload = realtime.encode(realtime.Snapshot(5, 1, 1, values))
    prefix = block.FRAME.pack(len(payload), 1, realtime.SNAPSHOT) + payload[:34]
    transformed = prefix + b'\x04\x00TEST\x02\x02\x02'
    expected = block.FRAME.pack(len(payload), 1, realtime.SNAPSHOT) + payload
    if candidate.restore(transformed, len(expected), 1) != expected:
        raise RuntimeError('numeric codec self-test mismatch')


def available():
    decoder = _load()
    return block.available() and decoder is not None and decoder.supported()


def decode(payload, *, transport=False):
    if type(payload) is not bytes or not HEADER.size < len(payload) <= block.MAX_RAW_BYTES:
        raise ValueError('numeric envelope size')
    magic, version, method, flags, raw_size, count, reserved, size, transformed_size = HEADER.unpack_from(payload)
    if (magic != (b'RBN1' if transport else b'RBLX') or version != 1 or
            method != (1 if transport else 4) or flags or reserved or
            not 0 < raw_size <= block.MAX_RAW_BYTES or not 0 < count <= block.MAX_FRAMES or
            size != len(payload) - HEADER.size or not 0 < transformed_size <= 131072):
        raise ValueError('numeric envelope parameters')
    decoder = _load()
    if not block.available() or decoder is None:
        raise RuntimeError('numeric native codec unavailable')
    packed = payload[HEADER.size:]
    if packed[:4] != b'\x28\xb5\x2f\xfd':
        raise ValueError('numeric zstd magic')
    try:
        params = block._zstd.get_frame_parameters(packed)
        if (params.content_size != transformed_size or params.window_size > block.MAX_RAW_BYTES or
                params.dict_id or not params.has_checksum):
            raise ValueError('numeric zstd parameters')
        data = block._zstd.ZstdDecompressor(max_window_size=block.MAX_RAW_BYTES).decompress(
            packed, max_output_size=transformed_size, allow_extra_data=False)
    except block._zstd.ZstdError as error:
        raise ValueError('numeric zstd') from error
    if len(data) != transformed_size:
        raise ValueError('numeric transformed size')
    raw = decoder.restore(data, raw_size, count)
    if len(block.frames(raw)) != count:
        raise ValueError('numeric frame count')
    return raw
