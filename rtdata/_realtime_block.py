"""Bounded lossless block codec and connection-scoped negotiation payload."""

import struct

try:
    import zstandard as _zstd
except ImportError:
    _zstd = None

MAX_RAW_BYTES = 64 * 1024
MAX_FRAMES = 256
HEADER = struct.Struct('<4sBBHIHHI')
FRAME = struct.Struct('!IIH')
OFFER, ACK, COMMIT, READY, DATA = range(0x65, 0x6a)
NEGOTIATION = struct.Struct('<4sBBHIHHQQ')


def negotiation(client, server=0, *, numeric=False):
    if not 0 < client < 1 << 64 or not 0 <= server < 1 << 64:
        raise ValueError('block negotiation nonce')
    return NEGOTIATION.pack(b'RBC2' if numeric else b'RBC1', 2 if numeric else 1,
                            2 if numeric else 1, 0, MAX_RAW_BYTES, MAX_FRAMES, 0, client, server)


def parse_negotiation(payload, *, numeric=False):
    if len(payload) != NEGOTIATION.size:
        raise ValueError('block negotiation size')
    client, server = NEGOTIATION.unpack(payload)[-2:]
    if negotiation(client, server, numeric=numeric) != payload:
        raise ValueError('block negotiation parameters')
    return client, server


def available():
    return _zstd is not None


def frames(raw):
    """Validate the whole block before exposing any inner V2 payload."""
    if not 0 < len(raw) <= MAX_RAW_BYTES:
        raise ValueError('realtime block size')
    raw = memoryview(raw).toreadonly()
    result, offset = [], 0
    while offset < len(raw):
        if len(raw) - offset < FRAME.size:
            raise ValueError('realtime block truncated header')
        length, symbol, kind = FRAME.unpack_from(raw, offset)
        offset += FRAME.size
        if (kind != 0x63 or not symbol or length < 32 or length > len(raw) - offset
                or len(result) >= MAX_FRAMES):
            raise ValueError('realtime block inner frame')
        result.append((symbol, raw[offset:offset + length]))
        offset += length
    return tuple(result)


def encode(raw):
    count = len(frames(raw))
    if _zstd is None:
        raise RuntimeError('Zstd support is not available')
    compressed = _zstd.ZstdCompressor(level=1, write_checksum=True, write_content_size=True).compress(raw)
    if FRAME.size + HEADER.size + len(compressed) >= len(raw):
        return b''
    return HEADER.pack(b'RBLK', 1, 1, 0, len(raw), count, 0, len(compressed)) + compressed


def decode(payload):
    if not HEADER.size < len(payload) <= MAX_RAW_BYTES:
        raise ValueError('realtime block envelope')
    magic, version, codec, flags, size, count, reserved, compressed_size = HEADER.unpack_from(payload)
    if (magic != b'RBLK' or version != 1 or codec != 1 or flags or reserved
            or not 0 < size <= MAX_RAW_BYTES or not 0 < count <= MAX_FRAMES
            or compressed_size != len(payload) - HEADER.size or compressed_size < 6):
        raise ValueError('realtime block bounds')
    if _zstd is None:
        raise RuntimeError('Zstd support is not available')
    compressed = payload[HEADER.size:]
    if compressed[:4] != b'\x28\xb5\x2f\xfd':
        raise ValueError('realtime block zstd magic')
    try:
        params = _zstd.get_frame_parameters(compressed)
        if (params.content_size != size or params.window_size > MAX_RAW_BYTES
                or params.dict_id or not params.has_checksum):
            raise ValueError('realtime block zstd parameters')
        raw = _zstd.ZstdDecompressor(max_window_size=MAX_RAW_BYTES).decompress(
            compressed, max_output_size=size, allow_extra_data=False)
    except _zstd.ZstdError as error:
        raise ValueError('realtime block zstd') from error
    if len(raw) != size or len(frames(raw)) != count:
        raise ValueError('realtime block frame count')
    return raw
