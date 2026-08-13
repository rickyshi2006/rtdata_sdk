"""History V2 columnar delta and optional Zstd codec."""

import struct
from dataclasses import dataclass
from typing import List, Sequence, Tuple

from . import _history_v2_protocol as protocol

try:
    import zstandard as _zstd
except ImportError:
    _zstd = None


DecodedKline = Tuple[int, float, float, float, float, int, float, int]


@dataclass(frozen=True)
class EncodedColumnarBlock:
    row_count: int
    first_timestamp_ms: int
    last_timestamp_ms: int
    uncompressed: bytes
    compressed: bytes


def zstd_available() -> bool:
    return _zstd is not None


def _zigzag_encode(value: int) -> int:
    if not -(1 << 63) <= value < (1 << 63):
        raise ValueError("timestamp delta is outside int64")
    return (value << 1) ^ (value >> 63)


def _zigzag_decode(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


def _encode_varint(value: int) -> bytes:
    if not 0 <= value <= 0xFFFFFFFFFFFFFFFF:
        raise ValueError("timestamp varint is outside uint64")
    output = bytearray()
    while value >= 0x80:
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def _decode_varint(data: memoryview, offset: int) -> Tuple[int, int]:
    value = 0
    for index in range(10):
        if offset >= len(data):
            raise ValueError("truncated timestamp varint")
        byte = data[offset]
        offset += 1
        if index == 9 and byte & 0xFE:
            raise ValueError("timestamp varint overflow")
        value |= (byte & 0x7F) << (index * 7)
        if not byte & 0x80:
            return value, offset
    raise ValueError("unterminated timestamp varint")


def _validate_block_size(size: int, max_block_bytes: int) -> None:
    if not protocol.MIN_BLOCK_BYTES <= max_block_bytes <= protocol.MAX_BLOCK_BYTES:
        raise ValueError("invalid history V2 max block size")
    if not 0 < size <= max_block_bytes:
        raise ValueError("invalid history V2 block size")


def encode_columnar_block(
    rows: Sequence[Sequence],
    max_block_bytes: int = protocol.DEFAULT_BLOCK_BYTES,
) -> EncodedColumnarBlock:
    if not rows or len(rows) > 0xFFFFFFFF:
        raise ValueError("history V2 block row count is invalid")

    timestamps = [int(row[0]) for row in rows]
    timestamp_column = bytearray(struct.pack("!q", timestamps[0]))
    if len(timestamps) >= 2:
        previous_delta = timestamps[1] - timestamps[0]
        if not 0 < previous_delta < (1 << 63):
            raise ValueError("history timestamps must be increasing")
        timestamp_column.extend(_encode_varint(_zigzag_encode(previous_delta)))
        for index in range(2, len(timestamps)):
            current_delta = timestamps[index] - timestamps[index - 1]
            if not 0 < current_delta < (1 << 63):
                raise ValueError("history timestamps must be increasing")
            timestamp_column.extend(
                _encode_varint(_zigzag_encode(current_delta - previous_delta))
            )
            previous_delta = current_delta

    header = protocol.HistoryColumnsHeader(
        row_count=len(rows), timestamp_bytes=len(timestamp_column)
    ).encode()
    raw = bytearray(header)
    raw.extend(timestamp_column)
    for column_index in range(1, 5):
        raw.extend(
            struct.pack(
                f"<{len(rows)}f", *(float(row[column_index]) for row in rows)
            )
        )
    raw.extend(struct.pack(f"<{len(rows)}Q", *(int(row[5]) for row in rows)))
    _validate_block_size(len(raw), max_block_bytes)
    compressed = compress_zstd(bytes(raw)) if zstd_available() else b""
    if compressed:
        _validate_block_size(len(compressed), max_block_bytes)
    return EncodedColumnarBlock(
        row_count=len(rows),
        first_timestamp_ms=timestamps[0],
        last_timestamp_ms=timestamps[-1],
        uncompressed=bytes(raw),
        compressed=compressed,
    )


def decode_columnar_block(
    payload: bytes,
    expected_rows: int,
    max_block_bytes: int = protocol.DEFAULT_BLOCK_BYTES,
) -> List[DecodedKline]:
    if expected_rows <= 0:
        raise ValueError("history V2 expected row count is invalid")
    _validate_block_size(len(payload), max_block_bytes)
    header = protocol.HistoryColumnsHeader.decode(payload)
    if header.row_count != expected_rows:
        raise ValueError("history V2 column row count mismatch")

    view = memoryview(payload)
    timestamp_start = protocol.COLUMNS_HEADER_STRUCT.size
    timestamp_end = timestamp_start + header.timestamp_bytes
    timestamp_data = view[timestamp_start:timestamp_end]
    timestamps = [struct.unpack_from("!q", timestamp_data, 0)[0]]
    timestamp_offset = 8
    previous_delta = 0
    if expected_rows >= 2:
        value, timestamp_offset = _decode_varint(
            timestamp_data, timestamp_offset
        )
        previous_delta = _zigzag_decode(value)
        if previous_delta <= 0:
            raise ValueError("history timestamps are not increasing")
        second = timestamps[0] + previous_delta
        if not -(1 << 63) <= second < (1 << 63):
            raise ValueError("history timestamp overflow")
        timestamps.append(second)
        for _ in range(2, expected_rows):
            value, timestamp_offset = _decode_varint(
                timestamp_data, timestamp_offset
            )
            current_delta = previous_delta + _zigzag_decode(value)
            if not 0 < current_delta < (1 << 63):
                raise ValueError("history timestamp delta invalid")
            current = timestamps[-1] + current_delta
            if not -(1 << 63) <= current < (1 << 63):
                raise ValueError("history timestamp overflow")
            timestamps.append(current)
            previous_delta = current_delta
    if timestamp_offset != len(timestamp_data):
        raise ValueError("timestamp column has trailing bytes")

    offset = timestamp_end
    columns = []
    float_bytes = expected_rows * 4
    for _ in range(4):
        columns.append(struct.unpack_from(f"<{expected_rows}f", view, offset))
        offset += float_bytes
    volumes = struct.unpack_from(f"<{expected_rows}Q", view, offset)
    offset += expected_rows * 8

    if header.column_flags & protocol.COLUMN_TURNOVER:
        turnovers = struct.unpack_from(f"<{expected_rows}d", view, offset)
        offset += expected_rows * 8
    else:
        turnovers = (0.0,) * expected_rows
    if header.column_flags & protocol.COLUMN_OPEN_INTEREST:
        open_interests = struct.unpack_from(f"<{expected_rows}Q", view, offset)
        offset += expected_rows * 8
    else:
        open_interests = (0,) * expected_rows
    if offset != len(payload):
        raise ValueError("history column payload has trailing bytes")

    return [
        (
            timestamps[index],
            columns[0][index],
            columns[1][index],
            columns[2][index],
            columns[3][index],
            volumes[index],
            turnovers[index],
            open_interests[index],
        )
        for index in range(expected_rows)
    ]


def compress_zstd(payload: bytes) -> bytes:
    if _zstd is None:
        raise RuntimeError("Zstd support is not available")
    if not payload:
        raise ValueError("cannot compress an empty history block")
    compressor = _zstd.ZstdCompressor(
        level=1,
        write_checksum=True,
        write_content_size=True,
    )
    return compressor.compress(payload)


def decompress_zstd(payload: bytes, expected_size: int) -> bytes:
    if _zstd is None:
        raise RuntimeError("Zstd support is not available")
    if not payload or expected_size <= 0:
        raise ValueError("invalid Zstd history block")
    try:
        result = _zstd.ZstdDecompressor().decompress(
            payload, max_output_size=expected_size
        )
    except _zstd.ZstdError as exc:
        raise ValueError(f"Zstd decompression failed: {exc}") from exc
    if len(result) != expected_size:
        raise ValueError("Zstd decompressed size mismatch")
    return result
