"""History V2 D2 wire envelopes.

All envelope integers use network byte order. The compressed column payload has
its own schema and is decoded separately.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Tuple


MSG_HISTORY_BEGIN = 0x28
MSG_HISTORY_DATA = 0x29
MSG_HISTORY_END = 0x2A
MSG_HISTORY_ERROR = 0x2B
MSG_HISTORY_CANCEL = 0x2C
MSG_HISTORY_WINDOW_UPDATE = 0x2D

REQUEST_MAGIC = b"H2V1"
SCHEMA_VERSION = 1
REQUEST_MODE_V2_REQUIRED = 2
CODEC_COLUMNAR_DELTA_V1 = 1
COMPRESSION_ZSTD_FRAME_V1 = 1

FEATURE_WINDOW_UPDATE = 0x00000001
FEATURE_CANCEL = 0x00000002
FEATURE_CRC32C = 0x00000004
FEATURE_OPTIONAL_COLUMNS = 0x00000008
FEATURE_KNOWN_MASK = (
    FEATURE_WINDOW_UPDATE
    | FEATURE_CANCEL
    | FEATURE_CRC32C
    | FEATURE_OPTIONAL_COLUMNS
)
REQUIRED_FEATURES = (
    FEATURE_WINDOW_UPDATE | FEATURE_CANCEL | FEATURE_OPTIONAL_COLUMNS
)

COLUMN_TIMESTAMP = 0x0001
COLUMN_OPEN = 0x0002
COLUMN_HIGH = 0x0004
COLUMN_LOW = 0x0008
COLUMN_CLOSE = 0x0010
COLUMN_VOLUME = 0x0020
COLUMN_TURNOVER = 0x0040
COLUMN_OPEN_INTEREST = 0x0080
REQUIRED_COLUMNS = (
    COLUMN_TIMESTAMP
    | COLUMN_OPEN
    | COLUMN_HIGH
    | COLUMN_LOW
    | COLUMN_CLOSE
    | COLUMN_VOLUME
)
COLUMN_KNOWN_MASK = REQUIRED_COLUMNS | COLUMN_TURNOVER | COLUMN_OPEN_INTEREST

BEGIN_FLAG_ZSTD_CONTENT_CHECKSUM = 0x01
BEGIN_FLAG_CRC32C_DATA = 0x02
BEGIN_FLAG_OPTIONAL_COLUMNS = 0x04
BEGIN_FLAG_KNOWN_MASK = 0x07
DATA_FLAG_CRC32C_PRESENT = 0x0001

MIN_BLOCK_BYTES = 64 * 1024
MAX_BLOCK_BYTES = 4 * 1024 * 1024
MAX_WINDOW_BYTES = 4 * 1024 * 1024
DEFAULT_BLOCK_BYTES = 64 * 1024
DEFAULT_INITIAL_WINDOW_BYTES = 512 * 1024
OUTER_HEADER_SIZE = 10

REQUEST_OPTIONS_STRUCT = struct.Struct("!4sBBBBIIIHH")
BEGIN_STRUCT = struct.Struct("!IBBBBIB3xIIQqq")
DATA_HEADER_STRUCT = struct.Struct("!IIIIIqqIHH")
END_STRUCT = struct.Struct("!IQQQIIB3x")
ERROR_HEADER_STRUCT = struct.Struct("!IHHIQHH")
CANCEL_STRUCT = struct.Struct("!IHHI")
WINDOW_UPDATE_STRUCT = struct.Struct("!III")
COLUMNS_HEADER_STRUCT = struct.Struct("!BBHIII")


def _validate_block_size(value: int) -> None:
    if not MIN_BLOCK_BYTES <= value <= MAX_BLOCK_BYTES:
        raise ValueError("invalid history V2 block size")


@dataclass(frozen=True)
class RequestOptions:
    options_version: int = SCHEMA_VERSION
    mode: int = REQUEST_MODE_V2_REQUIRED
    codec: int = CODEC_COLUMNAR_DELTA_V1
    compression: int = COMPRESSION_ZSTD_FRAME_V1
    selected_features: int = REQUIRED_FEATURES
    max_block_bytes: int = DEFAULT_BLOCK_BYTES
    initial_window_bytes: int = DEFAULT_INITIAL_WINDOW_BYTES

    def validate(self) -> None:
        if (
            self.options_version != SCHEMA_VERSION
            or self.mode != REQUEST_MODE_V2_REQUIRED
            or self.codec != CODEC_COLUMNAR_DELTA_V1
            or self.compression != COMPRESSION_ZSTD_FRAME_V1
        ):
            raise ValueError("unsupported history V2 request options")
        if (
            self.selected_features & ~FEATURE_KNOWN_MASK
            or self.selected_features & REQUIRED_FEATURES != REQUIRED_FEATURES
        ):
            raise ValueError("invalid history V2 selected features")
        _validate_block_size(self.max_block_bytes)
        minimum_window = (
            OUTER_HEADER_SIZE + DATA_HEADER_STRUCT.size + self.max_block_bytes
        )
        if not minimum_window <= self.initial_window_bytes <= MAX_WINDOW_BYTES:
            raise ValueError("invalid history V2 initial window")

    def encode(self) -> bytes:
        self.validate()
        return REQUEST_OPTIONS_STRUCT.pack(
            REQUEST_MAGIC,
            self.options_version,
            self.mode,
            self.codec,
            self.compression,
            self.selected_features,
            self.max_block_bytes,
            self.initial_window_bytes,
            REQUEST_OPTIONS_STRUCT.size,
            0,
        )

    @classmethod
    def decode(cls, payload: bytes) -> "RequestOptions":
        if len(payload) != REQUEST_OPTIONS_STRUCT.size:
            raise ValueError("history V2 request options must be exactly 24 bytes")
        (
            magic,
            options_version,
            mode,
            codec,
            compression,
            selected_features,
            max_block_bytes,
            initial_window_bytes,
            options_length,
            reserved,
        ) = REQUEST_OPTIONS_STRUCT.unpack(payload)
        if (
            magic != REQUEST_MAGIC
            or options_length != REQUEST_OPTIONS_STRUCT.size
            or reserved != 0
        ):
            raise ValueError("invalid history V2 request options header")
        result = cls(
            options_version=options_version,
            mode=mode,
            codec=codec,
            compression=compression,
            selected_features=selected_features,
            max_block_bytes=max_block_bytes,
            initial_window_bytes=initial_window_bytes,
        )
        result.validate()
        return result


def has_request_magic(payload: bytes) -> bool:
    return len(payload) >= len(REQUEST_MAGIC) and payload[:4] == REQUEST_MAGIC


@dataclass(frozen=True)
class HistoryBegin:
    request_id: int
    symbol_id: int
    period: int
    estimated_rows: int
    start_time_ms: int
    end_time_ms: int
    schema_version: int = SCHEMA_VERSION
    codec: int = CODEC_COLUMNAR_DELTA_V1
    compression: int = COMPRESSION_ZSTD_FRAME_V1
    flags: int = (
        BEGIN_FLAG_ZSTD_CONTENT_CHECKSUM | BEGIN_FLAG_OPTIONAL_COLUMNS
    )
    column_flags: int = REQUIRED_COLUMNS
    max_block_bytes: int = DEFAULT_BLOCK_BYTES

    def validate(self) -> None:
        if (
            self.schema_version != SCHEMA_VERSION
            or self.codec != CODEC_COLUMNAR_DELTA_V1
            or self.compression != COMPRESSION_ZSTD_FRAME_V1
        ):
            raise ValueError("unsupported HISTORY_BEGIN schema or codec")
        required_flags = (
            BEGIN_FLAG_ZSTD_CONTENT_CHECKSUM | BEGIN_FLAG_OPTIONAL_COLUMNS
        )
        if (
            self.flags & ~BEGIN_FLAG_KNOWN_MASK
            or self.flags & required_flags != required_flags
        ):
            raise ValueError("invalid HISTORY_BEGIN flags")
        if not 0 < self.period <= 0xFF:
            raise ValueError("invalid HISTORY_BEGIN period")
        if (
            self.column_flags & ~COLUMN_KNOWN_MASK
            or self.column_flags & REQUIRED_COLUMNS != REQUIRED_COLUMNS
        ):
            raise ValueError("invalid HISTORY_BEGIN columns")
        _validate_block_size(self.max_block_bytes)
        if not 0 <= self.estimated_rows <= 0xFFFFFFFFFFFFFFFF:
            raise ValueError("invalid HISTORY_BEGIN estimated_rows")

    def encode(self) -> bytes:
        self.validate()
        return BEGIN_STRUCT.pack(
            self.request_id,
            self.schema_version,
            self.codec,
            self.compression,
            self.flags,
            self.symbol_id,
            self.period,
            self.column_flags,
            self.max_block_bytes,
            self.estimated_rows,
            self.start_time_ms,
            self.end_time_ms,
        )

    @classmethod
    def decode(cls, payload: bytes) -> "HistoryBegin":
        if len(payload) != BEGIN_STRUCT.size:
            raise ValueError("HISTORY_BEGIN payload must be exactly 48 bytes")
        if payload[13:16] != b"\x00\x00\x00":
            raise ValueError("HISTORY_BEGIN reserved fields must be zero")
        (
            request_id,
            schema_version,
            codec,
            compression,
            flags,
            symbol_id,
            period,
            column_flags,
            max_block_bytes,
            estimated_rows,
            start_time_ms,
            end_time_ms,
        ) = BEGIN_STRUCT.unpack(payload)
        result = cls(
            request_id=request_id,
            symbol_id=symbol_id,
            period=period,
            estimated_rows=estimated_rows,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            schema_version=schema_version,
            codec=codec,
            compression=compression,
            flags=flags,
            column_flags=column_flags,
            max_block_bytes=max_block_bytes,
        )
        result.validate()
        return result


@dataclass(frozen=True)
class HistoryDataHeader:
    request_id: int
    chunk_seq: int
    row_count: int
    uncompressed_size: int
    compressed_size: int
    first_timestamp_ms: int
    last_timestamp_ms: int
    checksum: int = 0
    flags: int = 0

    def validate(self, max_block_bytes: int = MAX_BLOCK_BYTES) -> None:
        _validate_block_size(max_block_bytes)
        if (
            self.row_count <= 0
            or self.uncompressed_size <= 0
            or self.compressed_size <= 0
            or self.uncompressed_size > max_block_bytes
            or self.compressed_size > max_block_bytes
        ):
            raise ValueError("invalid HISTORY_DATA sizes")
        if self.first_timestamp_ms > self.last_timestamp_ms:
            raise ValueError("invalid HISTORY_DATA timestamp range")
        if self.flags & ~DATA_FLAG_CRC32C_PRESENT:
            raise ValueError("invalid HISTORY_DATA flags")
        crc_present = bool(self.flags & DATA_FLAG_CRC32C_PRESENT)
        if crc_present == (self.checksum == 0):
            raise ValueError("invalid HISTORY_DATA checksum")

    def encode(self, max_block_bytes: int = MAX_BLOCK_BYTES) -> bytes:
        self.validate(max_block_bytes)
        return DATA_HEADER_STRUCT.pack(
            self.request_id,
            self.chunk_seq,
            self.row_count,
            self.uncompressed_size,
            self.compressed_size,
            self.first_timestamp_ms,
            self.last_timestamp_ms,
            self.checksum,
            self.flags,
            0,
        )

    @classmethod
    def decode(
        cls, payload: bytes, max_block_bytes: int = MAX_BLOCK_BYTES
    ) -> Tuple["HistoryDataHeader", memoryview]:
        if len(payload) < DATA_HEADER_STRUCT.size:
            raise ValueError("HISTORY_DATA payload is shorter than 44 bytes")
        fields = DATA_HEADER_STRUCT.unpack_from(payload)
        if fields[-1] != 0:
            raise ValueError("HISTORY_DATA reserved field must be zero")
        result = cls(*fields[:-1])
        result.validate(max_block_bytes)
        compressed = memoryview(payload)[DATA_HEADER_STRUCT.size :]
        if len(compressed) != result.compressed_size:
            raise ValueError("HISTORY_DATA compressed size does not match payload")
        return result, compressed


@dataclass(frozen=True)
class HistoryEnd:
    request_id: int
    actual_total_rows: int
    actual_uncompressed_bytes: int
    actual_compressed_bytes: int
    chunk_count: int
    last_chunk_seq: int
    status: int = 0

    def validate(self) -> None:
        valid_empty = (
            self.chunk_count == 0
            and self.last_chunk_seq == 0xFFFFFFFF
            and self.actual_total_rows == 0
            and self.actual_uncompressed_bytes == 0
            and self.actual_compressed_bytes == 0
        )
        valid_nonempty = (
            self.chunk_count > 0
            and self.last_chunk_seq == self.chunk_count - 1
            and self.actual_total_rows > 0
            and self.actual_uncompressed_bytes > 0
            and self.actual_compressed_bytes > 0
        )
        if self.status != 0 or not (valid_empty or valid_nonempty):
            raise ValueError("invalid HISTORY_END counters")

    def encode(self) -> bytes:
        self.validate()
        return END_STRUCT.pack(
            self.request_id,
            self.actual_total_rows,
            self.actual_uncompressed_bytes,
            self.actual_compressed_bytes,
            self.chunk_count,
            self.last_chunk_seq,
            self.status,
        )

    @classmethod
    def decode(cls, payload: bytes) -> "HistoryEnd":
        if len(payload) != END_STRUCT.size:
            raise ValueError("HISTORY_END payload must be exactly 40 bytes")
        result = cls(*END_STRUCT.unpack(payload))
        result.validate()
        return result


class ErrorCode(IntEnum):
    UNSUPPORTED = 1
    INVALID_REQUEST = 2
    DDB_ERROR = 3
    ENCODE_ERROR = 4
    CANCELLED = 5
    TIMEOUT = 6
    BACKPRESSURE = 7
    UPSTREAM_LOST = 8
    INTERNAL = 9
    INTEGRITY_ERROR = 10


@dataclass(frozen=True)
class HistoryError:
    request_id: int
    error_code: ErrorCode
    flags: int = 0
    last_chunk_seq: int = 0xFFFFFFFF
    delivered_rows: int = 0
    message: str = ""

    MAX_MESSAGE_BYTES = 4096

    def encode(self) -> bytes:
        try:
            error_code = ErrorCode(self.error_code)
        except ValueError as exc:
            raise ValueError("invalid HISTORY_ERROR code") from exc
        message = self.message.encode("utf-8")
        if self.flags & ~1 or len(message) > self.MAX_MESSAGE_BYTES:
            raise ValueError("invalid HISTORY_ERROR flags or message")
        return ERROR_HEADER_STRUCT.pack(
            self.request_id,
            int(error_code),
            self.flags,
            self.last_chunk_seq,
            self.delivered_rows,
            len(message),
            0,
        ) + message

    @classmethod
    def decode(cls, payload: bytes) -> "HistoryError":
        if len(payload) < ERROR_HEADER_STRUCT.size:
            raise ValueError("HISTORY_ERROR payload is shorter than 24 bytes")
        (
            request_id,
            error_code,
            flags,
            last_chunk_seq,
            delivered_rows,
            message_length,
            reserved,
        ) = ERROR_HEADER_STRUCT.unpack_from(payload)
        if (
            reserved != 0
            or flags & ~1
            or message_length > cls.MAX_MESSAGE_BYTES
            or len(payload) != ERROR_HEADER_STRUCT.size + message_length
        ):
            raise ValueError("invalid HISTORY_ERROR fields")
        try:
            code = ErrorCode(error_code)
        except ValueError as exc:
            raise ValueError("invalid HISTORY_ERROR code") from exc
        try:
            message = payload[ERROR_HEADER_STRUCT.size :].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid HISTORY_ERROR UTF-8") from exc
        return cls(
            request_id=request_id,
            error_code=code,
            flags=flags,
            last_chunk_seq=last_chunk_seq,
            delivered_rows=delivered_rows,
            message=message,
        )


class CancelReason(IntEnum):
    CLIENT_CANCEL = 1
    CLIENT_DISCONNECT = 2
    TIMEOUT = 3
    TOKEN_INVALID = 4
    BACKPRESSURE = 5
    SHUTDOWN = 6


@dataclass(frozen=True)
class HistoryCancel:
    request_id: int
    reason: CancelReason
    last_seen_seq: int = 0xFFFFFFFF

    def encode(self) -> bytes:
        try:
            reason = CancelReason(self.reason)
        except ValueError as exc:
            raise ValueError("invalid HISTORY_CANCEL reason") from exc
        return CANCEL_STRUCT.pack(self.request_id, int(reason), 0, self.last_seen_seq)

    @classmethod
    def decode(cls, payload: bytes) -> "HistoryCancel":
        if len(payload) != CANCEL_STRUCT.size:
            raise ValueError("HISTORY_CANCEL payload must be exactly 12 bytes")
        request_id, reason, reserved, last_seen_seq = CANCEL_STRUCT.unpack(payload)
        if reserved != 0:
            raise ValueError("HISTORY_CANCEL reserved field must be zero")
        try:
            decoded_reason = CancelReason(reason)
        except ValueError as exc:
            raise ValueError("invalid HISTORY_CANCEL reason") from exc
        return cls(request_id, decoded_reason, last_seen_seq)


@dataclass(frozen=True)
class HistoryWindowUpdate:
    request_id: int
    grant_bytes: int
    received_through_seq: int = 0xFFFFFFFF

    def encode(self) -> bytes:
        if not 0 < self.grant_bytes <= MAX_WINDOW_BYTES:
            raise ValueError("invalid HISTORY_WINDOW_UPDATE grant")
        return WINDOW_UPDATE_STRUCT.pack(
            self.request_id, self.grant_bytes, self.received_through_seq
        )

    @classmethod
    def decode(cls, payload: bytes) -> "HistoryWindowUpdate":
        if len(payload) != WINDOW_UPDATE_STRUCT.size:
            raise ValueError(
                "HISTORY_WINDOW_UPDATE payload must be exactly 12 bytes"
            )
        result = cls(*WINDOW_UPDATE_STRUCT.unpack(payload))
        if not 0 < result.grant_bytes <= MAX_WINDOW_BYTES:
            raise ValueError("invalid HISTORY_WINDOW_UPDATE grant")
        return result


@dataclass(frozen=True)
class HistoryColumnsHeader:
    row_count: int
    timestamp_bytes: int
    schema_version: int = SCHEMA_VERSION
    timestamp_encoding: int = 2
    column_flags: int = REQUIRED_COLUMNS

    def validate(self) -> None:
        if (
            self.schema_version != SCHEMA_VERSION
            or self.timestamp_encoding != 2
            or self.column_flags & ~COLUMN_KNOWN_MASK
            or self.column_flags & REQUIRED_COLUMNS != REQUIRED_COLUMNS
            or self.row_count <= 0
            or self.timestamp_bytes < 8
        ):
            raise ValueError("invalid HISTORY_COLUMNS_V1 header")

    def encode(self) -> bytes:
        self.validate()
        return COLUMNS_HEADER_STRUCT.pack(
            self.schema_version,
            self.timestamp_encoding,
            self.column_flags,
            self.row_count,
            self.timestamp_bytes,
            0,
        )

    @classmethod
    def decode(cls, payload: bytes) -> "HistoryColumnsHeader":
        if len(payload) < COLUMNS_HEADER_STRUCT.size:
            raise ValueError("HISTORY_COLUMNS_V1 payload is shorter than 16 bytes")
        (
            schema_version,
            timestamp_encoding,
            column_flags,
            row_count,
            timestamp_bytes,
            reserved,
        ) = COLUMNS_HEADER_STRUCT.unpack_from(payload)
        if reserved != 0:
            raise ValueError("HISTORY_COLUMNS_V1 reserved field must be zero")
        result = cls(
            row_count=row_count,
            timestamp_bytes=timestamp_bytes,
            schema_version=schema_version,
            timestamp_encoding=timestamp_encoding,
            column_flags=column_flags,
        )
        result.validate()
        bytes_per_row = 4 * 4 + 8
        if column_flags & COLUMN_TURNOVER:
            bytes_per_row += 8
        if column_flags & COLUMN_OPEN_INTEREST:
            bytes_per_row += 8
        expected = (
            COLUMNS_HEADER_STRUCT.size
            + timestamp_bytes
            + row_count * bytes_per_row
        )
        if len(payload) != expected:
            raise ValueError("HISTORY_COLUMNS_V1 payload length mismatch")
        return result
