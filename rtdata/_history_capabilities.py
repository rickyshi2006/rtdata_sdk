"""History V2 capability negotiation wire contract."""

import struct
from dataclasses import dataclass
from enum import IntEnum


SCHEMA_VERSION = 1
WIRE_STRUCT = struct.Struct("!BBHHHIII")

PROTOCOL_V1 = 0x0001
PROTOCOL_V2 = 0x0002
PROTOCOL_KNOWN_MASK = PROTOCOL_V1 | PROTOCOL_V2

CODEC_V1_ROW48 = 0x0001
CODEC_COLUMNAR_DELTA_V1 = 0x0002
CODEC_KNOWN_MASK = CODEC_V1_ROW48 | CODEC_COLUMNAR_DELTA_V1

COMPRESSION_NONE = 0x0001
COMPRESSION_ZSTD = 0x0002
COMPRESSION_LZ4 = 0x0004
COMPRESSION_KNOWN_MASK = COMPRESSION_NONE | COMPRESSION_ZSTD | COMPRESSION_LZ4

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

SCHEMA_V1_KLINE48 = 0x00000001
SCHEMA_HISTORY_COLUMNS_V1 = 0x00000002
COLUMN_SCHEMA_KNOWN_MASK = SCHEMA_V1_KLINE48 | SCHEMA_HISTORY_COLUMNS_V1

MIN_BLOCK_BYTES = 64 * 1024
MAX_BLOCK_BYTES = 4 * 1024 * 1024


class CapabilityRole(IntEnum):
    UPCLOUD = 1
    CLOUD = 2
    RTDATA = 3


@dataclass(frozen=True)
class HistoryCapabilities:
    role: CapabilityRole
    history_protocol_mask: int = PROTOCOL_V1
    codec_mask: int = CODEC_V1_ROW48
    compression_mask: int = COMPRESSION_NONE
    feature_mask: int = 0
    max_block_bytes: int = 0
    column_schema_mask: int = SCHEMA_V1_KLINE48
    schema_version: int = SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("unsupported history capability schema version")
        try:
            CapabilityRole(self.role)
        except ValueError as exc:
            raise ValueError("invalid history capability role") from exc
        if (
            not self.history_protocol_mask & PROTOCOL_V1
            or self.history_protocol_mask & ~PROTOCOL_KNOWN_MASK
        ):
            raise ValueError("invalid history protocol mask")
        if (
            not self.codec_mask & CODEC_V1_ROW48
            or self.codec_mask & ~CODEC_KNOWN_MASK
        ):
            raise ValueError("invalid history codec mask")
        if (
            not self.compression_mask & COMPRESSION_NONE
            or self.compression_mask & ~COMPRESSION_KNOWN_MASK
        ):
            raise ValueError("invalid history compression mask")
        if self.feature_mask & ~FEATURE_KNOWN_MASK:
            raise ValueError("invalid history feature mask")
        if (
            not self.column_schema_mask & SCHEMA_V1_KLINE48
            or self.column_schema_mask & ~COLUMN_SCHEMA_KNOWN_MASK
        ):
            raise ValueError("invalid history column schema mask")
        if self.history_protocol_mask & PROTOCOL_V2:
            if not MIN_BLOCK_BYTES <= self.max_block_bytes <= MAX_BLOCK_BYTES:
                raise ValueError("invalid history V2 block size")
        elif self.max_block_bytes != 0:
            raise ValueError("V1-only history capabilities must not set block size")

    def encode(self) -> bytes:
        self.validate()
        return WIRE_STRUCT.pack(
            self.schema_version,
            int(self.role),
            self.history_protocol_mask,
            self.codec_mask,
            self.compression_mask,
            self.feature_mask,
            self.max_block_bytes,
            self.column_schema_mask,
        )

    @classmethod
    def decode(cls, payload: bytes) -> "HistoryCapabilities":
        if len(payload) != WIRE_STRUCT.size:
            raise ValueError("history capability payload must be exactly 20 bytes")
        (
            schema_version,
            role,
            protocol_mask,
            codec_mask,
            compression_mask,
            feature_mask,
            max_block_bytes,
            column_schema_mask,
        ) = WIRE_STRUCT.unpack(payload)
        try:
            decoded_role = CapabilityRole(role)
        except ValueError as exc:
            raise ValueError("invalid history capability role") from exc
        result = cls(
            schema_version=schema_version,
            role=decoded_role,
            history_protocol_mask=protocol_mask,
            codec_mask=codec_mask,
            compression_mask=compression_mask,
            feature_mask=feature_mask,
            max_block_bytes=max_block_bytes,
            column_schema_mask=column_schema_mask,
        )
        result.validate()
        return result


def v1_capabilities(role: CapabilityRole) -> HistoryCapabilities:
    return HistoryCapabilities(role=role)


def v2_capabilities(
    role: CapabilityRole,
    max_block_bytes: int = 256 * 1024,
) -> HistoryCapabilities:
    result = HistoryCapabilities(
        role=role,
        history_protocol_mask=PROTOCOL_KNOWN_MASK,
        codec_mask=CODEC_KNOWN_MASK,
        compression_mask=COMPRESSION_NONE | COMPRESSION_ZSTD,
        feature_mask=(
            FEATURE_WINDOW_UPDATE | FEATURE_CANCEL | FEATURE_OPTIONAL_COLUMNS
        ),
        max_block_bytes=max_block_bytes,
        column_schema_mask=COLUMN_SCHEMA_KNOWN_MASK,
    )
    result.validate()
    return result


def intersect_capabilities(
    left: HistoryCapabilities,
    right: HistoryCapabilities,
    result_role: CapabilityRole,
) -> HistoryCapabilities:
    left.validate()
    right.validate()
    protocol_mask = left.history_protocol_mask & right.history_protocol_mask
    if protocol_mask & PROTOCOL_V2:
        if left.max_block_bytes == 0:
            max_block_bytes = right.max_block_bytes
        elif right.max_block_bytes == 0:
            max_block_bytes = left.max_block_bytes
        else:
            max_block_bytes = min(left.max_block_bytes, right.max_block_bytes)
    else:
        max_block_bytes = 0
    result = HistoryCapabilities(
        role=result_role,
        history_protocol_mask=protocol_mask,
        codec_mask=left.codec_mask & right.codec_mask,
        compression_mask=left.compression_mask & right.compression_mask,
        feature_mask=left.feature_mask & right.feature_mask,
        max_block_bytes=max_block_bytes,
        column_schema_mask=left.column_schema_mask & right.column_schema_mask,
    )
    result.validate()
    return result


def v2_eligible(capabilities: HistoryCapabilities) -> bool:
    try:
        capabilities.validate()
    except ValueError:
        return False
    required_features = (
        FEATURE_WINDOW_UPDATE | FEATURE_CANCEL | FEATURE_OPTIONAL_COLUMNS
    )
    return bool(
        capabilities.history_protocol_mask & PROTOCOL_V2
        and capabilities.codec_mask & CODEC_COLUMNAR_DELTA_V1
        and capabilities.compression_mask & COMPRESSION_ZSTD
        and capabilities.feature_mask & required_features == required_features
        and capabilities.column_schema_mask & SCHEMA_HISTORY_COLUMNS_V1
    )
