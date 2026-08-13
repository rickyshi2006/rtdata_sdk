"""SESSION_REHOME v1 control-frame codec."""

import struct
from dataclasses import dataclass
from enum import IntEnum


SCHEMA_VERSION = 1
FLAGS = 0
PREFIX_STRUCT = struct.Struct("!BBHQH")
MAX_TARGET_NODE_ID_BYTES = 128


class RehomeReason(IntEnum):
    CLUSTER_FAILBACK = 1
    NODE_DEGRADED = 2


@dataclass(frozen=True)
class RehomeRequest:
    migration_id: int
    target_node_id: str
    reason: RehomeReason = RehomeReason.CLUSTER_FAILBACK
    flags: int = FLAGS
    schema_version: int = SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("unsupported session rehome schema version")
        try:
            RehomeReason(self.reason)
        except ValueError as exc:
            raise ValueError("invalid session rehome reason") from exc
        if self.flags != FLAGS:
            raise ValueError("invalid session rehome flags")
        if not 0 < self.migration_id <= 0xFFFFFFFFFFFFFFFF:
            raise ValueError("session rehome migration id must be non-zero")
        if not isinstance(self.target_node_id, str):
            raise ValueError("session rehome target node id must be a string")
        target = self.target_node_id.encode("utf-8")
        if not target or len(target) > MAX_TARGET_NODE_ID_BYTES or b"\x00" in target:
            raise ValueError("invalid session rehome target node id")

    def encode(self) -> bytes:
        self.validate()
        target = self.target_node_id.encode("utf-8")
        return PREFIX_STRUCT.pack(
            self.schema_version,
            int(self.reason),
            self.flags,
            self.migration_id,
            len(target),
        ) + target

    @classmethod
    def decode(cls, payload: bytes) -> "RehomeRequest":
        if len(payload) < PREFIX_STRUCT.size:
            raise ValueError("session rehome payload is too short")
        schema_version, reason, flags, migration_id, target_len = (
            PREFIX_STRUCT.unpack(payload[:PREFIX_STRUCT.size])
        )
        if target_len > MAX_TARGET_NODE_ID_BYTES or (
            len(payload) != PREFIX_STRUCT.size + target_len
        ):
            raise ValueError("invalid session rehome target node length")
        try:
            target_node_id = payload[PREFIX_STRUCT.size:].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid session rehome target node encoding") from exc
        try:
            decoded_reason = RehomeReason(reason)
        except ValueError as exc:
            raise ValueError("invalid session rehome reason") from exc
        result = cls(
            schema_version=schema_version,
            reason=decoded_reason,
            flags=flags,
            migration_id=migration_id,
            target_node_id=target_node_id,
        )
        result.validate()
        return result
