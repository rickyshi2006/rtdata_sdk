"""SESSION_REHOME v1 control-frame codec."""

import struct
from dataclasses import dataclass
from enum import IntEnum


SCHEMA_VERSION = 1
FLAGS = 0
FLAG_HANDOFF_TICKET = 1 << 0
KNOWN_FLAGS = FLAG_HANDOFF_TICKET
PREFIX_STRUCT = struct.Struct("!BBHQH")
MAX_TARGET_NODE_ID_BYTES = 128
MAX_HANDOFF_TICKET_BYTES = 8192


class RehomeReason(IntEnum):
    CLUSTER_FAILBACK = 1
    NODE_DEGRADED = 2


@dataclass(frozen=True)
class RehomeRequest:
    migration_id: int
    target_node_id: str
    reason: RehomeReason = RehomeReason.CLUSTER_FAILBACK
    flags: int = FLAGS
    handoff_ticket: bytes = b""
    schema_version: int = SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("unsupported session rehome schema version")
        try:
            RehomeReason(self.reason)
        except ValueError as exc:
            raise ValueError("invalid session rehome reason") from exc
        if self.flags & ~KNOWN_FLAGS:
            raise ValueError("invalid session rehome flags")
        has_ticket = bool(self.flags & FLAG_HANDOFF_TICKET)
        if has_ticket != bool(self.handoff_ticket):
            raise ValueError("invalid session rehome handoff ticket")
        if len(self.handoff_ticket) > MAX_HANDOFF_TICKET_BYTES:
            raise ValueError("session rehome handoff ticket is too large")
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
        payload = PREFIX_STRUCT.pack(
            self.schema_version,
            int(self.reason),
            self.flags,
            self.migration_id,
            len(target),
        ) + target
        if self.handoff_ticket:
            payload += struct.pack("!I", len(self.handoff_ticket))
            payload += self.handoff_ticket
        return payload

    @classmethod
    def decode(cls, payload: bytes) -> "RehomeRequest":
        if len(payload) < PREFIX_STRUCT.size:
            raise ValueError("session rehome payload is too short")
        schema_version, reason, flags, migration_id, target_len = (
            PREFIX_STRUCT.unpack(payload[:PREFIX_STRUCT.size])
        )
        base_len = PREFIX_STRUCT.size + target_len
        if target_len > MAX_TARGET_NODE_ID_BYTES or len(payload) < base_len:
            raise ValueError("invalid session rehome target node length")
        try:
            target_node_id = payload[PREFIX_STRUCT.size:base_len].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid session rehome target node encoding") from exc
        try:
            decoded_reason = RehomeReason(reason)
        except ValueError as exc:
            raise ValueError("invalid session rehome reason") from exc
        handoff_ticket = b""
        if flags & FLAG_HANDOFF_TICKET:
            if len(payload) < base_len + 4:
                raise ValueError("session rehome handoff ticket length is missing")
            ticket_len = struct.unpack("!I", payload[base_len:base_len + 4])[0]
            if (ticket_len == 0 or ticket_len > MAX_HANDOFF_TICKET_BYTES or
                    len(payload) != base_len + 4 + ticket_len):
                raise ValueError("invalid session rehome handoff ticket length")
            handoff_ticket = payload[base_len + 4:]
        elif len(payload) != base_len:
            raise ValueError("unexpected session rehome extension")
        result = cls(
            schema_version=schema_version,
            reason=decoded_reason,
            flags=flags,
            migration_id=migration_id,
            target_node_id=target_node_id,
            handoff_ticket=handoff_ticket,
        )
        result.validate()
        return result
