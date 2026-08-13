"""Connection-lifecycle capability negotiation wire contract."""

import struct
from dataclasses import dataclass
from enum import IntEnum


SCHEMA_VERSION = 1
WIRE_STRUCT = struct.Struct("!BBHII")

FEATURE_REHOME = 1 << 0
KNOWN_FEATURE_MASK = FEATURE_REHOME
FLAG_DISCOVERY_REQUIRED = 1 << 0
KNOWN_FLAG_MASK = FLAG_DISCOVERY_REQUIRED


class Role(IntEnum):
    CLOUD = 2
    RTDATA = 3


@dataclass(frozen=True)
class SessionCapabilities:
    role: Role
    flags: int = 0
    feature_mask: int = 0
    reserved: int = 0
    schema_version: int = SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("unsupported session capability schema version")
        try:
            Role(self.role)
        except ValueError as exc:
            raise ValueError("invalid session capability role") from exc
        if self.flags & ~KNOWN_FLAG_MASK:
            raise ValueError("invalid session capability flags")
        if self.feature_mask & ~KNOWN_FEATURE_MASK:
            raise ValueError("invalid session capability feature mask")
        if self.reserved != 0:
            raise ValueError("session capability reserved field must be zero")

    def encode(self) -> bytes:
        self.validate()
        return WIRE_STRUCT.pack(
            self.schema_version,
            int(self.role),
            self.flags,
            self.feature_mask,
            self.reserved,
        )

    @classmethod
    def decode(cls, payload: bytes) -> "SessionCapabilities":
        if len(payload) != WIRE_STRUCT.size:
            raise ValueError("session capability payload must be exactly 12 bytes")
        schema_version, role, flags, feature_mask, reserved = (
            WIRE_STRUCT.unpack(payload)
        )
        try:
            decoded_role = Role(role)
        except ValueError as exc:
            raise ValueError("invalid session capability role") from exc
        result = cls(
            schema_version=schema_version,
            role=decoded_role,
            flags=flags,
            feature_mask=feature_mask,
            reserved=reserved,
        )
        result.validate()
        return result


def rehome_capabilities(role: Role = Role.RTDATA) -> SessionCapabilities:
    return SessionCapabilities(
        role=role,
        flags=FLAG_DISCOVERY_REQUIRED,
        feature_mask=FEATURE_REHOME,
    )


def rehome_eligible(value: SessionCapabilities) -> bool:
    try:
        value.validate()
    except ValueError:
        return False
    return bool(
        value.feature_mask & FEATURE_REHOME
        and value.flags & FLAG_DISCOVERY_REQUIRED
    )
