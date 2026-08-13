"""Signed SESSION_HANDOFF_OFFER ticket codec."""

import hashlib
import hmac
import struct
import time
from dataclasses import dataclass


DOMAIN = b"cloud-gateway/session-handoff/v1"
PREFIX = struct.Struct("!BBHQQQHHH8x")
SIGNATURE_SIZE = 32
MAX_BYTES = 8192


@dataclass(frozen=True)
class HandoffTicket:
    token: str
    source_node_id: str
    target_node_id: str
    migration_id: int
    issued_at_ms: int
    expires_at_ms: int
    signature: bytes = b""

    def _unsigned(self) -> bytes:
        token = self.token.encode("utf-8")
        source = self.source_node_id.encode("utf-8")
        target = self.target_node_id.encode("utf-8")
        if not token or not source or not target:
            raise ValueError("handoff ticket text fields must not be empty")
        if any(len(value) > 4096 for value in (token,)) or any(
            len(value) > 128 for value in (source, target)
        ):
            raise ValueError("handoff ticket field is too large")
        return PREFIX.pack(
            1, 0, 0, self.issued_at_ms, self.expires_at_ms,
            self.migration_id, len(token), len(source), len(target)
        ) + token + source + target

    def sign(self, secret: str) -> "HandoffTicket":
        signature = hmac.new(
            secret.encode("utf-8"), DOMAIN + self._unsigned(), hashlib.sha256
        ).digest()
        return HandoffTicket(**{**self.__dict__, "signature": signature})

    def encode(self) -> bytes:
        if len(self.signature) != SIGNATURE_SIZE:
            raise ValueError("handoff ticket is not signed")
        value = self._unsigned() + self.signature
        if len(value) > MAX_BYTES:
            raise ValueError("handoff ticket is too large")
        return value

    @classmethod
    def decode(cls, payload: bytes) -> "HandoffTicket":
        if len(payload) < PREFIX.size + SIGNATURE_SIZE or len(payload) > MAX_BYTES:
            raise ValueError("invalid handoff ticket length")
        (version, flags, _reserved, issued, expires, migration,
         token_len, source_len, target_len) = PREFIX.unpack(payload[:PREFIX.size])
        if version != 1 or flags != 0:
            raise ValueError("invalid handoff ticket header")
        fields_len = PREFIX.size + token_len + source_len + target_len
        if fields_len + SIGNATURE_SIZE != len(payload):
            raise ValueError("invalid handoff ticket fields")
        offset = PREFIX.size
        values = []
        for size in (token_len, source_len, target_len):
            values.append(payload[offset:offset + size].decode("utf-8"))
            offset += size
        return cls(values[0], values[1], values[2], migration, issued, expires,
                   payload[fields_len:])


def make_ticket(token: str, source: str, target: str, migration_id: int,
                secret: str, ttl_ms: int = 30000) -> bytes:
    issued = int(time.time() * 1000)
    return HandoffTicket(token, source, target, migration_id, issued,
                         issued + ttl_ms).sign(secret).encode()
