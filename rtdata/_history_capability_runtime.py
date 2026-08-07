"""Connection-scoped History V2 capability negotiation state."""

import threading
from dataclasses import dataclass
from typing import Callable, Optional

from . import _history_capabilities as capabilities


@dataclass(frozen=True)
class HistoryCapabilitySnapshot:
    state: str
    negotiated: bool
    capabilities: capabilities.HistoryCapabilities
    v2_eligible: bool
    generation: int
    fallback_reason: str


class HistoryCapabilityRuntime:
    def __init__(
        self,
        *,
        advertise: bool = False,
        default_enabled: bool = False,
        max_block_bytes: int = 256 * 1024,
        ack_timeout: float = 1.0,
    ):
        if not capabilities.MIN_BLOCK_BYTES <= max_block_bytes <= capabilities.MAX_BLOCK_BYTES:
            raise ValueError(
                "history_v2_max_block_bytes must be between "
                f"{capabilities.MIN_BLOCK_BYTES} and {capabilities.MAX_BLOCK_BYTES}"
            )
        if ack_timeout <= 0:
            raise ValueError("history_capability_ack_timeout must be positive")

        self.advertise = advertise
        self.default_enabled = default_enabled
        self.max_block_bytes = max_block_bytes
        self.ack_timeout = ack_timeout
        self._local_offer = capabilities.v2_capabilities(
            capabilities.CapabilityRole.RTDATA,
            max_block_bytes=max_block_bytes,
        )
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._generation = 0
        self._state = "disabled" if not advertise else "idle"
        self._negotiated = False
        self._capabilities = capabilities.v1_capabilities(
            capabilities.CapabilityRole.CLOUD
        )
        self._fallback_reason = ""

    @property
    def offer_payload(self) -> bytes:
        return self._local_offer.encode()

    def reset(self, reason: str = "") -> None:
        with self._lock:
            self._cancel_timer_locked()
            self._generation += 1
            self._state = "disabled" if not self.advertise else "idle"
            self._negotiated = False
            self._capabilities = capabilities.v1_capabilities(
                capabilities.CapabilityRole.CLOUD
            )
            self._fallback_reason = reason

    def mark_peer_unsupported(self) -> None:
        with self._lock:
            self._set_fallback_locked(
                "peer_does_not_advertise_history_capability_v1",
                advance_generation=True,
            )

    def begin_offer(self, send_payload: Callable[[bytes], bool]) -> bool:
        if not self.advertise:
            return False

        with self._lock:
            self._cancel_timer_locked()
            self._generation += 1
            generation = self._generation
            self._state = "waiting_ack"
            self._negotiated = False
            self._capabilities = capabilities.v1_capabilities(
                capabilities.CapabilityRole.CLOUD
            )
            self._fallback_reason = ""

        sent = False
        try:
            sent = bool(send_payload(self.offer_payload))
        except Exception:
            sent = False

        with self._lock:
            if generation != self._generation:
                return sent
            if not sent:
                self._set_fallback_locked("offer_send_failed")
                return False
            if self._state != "waiting_ack":
                return True
            timer = threading.Timer(
                self.ack_timeout,
                self._handle_timeout,
                args=(generation,),
            )
            timer.daemon = True
            self._timer = timer
            timer.start()
        return True

    def handle_ack(self, payload: bytes) -> bool:
        try:
            acknowledged = capabilities.HistoryCapabilities.decode(payload)
            error = self._validate_ack(acknowledged)
        except ValueError as exc:
            acknowledged = None
            error = str(exc)

        with self._lock:
            if self._state != "waiting_ack":
                return False
            self._cancel_timer_locked()
            if acknowledged is None or error:
                self._set_fallback_locked(f"invalid_ack: {error}")
                return True
            self._state = "negotiated"
            self._negotiated = True
            self._capabilities = acknowledged
            self._fallback_reason = ""
            return True

    def snapshot(self) -> HistoryCapabilitySnapshot:
        with self._lock:
            negotiated = self._negotiated
            current = self._capabilities
            return HistoryCapabilitySnapshot(
                state=self._state,
                negotiated=negotiated,
                capabilities=current,
                v2_eligible=(
                    negotiated and capabilities.v2_eligible(current)
                ),
                generation=self._generation,
                fallback_reason=self._fallback_reason,
            )

    def _validate_ack(
        self,
        acknowledged: capabilities.HistoryCapabilities,
    ) -> str:
        if acknowledged.role != capabilities.CapabilityRole.CLOUD:
            return "unexpected capability role"

        masks = (
            (acknowledged.history_protocol_mask, self._local_offer.history_protocol_mask),
            (acknowledged.codec_mask, self._local_offer.codec_mask),
            (acknowledged.compression_mask, self._local_offer.compression_mask),
            (acknowledged.feature_mask, self._local_offer.feature_mask),
            (acknowledged.column_schema_mask, self._local_offer.column_schema_mask),
        )
        if any(value & ~offered for value, offered in masks):
            return "ACK contains capabilities not present in OFFER"
        if (
            acknowledged.history_protocol_mask & capabilities.PROTOCOL_V2
            and acknowledged.max_block_bytes > self._local_offer.max_block_bytes
        ):
            return "ACK block size exceeds OFFER"
        return ""

    def _handle_timeout(self, generation: int) -> None:
        with self._lock:
            if generation != self._generation or self._state != "waiting_ack":
                return
            self._timer = None
            self._set_fallback_locked("ack_timeout")

    def _set_fallback_locked(
        self,
        reason: str,
        *,
        advance_generation: bool = False,
    ) -> None:
        self._cancel_timer_locked()
        if advance_generation:
            self._generation += 1
        self._state = "fallback"
        self._negotiated = False
        self._capabilities = capabilities.v1_capabilities(
            capabilities.CapabilityRole.CLOUD
        )
        self._fallback_reason = reason

    def _cancel_timer_locked(self) -> None:
        timer = self._timer
        self._timer = None
        if timer is not None:
            timer.cancel()
