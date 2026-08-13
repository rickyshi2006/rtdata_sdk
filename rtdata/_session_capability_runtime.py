"""Connection-scoped session rehome capability state."""

import threading
from dataclasses import dataclass
from typing import Callable, Optional

from . import _session_capabilities as capabilities


@dataclass(frozen=True)
class SessionCapabilitySnapshot:
    state: str
    negotiated: bool
    rehome_eligible: bool
    fallback_reason: str


class SessionCapabilityRuntime:
    def __init__(self, *, advertise: bool = False, ack_timeout: float = 1.0):
        if ack_timeout <= 0:
            raise ValueError("session_capability_ack_timeout must be positive")
        self.advertise = advertise
        self.ack_timeout = ack_timeout
        self._offer = capabilities.rehome_capabilities()
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._generation = 0
        self._state = "disabled" if not advertise else "idle"
        self._negotiated = False
        self._fallback_reason = ""

    @property
    def offer_payload(self) -> bytes:
        return self._offer.encode()

    def reset(self, reason: str = "") -> None:
        with self._lock:
            self._cancel_timer_locked()
            self._generation += 1
            self._state = "disabled" if not self.advertise else "idle"
            self._negotiated = False
            self._fallback_reason = reason

    def mark_peer_unsupported(self) -> None:
        with self._lock:
            self._set_fallback_locked(
                "peer_does_not_advertise_session_rehome_v1"
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
            self._fallback_reason = ""
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
                self.ack_timeout, self._handle_timeout, args=(generation,)
            )
            timer.daemon = True
            self._timer = timer
            timer.start()
        return True

    def handle_ack(self, payload: bytes) -> bool:
        try:
            acknowledged = capabilities.SessionCapabilities.decode(payload)
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
            self._fallback_reason = ""
            return True

    def snapshot(self) -> SessionCapabilitySnapshot:
        with self._lock:
            return SessionCapabilitySnapshot(
                state=self._state,
                negotiated=self._negotiated,
                rehome_eligible=self._negotiated,
                fallback_reason=self._fallback_reason,
            )

    def _validate_ack(
        self, acknowledged: capabilities.SessionCapabilities
    ) -> str:
        if acknowledged.role != capabilities.Role.CLOUD:
            return "unexpected capability role"
        if acknowledged.flags & ~self._offer.flags:
            return "ACK contains flags not present in OFFER"
        if acknowledged.feature_mask & ~self._offer.feature_mask:
            return "ACK contains features not present in OFFER"
        if not capabilities.rehome_eligible(acknowledged):
            return "ACK does not enable safe rehome"
        return ""

    def _handle_timeout(self, generation: int) -> None:
        with self._lock:
            if generation != self._generation or self._state != "waiting_ack":
                return
            self._timer = None
            self._set_fallback_locked("ack_timeout")

    def _set_fallback_locked(self, reason: str) -> None:
        self._cancel_timer_locked()
        self._state = "fallback"
        self._negotiated = False
        self._fallback_reason = reason

    def _cancel_timer_locked(self) -> None:
        timer = self._timer
        self._timer = None
        if timer is not None:
            timer.cancel()
