"""Per-request History V2 receive state and integrity checks."""

import threading
from dataclasses import dataclass
from typing import List, Optional

from . import _history_v2_codec as codec
from . import _history_v2_protocol as protocol


UINT32_MAX = 0xFFFFFFFF
UINT64_MAX = 0xFFFFFFFFFFFFFFFF


@dataclass(frozen=True)
class HistoryStreamResult:
    terminal: bool = False
    error: str = ""
    window_grant_bytes: int = 0
    received_through_seq: int = UINT32_MAX


@dataclass(frozen=True)
class HistoryStreamSnapshot:
    active: bool
    begin_received: bool
    terminal_received: bool
    next_chunk_seq: int
    observed_rows: int
    observed_uncompressed_bytes: int
    observed_compressed_bytes: int
    last_timestamp_ms: Optional[int]


class HistoryV2RequestState:
    def __init__(
        self,
        *,
        request_id: int,
        options: protocol.RequestOptions,
        capability_generation: int,
        expected_symbol_id: int = 0,
        expected_period: int = 0,
    ):
        options.validate()
        self.request_id = request_id
        self.options = options
        self.capability_generation = capability_generation
        self.expected_symbol_id = expected_symbol_id
        self.expected_period = expected_period

        self._lock = threading.Lock()
        self._active = True
        self._begin_received = False
        self._terminal_received = False
        self._next_chunk_seq = 0
        self._observed_rows = 0
        self._observed_uncompressed_bytes = 0
        self._observed_compressed_bytes = 0
        self._last_timestamp_ms: Optional[int] = None
        self._rows: List[codec.DecodedKline] = []

    def handle_frame(self, msg_type: int, payload: bytes) -> HistoryStreamResult:
        if msg_type == protocol.MSG_HISTORY_BEGIN:
            return self._handle_begin(payload)
        if msg_type == protocol.MSG_HISTORY_DATA:
            return self._handle_data(payload)
        if msg_type == protocol.MSG_HISTORY_END:
            return self._handle_end(payload)
        if msg_type == protocol.MSG_HISTORY_ERROR:
            return self._handle_error(payload)
        raise ValueError("unexpected History V2 server frame")

    def _handle_begin(self, payload: bytes) -> HistoryStreamResult:
        begin = protocol.HistoryBegin.decode(payload)
        with self._lock:
            self._ensure_active_locked()
            if self._begin_received or self._terminal_received:
                raise ValueError("duplicate or late HISTORY_BEGIN")
            if begin.request_id != self.request_id:
                raise ValueError("HISTORY_BEGIN request_id mismatch")
            if begin.max_block_bytes != self.options.max_block_bytes:
                raise ValueError("HISTORY_BEGIN block size mismatch")
            if (
                self.expected_symbol_id
                and begin.symbol_id != self.expected_symbol_id
            ):
                raise ValueError("HISTORY_BEGIN symbol_id mismatch")
            if self.expected_period and begin.period != self.expected_period:
                raise ValueError("HISTORY_BEGIN period mismatch")
            crc_selected = bool(
                self.options.selected_features & protocol.FEATURE_CRC32C
            )
            crc_announced = bool(
                begin.flags & protocol.BEGIN_FLAG_CRC32C_DATA
            )
            if crc_selected != crc_announced:
                raise ValueError("HISTORY_BEGIN CRC32C selection mismatch")
            self._begin_received = True
        return HistoryStreamResult()

    def _handle_data(self, payload: bytes) -> HistoryStreamResult:
        header, compressed = protocol.HistoryDataHeader.decode(
            payload, self.options.max_block_bytes
        )
        with self._lock:
            self._ensure_active_locked()
            self._validate_data_header_locked(header)

        if header.flags & protocol.DATA_FLAG_CRC32C_PRESENT:
            raise ValueError("CRC32C History V2 blocks are not negotiated")
        raw = codec.decompress_zstd(
            bytes(compressed), header.uncompressed_size
        )
        rows = codec.decode_columnar_block(
            raw,
            expected_rows=header.row_count,
            max_block_bytes=self.options.max_block_bytes,
        )
        if not rows:
            raise ValueError("HISTORY_DATA decoded an empty block")
        if (
            rows[0][0] != header.first_timestamp_ms
            or rows[-1][0] != header.last_timestamp_ms
        ):
            raise ValueError("HISTORY_DATA timestamp envelope mismatch")

        with self._lock:
            self._ensure_active_locked()
            self._validate_data_header_locked(header)
            if self._observed_rows > UINT64_MAX - header.row_count:
                raise ValueError("History V2 row counter overflow")
            if (
                self._observed_uncompressed_bytes
                > UINT64_MAX - header.uncompressed_size
                or self._observed_compressed_bytes
                > UINT64_MAX - header.compressed_size
            ):
                raise ValueError("History V2 byte counter overflow")
            if self._next_chunk_seq == UINT32_MAX:
                raise ValueError("History V2 chunk sequence overflow")

            self._rows.extend(rows)
            self._observed_rows += header.row_count
            self._observed_uncompressed_bytes += header.uncompressed_size
            self._observed_compressed_bytes += header.compressed_size
            self._last_timestamp_ms = header.last_timestamp_ms
            self._next_chunk_seq += 1

        return HistoryStreamResult(
            window_grant_bytes=protocol.OUTER_HEADER_SIZE + len(payload),
            received_through_seq=header.chunk_seq,
        )

    def _validate_data_header_locked(
        self, header: protocol.HistoryDataHeader
    ) -> None:
        if not self._begin_received or self._terminal_received:
            raise ValueError("HISTORY_DATA arrived outside an active stream")
        if header.request_id != self.request_id:
            raise ValueError("HISTORY_DATA request_id mismatch")
        if header.chunk_seq != self._next_chunk_seq:
            raise ValueError("HISTORY_DATA chunk sequence mismatch")
        if (
            self._last_timestamp_ms is not None
            and header.first_timestamp_ms <= self._last_timestamp_ms
        ):
            raise ValueError("HISTORY_DATA timestamps are not increasing")

    def _handle_end(self, payload: bytes) -> HistoryStreamResult:
        end = protocol.HistoryEnd.decode(payload)
        with self._lock:
            self._ensure_active_locked()
            if not self._begin_received or self._terminal_received:
                raise ValueError("HISTORY_END arrived outside an active stream")
            expected_last_seq = (
                UINT32_MAX
                if self._next_chunk_seq == 0
                else self._next_chunk_seq - 1
            )
            if end.request_id != self.request_id:
                raise ValueError("HISTORY_END request_id mismatch")
            if (
                end.actual_total_rows != self._observed_rows
                or end.actual_uncompressed_bytes
                != self._observed_uncompressed_bytes
                or end.actual_compressed_bytes
                != self._observed_compressed_bytes
                or end.chunk_count != self._next_chunk_seq
                or end.last_chunk_seq != expected_last_seq
            ):
                raise ValueError("HISTORY_END counters mismatch")
            self._active = False
            self._terminal_received = True
        return HistoryStreamResult(terminal=True)

    def _handle_error(self, payload: bytes) -> HistoryStreamResult:
        failure = protocol.HistoryError.decode(payload)
        with self._lock:
            self._ensure_active_locked()
            expected_last_seq = (
                UINT32_MAX
                if self._next_chunk_seq == 0
                else self._next_chunk_seq - 1
            )
            if failure.request_id != self.request_id:
                raise ValueError("HISTORY_ERROR request_id mismatch")
            if (
                failure.delivered_rows != self._observed_rows
                or failure.last_chunk_seq != expected_last_seq
            ):
                raise ValueError("HISTORY_ERROR counters mismatch")
            self._active = False
            self._terminal_received = True
        detail = failure.message or failure.error_code.name.lower()
        return HistoryStreamResult(
            terminal=True,
            error=(
                f"History V2 {failure.error_code.name.lower()}: {detail}"
            ),
        )

    def cancel(
        self, reason: protocol.CancelReason
    ) -> Optional[protocol.HistoryCancel]:
        with self._lock:
            if not self._active:
                return None
            self._active = False
            last_seen_seq = (
                UINT32_MAX
                if self._next_chunk_seq == 0
                else self._next_chunk_seq - 1
            )
        return protocol.HistoryCancel(
            request_id=self.request_id,
            reason=reason,
            last_seen_seq=last_seen_seq,
        )

    def take_rows(self) -> List[codec.DecodedKline]:
        with self._lock:
            if not self._terminal_received:
                raise RuntimeError("History V2 stream is not terminal")
            rows = self._rows
            self._rows = []
            return rows

    def snapshot(self) -> HistoryStreamSnapshot:
        with self._lock:
            return HistoryStreamSnapshot(
                active=self._active,
                begin_received=self._begin_received,
                terminal_received=self._terminal_received,
                next_chunk_seq=self._next_chunk_seq,
                observed_rows=self._observed_rows,
                observed_uncompressed_bytes=(
                    self._observed_uncompressed_bytes
                ),
                observed_compressed_bytes=self._observed_compressed_bytes,
                last_timestamp_ms=self._last_timestamp_ms,
            )

    def _ensure_active_locked(self) -> None:
        if not self._active:
            raise ValueError("History V2 request is no longer active")
