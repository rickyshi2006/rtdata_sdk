"""Connection-bound realtime adapter. No user callback runs under the state lock."""

import logging
import queue
import secrets
import threading
import time
from types import MappingProxyType

from . import _protocol as proto
from . import _realtime_capability as cap
from . import _realtime_protocol as wire
from ._realtime_schema import SOURCES
from .exceptions import RealtimeUnavailableError
from .models import Quote, RealtimeStatus, RealtimeTableStatus

logger = logging.getLogger(__name__)


class RealtimeClientMixin:
    def _init_realtime(self, mode):
        self._realtime = cap.Session(cap.Role.SDK, cap.Role.CLOUD, mode)
        self._realtime_lock = threading.RLock()
        self._realtime_changed = threading.Condition(self._realtime_lock)
        self._realtime_connection = None
        self._realtime_transport_generation = 0
        self._realtime_timer = None
        self._realtime_callbacks = []
        self._realtime_last_status = None
        self._realtime_last_error = None
        self._realtime_watermarks = {}
        self._realtime_cache_sizes = {}
        self._realtime_cache_bytes = 0
        self._realtime_rejected = 0

    @property
    def on_realtime_status(self):
        def decorator(fn):
            self._realtime_callbacks.append(fn)
            return fn
        return decorator

    def _realtime_status_locked(self):
        session = self._realtime
        states = {s.id: s for s in session.states}
        tables = {}
        for sid, (table, schema) in SOURCES.items():
            state = states.get(sid)
            tables[table] = RealtimeTableStatus(table, schema,
                bool(state and state.ready), state.reason.name.lower() if state else
                (session.reason or ('legacy_mode' if session.mode == 'legacy' else 'not_negotiated')),
                state.epoch if state else 0)
        return RealtimeStatus(session.mode, session.phase, session.phase == 'active',
                              session.reason, session.generation, session.revision,
                              MappingProxyType(tables))

    @property
    def realtime_status(self):
        with self._realtime_lock:
            return self._realtime_status_locked()

    @property
    def realtime_statistics(self):
        with self._realtime_lock:
            return MappingProxyType(dict(rejected=self._realtime_rejected,
                cached_wire_bytes=self._realtime_cache_bytes,
                tracked_symbols=len(self._realtime_watermarks)))

    def _notify_realtime_status(self):
        with self._realtime_lock:
            status = self._realtime_status_locked()
            if status == self._realtime_last_status:
                return
            self._realtime_last_status = status
        for callback in self._realtime_callbacks:
            try:
                callback(status)
            except Exception:
                logger.exception('Realtime status callback failed')
        if status.mode == 'required_v2' and status.phase in ('fallback', 'closed'):
            self._realtime_error(status.reason or status.phase)
        elif status.mode == 'required_v2' and status.negotiated and status.revision:
            missing = [table.source_table + ':' + table.reason for table in status.tables.values() if not table.ready]
            if missing:
                self._realtime_error('full fields unavailable: ' + ', '.join(missing))

    def _realtime_error(self, reason):
        with self._realtime_lock:
            key = (self._realtime.generation, reason)
            if key == self._realtime_last_error:
                return
            self._realtime_last_error = key
        error = RealtimeUnavailableError(reason)
        for callback in self._error_callbacks:
            try:
                callback(error)
            except Exception:
                logger.exception('Realtime error callback failed')

    def _invalidate_realtime_locked(self, mask=cap.SOURCE_MASK, all_quotes=False):
        with self._quote_cache_lock:
            for code, quote in list(self._quote_cache.items()):
                if all_quotes or quote.source_id is None or mask & cap.source_bit(quote.source_id):
                    del self._quote_cache[code]
                    self._realtime_cache_bytes -= self._realtime_cache_sizes.pop(code, 0)
        for sid, watermark in list(self._realtime_watermarks.items()):
            if all_quotes or mask & cap.source_bit(watermark[0]):
                del self._realtime_watermarks[sid]
        self._realtime_changed.notify_all()

    def _reset_realtime(self, reason):
        with self._realtime_lock:
            if self._realtime_timer:
                self._realtime_timer.cancel()
                self._realtime_timer = None
            self._realtime.reset(self._realtime.generation + 1, False)
            self._realtime.reason = reason
            self._realtime_connection = None
            self._invalidate_realtime_locked(all_quotes=True)
            while True:
                try:
                    self._callback_queue.get_nowait()
                except queue.Empty:
                    break

    @staticmethod
    def _realtime_now():
        return time.monotonic_ns() // 1_000_000

    def _realtime_current_locked(self, generation=None, connection=None):
        conn = self._realtime_connection
        return (self._realtime.authenticated and conn is not None and conn is self._conn
                and conn.connected and conn.generation == self._realtime_transport_generation
                and (connection is None or connection is conn)
                and (generation is None or generation == self._realtime_transport_generation))

    def _realtime_send_locked(self, action):
        session = self._realtime
        generation = session.generation
        conn = self._realtime_connection
        # recv is gated by this same lock until COMMIT send and activation complete.
        success = conn.send_for_generation(proto.build_message(action.type, 0, action.payload),
                                           self._realtime_transport_generation)
        session.sent(generation, action, success, self._realtime_now())

    def _realtime_apply_locked(self):
        mask = self._realtime.take_invalidations()
        if mask:
            self._invalidate_realtime_locked(mask)
        if self._realtime.phase == 'closed':
            self._invalidate_realtime_locked(all_quotes=True)
            if self._realtime_connection:
                self._realtime_connection.abort_generation(self._realtime_transport_generation)
        if self._realtime_timer:
            self._realtime_timer.cancel()
            self._realtime_timer = None
        if self._realtime.deadline:
            generation = self._realtime.generation
            self._realtime_timer = threading.Timer(
                max(0.001, (self._realtime.deadline - self._realtime_now()) / 1000),
                self._realtime_timeout, args=(generation,))
            self._realtime_timer.daemon = True
            self._realtime_timer.start()
        self._realtime_changed.notify_all()

    def _realtime_timeout(self, generation):
        with self._realtime_lock:
            if generation != self._realtime.generation or not self._realtime_current_locked():
                return
            self._realtime.tick(generation, self._realtime_now())
            self._realtime_apply_locked()
        self._notify_realtime_status()

    def _begin_realtime(self):
        if self._realtime.mode == 'legacy':
            return
        with self._realtime_lock:
            conn = self._conn
            if conn is None or not conn.connected:
                return
            if self._realtime_current_locked():
                return
            self._realtime.reset(self._realtime.generation + 1, True)
            self._realtime_connection = conn
            self._realtime_transport_generation = conn.generation
            self._invalidate_realtime_locked(all_quotes=True)
            action = self._realtime.begin(self._realtime.generation,
                                           secrets.randbits(64) or 1, self._realtime_now())
            self._realtime_send_locked(action)
            self._realtime_apply_locked()
        self._notify_realtime_status()

    def _handle_realtime_control(self, msg_type, symbol, payload, generation, connection):
        with self._realtime_lock:
            if not self._realtime_current_locked(generation, connection):
                return
            if symbol or len(payload) > 632:
                self._realtime_connection.abort_generation(self._realtime_transport_generation)
                self._invalidate_realtime_locked(all_quotes=True)
                return
            action = self._realtime.receive(self._realtime.generation, msg_type, payload, self._realtime_now())
            if action:
                self._realtime_send_locked(action)
            self._realtime_apply_locked()
        self._notify_realtime_status()

    def _handle_full_snapshot(self, symbol, payload, generation, connection):
        with self._realtime_lock:
            if not self._realtime_current_locked(generation, connection):
                return
            try:
                row = wire.decode(payload)
                code = self._symbol_map.id_to_code(symbol)
                if not symbol or code != row.fields['code'] or not self._realtime.admits(
                        row.source_id, row.source_epoch, len(payload)):
                    raise ValueError('realtime identity, readiness or epoch mismatch')
                previous = self._realtime_watermarks.get(symbol)
                if previous:
                    source, epoch, seq = previous
                    if source != row.source_id:
                        raise ValueError('realtime table changed without invalidation')
                    if epoch == row.source_epoch and seq >= row.sequence:
                        self._realtime_rejected += 1
                        return
                if previous is None and len(self._realtime_watermarks) >= 65536:
                    raise ValueError('realtime symbol capacity exceeded')
                old_size = self._realtime_cache_sizes.get(code, 0)
                if self._realtime_cache_bytes - old_size + len(payload) > 16 * 1024 * 1024:
                    with self._quote_cache_lock:
                        self._quote_cache.pop(code, None)
                    self._realtime_cache_bytes -= self._realtime_cache_sizes.pop(code, 0)
                    self._realtime_watermarks[symbol] = (row.source_id, row.source_epoch, row.sequence)
                    self._realtime_changed.notify_all()
                    raise ValueError('realtime cache byte budget exceeded')
                quote = Quote.from_snapshot(row, symbol, code)
                self._realtime_watermarks[symbol] = (row.source_id, row.source_epoch, row.sequence)
                with self._quote_cache_lock:
                    self._quote_cache[code] = quote
                self._realtime_cache_bytes += len(payload) - old_size
                self._realtime_cache_sizes[code] = len(payload)
                self._realtime_changed.notify_all()
            except ValueError as exc:
                self._realtime_rejected += 1
                logger.warning('Rejected full-field quote: %s', exc)
                return
        with self._stats_lock:
            self._quotes_received += 1
        self._dispatch_quote(quote)

    def _quote_is_current(self, quote):
        with self._realtime_lock:
            with self._quote_cache_lock:
                return self._quote_cache.get(quote.symbol) is quote

    def wait_for_realtime(self, timeout=2.0):
        """Wait for protocol negotiation, not table readiness or a first quote."""
        deadline = time.monotonic() + timeout
        with self._realtime_changed:
            while self._realtime.phase != 'active':
                if self._realtime.mode == 'legacy' or self._realtime.phase in ('fallback', 'closed'):
                    raise RealtimeUnavailableError(self._realtime.reason or 'legacy_mode')
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RealtimeUnavailableError('realtime negotiation timeout')
                self._realtime_changed.wait(remaining)
            return self._realtime_status_locked()

    def wait_for_quote(self, symbol, timeout=5.0):
        """Wait for a current quote; required_v2 never returns a six-field quote."""
        deadline = time.monotonic() + timeout
        with self._realtime_changed:
            while True:
                with self._quote_cache_lock:
                    quote = self._quote_cache.get(symbol)
                if quote is not None and (self._realtime.mode != 'required_v2' or quote.wire_version == 2):
                    return quote
                if not self.is_connected:
                    raise RealtimeUnavailableError('realtime disconnected')
                if self._realtime.mode == 'required_v2' and self._realtime.phase in ('closed', 'fallback'):
                    raise RealtimeUnavailableError(self._realtime.reason)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RealtimeUnavailableError(f'no current quote for {symbol}; inspect realtime_status.tables')
                self._realtime_changed.wait(remaining)
