"""Subscription intent and ordered, connection-scoped acknowledgements."""
import logging
import threading
from typing import List, Optional

from . import _protocol as proto
from .exceptions import ConnectionError, SymbolNotFoundError

logger = logging.getLogger(__name__)


class SubscriptionClientMixin:
    def _init_subscriptions(self):
        self._subscribed_lock = threading.Lock()
        self._desired_subscriptions = {}
        self._subscription_revision = 0
        self._subscription_ids = {}
        self._subscribe_queue = set()
        self._subscribe_inflight = None
        self._subscribed_codes = []
        self._last_subscribe_warning = None
        self._last_subscribe_requested = []
        self._last_subscribe_confirmed = []
        self._last_subscribe_rejected = []
        self._subscription_refresh_attempt = None
        self._subscription_refresh_thread = None

    def _subscription_status_locked(self):
        self._subscribed_codes = list(self._subscription_ids)
        self._last_subscribe_requested = list(self._desired_subscriptions)
        self._last_subscribe_confirmed = list(self._subscribed_codes)
        missing = [code for code in self._desired_subscriptions
                   if code in self._subscribe_queue
                   and self._symbol_map.code_to_id(code) is None]
        messages = []
        if self._last_subscribe_rejected:
            messages.append(
                f"Subscribe partially accepted. Rejected symbols: {self._last_subscribe_rejected}. "
                "They may be blocked by market permissions, symbol limits, or server-side filtering.")
        if missing:
            messages.append(f"Subscription pending symbol map: {missing}; intent retained.")
        warning = ' '.join(messages) or None
        changed = warning != self._last_subscribe_warning
        self._last_subscribe_warning = warning
        return warning if changed else None

    def _report_subscription_warning(self, warning):
        if warning:
            logger.warning(warning)
            for callback in self._error_callbacks:
                try:
                    callback(warning)
                except Exception as exc:
                    logger.error("Error callback failed: %s", exc)

    def _reset_subscriptions(self):
        with self._subscribed_lock:
            self._authenticated = False
            self._subscription_ids.clear()
            self._subscribe_inflight = None
            self._subscribe_queue = set(self._desired_subscriptions)
            self._last_subscribe_rejected = []
            self._subscribed_codes = []
            self._last_subscribe_confirmed = []
            self._last_subscribe_requested = list(self._desired_subscriptions)
            self._last_subscribe_warning = None

    def _flush_subscriptions_locked(self):
        connection = self._conn
        if not self._authenticated or connection is None or self._subscribe_inflight:
            return
        requested = {}
        with self._symbol_map._lock:
            for code, revision in self._desired_subscriptions.items():
                if code in self._subscribe_queue:
                    sid = self._symbol_map._code_to_id.get(code)
                    if sid is not None:
                        requested[code] = (sid, revision)
        if not requested:
            return
        generation = connection.generation
        snapshot = {code: (sid, self._desired_subscriptions[code])
                    for code, sid in self._subscription_ids.items()}
        snapshot.update(requested)
        # The wire ACK has no request ID and contains the full subscription set.
        # Keep one request in flight, binding its IDs and intent revisions to it.
        self._subscribe_inflight = (connection, generation, requested, snapshot)
        frame = proto.encode_subscribe([sid for sid, _ in requested.values()])
        if not connection.send_for_generation(frame, generation):
            self._subscribe_inflight = None
            self._authenticated = False
            self._subscription_status_locked()
            raise ConnectionError("Connection lost while sending subscriptions; intent retained")
        self._subscribe_queue.difference_update(requested)

    def subscribe(self, symbols: List[str]):
        symbols = list(dict.fromkeys(symbols))
        with self._subscribed_lock:
            if not self._authenticated:
                raise ConnectionError("Not connected")
            if not any(self._symbol_map.code_to_id(code) is not None for code in symbols):
                raise SymbolNotFoundError(f"No valid symbols found in: {symbols}")
            for code in symbols:
                if code not in self._desired_subscriptions:
                    self._subscription_revision += 1
                    self._desired_subscriptions[code] = self._subscription_revision
                revision = self._desired_subscriptions[code]
                inflight = self._subscribe_inflight
                already_sent = (inflight is not None and code in inflight[2]
                                and inflight[2][code][1] == revision)
                if code not in self._subscription_ids and not already_sent:
                    self._subscribe_queue.add(code)
                if code in self._last_subscribe_rejected:
                    self._last_subscribe_rejected.remove(code)
            self._flush_subscriptions_locked()
            warning = self._subscription_status_locked()
        logger.info("Subscribe requested: %s", symbols)
        self._report_subscription_warning(warning)
        self._schedule_subscription_refresh()

    def unsubscribe(self, symbols: Optional[List[str]] = None):
        with self._subscribed_lock:
            removed = set(self._desired_subscriptions if symbols is None else symbols)
            ids = []
            for code in removed:
                sid = self._subscription_ids.get(code)
                if sid is None and self._subscribe_inflight:
                    sent = self._subscribe_inflight[2].get(code)
                    if sent is not None:
                        sid = sent[0]
                if sid is not None:
                    ids.append(sid)
                self._desired_subscriptions.pop(code, None)
                self._subscription_ids.pop(code, None)
            self._subscribe_queue.difference_update(removed)
            self._last_subscribe_rejected = [code for code in self._last_subscribe_rejected
                                             if code not in removed]
            self._subscription_status_locked()
            connection = self._conn
            if self._authenticated and connection is not None and (symbols is None or ids):
                frame = proto.encode_unsubscribe([] if symbols is None else ids)
                if not connection.send_for_generation(frame, connection.generation):
                    self._authenticated = False
                    raise ConnectionError("Connection lost while cancelling subscriptions")

    def _set_subscribed_codes_from_ids(self, ids, generation=None, connection=None):
        with self._subscribed_lock:
            inflight = self._subscribe_inflight
            if not self._authenticated or inflight is None:
                return
            sent_connection, sent_generation, _, snapshot = inflight
            if (sent_connection is not self._conn
                    or sent_generation != sent_connection.generation
                    or (connection is not None and connection is not sent_connection)
                    or (generation is not None and generation != sent_generation)):
                return
            accepted = set(ids)
            current = {code: sid for code, (sid, revision) in snapshot.items()
                       if self._desired_subscriptions.get(code) == revision}
            self._subscription_ids = {code: sid for code, sid in current.items() if sid in accepted}
            rejected = [code for code, sid in current.items() if sid not in accepted]
            self._last_subscribe_rejected = list(dict.fromkeys(
                [code for code in self._last_subscribe_rejected if code not in current] + rejected))
            self._subscribe_inflight = None
            self._flush_subscriptions_locked()
            warning = self._subscription_status_locked()
            confirmed = list(self._subscribed_codes)
        logger.info("Subscribe confirmed: %s", confirmed)
        self._report_subscription_warning(warning)
        self._schedule_subscription_refresh()

    def _resume_subscriptions(self):
        with self._subscribed_lock:
            self._flush_subscriptions_locked()
            warning = self._subscription_status_locked()
        self._report_subscription_warning(warning)
        self._schedule_subscription_refresh()

    def _schedule_subscription_refresh(self):
        # At most one unconditional HTTP refresh per connection generation.
        # Never block the receive loop or repeatedly retry a server rejection.
        with self._subscribed_lock:
            connection = self._conn
            if not self._api_url or not self._authenticated or connection is None:
                return
            key = (connection, connection.generation)
            if self._subscription_refresh_thread or self._subscription_refresh_attempt == key:
                return
            if not any(code in self._subscribe_queue and self._symbol_map.code_to_id(code) is None
                       for code in self._desired_subscriptions):
                return
            self._subscription_refresh_attempt = key
            worker = threading.Thread(target=self._refresh_subscription_symbols, args=key,
                                      name='rtdata-symbol-refresh', daemon=True)
            self._subscription_refresh_thread = worker
            worker.start()

    def _refresh_subscription_symbols(self, connection, generation):
        from . import _discovery as discovery

        with self._symbol_map._lock:
            original = self._symbol_map._id_to_code
        try:
            symbols, version = discovery.fetch_symbol_map(self._api_url, self._token, timeout=5.0)
            if symbols:
                with self._realtime_lock:
                    with self._subscribed_lock:
                        if (not self._authenticated or self._conn is not connection
                                or connection.generation != generation):
                            return
                        with self._symbol_map._lock:
                            if self._symbol_map._id_to_code is not original:
                                return
                        self._symbol_map.update_from_dict(symbols, version)
                        if original != symbols:
                            self._invalidate_realtime_locked(all_quotes=True)
                self._symbol_map_event.set()
                self._resume_subscriptions()
        except Exception as exc:
            logger.warning("Subscription symbol map refresh failed; intent retained: %s", exc)
        finally:
            with self._subscribed_lock:
                self._subscription_refresh_thread = None
            self._schedule_subscription_refresh()
