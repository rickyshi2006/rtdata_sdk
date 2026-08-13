"""rtdata SDK 客户端主类

用法:
    from rtdata import RtdataClient, Quote

    client = RtdataClient(host="...", port=9100, token="...")

    @client.on_quote
    def handle(quote: Quote):
        print(quote)

    client.connect()
    client.subscribe(["601919.SH", "rb2605.SHF"])
"""
import json
import threading
import time
import logging
import queue
import struct
from collections import deque
from datetime import datetime, date, time as dt_time
from typing import Callable, Optional, List, Dict, Iterator, Tuple, Union

from . import _protocol as proto
from . import _history_v2_protocol as history_v2
from ._connection import Connection
from ._history_capability_runtime import HistoryCapabilityRuntime
from ._session_capability_runtime import SessionCapabilityRuntime
from . import _session_rehome_protocol as session_rehome
from ._history_segment_cache import HistorySegmentCache
from ._history_v2_stream import HistoryV2RequestState
from ._symbol_map import SymbolMap
from .models import Quote, Kline, FinanceData, TokenStatus
from .exceptions import (
    AuthenticationError, ConnectionError, SymbolNotFoundError,
    QueryTimeoutError, QueryError, DiscoveryError, DisconnectedError,
)

logger = logging.getLogger(__name__)
VALID_ADJUSTS = {'none', 'forward', 'backward'}
TERMINAL_TOKEN_STATUSES = {'expired', 'disabled', 'revoked'}
HISTORY_V1_PAGE_ROWS = 5000
HISTORY_V2_PAGE_ROWS = 1_000_000
HISTORY_V2_DECODE_MAX_BYTES = 4 * 1024 * 1024
HISTORY_V2_MAX_RELAY_WINDOW_BYTES = 2 * 1024 * 1024


class _HistoryV2DecodeQueue:
    """Non-blocking FIFO bounded by queued wire-payload bytes, not frame count."""

    def __init__(self, max_bytes: int):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self._max_bytes = max_bytes
        self._queued_bytes = 0
        self._items = deque()
        self._condition = threading.Condition()

    def put_nowait(self, item):
        payload_bytes = 0 if item is None else len(item[1])
        if payload_bytes > self._max_bytes:
            raise queue.Full
        with self._condition:
            if self._queued_bytes + payload_bytes > self._max_bytes:
                raise queue.Full
            self._items.append((item, payload_bytes))
            self._queued_bytes += payload_bytes
            self._condition.notify()

    def get(self, timeout=None):
        with self._condition:
            deadline = None if timeout is None else time.monotonic() + timeout
            while not self._items:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise queue.Empty
                self._condition.wait(remaining)
            item, payload_bytes = self._items.popleft()
            self._queued_bytes -= payload_bytes
            return item


class RtdataClient:

    def __init__(
        self,
        token: str,
        host: str = '127.0.0.1',
        port: int = 9100,
        *,
        api_url: Optional[str] = None,
        heartbeat_interval: float = 20.0,
        auto_reconnect: bool = True,
        symbol_cache_dir: Optional[str] = None,
        history_cache_dir: Optional[str] = None,
        history_cache_enabled: bool = True,
        async_callbacks: bool = True,
        callback_queue_size: int = 1000,
        history_v2_advertise: bool = False,
        history_v2_default: bool = False,
        history_v2_max_block_bytes: int = 256 * 1024,
        history_capability_ack_timeout: float = 1.0,
        session_rehome_advertise: bool = False,
        session_capability_ack_timeout: float = 1.0,
    ):
        self._token = token
        self._host = host
        self._port = port
        self._api_url = api_url
        self._current_node_id = ""
        self._gateway_version = ""
        self._protocol_features: List[str] = []
        self._protocol_features_supported: List[str] = []
        self._heartbeat_interval = heartbeat_interval
        self._auto_reconnect = auto_reconnect
        self._async_callbacks = async_callbacks
        self._callback_queue_size = callback_queue_size
        self._history_capability = HistoryCapabilityRuntime(
            advertise=history_v2_advertise,
            default_enabled=history_v2_default,
            max_block_bytes=history_v2_max_block_bytes,
            ack_timeout=history_capability_ack_timeout,
        )
        self._session_capability = SessionCapabilityRuntime(
            advertise=bool(
                session_rehome_advertise
                and api_url
                and auto_reconnect
            ),
            ack_timeout=session_capability_ack_timeout,
        )
        self._session_rehome_requested = False
        self._session_rehome_target = ""
        self._session_handoff_ticket = b""
        self._handled_migration_ids = set()
        self._handled_migration_order = deque()

        self._symbol_map = SymbolMap(cache_dir=symbol_cache_dir)
        history_cache_base_dir = history_cache_dir if history_cache_dir is not None else symbol_cache_dir
        self._history_cache = HistorySegmentCache(
            cache_dir=history_cache_base_dir,
            enabled=history_cache_enabled,
        )
        self._conn: Optional[Connection] = None

        self._quote_callbacks: List[Callable] = []
        self._connect_callbacks: List[Callable] = []
        self._disconnect_callbacks: List[Callable] = []
        self._error_callbacks: List[Callable] = []
        self._token_status_callbacks: List[Callable] = []

        self._authenticated = False
        self._auth_event = threading.Event()
        self._auth_success = False
        self._auth_error = ""
        self._token_status_lock = threading.Lock()
        self._token_status: Optional[TokenStatus] = None
        self._symbol_map_event = threading.Event()
        self._subscribed_codes: List[str] = []
        self._pending_subscribe_codes: List[str] = []
        self._subscribed_lock = threading.Lock()

        self._last_subscribe_warning: Optional[str] = None
        self._last_subscribe_rejected: List[str] = []
        self._last_subscribe_confirmed: List[str] = []
        self._last_subscribe_requested: List[str] = []
        self._pending_subscribe_request: List[str] = []

        self._quote_cache: Dict[str, Quote] = {}
        self._quote_cache_lock = threading.Lock()

        self._next_request_id = 1
        self._request_id_lock = threading.Lock()
        self._pending_queries: Dict[int, dict] = {}
        self._pending_lock = threading.Lock()
        self._history_v2_worker_lock = threading.Lock()
        self._history_v2_queue = _HistoryV2DecodeQueue(
            HISTORY_V2_DECODE_MAX_BYTES
        )
        self._history_v2_worker_stop = threading.Event()
        self._history_v2_worker: Optional[threading.Thread] = None

        self._stats_lock = threading.Lock()
        self._messages_received = 0
        self._quotes_received = 0
        self._bytes_received = 0
        self._reconnect_count = 0
        self._quotes_dropped = 0

        self._callback_queue: "queue.Queue[Quote]" = queue.Queue(maxsize=max(1, callback_queue_size))
        self._callback_stop = threading.Event()
        self._callback_thread: Optional[threading.Thread] = None
        if self._async_callbacks:
            self._callback_thread = threading.Thread(
                target=self._callback_loop,
                name='rtdata-callback',
                daemon=True,
            )
            self._callback_thread.start()

    @property
    def on_quote(self):
        def decorator(fn: Callable):
            self._quote_callbacks.append(fn)
            return fn
        return decorator

    @property
    def on_connect(self):
        def decorator(fn: Callable):
            self._connect_callbacks.append(fn)
            return fn
        return decorator

    @property
    def on_disconnect(self):
        def decorator(fn: Callable):
            self._disconnect_callbacks.append(fn)
            return fn
        return decorator

    @property
    def on_error(self):
        def decorator(fn: Callable):
            self._error_callbacks.append(fn)
            return fn
        return decorator

    @property
    def on_token_status(self):
        def decorator(fn: Callable):
            self._token_status_callbacks.append(fn)
            return fn
        return decorator

    def connect(self, timeout: float = 15.0):
        if self._conn is not None:
            self._abort_pending_queries(
                "connection replaced",
                cancel_reason=history_v2.CancelReason.SHUTDOWN,
                send_cancel=True,
            )
            self._conn.close()
            self._conn = None
        self._history_capability.reset("new_connection")
        self._session_capability.reset("new_connection")
        self._session_rehome_requested = False
        self._session_rehome_target = ""
        self._session_handoff_ticket = b""
        self._symbol_map_event.clear()
        self._symbol_map.load_cache()

        if self._api_url:
            self._do_discovery(timeout)

        self._conn = Connection(
            host=self._host,
            port=self._port,
            on_message=self._dispatch_message,
            on_disconnected=self._handle_disconnected,
            heartbeat_interval=self._heartbeat_interval,
            auto_reconnect=self._auto_reconnect,
        )
        self._conn._on_reconnected = self._handle_reconnected
        self._conn._on_before_reconnect = self._before_reconnect
        self._conn._on_reconnect_completed = self._handle_reconnect_completed

        self._conn.connect(timeout=timeout)
        self._conn.start_recv_loop()

        self._auth_success = False
        self._auth_error = ""
        self._auth_event.clear()
        logger.debug("Sending AUTH message")
        if not self._conn.send(proto.encode_auth(self._token)):
            self._conn.close()
            raise ConnectionError("Connection lost while sending authentication")

        while not self._auth_event.wait(timeout=0.1):
            logger.debug("Waiting for AUTH_RESPONSE...")
            if not self._conn or not self._conn.connected:
                logger.debug("Connection lost before AUTH_RESPONSE arrived")
                self._conn.close()
                raise AuthenticationError(
                    f"Authentication failed: {self._auth_error or 'disconnected before auth response'}")
            timeout -= 0.1
            if timeout <= 0:
                logger.debug("Timed out waiting for AUTH_RESPONSE")
                self._conn.close()
                raise AuthenticationError("Authentication timeout")

        if not self._auth_success:
            self._conn.close()
            raise AuthenticationError(f"Authentication failed: {self._auth_error}")

        if self._symbol_map.size > 0:
            self._symbol_map_event.set()
            logger.info("Symbol map already available from cache or discovery API")
        elif not self._symbol_map_event.wait(timeout=timeout):
            logger.warning("Symbol map not received and no valid cache is available")

        if not self._conn or not self._conn.connected:
            self._authenticated = False
            raise ConnectionError("Connection lost during initial state restore")

        self._authenticated = True
        logger.info(
            "Ready. Symbols: %s node_id=%s",
            self._symbol_map.size,
            self.current_node_id or "unknown",
        )

    def _do_discovery(self, timeout: float):
        from . import _discovery as discovery

        info = discovery.discover_endpoint(self._api_url, self._token, timeout=timeout)
        self._host = info['tcp_host']
        self._port = info['tcp_port']
        self._current_node_id = info.get('node_id', "") or ""
        self._gateway_version = info.get('gateway_version', "") or ""
        protocol_info = info.get('protocol', {})
        self._protocol_features = []
        self._protocol_features_supported = []
        if isinstance(protocol_info, dict):
            features = protocol_info.get('features_enabled', [])
            self._protocol_features = (
                [str(value) for value in features]
                if isinstance(features, list)
                else []
            )
            supported = protocol_info.get('features_supported', [])
            self._protocol_features_supported = (
                [str(value) for value in supported]
                if isinstance(supported, list)
                else []
            )
        self._session_capability.set_handoff_enabled(
            "session_rehome_handoff_v1" in self._protocol_features
        )
        remote_version = info.get('symbol_map_version', 0)

        logger.info(
            "Discovery succeeded: node_id=%s",
            self._current_node_id or "unknown",
        )

        if remote_version > 0 and remote_version != self._symbol_map.version:
            symbols, version = discovery.fetch_symbol_map(
                self._api_url, self._token,
                local_version=self._symbol_map.version,
                timeout=timeout,
            )
            if symbols is not None:
                self._symbol_map.update_from_dict(symbols, version)
                logger.info(f"Symbol map updated to version {version} ({len(symbols)} symbols)")
        elif self._symbol_map.size == 0:
            symbols, version = discovery.fetch_symbol_map(
                self._api_url, self._token, timeout=timeout)
            if symbols is not None:
                self._symbol_map.update_from_dict(symbols, version)

    def close(self):
        self._authenticated = False
        self._abort_pending_queries(
            "client closed",
            cancel_reason=history_v2.CancelReason.SHUTDOWN,
            send_cancel=True,
        )
        self._history_capability.reset("closed")
        self._session_capability.reset("closed")
        self._callback_stop.set()
        if self._callback_thread and self._callback_thread.is_alive():
            self._callback_thread.join(timeout=3)
        if self._conn:
            self._conn.close()
            self._conn = None
        self._stop_history_v2_worker()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def is_connected(self) -> bool:
        return (
            self._authenticated
            and self._conn is not None
            and self._conn.connected
        )

    @property
    def is_reconnecting(self) -> bool:
        return self._conn is not None and self._conn.reconnecting

    def subscribe(self, symbols: List[str]):
        if not self._authenticated:
            raise ConnectionError("Not connected")

        ids = self._symbol_map.codes_to_ids(symbols)
        if not ids:
            raise SymbolNotFoundError(f"No valid symbols found in: {symbols}")

        with self._subscribed_lock:
            requested = list(dict.fromkeys(self._subscribed_codes + symbols))
            self._pending_subscribe_codes = requested
            self._pending_subscribe_request = list(dict.fromkeys(symbols))
            self._last_subscribe_warning = None
            self._last_subscribe_rejected = []
            self._last_subscribe_confirmed = []
            self._last_subscribe_requested = list(self._pending_subscribe_request)

        self._conn.send(proto.encode_subscribe(ids))
        logger.info(f"Subscribe requested: {symbols}")

    def unsubscribe(self, symbols: Optional[List[str]] = None):
        if not self._authenticated:
            return

        if symbols is None:
            self._conn.send(proto.encode_unsubscribe([]))
            with self._subscribed_lock:
                self._subscribed_codes.clear()
                self._pending_subscribe_codes.clear()
                self._pending_subscribe_request.clear()
                self._last_subscribe_warning = None
                self._last_subscribe_rejected = []
                self._last_subscribe_confirmed = []
                self._last_subscribe_requested = []
        else:
            ids = self._symbol_map.codes_to_ids(symbols)
            if ids:
                self._conn.send(proto.encode_unsubscribe(ids))
            with self._subscribed_lock:
                for s in symbols:
                    if s in self._subscribed_codes:
                        self._subscribed_codes.remove(s)
                    if s in self._pending_subscribe_codes:
                        self._pending_subscribe_codes.remove(s)
                    if s in self._pending_subscribe_request:
                        self._pending_subscribe_request.remove(s)
                    if s in self._last_subscribe_rejected:
                        self._last_subscribe_rejected.remove(s)
                    if s in self._last_subscribe_confirmed:
                        self._last_subscribe_confirmed.remove(s)
                self._last_subscribe_requested = [
                    code for code in self._last_subscribe_requested if code not in symbols
                ]
                if not self._last_subscribe_rejected:
                    self._last_subscribe_warning = None

    def _set_subscribed_codes_from_ids(self, ids: List[int]):
        codes = []
        seen = set()
        for sid in ids:
            code = self._symbol_map.id_to_code(sid)
            if code and code not in seen:
                seen.add(code)
                codes.append(code)
        with self._subscribed_lock:
            requested = list(self._pending_subscribe_request)
            rejected = [code for code in requested if code not in codes]
            self._subscribed_codes = codes
            self._pending_subscribe_codes = []
            self._pending_subscribe_request = []
            self._last_subscribe_confirmed = list(codes)
            self._last_subscribe_rejected = rejected
            if rejected:
                self._last_subscribe_warning = (
                    f"Subscribe partially accepted. Rejected symbols: {rejected}. "
                    "They may be blocked by market permissions, symbol limits, or server-side filtering."
                )
            else:
                self._last_subscribe_warning = None

        if self._last_subscribe_warning:
            logger.warning(self._last_subscribe_warning)
            for cb in self._error_callbacks:
                try:
                    cb(self._last_subscribe_warning)
                except Exception as e:
                    logger.error(f"Error callback failed: {e}")
        else:
            logger.info(f"Subscribe confirmed: {codes}")

    def _normalize_history_endpoint(self, value: Union[int, float, str, datetime, date], *, is_end: bool) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, datetime):
            return int(value.timestamp() * 1000)
        if isinstance(value, date):
            if is_end:
                dt = datetime.combine(value, dt_time(23, 59, 59, 999000))
            else:
                dt = datetime.combine(value, dt_time.min)
            return int(dt.timestamp() * 1000)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return 0
            if len(text) == 10 and text.count('-') == 2:
                parsed = datetime.strptime(text, '%Y-%m-%d').date()
                return self._normalize_history_endpoint(parsed, is_end=is_end)
            text = text.replace('T', ' ')
            for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
                try:
                    dt = datetime.strptime(text, fmt)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    continue
            raise ValueError(f'Unsupported datetime format: {value}')
        raise TypeError(f'Unsupported history time value: {value!r}')

    def _select_history_v2_request(self):
        snapshot = self._history_capability.snapshot()
        if (
            not self._history_capability.default_enabled
            or not snapshot.v2_eligible
        ):
            return None, snapshot.generation

        max_frame_block = (
            HISTORY_V2_MAX_RELAY_WINDOW_BYTES
            - history_v2.OUTER_HEADER_SIZE
            - history_v2.DATA_HEADER_STRUCT.size
        )
        max_block_bytes = min(
            self._history_capability.max_block_bytes,
            snapshot.capabilities.max_block_bytes,
            max_frame_block,
        )
        initial_window_bytes = max(
            history_v2.DEFAULT_INITIAL_WINDOW_BYTES,
            history_v2.OUTER_HEADER_SIZE
            + history_v2.DATA_HEADER_STRUCT.size
            + max_block_bytes,
        )
        options = history_v2.RequestOptions(
            max_block_bytes=max_block_bytes,
            initial_window_bytes=initial_window_bytes,
        )
        options.validate()
        return options, snapshot.generation

    def _ensure_history_v2_worker(self):
        with self._history_v2_worker_lock:
            if (
                self._history_v2_worker is not None
                and self._history_v2_worker.is_alive()
            ):
                return
            self._history_v2_queue = _HistoryV2DecodeQueue(
                HISTORY_V2_DECODE_MAX_BYTES
            )
            self._history_v2_worker_stop = threading.Event()
            work_queue = self._history_v2_queue
            stop_event = self._history_v2_worker_stop
            self._history_v2_worker = threading.Thread(
                target=self._history_v2_worker_loop,
                args=(work_queue, stop_event),
                name='rtdata-history-v2',
                daemon=True,
            )
            self._history_v2_worker.start()

    def _stop_history_v2_worker(self):
        with self._history_v2_worker_lock:
            worker = self._history_v2_worker
            if worker is None:
                return
            self._history_v2_worker_stop.set()
        if worker.is_alive() and worker is not threading.current_thread():
            worker.join(timeout=3)
        with self._history_v2_worker_lock:
            if self._history_v2_worker is worker:
                self._history_v2_worker = None

    def _history_v2_worker_loop(self, work_queue, stop_event):
        while not stop_event.is_set():
            try:
                item = work_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if item is None:
                return
            msg_type, payload, connection_generation = item
            try:
                self._process_history_v2_frame(
                    msg_type, payload, connection_generation
                )
            except Exception as exc:
                logger.exception("History V2 worker failure: %s", exc)

    def _enqueue_history_v2_frame(
        self,
        msg_type: int,
        payload: bytes,
        connection_generation: Optional[int],
    ):
        if len(payload) < 4:
            logger.warning("Ignoring History V2 frame without request_id")
            return
        request_id = struct.unpack_from('!I', payload)[0]
        with self._pending_lock:
            entry = self._pending_queries.get(request_id)
        if entry is None or entry.get('history_version') != 2:
            logger.debug(
                "No pending History V2 query for request_id=%s", request_id
            )
            return
        if (
            connection_generation is not None
            and entry['connection_generation'] != connection_generation
        ):
            return

        self._ensure_history_v2_worker()
        try:
            self._history_v2_queue.put_nowait(
                (msg_type, payload, connection_generation)
            )
        except queue.Full:
            self._fail_history_v2_entry(
                request_id,
                entry,
                "History V2 decode queue is full",
                history_v2.CancelReason.BACKPRESSURE,
            )

    def _process_history_v2_frame(
        self,
        msg_type: int,
        payload: bytes,
        connection_generation: Optional[int],
    ):
        if len(payload) < 4:
            return
        request_id = struct.unpack_from('!I', payload)[0]
        with self._pending_lock:
            entry = self._pending_queries.get(request_id)
        if entry is None or entry.get('history_version') != 2:
            return
        if (
            connection_generation is not None
            and entry['connection_generation'] != connection_generation
        ):
            return

        state: HistoryV2RequestState = entry['v2_state']
        capability = self._history_capability.snapshot()
        if capability.generation != state.capability_generation:
            self._fail_history_v2_entry(
                request_id,
                entry,
                "History V2 capability generation changed",
                history_v2.CancelReason.SHUTDOWN,
            )
            return

        try:
            result = state.handle_frame(msg_type, payload)
        except (RuntimeError, ValueError, struct.error, OverflowError) as exc:
            self._fail_history_v2_entry(
                request_id,
                entry,
                f"Invalid History V2 stream: {exc}",
                history_v2.CancelReason.BACKPRESSURE,
            )
            return

        if result.window_grant_bytes:
            update = history_v2.HistoryWindowUpdate(
                request_id=request_id,
                grant_bytes=result.window_grant_bytes,
                received_through_seq=result.received_through_seq,
            )
            if not self._send_history_v2_control(
                entry,
                proto.MsgType.HISTORY_WINDOW_UPDATE,
                update.encode(),
            ):
                self._fail_history_v2_entry(
                    request_id,
                    entry,
                    "Connection lost while replenishing History V2 window",
                    history_v2.CancelReason.SHUTDOWN,
                )
                return

        if not result.terminal:
            return
        with self._pending_lock:
            if self._pending_queries.get(request_id) is not entry:
                return
            if result.error:
                entry['error'] = result.error
            else:
                entry['klines'] = state.take_rows()
            entry['event'].set()

    def _send_history_v2_control(
        self, entry: dict, msg_type: int, payload: bytes
    ) -> bool:
        conn = self._conn
        if (
            conn is None
            or not conn.connected
            or conn.generation != entry['connection_generation']
        ):
            return False
        return conn.send(proto.build_message(msg_type, 0, payload))

    def _fail_history_v2_entry(
        self,
        request_id: int,
        entry: dict,
        message: str,
        cancel_reason: history_v2.CancelReason,
    ):
        state: HistoryV2RequestState = entry['v2_state']
        cancel = state.cancel(cancel_reason)
        if cancel is not None:
            self._send_history_v2_control(
                entry, proto.MsgType.HISTORY_CANCEL, cancel.encode()
            )
        with self._pending_lock:
            if self._pending_queries.get(request_id) is not entry:
                return
            if not entry['event'].is_set():
                entry['error'] = message
                entry['event'].set()

    def _abort_pending_queries(
        self,
        message: str,
        *,
        cancel_reason: history_v2.CancelReason,
        send_cancel: bool,
    ):
        with self._pending_lock:
            pending = list(self._pending_queries.values())
            self._pending_queries.clear()
        for entry in pending:
            if entry.get('history_version') == 2:
                cancel = entry['v2_state'].cancel(cancel_reason)
                if cancel is not None and send_cancel:
                    self._send_history_v2_control(
                        entry,
                        proto.MsgType.HISTORY_CANCEL,
                        cancel.encode(),
                    )
            entry['error'] = message
            entry['event'].set()

    def _perform_history_query(self, symbol: str, period: str,
                               start_ms: int, end_ms: int,
                               max_count: int, timeout: float,
                               adjust: str = 'none') -> List[Kline]:
        if not self._authenticated:
            raise ConnectionError("Not connected")

        conn = self._conn
        if conn is None or not conn.connected:
            raise ConnectionError("Not connected")

        symbol_id = self._symbol_map.code_to_id(symbol) or 0
        request_id = self._alloc_request_id()
        history_v2_options, capability_generation = (
            self._select_history_v2_request()
        )
        use_history_v2 = history_v2_options is not None
        effective_max_count = (
            HISTORY_V2_PAGE_ROWS
            if use_history_v2 and max_count == HISTORY_V1_PAGE_ROWS
            else max_count
        )
        entry = {
            'event': threading.Event(),
            'klines': [],
            'batches_received': set(),
            'batch_count': None,
            'error': None,
            'history_version': 2 if use_history_v2 else 1,
            'connection_generation': conn.generation,
        }
        if use_history_v2:
            self._ensure_history_v2_worker()
            entry['v2_state'] = HistoryV2RequestState(
                request_id=request_id,
                options=history_v2_options,
                capability_generation=capability_generation,
                expected_symbol_id=symbol_id,
                expected_period=proto.PERIOD_MAP.get(period, 1),
            )
        with self._pending_lock:
            self._pending_queries[request_id] = entry

        logger.debug(
            "Sending history request: symbol=%s period=%s request_id=%s "
            "start=%s end=%s count=%s adjust=%s version=%s",
            symbol,
            period,
            request_id,
            start_ms,
            end_ms,
            effective_max_count,
            adjust,
            2 if use_history_v2 else 1,
        )
        msg = proto.encode_history_request(
            request_id,
            symbol_id,
            period,
            start_ms,
            end_ms,
            effective_max_count,
            symbol,
            adjust=adjust,
            history_v2_options=(
                history_v2_options.encode() if use_history_v2 else b''
            ),
        )
        if (
            conn.generation != entry['connection_generation']
            or not conn.send(msg)
        ):
            with self._pending_lock:
                self._pending_queries.pop(request_id, None)
            if use_history_v2:
                entry['v2_state'].cancel(history_v2.CancelReason.SHUTDOWN)
            raise DisconnectedError(
                f"Connection lost while sending history query for {symbol}"
            )

        if not entry['event'].wait(timeout=timeout):
            timed_out = not entry['event'].is_set()
            if timed_out and use_history_v2:
                cancel = entry['v2_state'].cancel(
                    history_v2.CancelReason.TIMEOUT
                )
                if cancel is None:
                    timed_out = not entry['event'].wait(timeout=0.1)
                elif cancel is not None:
                    self._send_history_v2_control(
                        entry,
                        proto.MsgType.HISTORY_CANCEL,
                        cancel.encode(),
                    )
            elif timed_out:
                timed_out = not entry['event'].wait(timeout=0.01)
            if timed_out:
                with self._pending_lock:
                    self._pending_queries.pop(request_id, None)
                raise QueryTimeoutError(f"History query timeout for {symbol}")

        with self._pending_lock:
            self._pending_queries.pop(request_id, None)

        if entry['error']:
            if any(value in entry['error'].lower() for value in (
                "disconnected", "server closed", "upstream_lost",
                "connection lost",
            )):
                raise DisconnectedError(entry['error'])
            raise QueryError(entry['error'])

        if len(entry['klines']) > effective_max_count:
            raise QueryError(
                f"History query returned {len(entry['klines'])} rows beyond "
                f"the requested limit {effective_max_count}"
            )

        return [Kline(*k, symbol=symbol) for k in entry['klines']]

    def _get_history_with_local_cache(self, symbol: str, period: str, adjust: str,
                                      start_ms: int, end_exclusive_ms: int,
                                      timeout: float) -> List[Kline]:
        missing_ranges = self._history_cache.get_missing_ranges(
            symbol, period, adjust, start_ms, end_exclusive_ms)
        if missing_ranges:
            logger.info(
                "History cache miss: symbol=%s period=%s adjust=%s missing_ranges=%s",
                symbol, period, adjust, missing_ranges,
            )
        deadline = time.monotonic() + timeout
        for missing_start, missing_end_exclusive in missing_ranges:
            for coverage_start, coverage_end, fetched in self._iter_history_range_pages(
                    symbol, period, adjust, missing_start,
                    missing_end_exclusive, deadline):
                self._history_cache.store_range(
                    symbol,
                    period,
                    adjust,
                    coverage_start,
                    coverage_end,
                    [
                        (
                            k.timestamp,
                            k.open,
                            k.high,
                            k.low,
                            k.close,
                            k.volume,
                            k.turnover,
                            k.open_interest,
                        )
                        for k in fetched
                    ],
                )

        cached_rows = self._history_cache.load_range(
            symbol, period, adjust, start_ms, end_exclusive_ms - 1)
        return [Kline(*row, symbol=symbol) for row in cached_rows]

    def _iter_history_range_pages(
        self,
        symbol: str,
        period: str,
        adjust: str,
        start_ms: int,
        end_exclusive_ms: int,
        deadline: float,
    ) -> Iterator[Tuple[int, int, List[Kline]]]:
        cursor = start_ms
        while cursor < end_exclusive_ms:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise QueryTimeoutError(f"History query timeout for {symbol}")

            fetched = self._perform_history_query(
                symbol,
                period,
                cursor,
                end_exclusive_ms - 1,
                HISTORY_V1_PAGE_ROWS,
                remaining,
                adjust=adjust,
            )
            if not fetched:
                return

            timestamps = [int(kline.timestamp) for kline in fetched]
            if timestamps[0] < cursor or timestamps[-1] >= end_exclusive_ms:
                raise QueryError("History query returned rows outside the requested range")
            if any(current <= previous
                   for previous, current in zip(timestamps, timestamps[1:])):
                raise QueryError("History query returned non-increasing timestamps")

            next_cursor = timestamps[-1] + 1
            if next_cursor <= cursor:
                raise QueryError("History query pagination made no forward progress")

            yield cursor, min(next_cursor, end_exclusive_ms), fetched
            cursor = next_cursor

    def get_kline(self, symbol: str, period: str = '1d',
                  start: Union[int, float, str, datetime, date] = 0,
                  end: Union[int, float, str, datetime, date] = 0,
                  timeout: float = 30.0,
                  adjust: str = 'none',
                  **legacy_kwargs) -> List[Kline]:
        if 'start_time' in legacy_kwargs and not start:
            start = legacy_kwargs.pop('start_time')
        if 'end_time' in legacy_kwargs and not end:
            end = legacy_kwargs.pop('end_time')
        if legacy_kwargs:
            unexpected = ', '.join(sorted(legacy_kwargs.keys()))
            raise TypeError(f'Unexpected keyword arguments: {unexpected}')
        adjust = str(adjust).lower()
        if adjust not in VALID_ADJUSTS:
            raise ValueError(f'Unsupported adjust value: {adjust}')

        start_ms = self._normalize_history_endpoint(start, is_end=False) if start else 0
        end_ms = self._normalize_history_endpoint(end, is_end=True) if end else 0
        if start_ms and end_ms and start_ms > end_ms:
            raise ValueError('start must be <= end')

        if start_ms and end_ms and self._history_cache.enabled:
            return self._get_history_with_local_cache(
                symbol, period, adjust, start_ms, end_ms + 1, timeout)

        if start_ms and end_ms:
            klines: List[Kline] = []
            deadline = time.monotonic() + timeout
            for _coverage_start, _coverage_end, fetched in self._iter_history_range_pages(
                    symbol, period, adjust, start_ms, end_ms + 1, deadline):
                klines.extend(fetched)
            return klines

        return self._perform_history_query(
            symbol, period, start_ms, end_ms,
            HISTORY_V1_PAGE_ROWS, timeout, adjust=adjust)

    def get_kline_range(self, symbol: str, period: str = '1d',
                        start: Union[int, float, str, datetime, date] = 0,
                        end: Union[int, float, str, datetime, date] = 0,
                        adjust: str = 'none',
                        timeout: float = 30.0) -> List[Kline]:
        return self.get_kline(symbol, period=period, start=start, end=end, timeout=timeout, adjust=adjust)

    def get_kline_for_day(self, symbol: str, day: Union[str, date, datetime],
                          period: str = '1d', timeout: float = 30.0,
                          adjust: str = 'none') -> List[Kline]:
        return self.get_kline(symbol, period=period, start=day, end=day, timeout=timeout, adjust=adjust)

    def get_kline_for_today(self, symbol: str, period: str = '1d', timeout: float = 30.0,
                            adjust: str = 'none') -> List[Kline]:
        return self.get_kline(symbol, period=period, start=date.today(), end=date.today(),
                              timeout=timeout, adjust=adjust)

    def get_finance(self, stock_code: str, report_period: str = '',
                    query_type: int = 4, timeout: float = 30.0) -> FinanceData:
        return self._do_finance_query(
            proto.MsgType.FINANCE_REQUEST, stock_code, report_period, query_type, timeout)

    def get_finance_ttm(self, stock_code: str, as_of_date: str = '',
                        timeout: float = 30.0) -> FinanceData:
        return self._do_finance_query(
            proto.MsgType.FINANCE_TTM_REQUEST, stock_code, as_of_date, 0, timeout)

    def get_finance_pit(self, stock_code: str, trade_date: str = '',
                        query_type: int = 0, timeout: float = 30.0) -> FinanceData:
        return self._do_finance_query(
            proto.MsgType.FINANCE_PIT_REQUEST, stock_code, trade_date, query_type, timeout)

    def get_finance_ratios(self, stock_code: str, report_period: str = '',
                           timeout: float = 30.0) -> FinanceData:
        return self._do_finance_query(
            proto.MsgType.FINANCE_RATIOS_REQUEST, stock_code, report_period, 0, timeout)

    def _do_finance_query(self, msg_type: int, stock_code: str,
                          period: str, query_type: int,
                          timeout: float) -> FinanceData:
        if not self._authenticated:
            raise ConnectionError("Not connected")

        request_id = self._alloc_request_id()
        entry = {
            'event': threading.Event(),
            'result': None,
            'error': None,
        }
        with self._pending_lock:
            self._pending_queries[request_id] = entry

        msg = proto.encode_finance_request(msg_type, request_id, stock_code, period, query_type)
        self._conn.send(msg)

        if not entry['event'].wait(timeout=timeout):
            with self._pending_lock:
                self._pending_queries.pop(request_id, None)
            raise QueryTimeoutError(f"Finance query timeout for {stock_code}")

        with self._pending_lock:
            self._pending_queries.pop(request_id, None)

        if entry['error']:
            if "disconnected" in entry['error'] or "server closed" in entry['error']:
                raise DisconnectedError(entry['error'])
            raise QueryError(entry['error'])

        return entry['result']

    def get_quote(self, symbol: str) -> Optional[Quote]:
        with self._quote_cache_lock:
            return self._quote_cache.get(symbol)

    def get_subscribed_symbols(self) -> List[str]:
        with self._subscribed_lock:
            return list(self._subscribed_codes)

    @property
    def symbols(self) -> Dict[int, str]:
        with self._symbol_map._lock:
            return dict(self._symbol_map._id_to_code)

    def _dispatch_message(
        self,
        msg_type: int,
        symbol_id: int,
        payload: bytes,
        connection_generation: Optional[int] = None,
        source_connection: Optional[Connection] = None,
    ):
        if (
            source_connection is not None
            and self._conn is not source_connection
        ):
            logger.debug("Ignoring message from replaced connection")
            return
        current_connection = source_connection or self._conn
        if (
            connection_generation is not None
            and current_connection is not None
            and connection_generation != current_connection.generation
        ):
            logger.debug("Ignoring message from stale connection generation")
            return
        logger.debug(f"_dispatch_message: msg_type=0x{msg_type:04x} symbol_id={symbol_id} payload_len={len(payload)}")
        with self._stats_lock:
            self._messages_received += 1
            self._bytes_received += proto.HEADER_SIZE + len(payload)

        if msg_type == proto.MsgType.AUTH_RESPONSE:
            self._handle_auth_response(payload)
        elif msg_type == proto.MsgType.CAPABILITY_ACK:
            self._handle_history_capability_ack(payload)
        elif msg_type == proto.MsgType.SESSION_CAPABILITY_ACK:
            self._handle_session_capability_ack(payload)
        elif msg_type == proto.MsgType.SESSION_REHOME:
            self._handle_session_rehome(payload)
        elif msg_type == proto.MsgType.SYMBOL_MAP:
            self._handle_symbol_map(payload)
        elif msg_type in (proto.MsgType.SNAPSHOT_FULL, proto.MsgType.SNAPSHOT_DELTA):
            self._handle_snapshot(symbol_id, payload)
        elif msg_type == proto.MsgType.SUBSCRIBE_RESPONSE:
            ids = proto.decode_subscribe_response(payload)
            self._set_subscribed_codes_from_ids(ids)
            logger.debug(f"Subscribe response received: {len(ids)} symbols")
        elif msg_type == proto.MsgType.HEARTBEAT:
            pass
        elif msg_type == proto.MsgType.TOKEN_STATUS:
            self._handle_token_status(payload)
        elif msg_type in (
            proto.MsgType.HISTORY_BEGIN,
            proto.MsgType.HISTORY_DATA,
            proto.MsgType.HISTORY_END,
            proto.MsgType.HISTORY_ERROR,
        ):
            self._enqueue_history_v2_frame(
                msg_type, payload, connection_generation
            )
        elif msg_type == proto.MsgType.HISTORY_RESPONSE:
            logger.info(f"Received HISTORY_RESPONSE, payload_len={len(payload)}")
            self._handle_history_response(payload)
        elif msg_type in proto.RESPONSE_QUERY_MAP:
            self._handle_finance_response(payload)
        else:
            logger.debug(f"Unknown msg_type: 0x{msg_type:04x}")

    def _handle_auth_response(self, payload: bytes):
        success, error_msg = proto.decode_auth_response(payload)
        logger.debug(f"AUTH_RESPONSE payload_len={len(payload)} success={success} error='{error_msg}'")
        self._auth_success = success
        self._auth_error = error_msg
        if success:
            with self._token_status_lock:
                self._token_status = None
        self._auth_event.set()
        if success:
            logger.info("Authenticated")
            self._begin_history_capability_negotiation()
            self._begin_session_capability_negotiation()
        else:
            if self._is_terminal_auth_error(error_msg) and self._conn:
                self._conn.suspend_auto_reconnect()
            logger.error(f"Auth failed: {error_msg}")

    def _begin_history_capability_negotiation(self):
        if not self._history_capability.advertise:
            return
        if (
            self._api_url
            and "history_capability_v1" not in self._protocol_features_supported
        ):
            self._history_capability.mark_peer_unsupported()
            logger.info("History capability discovery unavailable; using V1")
            return

        conn = self._conn
        if conn is None:
            self._history_capability.reset("connection_unavailable")
            return

        message_builder = lambda payload: conn.send(
            proto.build_message(proto.MsgType.CAPABILITY_OFFER, 0, payload)
        )
        if self._history_capability.begin_offer(message_builder):
            logger.info("History capability OFFER sent")
        else:
            logger.warning("History capability OFFER send failed; using V1")

    def _handle_history_capability_ack(self, payload: bytes):
        consumed = self._history_capability.handle_ack(payload)
        if not consumed:
            logger.debug("Ignoring unexpected or late history capability ACK")
            return
        snapshot = self._history_capability.snapshot()
        if snapshot.negotiated:
            logger.info(
                "History capability ACK accepted: v2_eligible=%s "
                "default_enabled=%s",
                snapshot.v2_eligible,
                self._history_capability.default_enabled,
            )
        else:
            logger.warning(
                "Invalid history capability ACK; using V1: %s",
                snapshot.fallback_reason,
            )

    def _begin_session_capability_negotiation(self):
        if not self._session_capability.advertise:
            return
        self._session_capability.set_handoff_enabled(
            "session_rehome_handoff_v1" in self._protocol_features
        )
        if "session_rehome_v1" not in self._protocol_features_supported:
            self._session_capability.mark_peer_unsupported()
            logger.info("Session rehome capability unavailable")
            return
        if "session_rehome_handoff_v1" not in self._protocol_features:
            self._session_capability.mark_peer_unsupported()
            logger.info("Session handoff capability unavailable")
            return
        conn = self._conn
        if conn is None:
            self._session_capability.reset("connection_unavailable")
            return
        if self._session_capability.begin_offer(
            lambda payload: conn.send(
                proto.build_message(
                    proto.MsgType.SESSION_CAPABILITY_OFFER, 0, payload
                )
            )
        ):
            logger.info("Session rehome capability OFFER sent")
        else:
            logger.warning("Session rehome capability OFFER send failed")

    def _handle_session_capability_ack(self, payload: bytes):
        if not self._session_capability.handle_ack(payload):
            logger.debug("Ignoring unexpected session capability ACK")
            return
        snapshot = self._session_capability.snapshot()
        if snapshot.negotiated:
            logger.info("Session rehome capability ACK accepted")
        else:
            logger.warning(
                "Invalid session rehome capability ACK: %s",
                snapshot.fallback_reason,
            )

    def _handle_session_rehome(self, payload: bytes):
        capability = self._session_capability.snapshot()
        if not capability.handoff_eligible:
            logger.warning("Ignoring SESSION_REHOME without negotiated capability")
            return
        try:
            request = session_rehome.RehomeRequest.decode(payload)
        except ValueError as exc:
            logger.warning("Invalid SESSION_REHOME payload: %s", exc)
            return
        if request.migration_id in self._handled_migration_ids:
            logger.info(
                "Ignoring duplicate SESSION_REHOME migration_id=%s",
                request.migration_id,
            )
            return
        self._handled_migration_ids.add(request.migration_id)
        self._handled_migration_order.append(request.migration_id)
        while len(self._handled_migration_order) > 256:
            expired = self._handled_migration_order.popleft()
            self._handled_migration_ids.discard(expired)

        conn = self._conn
        if conn is None:
            return
        self._session_rehome_requested = True
        self._session_rehome_target = request.target_node_id
        self._session_handoff_ticket = request.handoff_ticket
        if not self._session_handoff_ticket:
            self._session_rehome_requested = False
            self._session_rehome_target = ""
            logger.warning("Ignoring SESSION_REHOME without handoff ticket")
            return
        if not conn.request_reconnect(
            "session rehome requested",
            require_pre_reconnect_success=True,
        ):
            self._session_rehome_requested = False
            self._session_rehome_target = ""
            self._session_handoff_ticket = b""
            logger.warning("SESSION_REHOME could not start reconnect")

    @staticmethod
    def _is_terminal_auth_error(error_msg: str) -> bool:
        normalized = (error_msg or "").lower()
        return any(value in normalized for value in (
            "expired", "disabled", "revoked",
        ))

    def _handle_token_status(self, payload: bytes):
        try:
            status = proto.decode_token_status(payload)
        except ValueError as exc:
            message = f"Invalid TOKEN_STATUS message: {exc}"
            logger.warning(message)
            for cb in self._error_callbacks:
                try:
                    cb(message)
                except Exception as callback_exc:
                    logger.error(f"Error callback failed: {callback_exc}")
            return

        with self._token_status_lock:
            self._token_status = status

        if status.status in TERMINAL_TOKEN_STATUSES:
            self._authenticated = False
            if self._conn:
                self._conn.suspend_auto_reconnect()

        expires_at = status.expires_at
        expiry_text = expires_at.isoformat() if expires_at is not None else "never"
        log_message = (
            f"Token status={status.status} severity={status.severity} "
            f"reason={status.reason} expires_at={expiry_text}"
        )
        if status.severity == 'critical' or status.status in {'expired', 'disabled', 'revoked'}:
            logger.error(log_message)
        elif status.severity == 'warning' or status.status == 'expiring':
            logger.warning(log_message)
        else:
            logger.info(log_message)

        for cb in self._token_status_callbacks:
            try:
                cb(status)
            except Exception as exc:
                logger.error(f"Token status callback failed: {exc}")

    def _handle_symbol_map(self, payload: bytes):
        self._symbol_map.update_from_payload(payload)
        self._symbol_map_event.set()

    def _handle_snapshot(self, header_symbol_id: int, payload: bytes):
        quotes_raw = proto.decode_snapshot(payload, header_symbol_id)
        for (sid, bid, ask, last, volume, timestamp) in quotes_raw:
            code = self._symbol_map.id_to_code(sid)
            if code is None:
                code = self._symbol_map.id_to_code(header_symbol_id)
            if code is None:
                code = f"UNKNOWN_{sid}"

            quote = Quote(
                symbol=code, symbol_id=sid,
                bid=bid, ask=ask, last=last,
                volume=volume, timestamp=timestamp,
            )

            with self._quote_cache_lock:
                self._quote_cache[code] = quote

            with self._stats_lock:
                self._quotes_received += 1

            self._dispatch_quote(quote)

    def _dispatch_quote(self, quote: Quote):
        if not self._quote_callbacks:
            return

        if not self._async_callbacks:
            self._run_quote_callbacks(quote)
            return

        try:
            self._callback_queue.put_nowait(quote)
        except queue.Full:
            try:
                self._callback_queue.get_nowait()
            except queue.Empty:
                pass
            with self._stats_lock:
                self._quotes_dropped += 1
            try:
                self._callback_queue.put_nowait(quote)
            except queue.Full:
                with self._stats_lock:
                    self._quotes_dropped += 1

    def _callback_loop(self):
        while not self._callback_stop.is_set():
            try:
                quote = self._callback_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            self._run_quote_callbacks(quote)

    def _run_quote_callbacks(self, quote: Quote):
        for cb in self._quote_callbacks:
            try:
                cb(quote)
            except Exception as e:
                logger.error(f"Quote callback error: {e}")

    def _handle_history_response(self, payload: bytes):
        if len(payload) >= 4:
            request_id = struct.unpack_from('!I', payload)[0]
            with self._pending_lock:
                entry = self._pending_queries.get(request_id)
            if entry is not None and entry.get('history_version') == 2:
                self._fail_history_v2_entry(
                    request_id,
                    entry,
                    "History V2 request received an incompatible V1 response",
                    history_v2.CancelReason.SHUTDOWN,
                )
                return

        header_info, klines = proto.decode_history_response(payload)
        if header_info is None:
            return

        request_id = header_info['request_id']
        with self._pending_lock:
            entry = self._pending_queries.get(request_id)

        if entry is None:
            logger.debug(f"No pending query for request_id={request_id}")
            return

        if not header_info.get('success', True):
            entry['error'] = header_info.get('error') or "History query failed"
            entry['event'].set()
            return

        entry['klines'].extend(klines)
        entry['batches_received'].add(header_info['batch_index'])

        if entry['batch_count'] is None:
            entry['batch_count'] = header_info['batch_count']

        if len(entry['batches_received']) >= entry['batch_count']:
            entry['event'].set()

    def _handle_finance_response(self, payload: bytes):
        request_id, success, error_msg, json_str = proto.decode_finance_response(payload)

        with self._pending_lock:
            entry = self._pending_queries.get(request_id)

        if entry is None:
            logger.warning(f"No pending query for request_id={request_id}")
            return

        if success and json_str:
            try:
                data = json.loads(json_str)
            except Exception:
                data = {}
            entry['result'] = FinanceData(
                stock_code=data.get('stock_code', ''),
                report_period=data.get('report_period', ''),
                data=data,
            )
        else:
            entry['error'] = error_msg or "Finance query failed"

        entry['event'].set()

    def _handle_disconnected(self, reason: str):
        self._authenticated = False
        self._history_capability.reset(reason or "disconnected")
        self._session_capability.reset(reason or "disconnected")

        if not self._auth_event.is_set():
            self._auth_success = False
            self._auth_error = reason or "disconnected before auth response"
            logger.debug(f"Auth wait aborted by disconnect: {self._auth_error}")
            self._auth_event.set()

        disconnect_reason = reason or "connection closed"
        if "closed by server" in disconnect_reason:
            disconnect_reason = "server closed connection (possible slow-consumer protection)"

        self._abort_pending_queries(
            disconnect_reason,
            cancel_reason=history_v2.CancelReason.CLIENT_DISCONNECT,
            send_cancel=False,
        )

        logger.warning(f"Disconnected: {disconnect_reason}")
        for cb in self._disconnect_callbacks:
            try:
                cb(disconnect_reason)
            except Exception:
                pass

    def _before_reconnect(self):
        if self._api_url:
            try:
                self._do_discovery(timeout=10.0)
                self._conn._host = self._host
                self._conn._port = self._port
                if (
                    self._session_rehome_requested
                    and self._session_rehome_target
                    and self._current_node_id != self._session_rehome_target
                ):
                    raise DiscoveryError(
                        "discovery returned node "
                        f"{self._current_node_id or '<unknown>'}, expected "
                        f"{self._session_rehome_target}"
                    )
                return True
            except Exception as e:
                if self._session_rehome_requested:
                    logger.warning(
                        "Required rehome discovery failed; old endpoint will not be reused: %s",
                        e,
                    )
                    raise
                logger.warning(f"Re-discovery failed, using cached endpoint: {e}")
                return False
        return not self._session_rehome_requested

    def _handle_reconnected(self):
        self._auth_success = False
        self._auth_error = ""
        self._auth_event.clear()
        self._symbol_map_event.clear()
        if (self._session_rehome_requested and self._session_handoff_ticket and
                (not self._conn or not self._conn.send(proto.build_message(
                    proto.MsgType.SESSION_HANDOFF_OFFER, 0,
                    self._session_handoff_ticket)))):
            raise RuntimeError("Connection lost while sending session handoff ticket")
        if not self._conn or not self._conn.send(proto.encode_auth(self._token)):
            raise RuntimeError("Connection lost while sending re-authentication")

        if not self._auth_event.wait(timeout=30):
            if not self._conn or not self._conn.connected:
                raise RuntimeError(
                    f"Re-auth failed: {self._auth_error or 'disconnected before auth response'}")
            raise RuntimeError("Re-auth timeout after reconnect")

        if not self._auth_success:
            raise RuntimeError(f"Re-auth failed: {self._auth_error}")

        if self._symbol_map.size > 0:
            self._symbol_map_event.set()
            logger.info("Using cached symbol map while restoring connection")
        elif not self._symbol_map_event.wait(timeout=30):
            if not self._conn or not self._conn.connected:
                raise RuntimeError(
                    "Connection lost while waiting for symbol map after reconnect")
            logger.warning("Symbol map not received after reconnect, using cached map")

        if not self._conn or not self._conn.connected:
            raise RuntimeError("Connection lost during reconnect state restore")

        if self._symbol_map.size == 0:
            raise RuntimeError("Symbol map is empty after reconnect, cannot restore subscriptions")

        self._authenticated = True

        with self._subscribed_lock:
            codes = list(self._subscribed_codes)
        if codes:
            ids = self._symbol_map.codes_to_ids(codes)
            if ids:
                if not self._conn.send(proto.encode_subscribe(ids)):
                    self._authenticated = False
                    raise RuntimeError(
                        "Connection lost while restoring subscriptions")
                logger.info(f"Re-subscribed {len(ids)} symbols after reconnect")
            else:
                raise RuntimeError(f"codes_to_ids returned empty for {codes}")

        if not self._conn.connected:
            self._authenticated = False
            raise RuntimeError("Connection lost before reconnect completed")

        for cb in self._connect_callbacks:
            try:
                cb()
            except Exception:
                pass

        logger.info(
            "Reconnect state restored: node_id=%s",
            self.current_node_id or "unknown",
        )

    def _handle_reconnect_completed(self):
        self._session_rehome_requested = False
        self._session_rehome_target = ""
        self._session_handoff_ticket = b""

    def _alloc_request_id(self) -> int:
        with self._request_id_lock:
            rid = self._next_request_id
            self._next_request_id += 1
            return rid

    @property
    def last_subscribe_warning(self) -> Optional[str]:
        return self._last_subscribe_warning

    @property
    def last_subscribe_rejected(self) -> List[str]:
        return list(self._last_subscribe_rejected)

    @property
    def last_subscribe_confirmed(self) -> List[str]:
        return list(self._last_subscribe_confirmed)

    @property
    def last_subscribe_requested(self) -> List[str]:
        return list(self._last_subscribe_requested)

    @property
    def current_host(self) -> str:
        return self._host

    @property
    def current_port(self) -> int:
        return self._port

    @property
    def current_node_id(self) -> str:
        return self._current_node_id

    @property
    def gateway_version(self) -> str:
        return self._gateway_version

    @property
    def protocol_features(self) -> List[str]:
        return list(self._protocol_features)

    @property
    def protocol_features_supported(self) -> List[str]:
        return list(self._protocol_features_supported)

    @property
    def history_capabilities(self):
        return self._history_capability.snapshot().capabilities

    @property
    def history_v2_eligible(self) -> bool:
        return self._history_capability.snapshot().v2_eligible

    @property
    def history_capability_state(self) -> str:
        return self._history_capability.snapshot().state

    @property
    def history_capability_fallback_reason(self) -> str:
        return self._history_capability.snapshot().fallback_reason

    @property
    def session_rehome_negotiated(self) -> bool:
        return self._session_capability.snapshot().rehome_eligible

    @property
    def session_capability_state(self) -> str:
        return self._session_capability.snapshot().state

    @property
    def session_capability_fallback_reason(self) -> str:
        return self._session_capability.snapshot().fallback_reason

    @property
    def token_status(self) -> Optional[TokenStatus]:
        with self._token_status_lock:
            return self._token_status

    @property
    def token_expires_ms(self) -> Optional[int]:
        status = self.token_status
        if status is None or status.never_expires:
            return None
        return status.expires_ms

    @property
    def token_expires_at(self):
        status = self.token_status
        return status.expires_at if status is not None else None

    @property
    def current_endpoint(self) -> str:
        return f"{self._host}:{self._port}"
