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
from datetime import datetime, date, time as dt_time
from typing import Callable, Optional, List, Dict, Union

from . import _protocol as proto
from ._connection import Connection
from ._history_segment_cache import HistorySegmentCache
from ._symbol_map import SymbolMap
from .models import Quote, Kline, FinanceData, TokenStatus
from .exceptions import (
    AuthenticationError, ConnectionError, SymbolNotFoundError,
    QueryTimeoutError, QueryError, DiscoveryError, DisconnectedError,
)

logger = logging.getLogger(__name__)
VALID_ADJUSTS = {'none', 'forward', 'backward'}
TERMINAL_TOKEN_STATUSES = {'expired', 'disabled', 'revoked'}


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
    ):
        self._token = token
        self._host = host
        self._port = port
        self._api_url = api_url
        self._current_node_id = ""
        self._gateway_version = ""
        self._protocol_features: List[str] = []
        self._heartbeat_interval = heartbeat_interval
        self._auto_reconnect = auto_reconnect
        self._async_callbacks = async_callbacks
        self._callback_queue_size = callback_queue_size

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
            self._conn.close()
            self._conn = None
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

        if self._api_url and self._symbol_map.size > 0:
            self._symbol_map_event.set()
            logger.info("Symbol map already loaded via discovery API")
        elif not self._symbol_map_event.wait(timeout=timeout):
            logger.warning("Symbol map not received, using cache if available")

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
        if isinstance(protocol_info, dict):
            features = protocol_info.get('features_enabled', [])
            self._protocol_features = (
                [str(value) for value in features]
                if isinstance(features, list)
                else []
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
        self._callback_stop.set()
        if self._callback_thread and self._callback_thread.is_alive():
            self._callback_thread.join(timeout=3)
        if self._conn:
            self._conn.close()
            self._conn = None

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

    def _perform_history_query(self, symbol: str, period: str,
                               start_ms: int, end_ms: int,
                               max_count: int, timeout: float,
                               adjust: str = 'none') -> List[Kline]:
        if not self._authenticated:
            raise ConnectionError("Not connected")

        symbol_id = self._symbol_map.code_to_id(symbol) or 0
        request_id = self._alloc_request_id()
        entry = {
            'event': threading.Event(),
            'klines': [],
            'batches_received': set(),
            'batch_count': None,
            'error': None,
        }
        with self._pending_lock:
            self._pending_queries[request_id] = entry

        logger.debug(
            f"Sending history request: symbol={symbol} period={period} request_id={request_id} "
            f"start={start_ms} end={end_ms} count={max_count} adjust={adjust}"
        )
        msg = proto.encode_history_request(
            request_id, symbol_id, period, start_ms, end_ms, max_count, symbol, adjust=adjust)
        self._conn.send(msg)

        if not entry['event'].wait(timeout=timeout):
            with self._pending_lock:
                self._pending_queries.pop(request_id, None)
            raise QueryTimeoutError(f"History query timeout for {symbol}")

        with self._pending_lock:
            self._pending_queries.pop(request_id, None)

        if entry['error']:
            if "disconnected" in entry['error'] or "server closed" in entry['error']:
                raise DisconnectedError(entry['error'])
            raise QueryError(entry['error'])

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
        for missing_start, missing_end_exclusive in missing_ranges:
            fetch_end_ms = max(missing_start, missing_end_exclusive - 1)
            fetched = self._perform_history_query(
                symbol, period, missing_start, fetch_end_ms, 5000, timeout, adjust=adjust)
            self._history_cache.store_range(
                symbol,
                period,
                adjust,
                missing_start,
                missing_end_exclusive,
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

        return self._perform_history_query(
            symbol, period, start_ms, end_ms, 5000, timeout, adjust=adjust)

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

    def _dispatch_message(self, msg_type: int, symbol_id: int, payload: bytes):
        logger.debug(f"_dispatch_message: msg_type=0x{msg_type:04x} symbol_id={symbol_id} payload_len={len(payload)}")
        with self._stats_lock:
            self._messages_received += 1
            self._bytes_received += proto.HEADER_SIZE + len(payload)

        if msg_type == proto.MsgType.AUTH_RESPONSE:
            self._handle_auth_response(payload)
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
        else:
            if self._is_terminal_auth_error(error_msg) and self._conn:
                self._conn.suspend_auto_reconnect()
            logger.error(f"Auth failed: {error_msg}")

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

        if not self._auth_event.is_set():
            self._auth_success = False
            self._auth_error = reason or "disconnected before auth response"
            logger.debug(f"Auth wait aborted by disconnect: {self._auth_error}")
            self._auth_event.set()

        disconnect_reason = reason or "connection closed"
        if "closed by server" in disconnect_reason:
            disconnect_reason = "server closed connection (possible slow-consumer protection)"

        with self._pending_lock:
            pending = list(self._pending_queries.values())
            self._pending_queries.clear()

        for entry in pending:
            entry['error'] = disconnect_reason
            entry['event'].set()

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
            except Exception as e:
                logger.warning(f"Re-discovery failed, using cached endpoint: {e}")

    def _handle_reconnected(self):
        self._auth_success = False
        self._auth_error = ""
        self._auth_event.clear()
        self._symbol_map_event.clear()
        if not self._conn or not self._conn.send(proto.encode_auth(self._token)):
            raise RuntimeError("Connection lost while sending re-authentication")

        if not self._auth_event.wait(timeout=30):
            if not self._conn or not self._conn.connected:
                raise RuntimeError(
                    f"Re-auth failed: {self._auth_error or 'disconnected before auth response'}")
            raise RuntimeError("Re-auth timeout after reconnect")

        if not self._auth_success:
            raise RuntimeError(f"Re-auth failed: {self._auth_error}")

        if not self._symbol_map_event.wait(timeout=30):
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
