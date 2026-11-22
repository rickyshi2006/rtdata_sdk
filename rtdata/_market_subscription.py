"""连接级市场分类意图、增量身份屏障与小型订阅回执。"""

from collections import deque
import json
import threading
import copy
import time
import logging

from . import _protocol as proto
from .exceptions import ConnectionError, RtdataError

MARKETS = ('cn_equity', 'hk_equity', 'futures', 'us_equity', 'fx',
           'overseas_futures', 'crypto')
REQUEST, RESPONSE, IDENTITIES, IDENTITY_ACK = 0x70, 0x71, 0x72, 0x73
MAX_SYMBOLS, MAX_CONTROL_BYTES = 250000, 65536
MAX_QUEUED_OPERATIONS, MAX_QUEUED_BYTES = 64, 1024 * 1024
TERMINAL = ('applied', 'rejected', 'incomplete', 'disconnected', 'negotiated', 'catalog', 'issues')
logger = logging.getLogger(__name__)


class MarketSubscription:
    """一次订阅操作。应用成功不代表已收到行情或上游完整覆盖。"""

    def __init__(self, client, fields):
        self._client = client
        self._fields = fields
        self._result = {'state': 'queued', 'upstream_state': 'unknown', 'acceptance_is_data': False}
        self._event = threading.Event()
        self._bytes = 0
        self._deadline = None

    def wait_applied(self, timeout=30):
        """等待本次操作应用完成；超时不会自动取消服务器上的操作。"""
        if not self._event.wait(timeout):
            raise TimeoutError('market subscription timeout; inspect status before retrying')
        result = self.status()
        if result['state'] not in ('applied', 'negotiated', 'catalog', 'issues'):
            raise RtdataError(result.get('reason') or result['state'])
        return result

    def status(self):
        """返回本次操作最近回执；自动目录更新见客户端 market_subscription_status。"""
        with self._client._market_changed:
            return copy.deepcopy(self._result)

    def list_issues(self, limit=100, offset=0, catalog_revision=None, *, scope='catalog',
                    upstream_revision=None, subscription_generation=None):
        return self._client.list_market_issues(limit, offset, catalog_revision,
                                             scope=scope, upstream_revision=upstream_revision,
                                             subscription_generation=subscription_generation)


class MarketSubscriptionMixin:
    def _init_markets(self):
        self._market_changed = threading.Condition(self._subscribed_lock)
        self._desired_markets = set()
        self._market_queue = deque()
        self._market_inflight = None
        self._market_peer = False
        self._market_ready = False
        self._market_request_id = 0
        self._market_connection = None
        self._market_generation = 0
        self._market_stage = None
        self._market_identities = {}
        self._market_codes = {}
        self._market_sources = {}
        self._market_status = {'state': 'unsupported'}
        self._market_feed_status = {'state': 'unsupported', 'markets': []}
        self._market_feed_callbacks = []
        self._market_feed_supported = False
        self._market_feed_revision = 0
        self._market_committed_generation = 0
        self._market_restore = False
        self._market_restore_codes = ()
        self._market_restore_offset = 0
        self._market_restore_markets = ()
        self._market_queued_bytes = 0
        self._market_retry_at = 0
        self._market_stage_deadline = None

    def _reset_markets(self):
        with self._market_changed:
            pending = list(self._market_queue)
            if self._market_inflight:
                pending.append(self._market_inflight)
            for handle in pending:
                handle._result = {'state': 'disconnected', 'reason': 'connection_lost'}
                handle._event.set()
            self._market_queue.clear()
            self._market_queued_bytes = 0
            self._market_restore_codes = ()
            self._market_restore_markets = ()
            self._market_restore_offset = 0
            self._market_restore = False
            self._market_retry_at = 0
            self._market_stage_deadline = None
            self._market_inflight = None
            self._market_ready = self._market_peer = False
            self._market_connection = None
            self._market_stage = None
            self._market_identities.clear()
            self._market_codes.clear()
            self._market_sources.clear()
            self._market_status = {'state': 'disconnected'}
            self._market_feed_status = {'state': 'disconnected', 'markets': []}
            self._market_feed_supported = False
            self._market_feed_revision = self._market_committed_generation = 0
            self._market_changed.notify_all()

    def _begin_markets(self, payload):
        self._reset_markets()
        try:
            hint = json.loads(payload[1:]) if 1 < len(payload) <= 4096 else {}
        except (ValueError, TypeError):
            hint = {}
        with self._market_changed:
            if not isinstance(hint, dict) or hint.get('market_subscription_v1') is not True:
                return
            self._market_peer = True
            self._market_request_id = 0
            self._market_connection = self._conn
            self._market_generation = self._conn.generation
            self._market_restore = True
            handle = MarketSubscription(self, {'op': 'hello', 'symbol_capacity': MAX_SYMBOLS, 'feed_status_v1': True})
            self._enqueue_market_locked(handle)
            self._flush_markets_locked()

    @staticmethod
    def _market_size(fields):
        probe = dict(fields, version=1, request_id=2**64 - 1)
        return len(json.dumps(probe, ensure_ascii=True, separators=(',', ':')).encode('ascii'))

    def _enqueue_market_locked(self, handle):
        handle._bytes = self._market_size(handle._fields)
        if handle._bytes > MAX_CONTROL_BYTES:
            raise ValueError('market control byte limit; split manual symbols into smaller requests')
        count = len(self._market_queue) + (self._market_inflight is not None)
        reserve = bool(self._market_restore or self._market_restore_codes or self._market_restore_markets)
        if (count >= MAX_QUEUED_OPERATIONS - reserve or
                self._market_queued_bytes + handle._bytes > MAX_QUEUED_BYTES - reserve * MAX_CONTROL_BYTES):
            raise RtdataError('market operation queue full')
        self._market_queued_bytes += handle._bytes
        self._market_queue.append(handle)

    def _abort_markets_locked(self, reason):
        conn = self._market_connection
        if conn is not None:
            conn.abort_generation(self._market_generation)
        self._reset_markets()
        self._authenticated = False
        self._market_status = {'state': 'disconnected', 'reason': reason}

    def _poll_markets(self, connection, generation):
        with self._market_changed:
            if connection is not self._market_connection or generation != self._market_generation:
                return
            now = time.monotonic()
            handle = self._market_inflight
            if handle is None and self._market_queue:
                handle = self._market_queue[0]
            if ((handle is not None and handle._deadline is not None and now >= handle._deadline)
                    or (self._market_stage_deadline is not None and now >= self._market_stage_deadline)):
                self._abort_markets_locked('market_control_timeout')
                return
            try:
                self._flush_markets_locked()
            except ConnectionError:
                pass

    def _market_send_locked(self, kind, body):
        payload = json.dumps(body, ensure_ascii=True, separators=(',', ':')).encode('ascii')
        if len(payload) > MAX_CONTROL_BYTES:
            raise ValueError('market control byte limit')
        conn = self._market_connection
        if (conn is None or conn is not self._conn or conn.generation != self._market_generation
                or not conn.send_for_generation(proto.build_message(kind, 0, payload), self._market_generation)):
            raise ConnectionError('market subscription connection lost')

    def _flush_markets_locked(self):
        if self._market_inflight or self._market_stage is not None or time.monotonic() < self._market_retry_at:
            return
        ready = self._market_ready and self._authenticated and self._subscription_protocol_ready
        if ready and self._market_restore:
            self._market_restore = False
            self._market_restore_markets = tuple(sorted(self._desired_markets))
            self._market_restore_codes = tuple(self._desired_subscriptions)
            self._market_restore_offset = 0
            if len(self._market_restore_codes) > MAX_SYMBOLS:
                self._abort_markets_locked('market_restore_capacity')
                raise ConnectionError('market restore exceeds symbol capacity')
        # Restoration keeps one bounded frame, not hundreds of queued requests.
        restore = None
        retry = bool(self._market_queue and self._market_queue[0]._deadline is not None)
        if ready and not retry and self._market_restore_markets:
            restore = MarketSubscription(self, dict(op='subscribe', markets=list(self._market_restore_markets)))
            self._market_restore_markets = ()
        elif ready and not retry and self._market_restore_offset < len(self._market_restore_codes):
            codes, size = [], 128
            while self._market_restore_offset < len(self._market_restore_codes) and len(codes) < 512:
                code = self._market_restore_codes[self._market_restore_offset]
                encoded_size = len(json.dumps(code, ensure_ascii=True)) + 1
                if size + encoded_size > MAX_CONTROL_BYTES:
                    break
                codes.append(code)
                size += encoded_size
                self._market_restore_offset += 1
            if not codes:
                self._abort_markets_locked('invalid_restore_symbol')
                raise ConnectionError('invalid restore symbol')
            restore = MarketSubscription(self, dict(op='subscribe', symbols=codes))
            if self._market_restore_offset == len(self._market_restore_codes):
                self._market_restore_codes = ()
                self._market_restore_offset = 0
        if restore is not None:
            restore._bytes = self._market_size(restore._fields)
            # User enqueue reserves space for this single restoration frame.
            self._market_queued_bytes += restore._bytes
        elif not self._market_queue:
            return
        handle = restore or self._market_queue[0]
        if handle._fields['op'] != 'hello' and not (
                ready):
            return
        self._market_request_id += 1
        handle._fields.update(version=1, request_id=self._market_request_id)
        self._market_inflight = restore or self._market_queue.popleft()
        if handle._deadline is None:
            handle._deadline = time.monotonic() + (10 if handle._fields['op'] == 'hello' else 35)
        try:
            self._market_send_locked(REQUEST, handle._fields)
        except ConnectionError:
            self._abort_markets_locked('market_control_send_failed')
            raise

    def _resume_markets(self):
        with self._market_changed:
            if not self._market_peer:
                if self._desired_markets:
                    self._market_status = {'state': 'unsupported', 'reason': 'market_subscription_unavailable'}
                return False
            self._flush_markets_locked()
            return True

    def _market_request(self, op, **fields):
        with self._market_changed:
            if not self._authenticated:
                raise ConnectionError('Not connected')
            if not self._market_peer:
                raise RtdataError('market_subscription_unavailable on the connected endpoint')
            for code in fields.get('symbols', ()):
                if (not isinstance(code, str) or not 1 <= len(code) <= 256
                        or any(not 32 <= ord(c) < 127 for c in code)):
                    raise ValueError('invalid symbol code')
            handle = MarketSubscription(self, dict(op=op, **fields))
            self._enqueue_market_locked(handle)
            self._flush_markets_locked()
            return handle

    def get_markets(self, timeout=10):
        """查询当前连接节点的七类目录汇总，不代表交易所完整覆盖。"""
        return self._market_request('catalog').wait_applied(timeout)['catalog']

    def subscribe_market(self, market):
        return self.subscribe_markets([market])

    def subscribe_markets(self, markets):
        if isinstance(markets, str):
            raise ValueError('markets must be a sequence of category names')
        markets = list(dict.fromkeys(markets))
        if not markets or any(market not in MARKETS for market in markets):
            raise ValueError('unknown or empty market category')
        return self._market_request('subscribe', markets=markets)

    def unsubscribe_market(self, market):
        if market not in MARKETS:
            raise ValueError('unknown market category')
        return self._market_request('unsubscribe', markets=[market])

    @property
    def market_subscription_status(self):
        with self._market_changed:
            return copy.deepcopy(self._market_status)

    @property
    def market_feed_status(self):
        """最近分类供数采样；与订阅操作回执分开，不保证逐笔无损。"""
        with self._market_changed:
            return copy.deepcopy(self._market_feed_status)

    @property
    def on_market_feed_status(self):
        def decorator(callback):
            with self._market_changed:
                self._market_feed_callbacks.append(callback)
            return callback
        return decorator

    def _handle_market_feed(self, body, symbol, generation, connection):
        # Optional telemetry must not complete, reject, or corrupt a transaction.
        with self._market_changed:
            if (not self._market_feed_supported or symbol or connection is not self._market_connection
                    or connection is not self._conn or connection is None
                    or connection.generation != self._market_generation
                    or generation != self._market_generation):
                return
            try:
                for key in ('version', 'request_id', 'revision', 'subscription_generation', 'sample_duration_ms'):
                    if type(body.get(key)) is not int or body[key] < 0:
                        raise ValueError('invalid feed integer')
                if (body['version'] != 1 or body['request_id'] != 0 or body.get('atomic') is not False
                        or body['subscription_generation'] != self._market_committed_generation
                        or body['revision'] <= self._market_feed_revision):
                    return
                rows = body['markets']
                if not isinstance(rows, list) or len(rows) > 7:
                    raise ValueError('invalid feed categories')
                categories = set()
                for row in rows:
                    category = row['category']
                    if category not in self._desired_markets or category in categories:
                        raise ValueError('unexpected feed category')
                    categories.add(category)
                    for key in ('admitted_count', 'available_count', 'primary_count', 'backup_count',
                                'unavailable_count', 'unknown_count'):
                        if type(row.get(key)) is not int or not 0 <= row[key] <= MAX_SYMBOLS:
                            raise ValueError('invalid feed count')
                    total, available = row['admitted_count'], row['available_count']
                    expected = ('empty' if not total else 'normal' if available == total else
                                'partial' if available else 'pending' if row['unknown_count'] else 'paused')
                    if (available != row['primary_count'] + row['backup_count']
                            or total != available + row['unavailable_count'] + row['unknown_count']
                            or row['state'] != expected or row['event'] not in ('updated', 'recovered')
                            or not isinstance(row['reason'], str)):
                        raise ValueError('inconsistent feed summary')
                if categories != self._desired_markets:
                    raise ValueError('incomplete feed categories')
                self._market_feed_revision = body['revision']
                self._market_feed_status = copy.deepcopy(body)
                callbacks = tuple(self._market_feed_callbacks)
            except (ValueError, KeyError, TypeError):
                logger.warning('Ignored invalid market feed status')
                return
        for callback in callbacks:
            try:
                callback(copy.deepcopy(body))
            except Exception:
                logger.exception('Market feed status callback failed')

    def list_market_issues(self, limit=100, offset=0, catalog_revision=None, timeout=10,
                          *, scope='catalog', upstream_revision=None, subscription_generation=None):
        if type(limit) is not int or not 1 <= limit <= 512 or type(offset) is not int or offset < 0:
            raise ValueError('invalid issue page')
        if scope not in ('catalog', 'upstream'):
            raise ValueError('invalid issue scope')
        fields = dict(limit=limit, offset=offset, scope=scope)
        for key, value in (('upstream_revision', upstream_revision), ('subscription_generation', subscription_generation)):
            if value is not None:
                if type(value) is not int or value < 0:
                    raise ValueError('invalid issue revision')
                fields[key] = value
        if catalog_revision is not None:
            fields['catalog_revision'] = catalog_revision
        return self._market_request('issues', **fields).wait_applied(timeout)['page']

    def _market_code(self, symbol):
        with self._market_changed:
            if self._market_ready:
                return self._market_identities.get(symbol)
        return self._symbol_map.id_to_code(symbol)

    def _market_source(self, symbol):
        with self._market_changed:
            return self._market_sources.get(symbol, 0)

    def _handle_market_control(self, kind, symbol, payload, generation, connection):
        if kind == RESPONSE and len(payload) <= MAX_CONTROL_BYTES:
            try:
                optional = json.loads(payload)
            except (ValueError, TypeError, RecursionError):
                optional = None
            if isinstance(optional, dict) and optional.get('state') == 'feed_status':
                self._handle_market_feed(optional, symbol, generation, connection)
                return
        with self._market_changed:
            conn = self._market_connection
            if (not self._market_peer or conn is not self._conn or conn is None
                    or conn.generation != self._market_generation
                    or (connection is not None and connection is not conn)
                    or (generation is not None and generation != self._market_generation)):
                return
            try:
                if symbol or len(payload) > MAX_CONTROL_BYTES:
                    raise ValueError('invalid market frame')
                body = json.loads(payload)
                if not isinstance(body, dict) or type(body.get('version')) is not int or body['version'] != 1:
                    raise ValueError('invalid market version')
                if kind == IDENTITIES:
                    self._market_identity_locked(body)
                    return
                rid = body.get('request_id')
                if type(rid) is not int or rid < 0:
                    raise ValueError('invalid market request id')
                handle = self._market_inflight
                if rid != 0 and (handle is None or rid != handle._fields['request_id']):
                    raise ValueError('unexpected market response')
                state = body['state']
                if state not in TERMINAL + ('checking', 'applying', 'updating', 'idle'):
                    raise ValueError('invalid market state')
                if state == 'rejected' and rid and handle and body.get('reason') == 'subscription_busy':
                    self._market_inflight = None
                    self._market_queue.appendleft(handle)
                    self._market_retry_at = time.monotonic() + .1
                    return
                if rid and handle and handle._fields['op'] == 'hello' and state != 'negotiated':
                    raise ValueError('market negotiation rejected')
                if state == 'negotiated':
                    if not rid or handle is None or handle._fields['op'] != 'hello':
                        raise ValueError('unexpected market negotiation')
                    self._market_ready = True
                    self._market_feed_supported = body.get('feed_status_v1') is True
                    self._market_feed_status = {'state': 'pending' if self._market_feed_supported else 'unsupported', 'markets': []}
                retired = ()
                if state == 'applied':
                    stage = self._market_stage
                    if (stage is None or stage['generation'] != body['generation'] or not stage['done']
                            or stage['request_id'] != rid or type(body['admitted_count']) is not int):
                        raise ValueError('application before identity barrier')
                    identities = self._market_identities
                    retired = tuple((sid, identities.get(sid)) for sid in stage['removed'])
                    for sid in stage['removed']:
                        code = identities.pop(sid, None)
                        self._market_codes.pop(code, None)
                        self._market_sources.pop(sid, None)
                    for sid, code in stage['added'].items():
                        if code in self._market_codes:
                            raise ValueError('duplicate committed code')
                    identities.update(stage['added'])
                    self._market_sources.update(stage['sources'])
                    self._market_codes.update((code, sid) for sid, code in stage['added'].items())
                    if len(identities) != body['admitted_count'] or len(identities) > MAX_SYMBOLS:
                        raise ValueError('market identity count mismatch')
                    self._market_stage = None
                    self._market_stage_deadline = None
                    if rid and handle:
                        self._market_commit_intent_locked(handle._fields)
                    self._market_committed_generation = body['generation']
                    if self._market_feed_supported:
                        self._market_feed_status = {'state': 'pending', 'subscription_generation': body['generation'], 'markets': []}
                elif state in ('rejected', 'incomplete'):
                    if self._market_stage and self._market_stage['request_id'] == rid:
                        self._market_stage = None
                        self._market_stage_deadline = None
                if state not in ('catalog', 'issues'):
                    self._market_status = body
                if rid and handle:
                    handle._result = body
                    if state in TERMINAL:
                        handle._event.set()
                        self._market_inflight = None
                        self._market_queued_bytes -= handle._bytes
                self._market_changed.notify_all()
                self._flush_markets_locked()
            except (ValueError, KeyError, TypeError, AttributeError, RecursionError, ConnectionError) as exc:
                self._abort_markets_locked(str(exc))
                return
        self._resume_markets()
        if retired:
            with self._realtime_lock:
                with self._quote_cache_lock:
                    for sid, code in retired:
                        self._realtime_watermarks.pop(sid, None)
                        self._quote_cache.pop(code, None)
                        self._realtime_cache_bytes -= self._realtime_cache_sizes.pop(code, 0)
                self._realtime_changed.notify_all()

    def _market_commit_intent_locked(self, fields):
        op = fields['op']
        if op == 'clear':
            self._desired_markets.clear()
            self._desired_subscriptions.clear()
        elif op == 'subscribe':
            self._desired_markets.update(fields.get('markets', ()))
            for code in fields.get('symbols', ()):
                self._desired_subscriptions[code] = 0
        elif op == 'unsubscribe':
            self._desired_markets.difference_update(fields.get('markets', ()))
            for code in fields.get('symbols', ()):
                self._desired_subscriptions.pop(code, None)

    def _market_identity_locked(self, body):
        for key in ('generation', 'request_id', 'offset', 'next_offset', 'total'):
            if type(body.get(key)) is not int or body[key] < 0:
                raise ValueError('invalid identity cursor')
        if not isinstance(body.get('catalog_revision'), str) or not body['catalog_revision']:
            raise ValueError('invalid catalog revision')
        entries = body['entries']
        if (not isinstance(entries, list) or len(entries) > 512 or body['total'] > MAX_SYMBOLS * 2
                or body['next_offset'] != body['offset'] + len(entries)
                or body['next_offset'] > body['total']
                or type(body['done']) is not bool or body['done'] != (body['next_offset'] == body['total'])
                or (not entries and not body['done'])):
            raise ValueError('invalid identity page')
        handle = self._market_inflight
        if body['request_id'] and (not handle or body['request_id'] != handle._fields['request_id']):
            raise ValueError('unexpected identity request')
        stage = self._market_stage
        if stage is None:
            stage = {key: body[key] for key in ('generation', 'request_id', 'catalog_revision', 'total')}
            stage.update(offset=0, added={}, sources={}, removed=set(), codes=set(), done=False)
            self._market_stage = stage
            self._market_stage_deadline = time.monotonic() + 35
        if (stage['done'] or stage['offset'] != body['offset'] or any(stage[key] != body[key]
                for key in ('generation', 'request_id', 'catalog_revision', 'total'))):
            raise ValueError('identity generation or cursor mismatch')
        for entry in entries:
            sid = entry['symbol_id']
            if type(sid) is not int or not 0 < sid <= 0xffffffff or sid in stage['added'] or sid in stage['removed']:
                raise ValueError('duplicate or invalid identity')
            if entry['action'] == 'remove':
                if sid not in self._market_identities:
                    raise ValueError('removing unknown identity')
                stage['removed'].add(sid)
            elif entry['action'] == 'add':
                code = entry['code']
                if (not isinstance(code, str) or not 1 <= len(code) <= 256
                        or any(not 32 <= ord(c) < 127 for c in code) or code in stage['codes']
                        or sid in self._market_identities):
                    raise ValueError('invalid identity code')
                stage['added'][sid] = code
                stage['codes'].add(code)
                category = entry.get('category', '')
                if category not in MARKETS and category != '':
                    raise ValueError('invalid identity category')
                stage['sources'][sid] = MARKETS.index(category) + 1 if category else 0
            else:
                raise ValueError('invalid identity action')
        stage['offset'], stage['done'] = body['next_offset'], body['done']
        self._market_send_locked(IDENTITY_ACK, {key: body[key] for key in
            ('version', 'generation', 'catalog_revision', 'next_offset')})
