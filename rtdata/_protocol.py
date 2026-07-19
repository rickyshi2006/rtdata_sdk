"""rtdata SDK 二进制协议编解码

10 字节 Header (big-endian):
  [0..3] payload_length  uint32
  [4..7] symbol_id       uint32
  [8..9] msg_type        uint16

字节序规则:
  - 整数 (uint16/32/64, int64): 大端 (network order)
  - float/double: 小端原生序 (C++ memcpy, x86_64 little-endian)
"""
import struct
import time
import logging
import json

from .models import TokenStatus

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================

HEADER_SIZE = 10
HEADER_FMT = '!IIH'  # big-endian: payload_length, symbol_id, msg_type
MAX_PAYLOAD_SIZE = 16 * 1024 * 1024

QUOTE_SIZE = 32   # 每条 Quote 32 字节
KLINE_SIZE = 48   # 每条 Kline 48 字节
HISTORY_RESP_HEADER_SIZE = 19


class MsgType:
    SNAPSHOT_FULL       = 0x01
    SNAPSHOT_DELTA      = 0x02
    HEARTBEAT           = 0x03
    AUTH                = 0x04
    SYMBOL_MAP          = 0x05
    TOKEN_SYNC          = 0x06
    TOKEN_FULL_SYNC     = 0x07
    SUBSCRIPTION_DEMAND = 0x0A

    HISTORY_REQUEST     = 0x10
    HISTORY_RESPONSE    = 0x11
    SYMBOL_LIST_REQUEST = 0x12
    SYMBOL_LIST_RESPONSE = 0x13

    FINANCE_REQUEST         = 0x20
    FINANCE_RESPONSE        = 0x21
    FINANCE_TTM_REQUEST     = 0x22
    FINANCE_TTM_RESPONSE    = 0x23
    FINANCE_PIT_REQUEST     = 0x24
    FINANCE_PIT_RESPONSE    = 0x25
    FINANCE_RATIOS_REQUEST  = 0x26
    FINANCE_RATIOS_RESPONSE = 0x27

    SUBSCRIBE_REQUEST   = 0x30
    UNSUBSCRIBE_REQUEST = 0x31
    AUTH_RESPONSE       = 0x40
    SUBSCRIBE_RESPONSE  = 0x41
    TOKEN_STATUS        = 0x42


# 周期映射: 字符串 → uint8
PERIOD_MAP = {
    '1m': 1, '5m': 2, '15m': 3, '30m': 4,
    '1h': 5, '1d': 6, '1w': 7, '1M': 8,
    '2h': 9, '4h': 10,
}

ADJUST_MAP = {
    'none': 0,
    'forward': 1,
    'backward': 2,
}

# 查询消息类型: request → response
QUERY_RESPONSE_MAP = {
    MsgType.HISTORY_REQUEST: MsgType.HISTORY_RESPONSE,
    MsgType.FINANCE_REQUEST: MsgType.FINANCE_RESPONSE,
    MsgType.FINANCE_TTM_REQUEST: MsgType.FINANCE_TTM_RESPONSE,
    MsgType.FINANCE_PIT_REQUEST: MsgType.FINANCE_PIT_RESPONSE,
    MsgType.FINANCE_RATIOS_REQUEST: MsgType.FINANCE_RATIOS_RESPONSE,
}

# response → request (反向映射)
RESPONSE_QUERY_MAP = {v: k for k, v in QUERY_RESPONSE_MAP.items()}


# ============================================================================
# Header 编解码
# ============================================================================

def encode_header(payload_length: int, symbol_id: int, msg_type: int) -> bytes:
    return struct.pack(HEADER_FMT, payload_length, symbol_id, msg_type)


def decode_header(data: bytes):
    """返回 (payload_length, symbol_id, msg_type)"""
    return struct.unpack(HEADER_FMT, data[:HEADER_SIZE])


def build_message(msg_type: int, symbol_id: int, payload: bytes) -> bytes:
    header = encode_header(len(payload), symbol_id, msg_type)
    return header + payload


# ============================================================================
# AUTH
# ============================================================================

def encode_auth(token: str) -> bytes:
    payload = token.encode('utf-8')
    return build_message(MsgType.AUTH, 0, payload)


def decode_auth_response(payload: bytes):
    """返回 (success: bool, error_msg: str)"""
    if len(payload) < 1:
        return False, "empty response"
    success = payload[0] != 0
    error_msg = ""
    if len(payload) > 1:
        error_msg = payload[1:].decode('utf-8', errors='replace')
    return success, error_msg


def decode_token_status(payload: bytes) -> TokenStatus:
    """解析 TOKEN_STATUS v1；未知字段会被忽略。"""
    if not payload:
        raise ValueError("empty TOKEN_STATUS payload")
    if len(payload) > 16 * 1024:
        raise ValueError("TOKEN_STATUS payload too large")

    try:
        raw = json.loads(payload.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid TOKEN_STATUS JSON: {exc}") from exc

    if not isinstance(raw, dict):
        raise ValueError("TOKEN_STATUS payload must be an object")

    required = (
        'schema_version', 'sequence', 'status', 'severity', 'reason',
        'server_time_ms', 'expires_ms', 'message',
    )
    missing = [name for name in required if name not in raw]
    if missing:
        raise ValueError(f"TOKEN_STATUS missing fields: {', '.join(missing)}")

    integer_fields = ('schema_version', 'sequence', 'server_time_ms', 'expires_ms')
    for name in integer_fields:
        if not isinstance(raw[name], int) or isinstance(raw[name], bool):
            raise ValueError(f"TOKEN_STATUS field {name} must be an integer")
    for name in ('status', 'severity', 'reason', 'message'):
        if not isinstance(raw[name], str):
            raise ValueError(f"TOKEN_STATUS field {name} must be a string")
    if raw['schema_version'] <= 0:
        raise ValueError("TOKEN_STATUS schema_version must be positive")
    if raw['sequence'] < 0:
        raise ValueError("TOKEN_STATUS sequence must not be negative")
    if raw['expires_ms'] < 0:
        raise ValueError("TOKEN_STATUS expires_ms must not be negative")

    return TokenStatus(
        schema_version=raw['schema_version'],
        sequence=raw['sequence'],
        status=raw['status'],
        severity=raw['severity'],
        reason=raw['reason'],
        server_time_ms=raw['server_time_ms'],
        expires_ms=raw['expires_ms'],
        message=raw['message'],
    )


# ============================================================================
# HEARTBEAT
# ============================================================================

def encode_heartbeat() -> bytes:
    ts_ms = int(time.time() * 1000)
    payload = struct.pack('!q', ts_ms)
    return build_message(MsgType.HEARTBEAT, 0, payload)


# ============================================================================
# SUBSCRIBE / UNSUBSCRIBE
# ============================================================================

def encode_subscribe(symbol_ids: list) -> bytes:
    count = len(symbol_ids)
    payload = struct.pack('!I', count)
    for sid in symbol_ids:
        payload += struct.pack('!I', sid)
    return build_message(MsgType.SUBSCRIBE_REQUEST, 0, payload)


def encode_unsubscribe(symbol_ids: list) -> bytes:
    """symbol_ids 为空列表表示取消全部订阅"""
    count = len(symbol_ids)
    payload = struct.pack('!I', count)
    for sid in symbol_ids:
        payload += struct.pack('!I', sid)
    return build_message(MsgType.UNSUBSCRIBE_REQUEST, 0, payload)


def decode_subscribe_response(payload: bytes):
    """返回已订阅的 symbol_id 列表"""
    if len(payload) < 4:
        return []
    count = struct.unpack('!I', payload[:4])[0]
    ids = []
    for i in range(count):
        offset = 4 + i * 4
        if offset + 4 > len(payload):
            break
        sid = struct.unpack('!I', payload[offset:offset + 4])[0]
        ids.append(sid)
    return ids


# ============================================================================
# SYMBOL_MAP
# ============================================================================

def decode_symbol_map(payload: bytes):
    """返回 dict[symbol_id: int, code: str]"""
    if len(payload) < 4:
        return {}
    count = struct.unpack('!I', payload[:4])[0]
    result = {}
    offset = 4
    for _ in range(count):
        if offset + 6 > len(payload):
            break
        symbol_id = struct.unpack('!I', payload[offset:offset + 4])[0]
        code_len = struct.unpack('!H', payload[offset + 4:offset + 6])[0]
        offset += 6
        if offset + code_len > len(payload):
            break
        code = payload[offset:offset + code_len].decode('utf-8', errors='replace')
        offset += code_len
        result[symbol_id] = code
    return result


# ============================================================================
# SNAPSHOT
# ============================================================================

def decode_snapshot(payload: bytes, header_symbol_id: int):
    """返回 list of (symbol_id, bid, ask, last, volume, timestamp)

    每条 Quote 32 字节:
      symbol_id(4, big-endian) + bid(4, float LE) + ask(4, float LE)
      + last(4, float LE) + volume(8, big-endian) + timestamp(8, big-endian)
    """
    quotes = []
    offset = 0
    while offset + QUOTE_SIZE <= len(payload):
        symbol_id = struct.unpack('!I', payload[offset:offset + 4])[0]
        bid, ask, last = struct.unpack('<fff', payload[offset + 4:offset + 16])
        volume = struct.unpack('!Q', payload[offset + 16:offset + 24])[0]
        timestamp = struct.unpack('!q', payload[offset + 24:offset + 32])[0]
        quotes.append((symbol_id, bid, ask, last, volume, timestamp))
        offset += QUOTE_SIZE
    return quotes


# ============================================================================
# HISTORY
# ============================================================================

def encode_history_request(request_id: int, symbol_id: int, period_str: str,
                           start_time: int, end_time: int,
                           max_count: int, symbol_code: str,
                           adjust: str = 'none') -> bytes:
    """构建 HISTORY_REQUEST 完整消息

    payload 格式 (29 + 2 + code_len [+ adjust]):
      request_id(4) + symbol_id(4) + period(1)
      + start_time(8) + end_time(8) + max_count(4)
      + code_len(2) + code(N)
      + adjust(1, 可选扩展: 0=none, 1=forward, 2=backward)
    """
    period = PERIOD_MAP.get(period_str, 1)
    adjust_code = ADJUST_MAP.get(adjust)
    if adjust_code is None:
        raise ValueError(f'Unsupported adjust value: {adjust}')
    logger.debug(f"encode_history_request: period_str={period_str} -> period={period}")
    code_bytes = symbol_code.encode('utf-8')

    payload = struct.pack('!II', request_id, symbol_id)
    payload += struct.pack('B', period)
    payload += struct.pack('!qq', start_time, end_time)
    payload += struct.pack('!I', max_count)
    payload += struct.pack('!H', len(code_bytes))
    payload += code_bytes
    payload += struct.pack('B', adjust_code)

    return build_message(MsgType.HISTORY_REQUEST, 0, payload)


def decode_history_response(payload: bytes):
    """返回 (header_info, klines)

    成功响应:
      header_info: dict with request_id, symbol_id, period, total_count,
                   batch_index, batch_count, kline_count, success=True
      klines: list of (timestamp, open, high, low, close, volume, turnover, open_interest)

    失败响应（兼容云网关显式失败语义）:
      payload: request_id(4) + success(1=0) + error_msg(N)
      header_info: dict with request_id, success=False, error=<msg>
      klines: []
    """
    if len(payload) < 4:
        return None, []

    def decode_failure():
        request_id = struct.unpack('!I', payload[:4])[0]
        success = len(payload) >= 5 and payload[4] != 0
        error_msg = ""
        if len(payload) > 5:
            error_msg = payload[5:].decode('utf-8', errors='replace')
        header_info = {
            'request_id': request_id,
            'success': success,
            'error': error_msg or 'History query failed',
            'symbol_id': 0,
            'period': 0,
            'total_count': 0,
            'batch_index': 0,
            'batch_count': 1,
            'kline_count': 0,
        }
        return header_info, []

    if len(payload) < HISTORY_RESP_HEADER_SIZE:
        return decode_failure()

    request_id, symbol_id = struct.unpack('!II', payload[0:8])
    period = payload[8]
    total_count = struct.unpack('!I', payload[9:13])[0]
    batch_index = struct.unpack('!H', payload[13:15])[0]
    batch_count = struct.unpack('!H', payload[15:17])[0]
    kline_count = struct.unpack('!H', payload[17:19])[0]

    expected_len = HISTORY_RESP_HEADER_SIZE + kline_count * KLINE_SIZE
    valid_success_frame = (
        period in PERIOD_MAP.values() and
        batch_count >= 1 and
        batch_index < batch_count and
        expected_len == len(payload)
    )
    if not valid_success_frame:
        return decode_failure()

    header_info = {
        'request_id': request_id,
        'symbol_id': symbol_id,
        'period': period,
        'total_count': total_count,
        'batch_index': batch_index,
        'batch_count': batch_count,
        'kline_count': kline_count,
        'success': True,
        'error': '',
    }

    klines = []
    offset = HISTORY_RESP_HEADER_SIZE
    for _ in range(kline_count):
        if offset + KLINE_SIZE > len(payload):
            break
        timestamp = struct.unpack('!q', payload[offset:offset + 8])[0]
        o, h, l, c = struct.unpack('<ffff', payload[offset + 8:offset + 24])
        volume = struct.unpack('!Q', payload[offset + 24:offset + 32])[0]
        turnover = struct.unpack('<d', payload[offset + 32:offset + 40])[0]
        open_interest = struct.unpack('!Q', payload[offset + 40:offset + 48])[0]
        klines.append((timestamp, o, h, l, c, volume, turnover, open_interest))
        offset += KLINE_SIZE

    return header_info, klines


# ============================================================================
# FINANCE
# ============================================================================

def encode_finance_request(msg_type: int, request_id: int,
                           stock_code: str, report_period: str,
                           query_type: int = 0) -> bytes:
    """构建财务查询请求

    payload: request_id(4) + code_len(2) + code(N)
             + period_len(2) + period(N) + query_type(1)
    """
    code_bytes = stock_code.encode('utf-8')
    period_bytes = report_period.encode('utf-8')

    payload = struct.pack('!I', request_id)
    payload += struct.pack('!H', len(code_bytes)) + code_bytes
    payload += struct.pack('!H', len(period_bytes)) + period_bytes
    payload += struct.pack('B', query_type)

    return build_message(msg_type, 0, payload)


def decode_finance_response(payload: bytes):
    """返回 (request_id, success, error_msg, json_data_str)

    payload: request_id(4) + success(1) + err_len(2) + err(N)
             + json_len(4) + json(N)
    """
    if len(payload) < 7:
        return 0, False, "insufficient data", ""

    offset = 0
    request_id = struct.unpack('!I', payload[offset:offset + 4])[0]
    offset += 4

    success = payload[offset] != 0
    offset += 1

    err_len = struct.unpack('!H', payload[offset:offset + 2])[0]
    offset += 2
    error_msg = ""
    if err_len > 0 and offset + err_len <= len(payload):
        error_msg = payload[offset:offset + err_len].decode('utf-8', errors='replace')
    offset += err_len

    json_data = ""
    if offset + 4 <= len(payload):
        json_len = struct.unpack('!I', payload[offset:offset + 4])[0]
        offset += 4
        if json_len > 0 and offset + json_len <= len(payload):
            json_data = payload[offset:offset + json_len].decode('utf-8', errors='replace')

    return request_id, success, error_msg, json_data
