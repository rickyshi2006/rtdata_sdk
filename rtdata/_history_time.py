"""History API times are market-local inputs and UTC instants on the wire."""

import math
import re
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MIN_MS = -(1 << 63)
_MAX_MS = (1 << 63) - 1
_CHINA_SUFFIXES = {'SH', 'SZ', 'BJ', 'SHF', 'CFE', 'CZC', 'DCE', 'INE', 'GFE'}
_CHINA_EXCHANGES = {'SSE', 'SZSE', 'BSE', 'SHFE', 'CFFEX', 'CZCE', 'DCE', 'INE', 'GFEX'}


def market_zone(symbol: str) -> ZoneInfo:
    _, _, suffix = symbol.rpartition('.')
    if suffix in _CHINA_SUFFIXES or symbol.split('.')[0] in _CHINA_EXCHANGES:
        return ZoneInfo('Asia/Shanghai')
    if suffix == 'HK' or symbol.startswith('HKEX.'):
        return ZoneInfo('Asia/Hong_Kong')
    if re.fullmatch(r'[A-Za-z][A-Za-z0-9.\-]{0,31}', symbol):
        return ZoneInfo('America/New_York')
    raise ValueError(f'Unknown market for {symbol!r}; use UTC milliseconds or aware datetime')


def _milliseconds(value: datetime) -> int:
    delta = value.astimezone(timezone.utc) - _EPOCH
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def _localize(value: datetime, zone: ZoneInfo) -> datetime:
    candidates = {}
    for fold in (0, 1):
        aware = value.replace(tzinfo=zone, fold=fold)
        if aware.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) == value:
            candidates[_milliseconds(aware)] = aware
    if len(candidates) != 1:
        raise ValueError('Ambiguous or nonexistent market wall time; supply an explicit UTC offset')
    return next(iter(candidates.values()))


def normalize_endpoint(value, *, symbol: str, is_end: bool) -> int:
    if isinstance(value, bool):
        raise TypeError('History time must not be bool')
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError('History time must be finite')
        result = int(value)
    else:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return 0
            if len(text) == 10:
                value = date.fromisoformat(text)
            else:
                value = datetime.fromisoformat(text[:-1] + '+00:00' if text.endswith('Z') else text)
        if isinstance(value, date) and not isinstance(value, datetime):
            value = datetime.combine(value, time(23, 59, 59, 999000) if is_end else time.min)
        if not isinstance(value, datetime):
            raise TypeError(f'Unsupported history time value: {value!r}')
        if value.utcoffset() is None:
            value = _localize(value, market_zone(symbol))
        result = _milliseconds(value)
    if not _MIN_MS <= result <= _MAX_MS:
        raise ValueError('History time exceeds signed int64 milliseconds')
    return result


def exclusive_end(inclusive_ms: int) -> int:
    if inclusive_ms == _MAX_MS:
        raise ValueError('Inclusive end cannot be represented as an int64 exclusive end')
    return inclusive_ms + 1
