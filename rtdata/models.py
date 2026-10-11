"""rtdata SDK 数据模型"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List
from types import MappingProxyType


@dataclass(frozen=True)
class TokenStatus:
    """云网关推送的 Token 状态。"""
    schema_version: int
    sequence: int
    status: str
    severity: str
    reason: str
    server_time_ms: int
    expires_ms: int
    message: str = ""

    @property
    def never_expires(self) -> bool:
        return self.expires_ms <= 0

    @property
    def remaining_ms(self) -> Optional[int]:
        if self.never_expires:
            return None
        return max(0, self.expires_ms - self.server_time_ms)

    @property
    def expires_at(self) -> Optional[datetime]:
        if self.never_expires:
            return None
        return datetime.fromtimestamp(self.expires_ms / 1000.0, tz=timezone.utc)


@dataclass
class Quote:
    """实时行情快照"""
    symbol: str         # "SSE.601919"
    symbol_id: int
    bid: Optional[float]
    ask: Optional[float]
    last: Optional[float]
    volume: Optional[int]
    timestamp: int      # 毫秒时间戳

    # 基础行情字段
    high: Optional[float] = 0.0
    low: Optional[float] = 0.0
    open: Optional[float] = 0.0
    prev_close: Optional[float] = 0.0
    turnover: Optional[float] = 0.0

    # 五档行情（可选，如果服务端提供）
    bid_prices: List[Optional[float]] = field(default_factory=list)   # 买盘价格（5档）
    bid_volumes: List[Optional[int]] = field(default_factory=list)    # 买盘量（5档）
    ask_prices: List[Optional[float]] = field(default_factory=list)   # 卖盘价格（5档）
    ask_volumes: List[Optional[int]] = field(default_factory=list)    # 卖盘量（5档）

    def __str__(self):
        price = lambda value: "None" if value is None else f"{value:.4f}"
        return (f"Quote({self.symbol}: last={price(self.last)}, "
                f"bid={price(self.bid)}, ask={price(self.ask)}, vol={self.volume})")

    @property
    def fields(self):
        return MappingProxyType(dict(bid_price1=self.bid, ask_price1=self.ask,
                                     last=self.last, volume=self.volume, trade_time=self.timestamp))

    @property
    def available_fields(self):
        return frozenset(self.fields)

    wire_version = 1
    schema_version = None
    schema_id = None
    source_id = None
    source_table = None
    source_epoch = None
    sequence = None

    @classmethod
    def from_snapshot(cls, snapshot, symbol_id: int, symbol: str):
        return _FullQuote(snapshot, symbol_id, symbol)


class _FullQuote(Quote):
    """Immutable V2 Quote: raw fields and compatibility aliases share one value store."""

    _aliases = {'bid': 'bid_price1', 'ask': 'ask_price1', 'timestamp': 'trade_time',
                'prev_close': 'pre_close', 'turnover': 'amount'}
    _books = {'bid_prices': 'bid_price', 'bid_volumes': 'bid_volume',
              'ask_prices': 'ask_price', 'ask_volumes': 'ask_volume'}

    def __init__(self, snapshot, symbol_id, symbol):
        object.__setattr__(self, '_fields', snapshot.fields)
        object.__setattr__(self, 'symbol', symbol)
        object.__setattr__(self, 'symbol_id', symbol_id)
        for name, value in dict(wire_version=2, schema_version=1, schema_id=snapshot.schema_id,
                                source_id=snapshot.source_id, source_table=snapshot.source_table,
                                source_epoch=snapshot.source_epoch, sequence=snapshot.sequence).items():
            object.__setattr__(self, name, value)

    def __getattribute__(self, name):
        if not name.startswith('_'):
            fields = object.__getattribute__(self, '_fields')
            raw = _FullQuote._aliases.get(name, name)
            if raw in fields:
                return fields[raw]
            if name in _FullQuote._books:
                prefix = _FullQuote._books[name]
                return [fields[prefix + str(i)] for i in range(1, 6) if prefix + str(i) in fields]
        return object.__getattribute__(self, name)

    @property
    def fields(self):
        return self._fields

    def __setattr__(self, name, value):
        raise AttributeError('V2 Quote is immutable')

    def __delattr__(self, name):
        raise AttributeError('V2 Quote is immutable')

    def __eq__(self, other):
        if not isinstance(other, _FullQuote):
            return NotImplemented
        return (self.symbol_id, self.source_id, self.source_epoch, self.sequence, self.fields) == (
            other.symbol_id, other.source_id, other.source_epoch, other.sequence, other.fields)


@dataclass(frozen=True)
class RealtimeTableStatus:
    source_table: str
    schema_id: int
    ready: bool
    reason: str
    epoch: int


@dataclass(frozen=True)
class RealtimeStatus:
    mode: str
    phase: str
    negotiated: bool
    reason: str
    generation: int
    revision: int
    tables: object


@dataclass
class Kline:
    """K线数据"""
    timestamp: int      # 毫秒时间戳
    open: float
    high: float
    low: float
    close: float
    volume: int
    turnover: float     # 成交额
    open_interest: int  # 持仓量
    symbol: str = ""    # 品种代码，如 "rb2610.SHF"

    def __str__(self):
        prefix = f"{self.symbol} " if self.symbol else ""
        return (f"Kline({prefix}ts={self.timestamp}, O={self.open:.4f}, H={self.high:.4f}, "
                f"L={self.low:.4f}, C={self.close:.4f}, V={self.volume})")


@dataclass
class FinanceData:
    """财务数据查询结果"""
    stock_code: str
    report_period: str
    data: dict

    def __str__(self):
        return f"FinanceData({self.stock_code}, {self.report_period})"
