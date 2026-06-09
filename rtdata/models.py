"""rtdata SDK 数据模型"""
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class Quote:
    """实时行情快照"""
    symbol: str         # "SSE.601919"
    symbol_id: int
    bid: float
    ask: float
    last: float
    volume: int
    timestamp: int      # 毫秒时间戳

    # 基础行情字段
    high: float = 0.0
    low: float = 0.0
    open: float = 0.0
    prev_close: float = 0.0
    turnover: float = 0.0

    # 五档行情（可选，如果服务端提供）
    bid_prices: List[float] = field(default_factory=list)   # 买盘价格（5档）
    bid_volumes: List[int] = field(default_factory=list)    # 买盘量（5档）
    ask_prices: List[float] = field(default_factory=list)   # 卖盘价格（5档）
    ask_volumes: List[int] = field(default_factory=list)    # 卖盘量（5档）

    def __str__(self):
        return (f"Quote({self.symbol}: last={self.last:.4f}, "
                f"bid={self.bid:.4f}, ask={self.ask:.4f}, vol={self.volume})")


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
