"""rtdata.api — 简化 API 入口

用法:
    import rtdata

    api = rtdata.API(token="your_token")

    # 查询历史K线（按时间范围）
    klines = api.get_kline("600519.SH", period="1d",
                             start="2025-12-01", end="2025-12-31")

    # 查询财务数据
    fd = api.get_finance("600519.SH", report_period="2017-12-31")

    # 订阅实时行情
    @api.on_quote
    def handle(quote):
        print(quote)
    api.subscribe(["600519.SH"])
"""
import logging
from datetime import date, datetime
from typing import Optional, List, Union

from .client import RtdataClient
from .models import Quote, Kline, FinanceData, TokenStatus

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://api.fengv2ray.tk"


class API:
    """简化的行情数据 API

    自动处理连接、认证、品种映射，用户只需关注数据本身。
    """

    def __init__(self, token: str, api_url: str = DEFAULT_API_URL,
                 *, async_callbacks: bool = True, callback_queue_size: int = 1000,
                 symbol_cache_dir: Optional[str] = None,
                 history_cache_dir: Optional[str] = None,
                 history_cache_enabled: bool = True):
        self._client = RtdataClient(
            token=token,
            api_url=api_url,
            async_callbacks=async_callbacks,
            callback_queue_size=callback_queue_size,
            symbol_cache_dir=symbol_cache_dir,
            history_cache_dir=history_cache_dir,
            history_cache_enabled=history_cache_enabled,
        )
        self._connected = False

    def _ensure_connected(self):
        if not self._connected:
            self._client.connect()
            self._connected = True

    def connect(self):
        """显式建立连接。"""
        self._ensure_connected()

    # ── 查询 ──────────────────────────────────────────────

    def get_kline(self, symbol: str, period: str = '1d',
                  start: Union[int, float, str, datetime, date] = 0,
                  end: Union[int, float, str, datetime, date] = 0,
                  timeout: float = 30.0,
                  adjust: str = 'none',
                  **legacy_kwargs) -> List[Kline]:
        """查询历史 K 线（按时间范围）

        Args:
            symbol: 品种代码，如 "600519.SH"
            period: K线周期: 1m, 5m, 15m, 30m, 1h, 2h, 4h, 1d, 1w, 1M
            start:  起始时间。支持毫秒时间戳、datetime、date、"YYYY-MM-DD"、"YYYY-MM-DD HH:MM[:SS]"
            end:    结束时间。规则同 start；若仅传日期，自动扩展到当天 23:59:59.999
            timeout: 超时秒数
            adjust: 复权方式: none / forward / backward

        兼容说明:
            - 旧参数 start_time / end_time 仍可用，但不再推荐
        """
        self._ensure_connected()
        return self._client.get_kline(
            symbol, period=period, start=start, end=end, timeout=timeout,
            adjust=adjust, **legacy_kwargs)

    def get_kline_range(self, symbol: str, period: str = '1d',
                        start: Union[int, float, str, datetime, date] = 0,
                        end: Union[int, float, str, datetime, date] = 0,
                        adjust: str = 'none',
                        timeout: float = 30.0) -> List[Kline]:
        self._ensure_connected()
        return self._client.get_kline_range(
            symbol, period=period, start=start, end=end, adjust=adjust, timeout=timeout)

    def get_kline_for_day(self, symbol: str, day: Union[str, date, datetime],
                          period: str = '1d', timeout: float = 30.0,
                          adjust: str = 'none') -> List[Kline]:
        self._ensure_connected()
        return self._client.get_kline_for_day(symbol, day, period=period, timeout=timeout, adjust=adjust)

    def get_kline_for_today(self, symbol: str, period: str = '1d', timeout: float = 30.0,
                            adjust: str = 'none') -> List[Kline]:
        self._ensure_connected()
        return self._client.get_kline_for_today(symbol, period=period, timeout=timeout, adjust=adjust)

    def get_finance(self, stock_code: str, report_period: str = '',
                    query_type: int = 4, timeout: float = 30.0) -> FinanceData:
        """查询财务报表（利润表/资产负债表/现金流量表）"""
        self._ensure_connected()
        return self._client.get_finance(stock_code, report_period, query_type, timeout)

    def get_finance_ttm(self, stock_code: str, as_of_date: str = '',
                        timeout: float = 30.0) -> FinanceData:
        """查询 TTM（滚动12个月）数据"""
        self._ensure_connected()
        return self._client.get_finance_ttm(stock_code, as_of_date, timeout)

    def get_finance_pit(self, stock_code: str, trade_date: str = '',
                        query_type: int = 0, timeout: float = 30.0) -> FinanceData:
        """查询 Point-in-Time（时点）数据"""
        self._ensure_connected()
        return self._client.get_finance_pit(stock_code, trade_date, query_type, timeout)

    def get_finance_ratios(self, stock_code: str, report_period: str = '',
                           timeout: float = 30.0) -> FinanceData:
        """查询财务比率"""
        self._ensure_connected()
        return self._client.get_finance_ratios(stock_code, report_period, timeout)

    def get_quote(self, symbol: str) -> Optional[Quote]:
        """获取最新缓存行情（需先 subscribe）"""
        self._ensure_connected()
        return self._client.get_quote(symbol)

    # ── 实时订阅 ──────────────────────────────────────────

    def subscribe(self, symbols: List[str]):
        """订阅实时行情"""
        self._ensure_connected()
        self._client.subscribe(symbols)

    def unsubscribe(self, symbols: Optional[List[str]] = None):
        """取消订阅"""
        self._ensure_connected()
        self._client.unsubscribe(symbols)

    @property
    def on_quote(self):
        """装饰器: 注册行情回调"""
        return self._client.on_quote

    @property
    def on_connect(self):
        return self._client.on_connect

    @property
    def on_disconnect(self):
        return self._client.on_disconnect

    @property
    def on_error(self):
        return self._client.on_error

    @property
    def on_token_status(self):
        """装饰器: 注册 Token 状态回调。"""
        return self._client.on_token_status

    # ── 工具 ──────────────────────────────────────────────

    @property
    def symbols(self) -> dict:
        """所有品种 {id: code}"""
        self._ensure_connected()
        return self._client.symbols

    @property
    def last_subscribe_warning(self) -> Optional[str]:
        return self._client.last_subscribe_warning

    @property
    def last_subscribe_requested(self) -> List[str]:
        return self._client.last_subscribe_requested

    @property
    def last_subscribe_confirmed(self) -> List[str]:
        return self._client.last_subscribe_confirmed

    @property
    def last_subscribe_rejected(self) -> List[str]:
        return self._client.last_subscribe_rejected

    @property
    def current_host(self) -> str:
        return self._client.current_host

    @property
    def current_port(self) -> int:
        return self._client.current_port

    @property
    def current_node_id(self) -> str:
        return self._client.current_node_id

    @property
    def gateway_version(self) -> str:
        return self._client.gateway_version

    @property
    def protocol_features(self) -> List[str]:
        return self._client.protocol_features

    @property
    def token_status(self) -> Optional[TokenStatus]:
        return self._client.token_status

    @property
    def token_expires_ms(self) -> Optional[int]:
        return self._client.token_expires_ms

    @property
    def token_expires_at(self):
        return self._client.token_expires_at

    @property
    def current_endpoint(self) -> str:
        return self._client.current_endpoint

    def close(self):
        if self._connected:
            self._client.close()
            self._connected = False

    def __enter__(self):
        self._ensure_connected()
        return self

    def __exit__(self, *args):
        self.close()
