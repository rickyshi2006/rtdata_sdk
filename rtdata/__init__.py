"""rtdata - 实时行情数据 SDK

用法:
    import rtdata

    api = rtdata.API(token="your_token")
    klines = api.get_kline("600519.SH", period="1d",
                             start="2025-12-01", end="2025-12-31")
"""

from .client import RtdataClient
from .api import API
from .models import Quote, Kline, FinanceData
from .exceptions import (
    RtdataError,
    AuthenticationError,
    ConnectionError,
    SymbolNotFoundError,
    QueryTimeoutError,
    QueryError,
    DisconnectedError,
    ProtocolError,
    DiscoveryError,
)

__version__ = '0.1.3'

__all__ = [
    'API',
    'RtdataClient',
    'Quote',
    'Kline',
    'FinanceData',
    'RtdataError',
    'AuthenticationError',
    'ConnectionError',
    'SymbolNotFoundError',
    'QueryTimeoutError',
    'QueryError',
    'DisconnectedError',
    'ProtocolError',
    'DiscoveryError',
]
