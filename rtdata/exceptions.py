"""rtdata SDK 异常定义"""


class RtdataError(Exception):
    """SDK 基础异常"""


class AuthenticationError(RtdataError):
    """认证失败"""


class ConnectionError(RtdataError):
    """连接失败"""


class SymbolNotFoundError(RtdataError):
    """品种代码未找到"""


class QueryTimeoutError(RtdataError):
    """查询超时"""


class QueryError(RtdataError):
    """查询返回错误"""


class DisconnectedError(RtdataError):
    """连接中断导致的请求失败"""


class ProtocolError(RtdataError):
    """协议解析错误"""


class DiscoveryError(RtdataError):
    """服务发现失败"""
