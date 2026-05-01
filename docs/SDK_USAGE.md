# rtdata SDK 使用文档

## 1. 概述

`rtdata` 是一个面向 Cloud Gateway 的 Python SDK，支持：

- HTTPS 服务发现
- TCP 实时行情订阅
- 历史 K 线查询
- 财务数据查询
- 本地 symbol map 缓存
- 本地历史分段二进制缓存
- 自动重连与自动恢复订阅

当前版本：`0.1.3`

## 2. 安装

### 2.1 本地开发安装

```bash
cd rtdata_sdk
pip install -e .
```

### 2.2 安装 wheel

```bash
pip install rtdata-0.1.3-py3-none-any.whl
```

## 3. 两套入口

### 3.1 推荐入口：`API`

`API` 会自动管理连接，适合绝大多数业务代码。

```python
import rtdata

api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
)
```

说明：

- `API` 支持懒连接，首次 `subscribe()` / `get_history()` / `get_finance()` 时会自动连接
- 也可以先显式调用 `api.connect()`
- 使用完成后应调用 `api.close()`，或直接使用 context manager

### 3.2 底层入口：`RtdataClient`

适合需要更细控制的场景。

```python
from rtdata import RtdataClient

client = RtdataClient(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
)
client.connect()
```

## 4. 连接方式

### 4.1 HTTPS 服务发现模式

推荐。

```python
client = RtdataClient(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
)
client.connect()
```

连接流程：

1. 加载本地 symbol map 缓存
2. `POST /api/v1/connect` 获取 TCP 节点地址
3. 按版本决定是否拉取新版 symbol map
4. 建立 TCP 连接并认证

连接后可查看当前接入节点：

```python
print(client.current_node_id)
print(client.current_host)
print(client.current_port)
print(client.current_endpoint)
```

### 4.2 直连 TCP 模式

仅建议内网调试或完全受控环境使用。

```python
client = RtdataClient(
    token="your_token",
    host="127.0.0.1",
    port=9100,
)
client.connect()
```

> 如果同时传了 `api_url`，服务发现返回的 host/port 会覆盖构造参数中的 `host` 和 `port`。

## 5. 构造参数

### 5.1 `API(...)`

```python
API(
    token: str,
    api_url: str = "https://api.fengv2ray.tk",
    *,
    async_callbacks: bool = True,
    callback_queue_size: int = 1000,
    symbol_cache_dir: str | None = None,
    history_cache_dir: str | None = None,
    history_cache_enabled: bool = True,
)
```

### 5.2 `RtdataClient(...)`

```python
RtdataClient(
    token: str,
    host: str = "127.0.0.1",
    port: int = 9100,
    *,
    api_url: str | None = None,
    heartbeat_interval: float = 20.0,
    auto_reconnect: bool = True,
    symbol_cache_dir: str | None = None,
    history_cache_dir: str | None = None,
    history_cache_enabled: bool = True,
    async_callbacks: bool = True,
    callback_queue_size: int = 1000,
)
```

## 6. 实时行情订阅

### 6.1 订阅

```python
api.subscribe(["601919.SH", "rb2610.SHF", "EUR.USD"])
```

### 6.2 取消订阅

```python
api.unsubscribe(["601919.SH"])
api.unsubscribe()   # 取消全部
```

### 6.3 回调

```python
@api.on_quote
def on_quote(q):
    print(q.symbol, q.last, q.bid, q.ask, q.volume, q.timestamp)
```

### 6.4 实时语义

实时通道是“最新值优先”的快照流，不保证逐条完整回放。

这意味着：

- 你看到的是当前最新状态
- 中间某些高频变化可能被覆盖
- 如果客户端消费太慢，旧快照可能被丢弃
- 极端情况下，服务端可能主动断开，然后 SDK 自动重连

如果你只关心“当前最新值”，推荐配合使用：

```python
quote = api.get_quote("rb2610.SHF")
```

`get_quote()` 只返回 SDK 当前内存中的最新缓存，不会主动向服务端发起一次查询。通常需要先订阅过该 symbol。

### 6.5 订阅结果检查

服务端可能只接受部分 symbol。

```python
api.subscribe(["601919.SH", "rb2610.SHF"])

print(api.last_subscribe_warning)
print(api.last_subscribe_requested)
print(api.last_subscribe_confirmed)
print(api.last_subscribe_rejected)
```

常见拒绝原因：

- token 没有对应市场权限
- 超过最大订阅数
- 服务端过滤该 symbol

## 7. 历史 K 线查询

### 7.1 推荐写法：按时间范围

```python
klines = api.get_history(
    symbol="000001.SZ",
    period="1d",
    start="2015-01-01",
    end="2015-12-31",
    timeout=30.0,
)
```

### 7.2 支持周期

- `1m`
- `5m`
- `15m`
- `30m`
- `1h`
- `1d`
- `1w`
- `1M`

### 7.3 时间参数规则

`start` 和 `end` 支持：

- 毫秒时间戳
- `datetime`
- `date`
- `"YYYY-MM-DD"`
- `"YYYY-MM-DD HH:MM"`
- `"YYYY-MM-DD HH:MM:SS"`
- `"YYYY-MM-DD HH:MM:SS.ffffff"`

规则：

- 如果只传日期，自动扩展为当天整天
- 如果带时间，按精确时间截取
- `start` 必须小于等于 `end`

### 7.4 分钟线示例

```python
klines = api.get_history(
    "rb2610.SHF",
    period="1m",
    start="2026-04-29",
    end="2026-04-30",
)
```

### 7.5 兼容接口：最近 N 根

```python
klines = api.get_history_by_count("600519.SH", period="1d", count=10)
```

> 该接口仅为兼容旧逻辑保留，新代码建议统一使用 `start/end`。

### 7.6 兼容参数

`get_history()` 仍兼容旧参数名：

- `start_time`
- `end_time`

如果向 `get_history()` 继续传 `count=...`，当前实现会记录 warning，但新代码不建议再这样使用。

## 8. 历史缓存

### 8.1 默认行为

当同时传入 `start` 和 `end` 时，历史查询默认启用本地缓存：

- 默认根目录：`~/.rtdata/`
- 历史缓存目录：`~/.rtdata/history_v1/`
- 缓存格式：分段二进制文件

### 8.2 命中规则

- 已有完整区间：直接本地返回
- 缺少部分区间：只回源缺失段
- 再次请求相同区间：优先读本地
- 只有同时给出 `start` 和 `end` 时才会走这套缓存
- `get_history_by_count()` 不使用本地历史缓存

### 8.3 关闭缓存

```python
api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    history_cache_enabled=False,
)
```

### 8.4 指定缓存目录

```python
api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    history_cache_dir="/data/rtdata_cache",
)
```

## 9. 财务查询

### 9.1 财务报表

```python
fd = api.get_finance(
    stock_code="600519.SH",
    report_period="2017-12-31",
    query_type=4,
)
```

### 9.2 TTM

```python
fd = api.get_finance_ttm(
    stock_code="000001.SZ",
    as_of_date="2017-12-31",
)
```

### 9.3 Point-in-Time

```python
fd = api.get_finance_pit(
    stock_code="601318.SH",
    trade_date="2017-06-30",
    query_type=0,
)
```

### 9.4 财务比率

```python
fd = api.get_finance_ratios(
    stock_code="600036.SH",
    report_period="2017-12-31",
)
```

## 10. 连接事件

```python
@api.on_connect
def on_connect():
    print("connected")

@api.on_disconnect
def on_disconnect(reason):
    print("disconnected:", reason)

@api.on_error
def on_error(err):
    print("error:", err)
```

说明：

- `on_connect`：首次连接成功或重连恢复成功
- `on_disconnect`：连接被断开
- `on_error`：订阅部分失败等非致命错误提示

## 11. 自动重连行为

当连接断开时，SDK 会：

1. 如果配置了 `api_url`，先重新做 discovery
2. 重新 TCP 连接
3. 重新认证
4. 按需刷新 symbol map
5. 自动恢复已有订阅

因此断线后你通常不需要手动再次 `subscribe()`。

## 12. 异常语义

### 12.1 `AuthenticationError`

- token 无效
- token 已被占用
- 认证超时

### 12.2 `DiscoveryError`

- discovery 域名不可达
- HTTP 失败
- 服务发现返回异常

### 12.3 `QueryError`

服务端明确拒绝当前查询，例如：

- `Market not allowed`
- `History download busy, retry later`
- `Upstream query channel unavailable`

### 12.4 `QueryTimeoutError`

只表示“等待响应超时”，不代表服务端一定拒绝。

### 12.5 `DisconnectedError`

查询过程中连接被断开。

## 13. 常见排查建议

### 13.1 订阅没有收到数据

先看：

```python
print(api.last_subscribe_warning)
print(api.last_subscribe_rejected)
```

再确认：

- token 是否有对应市场权限
- symbol 是否存在于 symbol map
- 是否超过订阅数限制

### 13.2 历史查询失败

优先区分异常类型：

- `QueryError`：服务端显式拒绝，直接看错误文本
- `QueryTimeoutError`：等待超时
- `DisconnectedError`：连接中断

### 13.3 怀疑缓存影响测试

关闭本地历史缓存重试：

```python
api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    history_cache_enabled=False,
)
```

## 14. Context Manager

```python
import rtdata

with rtdata.API(token="your_token", api_url="https://api.fengv2ray.tk") as api:
    rows = api.get_history("000001.SZ", period="1d", start="2015-01-01", end="2015-12-31")
```

底层客户端也支持：

```python
from rtdata import RtdataClient

with RtdataClient(token="your_token", api_url="https://api.fengv2ray.tk") as client:
    print(client.current_endpoint)
```

## 15. 数据结构

### 15.1 `Quote`

- `symbol`
- `symbol_id`
- `bid`
- `ask`
- `last`
- `volume`
- `timestamp`

### 15.2 `Kline`

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `turnover`
- `open_interest`

### 15.3 `FinanceData`

- `stock_code`
- `report_period`
- `data`

## 16. 示例文件

- `examples/basic_usage.py`
- `examples/history_kline.py`
- `examples/finance_query.py`
- `examples/test_subscribe2.py`
- `examples/diagnose_realtime_stall.py`
