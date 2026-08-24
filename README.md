# rtdata SDK

`rtdata` 是用于实时行情、历史 K 线和财务数据查询的 Python SDK。基础功能仅依赖
Python 标准库；History V2 高速历史流可选用 Zstandard。

当前文档对应版本：**0.3.3**。

## 功能概览

- HTTPS 服务发现，自动获取 TCP 接入节点
- 自动心跳、自动重连和订阅恢复
- 使用服务发现时默认声明安全 session rehome 能力：故障转移后可回到账号首选节点
- 实时行情采用“最新值优先”的快照流
- History V1 兼容路径和可选的 History V2 列式压缩流
- 本地历史分段二进制缓存，可按 `symbol + period + adjust` 缓存缺口
- A 股、港股、美股的历史 K 线复权查询（`none` / `forward` / `backward`）
- A 股、港股、美股财务报表、TTM 和财务比率查询
- Token 状态通知和订阅部分成功状态
- 查询失败区分服务端拒绝、超时和连接中断

## 支持范围

| 功能 | 市场/品种 |
| --- | --- |
| 实时行情 | A 股、港股、美股、期货、外汇（按账号权限） |
| 历史 K 线 | A 股、港股、美股、期货、期权（按账号权限） |
| 历史复权 | A 股、港股、美股（按账号权限）；期货/期权不适用 |
| 财务报表、TTM、财务比率 | A 股、港股、美股（按账号权限）；字段按市场原始 schema 返回 |
| PIT（Point-in-Time） | 当前支持 A 股；港股、美股暂不支持 |

## 安装

### 安装 wheel

```bash
pip install rtdata-0.3.3-py3-none-any.whl
```

需要 History V2 时安装可选依赖：

```bash
pip install "rtdata-0.3.3-py3-none-any.whl[history-v2]"
```

也可以从源码开发安装：

```bash
pip install -e .
```

未安装 `history-v2` 或连接端不支持 History V2 时，SDK 会自动使用兼容的 History V1；
实时订阅、财务查询和普通历史查询仍可用。

## 快速开始

推荐使用 `API` 和 HTTPS 服务发现：

```python
import rtdata

with rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
) as api:
    @api.on_quote
    def on_quote(q):
        print(q.symbol, q.last)

    api.subscribe(["601919.SH", "rb2610.SHF"])
```

`with` 会立即建立连接；实时示例需要在代码中继续等待或执行其他业务逻辑，退出代码块
时连接随即关闭。完整持续监听写法见 `examples/basic_usage.py`。

底层 `RtdataClient` 适合需要控制连接参数的场景：

```python
from rtdata import RtdataClient

with RtdataClient(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
) as client:
    print(client.current_endpoint)
```

如果拿到的是卡号或 UUID，可先在 token 兑换页兑换 token：
`https://rtdata.fengv2ray.tk`。

## 连接、自动重连与自动归位

使用 `api_url` 时，SDK 会先调用 discovery，再连接返回的节点。`API` 默认开启自动
重连和安全 session rehome：

```python
api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    session_rehome_advertise=True,  # v0.3.1 起为默认值
)
```

节点故障时，会话可迁移到健康节点；SDK 会重新 discovery、认证并恢复已有订阅。
首选节点恢复稳定后，会话可自动迁回首选节点。只有同时使用服务发现并保持
`auto_reconnect=True`（底层 `RtdataClient`）时才会声明该能力。固定 `host:port`、关闭
自动重连或显式设置 `session_rehome_advertise=False` 的客户端不会被主动迁移。

连接后可检查：

```python
print(api.current_node_id)
print(api.session_capability_state)
print(api.session_rehome_negotiated)
print(api.session_capability_fallback_reason)
```

## 实时订阅

实时通道是最新值优先的快照流，不保证逐条完整回放。消费过慢时旧快照可能被覆盖或
丢弃；极端情况下服务端会断开连接，SDK 随后自动重连并恢复订阅。

```python
@api.on_quote
def on_quote(q):
    print(q.symbol, q.last, q.volume, q.timestamp)

api.subscribe(["601919.SH", "00700.HK", "AAPL.US"])

print(api.last_subscribe_warning)
print(api.last_subscribe_requested)
print(api.last_subscribe_confirmed)
print(api.last_subscribe_rejected)
```

`get_quote(symbol)` 只读取 SDK 内存中的最新缓存，通常需要先订阅该品种，不会额外发起
一次服务端查询。

## 历史 K 线

推荐使用 `start/end` 时间范围：

```python
rows = api.get_kline(
    "000001.SZ",
    period="1d",
    start="2015-01-01",
    end="2015-12-31",
    adjust="none",
)
```

支持周期：`1m`、`5m`、`15m`、`30m`、`1h`、`2h`、`4h`、`1d`、`1w`、`1M`。

时间参数支持毫秒时间戳、`datetime`、`date` 和常用日期时间字符串。仅传日期时会扩展
为当天 `00:00:00 ~ 23:59:59.999`；`start` 必须不晚于 `end`。旧参数名
`start_time` / `end_time` 仍兼容。

### 复权

```python
none_rows = api.get_kline(
    "000001.SZ", period="1d", start="2025-06-02", end="2025-06-17", adjust="none"
)
forward_rows = api.get_kline(
    "00700.HK", period="1d", start="2025-06-02", end="2025-06-12", adjust="forward"
)
backward_rows = api.get_kline(
    "AAPL.US", period="1d", start="2020-08-26", end="2020-09-02", adjust="backward"
)
```

不同市场的数据字段和交易日历无需在客户端自行拼接。期货、期权等非股票品种不支持
复权。

### History V2

History V2 需要客户端安装 `zstandard`，并在连接时显式开启能力声明和默认选择：

```python
with rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    history_v2_advertise=True,
    history_v2_default=True,
    history_cache_enabled=False,
) as api:
    print(api.history_capability_state)
    print(api.history_v2_eligible)
    rows = api.get_kline(
        "000001.SZ", period="1d", start="2025-06-02", end="2025-06-17"
    )
```

`history_v2_advertise` 负责能力协商，`history_v2_default` 决定协商成功后是否优先发
送 V2 请求。协商超时、连接端不支持或缺少 Zstandard 时，SDK 自动回退 V1；可通过
`history_capability_state` 和 `history_capability_fallback_reason` 查看原因。

### 本地历史缓存

同时传入 `start` 和 `end` 时默认启用本地分段二进制缓存：

- 默认目录：`~/.rtdata/history_v1/`
- 缓存键：`symbol + period + adjust`
- 完整区间优先本地返回，缺口才回源
- 不依赖 sqlite

关闭或指定目录：

```python
api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    history_cache_enabled=False,
)
```

自定义缓存目录时改用 `history_cache_dir="/data/rtdata_cache"`；通常无需在关闭缓存时
同时设置目录。

## 财务数据

`query_type` 取值为：

| 值 | 内容 |
| ---: | --- |
| 1 | income（利润表） |
| 2 | balance（资产负债表） |
| 3 | cashflow（现金流量表） |
| 4 | all（三表，默认） |

普通财务报表和 PIT 的默认值都是 `query_type=4`：

```python
with rtdata.API(token="your_token", api_url="https://api.fengv2ray.tk") as api:
    a_data = api.get_finance("600519.SH", report_period="2025-12-31")
    hk_data = api.get_finance("00700.HK", report_period="2025-12-31")
    us_data = api.get_finance("AAPL.US", report_period="2025-12-31")

    ttm = api.get_finance_ttm("600519.SH", as_of_date="2025-12-31")
    ratios = api.get_finance_ratios("00700.HK", report_period="2025-12-31")

    # 默认 query_type=4；也可以显式传 1/2/3/4
    pit = api.get_finance_pit("600519.SH", trade_date="2025-12-31")
```

港股、美股财务字段与 A 股不同，`FinanceData.data` 会保留原始字段；业务代码应按
`market` 和实际字段判断，不要假设三地 schema 完全相同。港股、美股 PIT 当前会返回
明确的 `QueryError`：

```text
PIT query is unavailable for hk_stock: source data has no announcement_date
```

## Token 状态

SDK 可接收 token 有效、即将到期、过期、禁用或撤销状态通知：

```python
@api.on_token_status
def on_token_status(status):
    print(status.status, status.severity, status.expires_at)

print(api.token_status)
print(api.token_expires_at)
```

未收到状态通知时，`token_status` 保持为 `None`，不影响连接。

## 异常语义

- `AuthenticationError`：认证失败
- `DiscoveryError`：服务发现失败
- `ConnectionError`：TCP 连接失败或尚未连接
- `SymbolNotFoundError`：品种不在 symbol map
- `QueryError`：服务端明确拒绝，例如权限不足、非法 `query_type` 或 PIT 数据源不支持
- `QueryTimeoutError`：等待响应超时
- `DisconnectedError`：查询过程中连接中断
- `ProtocolError`：协议或数据帧解析错误

## 返回结构

- `get_quote()`：`Quote | None`
- `get_kline*()`：`list[Kline]`
- `get_finance*()`：`FinanceData`
- `Quote`：`symbol`、`symbol_id`、`bid`、`ask`、`last`、`volume`、`timestamp` 等
- `Kline`：`symbol`、`timestamp`、`open`、`high`、`low`、`close`、`volume`、`turnover`、`open_interest`
- `FinanceData`：`stock_code`、`report_period`、`data`

`API` 和 `RtdataClient` 都支持 context manager；非 context manager 用法结束时请调用
`close()`。

## 示例与文档

- [详细使用说明](./docs/SDK_USAGE.md)
- [Token 状态说明](./docs/TOKEN_STATUS.md)
- [示例目录](./examples/)
  - [`basic_usage.py`](./examples/basic_usage.py)：实时订阅
  - [`history_kline.py`](./examples/history_kline.py)：时间范围历史查询
  - [`history_adjust.py`](./examples/history_adjust.py)：A/HK/US 三市场复权
  - [`history_v2.py`](./examples/history_v2.py)：History V2 协商与回退
  - [`finance_query.py`](./examples/finance_query.py)：A/HK/US 财务、TTM、PIT、比率
  - [`session_rehome.py`](./examples/session_rehome.py)：自动故障迁移与首选节点归位
  - [`token_status.py`](./examples/token_status.py)：Token 状态通知

运行示例前请打开对应 `.py` 文件，将顶部的 `TOKEN` 替换为真实 token；其他参数也可
直接在文件顶部修改，然后直接运行 `python examples/文件名.py`。
