# rtdata SDK

`rtdata` 是一个零外部依赖的 Python SDK，用于通过 Cloud Gateway 获取实时行情、历史 K 线和财务数据。

## 特性
- 纯标准库实现，支持 Python `>= 3.9`
- 支持 HTTPS 服务发现，自动获取 TCP 接入地址
- 自动下载并缓存 symbol map，按版本增量更新
- 自动心跳、自动重连、断线后自动恢复订阅
- 实时订阅语义为“最新值优先”的快照流，不是逐笔事件流
- 历史 K 线支持本地分段二进制缓存，重复请求优先命中本地
- 历史/财务查询失败会抛明确异常，不再把服务端失败误报为超时

## 当前支持范围

- 实时数据：支持 A 股、期货、港股
- 历史 K 线：支持 A 股、期货、港股
- 财务数据：仅支持 A 股

后续如有市场范围调整，再按实际能力更新文档。

## 安装

开发模式：

```bash
cd rtdata_sdk
pip install -e .
```

安装打包产物：

```bash
pip install rtdata-0.1.7-py3-none-any.whl
```

## Token 兑换

如果你拿到的是卡号或 UUID，请先到下面的网址兑换 token：

`https://rtdata.fengv2ray.tk`

当前仓库内提供了一份卡号清单文件：

- [TOKEN_EXCHANGE_CARDS.txt](./TOKEN_EXCHANGE_CARDS.txt)

使用方式：

1. 打开兑换页面
2. 输入文件中的卡号或 UUID
3. 完成兑换后获取 token
4. 在 SDK 中把该 token 传给 `rtdata.API(...)` 或 `RtdataClient(...)`

## 快速开始

推荐使用 `API` 封装：

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

也可以直接使用底层 `RtdataClient`：

```python
from rtdata import RtdataClient

with RtdataClient(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
) as client:
    print(client.current_node_id)
```

## 当前行为说明

- 指定 `api_url` 后，SDK 会先做服务发现，再连接 discovery 返回的 TCP 节点。
- `API.subscribe()` / `API.get_kline()` / `API.get_finance()` 会在首次调用时自动连接；也可以先显式 `api.connect()`。
- 实时订阅不是逐条完整回放；如果本地消费过慢，网关可能丢弃旧快照，或主动断开后由 SDK 自动重连。
- 历史和财务查询是请求-响应语义。
- 服务端显式拒绝会抛 `QueryError`，例如：
  - `Market not allowed`
  - `History download busy, retry later`
- 查询超时才会抛 `QueryTimeoutError`。
- 如果连接中断导致当前查询失败，会抛 `DisconnectedError`。

## 历史查询

当前主推 `start/end` 时间范围语义：

```python
klines = api.get_kline(
    "000001.SZ",
    period="1d",
    start="2015-01-01",
    end="2015-12-31",
    adjust="none",
)
```

时间参数规则：

- 仅日期：自动扩展为当天 `00:00:00 ~ 23:59:59.999`
- 带时间：按精确时间截取
- 支持 `int/float` 毫秒时间戳、`datetime`、`date`、字符串日期时间
- 支持周期：`1m`、`5m`、`15m`、`30m`、`1h`、`2h`、`4h`、`1d`、`1w`、`1M`
- `adjust` 支持：
  - `none`：不复权
  - `forward`：前复权
  - `backward`：后复权

复权示例：

```python
none_rows = api.get_kline(
    "000001.SZ",
    period="1d",
    start="2015-01-01",
    end="2015-03-31",
    adjust="none",
)

forward_rows = api.get_kline(
    "000001.SZ",
    period="1d",
    start="2015-01-01",
    end="2015-03-31",
    adjust="forward",
)

backward_rows = api.get_kline(
    "000001.SZ",
    period="1d",
    start="2015-01-01",
    end="2015-03-31",
    adjust="backward",
)
```

说明：

- 当前仅 `SH` / `SZ` 股票支持 `forward` / `backward`
- 期货和其他市场品种如传入复权参数，服务端会拒绝

分钟线示例：

```python
klines = api.get_kline(
    "rb2610.SHF",
    period="1m",
    start="2026-04-29",
    end="2026-04-30",
)
```

## 本地历史缓存

当同时传入 `start` 和 `end` 时，SDK 默认开启本地历史缓存：

- 默认目录：`~/.rtdata/history_v1/`
- 缓存格式：分段二进制文件，不依赖 sqlite
- 重复请求相同区间时，优先读取本地
- 只对缺失时间段回源服务器
- 缓存维度包含 `symbol + period + adjust`
- 未给全 `start/end` 的查询不会走本地历史缓存

关闭历史缓存：

```python
api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    history_cache_enabled=False,
)
```

自定义缓存目录：

```python
api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    history_cache_dir="/data/rtdata_cache",
)
```

## 实时订阅结果检查

如果服务端只接受了部分订阅，SDK 会保留详细状态：

```python
api.subscribe(["601919.SH", "rb2610.SHF"])

print(api.last_subscribe_warning)
print(api.last_subscribe_requested)
print(api.last_subscribe_confirmed)
print(api.last_subscribe_rejected)
```

典型拒绝原因：

- token 没有对应市场权限
- 超过 token 的最大订阅数
- 服务端过滤了不允许的品种

## 主要异常

- `AuthenticationError`：认证失败
- `DiscoveryError`：服务发现失败
- `ConnectionError`：TCP 连接失败或尚未连接
- `SymbolNotFoundError`：symbol 不在 symbol map 中
- `QueryError`：服务端显式拒绝查询
- `QueryTimeoutError`：查询等待超时
- `DisconnectedError`：连接中断导致查询失败
- `ProtocolError`：协议解析错误

## Token 状态通知

支持新网关的 `TOKEN_STATUS` 通知时，可以注册独立回调：

```python
@api.on_token_status
def on_token_status(status):
    print(status.status, status.severity, status.expires_at)
```

也可以读取最后一次状态：

```python
print(api.token_status)
print(api.token_expires_at)
```

旧网关不会发送该消息，新 SDK 仍可正常连接，此时
`api.token_status` 为 `None`。详细说明见 `docs/TOKEN_STATUS.md`。

## 返回格式

- `subscribe()`：无返回值；实时数据通过 `@api.on_quote` 回调推送，回调参数类型是 `Quote`
- `get_quote(symbol)`：返回 `Quote` 或 `None`
- `get_kline(...)`：返回 `list[Kline]`
- `get_finance(...)`：返回 `FinanceData`
- `get_finance_ttm(...)`：返回 `FinanceData`
- `get_finance_pit(...)`：返回 `FinanceData`
- `get_finance_ratios(...)`：返回 `FinanceData`

主要数据结构字段：

- `Quote`：`symbol`、`symbol_id`、`bid`、`ask`、`last`、`volume`、`timestamp`，以及可选的高低开收、成交额和五档字段
- `Kline`：`symbol`、`timestamp`、`open`、`high`、`low`、`close`、`volume`、`turnover`、`open_interest`
- `FinanceData`：`stock_code`、`report_period`、`data`

## 资源释放

- `API` 和 `RtdataClient` 都支持 `with ... as ...`
- 如果不用 context manager，请在结束时调用 `close()`

## 更多文档

- 详细使用说明：[docs/SDK_USAGE.md](./docs/SDK_USAGE.md)
- 交付说明：[DELIVERY.md](./DELIVERY.md)
- 示例代码：[`examples/`](./examples/)

##推广
- https://linux.do
