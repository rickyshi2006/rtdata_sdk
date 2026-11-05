# rtdata SDK 使用手册

本文对应 main 源码，包版本号目前为 `0.3.7`。已发布 v0.3.7 不包含随后合入的
`Quote/Kline.to_datetime()`；新示例须使用当前源码，安装见 [README](../README.md)。
不把源码更新视为安装包已发布。Python 最低版本为 3.9。

## 1. 连接入口与生命周期

常规业务使用 `API(token=...)`，默认通过 HTTPS 服务发现连接行情节点。
构造对象不联网；`connect()`、`with` 或首次订阅/查询才连接。退出时调用 `close()`。
先注册事件，再连接，才能观察首次连接及登录状态通知。

```python
import os
import rtdata

api = rtdata.API(token=os.environ["RTDATA_TOKEN"])

@api.on_connect
def connected():
    print("connected", api.current_node_id)

try:
    api.connect()
    print(len(api.symbols))
finally:
    api.close()
```

`RtdataClient` 是底层入口，需显式 `connect()` 或使用 `with`。
服务发现需要传 `api_url`；不传时按 `host/port` 直连，仅适合受控调试。
同时传 `api_url` 和 `host/port` 时，以服务发现结果为准。

### API 构造参数

下列默认值与代码同步测试。除 `token`、`api_url` 外均为关键字参数。

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `token` | 必填 | 接入 token |
| `api_url` | `"https://api.vrrv.ccwu.cc"` | 服务发现地址 |
| `async_callbacks` | `True` | 行情回调在独立分发线程执行 |
| `callback_queue_size` | `1000` | 行情回调队列容量 |
| `symbol_cache_dir` | `None` | 品种缓存根目录；默认 `~/.rtdata/` |
| `history_cache_dir` | `None` | 历史缓存根目录，见缓存章节 |
| `history_cache_enabled` | `True` | 允许使用本地历史缓存 |
| `history_v2_advertise` | `True` | 声明历史 V2 能力 |
| `history_v2_default` | `True` | 协商成功后优先历史 V2 |
| `history_v2_max_block_bytes` | `262144` | 历史列式块协商大小（字节） |
| `history_capability_ack_timeout` | `5.0` | 历史能力 ACK 等待秒数 |
| `session_rehome_advertise` | `True` | 声明安全节点迁移能力 |
| `session_capability_ack_timeout` | `5.0` | 节点迁移能力 ACK 等待秒数 |
| `realtime_protocol` | `"auto"` | `auto`、`legacy`、`required_v2` |

### RtdataClient 构造参数

与 API 同名参数默认值相同，但 `api_url` 默认为 `None`，并额外接受下列参数。
`token/host/port` 可为位置参数，其余为关键字参数。

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `api_url` | `None` | 设置后启用服务发现 |
| `host` | `"127.0.0.1"` | 受控直连地址 |
| `port` | `9100` | 受控直连端口 |
| `heartbeat_interval` | `20.0` | 心跳间隔秒数 |
| `auto_reconnect` | `True` | 自动重连 |

普通安装包含 Zstandard；`history-v2` extra 仅保留旧安装命令兼容性。
`market-time` extra 安装 IANA 时区数据，推荐 Windows 和精简系统使用。

## 2. 实时订阅与完整行情

```python
@api.on_quote
def on_quote(q):
    print(q.symbol, q.to_datetime().isoformat(timespec="milliseconds"), q.last)
    if q.wire_version == 2:
        print(dict(q.fields))

api.subscribe(["601919.SH", "AAPL", "EUR.USD", "BTCUSDT.BINANCE"])
# 在主线程继续业务/等待；结束后 close()。不要订阅后立刻退出 with 块。
```

`subscribe(symbols)` / `unsubscribe(symbols=None)` 无返回值；不传取消列表即取消全部。
`subscribe` 发出请求不等于服务端确认，更不等于已收到行情。
`api.symbols` 返回 `{id: code}`；以该目录中的代码为准，不应使用 `EURUSD`、`BTC/USDT`
等自行构造的别名。参见 [代码及市场时区](TIME-AND-MARKETS.md)。

默认 `auto` 自动协商所有服务端声明支持的市场，支持 V2 时读取全字段；旧端可以回退。
`legacy` 显式选择兼容六字段；`required_v2` 不接受回退，连接时等待协议协商。
协议选择和市场权限不同，不能通过选择 V2 获得未授权市场。

| 接口 | 行为 |
| --- | --- |
| `get_quote(symbol)` | 读取当前内存快照，无数据返回 `None`；严格模式无 V2 快照则报错 |
| `wait_for_quote(symbol, timeout=5.0)` | 等待可用缓存快照，不隐式订阅；失败抛 `RealtimeUnavailableError` |
| `wait_for_realtime(timeout=6.0)` | 等待 V2 协商，不等待市场 ready 或首帧；包括 auto 回退在内的失败均报错 |
| `realtime_status` | `mode/phase/negotiated/reason/generation/revision/tables` |
| `realtime_statistics` | 只读统计：`rejected/cached_wire_bytes/tracked_symbols`，不是吞吐计数 |
| `on_realtime_status` | 协议或分类状态变化回调 |

`realtime_status.tables` 按市场类别索引，各项含 `ready/reason/epoch/schema_id`。
ready 不是单品种活跃保证。断开连接、认证失败、关闭会清理缓存，分类变更会使相关快照失效。
应用自己留存的 `Quote` 是旧快照，不代表连接仍健康。

### 数据模型

- `Quote` 常用属性：`symbol/symbol_id/last/bid/ask/volume/timestamp`，以及
  `open/high/low/prev_close/turnover` 和 `bid_prices/bid_volumes/ask_prices/ask_volumes`。
- V2 `fields` 是只读映射，`available_fields` 是字段集合；以 `fields` 判断某字段是否真的存在，
  不要仅靠继承自旧 Quote 的属性默认值判断可用性。
- NULL 为 `None`，真实零保留为 `0`；不要直接对可能为空的价格用 `:.3f`。
- V2 快照不可修改，导出用 `dict(q.fields)`，不要依靠旧 dataclass 导出获得全字段。
- V1 只有兼容基础字段，未传输的属性默认值不代表真实观测。
- 全字段、类别、档数、数量精度及别名见 [实时字段说明](REALTIME-FULL-FIELDS.md)。

```python
last = "N/A" if q.last is None else f"{q.last:.4f}"
bid1 = q.bid_prices[0] if q.bid_prices else None
oi = q.fields.get("open_interest")
```

### 订阅状态与恢复

- `last_subscribe_requested`：全部当前期望订阅。
- `last_subscribe_confirmed`：当前连接已确认，断线时清空。
- `last_subscribe_rejected`：服务端拒绝的已发送品种，不含等待映射的品种。
- `last_subscribe_warning`：部分拒绝、等待映射或发送失败提示。

这些状态异步更新；不要把订阅后立即读到的空列表当作最终结果。
重连保留全部期望订阅，映射恢复后自动补订，不需要在 `on_connect` 中重复调用。
所有输入均不在当前映射时，`subscribe` 抛出 `SymbolNotFoundError`，不接受该次调用；
部分有效时保留其余代码的待补订意图。断线期间 `unsubscribe` 仍可取消恢复意图。

行情是最新值优先的快照流，不保证每个中间事件。慢回调会丢失中间状态；回调应轻量。
`async_callbacks` 只针对行情分发，不意味着所有事件都在工作线程；连接、状态等回调
也不能做阻塞查询。同步行情回调尤其不能调用查询或等待接口，否则会阻塞接收线程。

## 3. 时间与历史查询

`Quote.timestamp`、`Kline.timestamp` 是 UTC epoch 毫秒。显示使用 `to_datetime()`，
输入无时区历史时间按品种市场解释。不要固定加减 8 小时；完整规则及混合市场示例见
[时间说明](TIME-AND-MARKETS.md)。旧服务端的错误时间编码不能靠该显示方法修复。

```python
rows = api.get_kline(
    "000001.SZ", period="1m", start="2026-11-02", end="2026-11-02",
    timeout=60.0, adjust="forward",
)
if rows:
    print(len(rows), rows[0].to_datetime(), rows[-1].to_datetime())
```

### 查询参数

`get_kline(symbol, period="1d", start=0, end=0, timeout=30.0, adjust="none")`
返回按时间升序排列的 `list[Kline]`，可能为空；实际覆盖区间以返回结果为准。
不要把查询成功当作请求区间内的全部数据一定可用。

- 周期大小写敏感：`1m/5m/15m/30m/1h/2h/4h/1d/1w/1M`；`1M` 是月线。
- 股票复权：`none` 不复权、`forward` 前复权、`backward` 后复权。
  不支持复权的期货/期权等品种使用 `none`。
- 起止可为毫秒、`datetime`、`date`、ISO 日期时间字符串，`end` 包含端点。
- 默认 `0`（或省略）表示未指定端点，不是强制指定 1970 年；`end=None` 不代表精确的
  当前时刻。如需“至今”，显式使用 `datetime.now(timezone.utc)`。
- `timeout` 是查询阶段的预算，包含等待正在进行的历史协商；API 首次服务发现/连接
  另有自身等待时间。不要假设它严格覆盖首次 API 调用的全部墙钟耗时。
- 兼容 `start_time/end_time`，不要与对应的新参数重复传入。

快捷接口也返回 `list[Kline]`，实际覆盖范围仍以返回结果为准：

```python
rows = api.get_kline_range("000001.SZ", start="2026-11-02", end="2026-11-03")
rows = api.get_kline_for_day("000001.SZ", day="2026-11-02", period="1m")
rows = api.get_kline_for_today("000001.SZ", period="1m")
```

`get_kline_for_day` 传日期字符串或 `date` 才是整天；传带时间的 `datetime` 不会去掉时间。
`get_kline_for_today` 使用品种市场的当前日期。国内期货自然日范围不等同于交易日夜盘归属。

`Kline` 字段为 `symbol/timestamp/open/high/low/close/volume/turnover/open_interest`。
`open_interest` 为持仓量；对不提供该字段的历史结果可能为 `0`，不能据此判断没有持仓。
日/周/月线的时间是服务返回的 K 线标签，SDK 显示时不会移动到开盘时间或重新取整。

### 历史 V2 与缓存

默认协商并优先历史 V2，普通安装自带 Zstandard。协商不支持/超时可兼容 V1；
手工省略依赖、缺少 Zstandard 也会回退。用 `history_capability_state`、
`history_v2_eligible`、`history_capability_fallback_reason` 观察，不能把 eligible
当成某次缓存读取真的经过了 V2 网络请求。

`history_v2_default=False` 选择兼容查询；再设 `history_v2_advertise=False` 可关闭声明。
一般无需调整，也不能通过切换协议改变查询权限。

本地分段缓存使用 `symbol + period + adjust` 分组，仅在指定有效起止范围时启用：

- 默认目录 `~/.rtdata/history_utc_v1/`，不复用也不删除旧 `history_v1`。
- 自定义 `history_cache_dir` 是根目录，实际在其下创建 `history_utc_v1/`。
  未指定时沿用 `symbol_cache_dir`，再未指定才使用 `~/.rtdata/`。
- 优先使用可验证覆盖的缓存；不满足时请求服务，不承诺只查询缺口或自动获得全部历史。
- 缓存不是实时刷新机制；复权修订、历史更正等核验可设置 `history_cache_enabled=False`。
  未闭合 K 线测试也应关闭缓存。不要共享可写缓存给不可信用户。

## 4. 财务数据

| 方法 | 可选日期参数 | 返回 |
| --- | --- | --- |
| `get_finance(stock_code, report_period="", query_type=4, timeout=30.0)` | 报告期 | `FinanceData` |
| `get_finance_ttm(stock_code, as_of_date="", timeout=30.0)` | 截止日期 | `FinanceData` |
| `get_finance_pit(stock_code, trade_date="", query_type=4, timeout=30.0)` | 时点日期 | `FinanceData` |
| `get_finance_ratios(stock_code, report_period="", timeout=30.0)` | 报告期 | `FinanceData` |

日期使用 `YYYY-MM-DD`。`query_type=1/2/3/4` 分别为 income/balance/cashflow/all。
`FinanceData` 包含 `stock_code/report_period/data`，业务字段读取 `data`。
支持的 A/HK/US 财务字段可能不同；PIT 当前仅 A 股，港股、美股不支持时抛 `QueryError`。
不要把未支持接口的错误吞掉后伪装为全零报表。

## 5. 连接事件与错误

装饰器签名：`on_quote(q)`、`on_connect()`、`on_disconnect(reason)`、`on_error(err)`、
`on_token_status(status)`、`on_realtime_status(status)`。回调应在连接前注册。
Token 说明见 [TOKEN_STATUS](TOKEN_STATUS.md)；未收到通知时 `token_status=None`。

`is_connected` 表示认证连接可用，不保证每个市场推送活跃。诊断属性还有
`current_node_id/current_host/current_port/current_endpoint/gateway_version/protocol_features`。
分享日志前注意隐藏 token 和连接地址。

自动重连重新服务发现、认证和恢复订阅。服务端支持时可进行安全节点迁移，需同时启用
服务发现及底层 `auto_reconnect=True`；固定地址或显式关闭 `session_rehome_advertise`
不声明此能力。可查看 `session_capability_state/session_rehome_negotiated/session_capability_fallback_reason`。
在途查询可能因断线/迁移中断，调用方按业务需要有界重试；不要无休止重发所有查询。

| 异常 | 含义 |
| --- | --- |
| `AuthenticationError` | 认证失败或认证等待超时 |
| `DiscoveryError` | 服务发现失败 |
| `ConnectionError` | 未连接、连接失败或正在重连（这是 rtdata 异常） |
| `SymbolNotFoundError` | 当前映射找不到请求代码 |
| `QueryError` | 查询被拒绝或响应未通过查询校验 |
| `QueryTimeoutError` | 查询等待超时，不等同于被拒绝 |
| `DisconnectedError` | 在途查询连接中断 |
| `RealtimeUnavailableError` | 严格实时模式或实时等待条件未满足 |
| `ProtocolError` | 协议解析错误 |

以上均继承 `RtdataError`。非法参数还可能抛 Python `ValueError/TypeError`；
时区数据库缺失会抛 `ZoneInfoNotFoundError`。

## 6. 排查顺序

1. 打印 `rtdata.__version__` 和 `rtdata.__file__`，确认运行的解释器和安装位置。
2. 查看 `is_connected`、订阅 requested/confirmed/rejected/warning，区分待确认与被拒绝。
3. 查看 `realtime_status`、实际 `q.wire_version`、字段空值及证券目录；不要只看价格是否变化。
4. 对比各品种回调间隔；`timestamp` 可在盘口变化时重复，行情年龄不等于网络延迟。
5. 历史问题先区分拒绝、超时、断线，再关闭缓存核验实际返回范围。

可运行 [诊断示例](../examples/diagnose_realtime_stall.py)，它只使用公开状态，不做时区偏移补丁。
全部示例及其适用场景见 [示例索引](../examples/README.md)。
