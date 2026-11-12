# 实时行情全字段说明

本文对应 v0.3.9，包含首次订阅协商与初始就绪通知修复，以及 `Quote/Kline.to_datetime()`。
安装方式见 [README](../README.md)，时间转换见 [时间说明](TIME-AND-MARKETS.md)。
包含持续监听和连接关闭的完整程序见 [示例索引](../examples/README.md)。

`API` 和 `RtdataClient` 默认自动协商 V2 全字段，原有连接和订阅调用无需增加参数。

```python
import rtdata

api = rtdata.API(token="your_token")

@api.on_quote
def on_quote(quote):
    print(quote.symbol, quote.last)
    print(dict(quote.fields))

api.subscribe(["600519.SH"])
# 继续等待或执行业务，退出时在 finally 中调用 api.close()。
```

## 协议选择

- `auto`（默认）：申请全字段；无法协商时回退兼容格式。实际格式查看 `quote.wire_version`。
- `legacy`：显式使用旧兼容格式，保留旧版可修改的 `Quote`，不进行自动全字段协商。
- `required_v2`：禁止回退。`connect()` 等待协议协商，但不等待首条行情；
  当前没有可用全字段快照时，`get_quote()` 抛出 `RealtimeUnavailableError`，
  `wait_for_quote()` 则等待至满足条件或超时。

两个构造入口都支持这些选项，`subscribe()` 的调用方式不变。
协商期间订阅意图保留，确认全字段协议后再发送，不阻塞调用线程。
旧服务端不响应协商时，默认模式在现有的5秒协商超时后发送订阅并接收兼容行情；
显式 `legacy` 模式不等待协商。
`wait_for_quote()` 不会自动订阅。协商成功或订阅确认均不表示首条行情已经到达。

## 市场类别与盘口档数

从 0.3.6 起，SDK 自动将服务端声明支持的市场纳入协商，包括海外期货和加密货币。
不需要启用市场的额外参数，也不需要选择后端数据源；客户端能力协商只决定传输格式。
具体市场分类与证券代码见 [市场品种与代码实例](MARKET-SYMBOLS.md)。

| 公开类别 | 字段结构编号 | 每侧定义的盘口档数 |
| --- | ---: | ---: |
| `cn_equity`, `hk_equity`, `us_equity` | 1 | 5 |
| `futures` | 2 | 5 |
| `fx` | 3 | 1 |
| `overseas_futures` | 4 | 10 |
| `crypto` | 5 | 10 |

上表表示字段结构（schema）的容量，不保证每个品种都提供足够档位。
美股当前仍使用五档公开结构，即使其他接口提供更多档也不代表 SDK 此结构包含十档。
海外期货和加密货币结构支持最多十档、64 位有符号数量及数量精度字段。
不要删除空档后把后面的档位前移，否则会改变档位含义。

登录成功响应可以声明支持的市场和帧大小，SDK 在每次连接时重新评估。
旧服务端不提供这些信息时，SDK 使用兼容旧解析器的五类别能力声明；
完全不支持 V2 的服务端仍可使用六字段兼容格式。既有类别的字段定义和结构标识不变。
从 0.3.6 起，已移除过时的数据源专用构造参数；调用方应删除这些参数，不需要替代参数。
不支持的关键字会抛出 `TypeError`，显式 `realtime_protocol` 选项仍有效。

### 原始字段与数量精度

`raw_bits` 是 JSON 字符串，将原始字段组编号映射到十六进制编码的 protobuf 字节，
用于保留尚未命名的原始字段，不擅自发明字段名称或单位。
命名字段缺失时仍为 `None`，包括某市场不提供的字段。

盘口数量保留原始整数；显示小数单位前应核对 `book_volume_precision`。
成交量精度可查看 `volume_precision`；并非所有 schema 都提供精度字段，
没有精度信息时不能按别的市场单位强行缩放。
`raw_bits` 最大 512 KiB，扩展行受协商上限约束，最大 1 MiB；
超限数据会被拒绝，不会静默截断。

## 字段读取与兼容性

V2 行情是只读 `Quote` 实例。可通过属性或只读映射 `quote.fields` 读取，
用 `dict(quote.fields)` 导出完整字段，旧 dataclass 序列化不等同于全字段导出。

- NULL 保留为 `None`，真实零仍为零。结构之外的字段不在 `fields` 中。
  以该映射或 `available_fields` 判断支持情况；继承自旧 Quote 的兼容属性可能仍有默认值。
- 浮点数保留双精度，整数保持 Python 整数；负数、NaN 和无穷值不会替换为零。
- 常用别名保持不变：`bid=bid_price1`、`ask=ask_price1`、
  `timestamp=trade_time`、`prev_close=pre_close`、`turnover=amount`。
- `bid_prices/bid_volumes/ask_prices/ask_volumes` 返回独立列表，保留空值位置；
  可用档数随市场和实际数据而变化。
- `wire_version/schema_id/schema_version/source_id/source_epoch/sequence`
  描述格式和快照代次、序列等信息，不应当作行情一定活跃的保证。
- 兼容属性 `source_table` 返回公开市场类别：
  `cn_equity/hk_equity/futures/us_equity/fx/overseas_futures/crypto`，不是后端源名称。

需要修改行情的业务应保存自己的副本。格式化空值时必须显式判断，不能把缺失当成零：

```python
last = "N/A" if quote.last is None else f"{quote.last:.6f}"
bid1 = quote.bid_prices[0] if quote.bid_prices else None
open_interest = quote.fields.get("open_interest")
exported = dict(quote.fields)
```

## 时间戳与市场时间显示

`quote.timestamp`（`trade_time` 的别名）及 `kline.timestamp` 保留收到的整数毫秒。
采用 UTC 规范的服务端发送真实 UTC 时刻，不因市场不同而改变 epoch 定义。
显示转换不会修改保存的时间戳：

```python
quote.to_datetime()                 # 证券市场当地时间，带时区
quote.to_datetime("UTC")            # 同一时刻的 UTC 显示
quote.to_datetime("Asia/Shanghai")  # 同一时刻的北京时间显示
kline.to_datetime()                 # 历史 K 线时间标签采用同一方法
```

`tz` 还可接受 `datetime.tzinfo`，例如 `timezone.utc`。
省略时，A 股/国内期货用上海时区，港股用香港时区，美股用纽约时区并处理夏令时；
海外期货采用对应市场时区，已识别的外汇和加密货币代码（含 `EUR.USD`）用 UTC。
无法识别的市场、没有 symbol 的自建 `Kline` 应显式提供时区。
时区识别不验证品种是否存在；以 [品种目录](MARKET-SYMBOLS.md) 为准。
系统没有 IANA 时区数据时可安装 `rtdata[market-time]`。

该方法只改变显示，不能修复旧服务端把无时区墙钟值错误编码为 epoch 的情况；
服务端必须先提供规范时间。盘口变化时成交时间可能重复，不要替换成本地接收时间，
也不要把历史 K 线标签重新取整或移动到开盘时刻。

历史无时区输入按市场解释：`.SGX/.HKFE/.OSE/.BMD/.CME/.CBOT/.CBOE/.NYMEX/.COMEX`
使用对应时区及夏令时；已确认的加密货币后缀、`.CFBM/.FX` 使用 UTC。
带时区的时间或 UTC 毫秒保持原真实时刻。历史输入转换不再次应用到收到的实时时间戳。

## 状态与生命周期

`realtime_status.tables` 以公开类别为键，状态包含 `ready/reason/epoch`；
完成协议协商不等于每个类别都已就绪。可通过 `on_realtime_status` 观察变化。
严格模式下，尚无首条行情或未订阅类别的不可用状态不触发 `on_error`；
已经接收全字段行情且仍被订阅的类别由可用转为不可用时，会触发错误通知。
协议协商失败、收到不允许的兼容行情仍报错；等待首条行情的超时由
`wait_for_quote()` 明确抛出，不把初始化状态伪装成已就绪。
回调应快速返回，不要在接收线程回调中调用同步查询或等待接口。

断线、认证失败和关闭会清理行情缓存，类别状态变化会使相应快照失效。
业务代码保存的快照是历史状态，不代表当前连接或市场仍然可用。

行情按每个品种的最新值分发，不是无损逐笔事件流。回调过慢时可能跳过已被更新的快照。
`realtime_statistics` 提供拒绝行数、缓存传输字节数和跟踪品种数；
回调队列及缓存有界，但传输字节限制不等于整个 Python 进程的内存限制。
