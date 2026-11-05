# 时间与市场代码

`Quote/Kline.to_datetime()` 从 v0.3.8 起提供；旧版用户先升级，
安装见 [README](../README.md)。

## 同时订阅 A 股和美股

```python
@api.on_quote
def on_quote(q):
    # 不需要自己按代码 if/else。
    dt = q.to_datetime()
    print(q.symbol, dt.isoformat(timespec="milliseconds"), q.last)

api.subscribe(["601919.SH", "AAPL"])
```

例如同一真实时刻 `2026-11-02T14:30:00Z`：

| 证券 | 默认显示 |
| --- | --- |
| `601919.SH` | `2026-11-02T22:30:00.000+08:00` |
| `AAPL` | `2026-11-02T09:30:00.000-05:00` |

这只说明时间换算，不代表两个市场此刻都开市。美股夏季可能是 `-04:00`，不能固定减 13 小时。

统一使用一个时区显示也可以：

```python
from datetime import timezone

q.to_datetime()                 # 品种的市场时区
q.to_datetime("Asia/Shanghai")  # 所有市场都显示北京时间
q.to_datetime("UTC")            # UTC
q.to_datetime(timezone.utc)     # 显式 tzinfo，不依赖 IANA 时区数据
k.to_datetime()                 # Kline 完全相同的用法
```

方法返回带时区的 `datetime`，不改动 `q.timestamp/k.timestamp`（UTC 毫秒）。
不要把转换后的小时偏移写回 timestamp。`datetime.fromtimestamp(ts / 1000)` 不传 `tz`
会使用运行电脑时区，不适合显示混合市场；显式 `tz=timezone.utc` 则是正确的 UTC 显示。

旧服务端若把无时区墙钟值当 epoch 发送，该方法不能猜测或修复它；需服务端规范时间。
盘口变化时 `trade_time` 可能不变，它不是本地接收时间，不能只用它判定断流。

## 市场代码与默认时区

市场品种分类、交易所名称和具体实例见 [市场品种与代码](MARKET-SYMBOLS.md)。
本节只说明时区映射，不作为品种类型或当前可用性清单。

**代码必须存在于 `api.symbols` 中。** 下表说明命名/显示规则，不保证示例合约永久挂牌，
也不保证每个品种都有所有历史周期。国内合约到期后应从目录选择活跃合约。

| 市场 | 代码例子或后缀 | 时区 |
| --- | --- | --- |
| A 股 | `601919.SH`、`000001.SZ`、`.BJ` | `Asia/Shanghai` |
| 国内期货 | `rb2701.SHF`、`.CFE/.CZC/.DCE/.INE/.GFE` | `Asia/Shanghai` |
| 港股 | `00700.HK` | `Asia/Hong_Kong` |
| 美股 | `AAPL`、`BRK.B`（目录中的 `.US` 代码也可识别时区） | `America/New_York` |
| 外汇 | `EUR.USD`；目录中 `.FX/.CFBM` 代码 | `UTC` |
| 加密货币 | `BTCUSDT.BINANCE`、`BTCUSDT.OKX` | `UTC` |
| 新加坡期货 | `CNmain.SGX` | `Asia/Singapore` |
| 香港期货 | `.HKFE` | `Asia/Hong_Kong` |
| 日本期货 | `.OSE` | `Asia/Tokyo` |
| 马来西亚期货 | `.BMD` | `Asia/Kuala_Lumpur` |
| 美国期货 | `.CME/.CBOT/.CBOE` | `America/Chicago` |
| 美国期货 | `.NYMEX/.COMEX` | `America/New_York` |

不要用 `EURUSD` 代替 `EUR.USD`，不要用 `BTC/USDT` 代替交易所限定的代码。
也不要仅因时区识别成功就认为代码有效：美股样式字符串可以被识别为纽约时区，但是否
可订阅仍以证券目录为准。无法识别的代码或自行创建的无 symbol 的 `Kline`，应显式指定 `tz`。

## 历史请求的时间输入

```python
from datetime import datetime, timezone

rows = api.get_kline(
    "CNmain.SGX", period="1m",
    start="2026-10-30 09:00:00", end="2026-10-30 10:00:00",
)
# 这里 09:00 是新加坡时间，对应 01:00 UTC，而不是运行电脑的 09:00。

rows = api.get_kline(
    "AAPL", period="1m",
    start="2026-07-01T09:30:00-04:00", end="2026-07-01T10:00:00-04:00",
)
# 带 UTC offset 时保持其真实时刻，不再重新套用市场时区。

end_now = datetime.now(timezone.utc)  # 精确的当前时刻；可作为 end 传入。
```

- `int/float` 按 UTC epoch **毫秒**解释，不自动识别秒；建议使用整数。
- `date` 或 `YYYY-MM-DD`：start 当天 00:00，end 当天 23:59:59.999。
- 无时区 `datetime` 或日期时间字符串：按市场时区解释，与电脑所在地无关。
- 带时区 `datetime` 或带 offset / `Z` 的 ISO 字符串：保留真实时刻。
- `end` 包含该毫秒；精度不足一毫秒的部分按毫秒表示。
- 夏令时跳变时不存在或重复的无时区时间会报 `ValueError`，需使用明确 offset 的时间。
- `get_kline_for_today()` 采用市场当前日期；国内期货夜盘的自然日不等于交易日标签。

系统需提供 IANA 时区数据。Windows 或精简系统可随下载的 wheel 安装：

```bash
python -m pip install --upgrade "./rtdata-0.3.8-py3-none-any.whl[market-time]"
```

时区依赖只提供转换数据，不能为旧版本增加 `to_datetime()` 方法。
