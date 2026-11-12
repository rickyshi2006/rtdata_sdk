# rtdata 行情 SDK

`rtdata` 提供实时行情订阅、历史 K 线和财务数据查询，支持 Python 3.9 及以上版本。

## 版本与安装

当前包版本为 **0.3.9**，修复首次订阅协商顺序和严格全字段模式的冷启动误报，
保留 `Quote/Kline.to_datetime()`、中文文档和配套示例。
旧 v0.3.7 没有该时间显示方法，使用新示例前请升级。
功能以对应标签和 [发布说明](docs/RELEASE-NOTES.md) 为准。

从 [GitHub Releases](https://github.com/rickyshi2006/rtdata_sdk/releases/tag/v0.3.9)
下载 wheel，以随包 `SHA256SUMS` 校验后安装（包括 Windows 等环境所需的时区数据）：

```bash
python -m pip install --upgrade "./rtdata-0.3.9-py3-none-any.whl[market-time]"
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__); print(hasattr(rtdata.Quote, 'to_datetime'))"
```

如需开发安装，在对应版本源码目录执行 `python -m pip install -e ".[market-time]"`。

普通安装包含 Zstandard，历史 V2 默认协商并优先使用，不需要额外安装 `history-v2`。
旧 extra 名仍兼容。更新安装后应重启 Python 进程；不要仅凭相同版本号推断源码能力。

## 领取测试 token

访问 [测试 token 领取页面](https://rtdata.vrrv.ccwu.cc/) 领取测试 token，
领取后将其设置为环境变量 `RTDATA_TOKEN`，即可运行下方示例。

## 快速开始：同时订阅多个市场

设置环境变量 `RTDATA_TOKEN` 为你的 token，不要把 token 提交到源码仓库。

```python
import os
import time
import rtdata

api = rtdata.API(token=os.environ["RTDATA_TOKEN"])

@api.on_quote
def on_quote(q):
    # 各品种自动使用其市场时区；输出包含 UTC offset。
    dt = q.to_datetime()
    last = "N/A" if q.last is None else f"{q.last:.4f}"
    print(q.symbol, dt.isoformat(timespec="milliseconds"), last)

try:
    api.subscribe(["601919.SH", "AAPL", "EUR.USD", "BTCUSDT.BINANCE"])
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    api.close()
```

实时默认为 `realtime_protocol="auto"`：自动协商 V2 全字段，旧服务端兼容回退。
不需要按市场手工启用协议，更不需要指定后端数据源。回调里的 `q.wire_version`
反映实际格式；“已连接”“已订阅”“已有行情”是不同状态。

完整示例：[多市场订阅](examples/subscribe_realtime.py)、[基本订阅](examples/basic_usage.py)。
示例脚本也接受可选的 `RTDATA_API_URL` 环境变量覆盖默认服务发现地址。

## 行情字段与时间

- `Quote` 保留 `last/bid/ask/volume/timestamp` 等常用属性，完整 V2 字段从
  `q.fields` 读取，使用 `dict(q.fields)` 导出。V2 快照只读。
- `None` 表示空值，不等于 `0`；档位列表可能为空或包含 `None`。
- 盘口档数由协商 schema 和实际数据共同决定，并非所有市场都有十档。
- `timestamp` 是 UTC epoch 毫秒；不要在整数上手工加减 8 小时。
  `q.to_datetime()` / `k.to_datetime()` 只转换显示，不修改原始整数。
- A 股、国内期货默认显示北京时间，港股显示香港时间；美股显示纽约时间，自动处理
  夏令时。统一显示北京时间使用 `q.to_datetime("Asia/Shanghai")`。

详见 [时间与市场代码](docs/TIME-AND-MARKETS.md)、[全字段说明](docs/REALTIME-FULL-FIELDS.md)。

实时数据是最新值优先的**快照流**，不是无损逐笔事件流。慢回调可能跳过中间状态。
不要在回调里做耗时 I/O 或阻塞查询；将工作交给自己的有界队列和工作线程。
`get_quote(symbol)` 读取最新缓存，不发起行情查询；订阅后可以用
`wait_for_quote(symbol, timeout=5.0)` 等待首个可用缓存快照。

## 历史 K 线

```python
import os
import rtdata

with rtdata.API(token=os.environ["RTDATA_TOKEN"]) as api:
    rows = api.get_kline(
        "000001.SZ", period="1m",
        start="2026-11-02 09:30:00", end="2026-11-02 10:00:00",
        adjust="forward", timeout=60.0,
    )
    print("返回根数:", len(rows))
    if rows:
        print("实际范围:", rows[0].to_datetime(), rows[-1].to_datetime())
    for k in rows[:3]:
        print(k.to_datetime().isoformat(), k.open, k.high, k.low, k.close, k.volume)
```

- 周期：`1m`、`5m`、`15m`、`30m`、`1h`、`2h`、`4h`、`1d`、`1w`、`1M`。
  品种、周期及历史覆盖范围以服务实际支持为准。
- `adjust="none"/"forward"/"backward"` 分别是不复权、前复权、后复权，适用于支持
  复权的股票；非股票品种使用 `none`。
- 无时区日期/时间按**证券市场时区**解释，不使用电脑本地时区；带时区输入保持真实时刻。
  数值是**毫秒，不是秒**。日期 `end` 包含全天，带时间的 `end` 包含该端点。
- 返回 `list[Kline]`，按时间升序；应检查实际返回区间，不保证一次调用覆盖全部请求历史。
- 默认协商历史 V2，兼容 V1；无需客户端拆解 wire 协议。
- 本地缓存默认位于 `~/.rtdata/history_utc_v1/`，按 `symbol + period + adjust` 区分。
  只在指定起止范围时使用；`history_cache_enabled=False` 可关闭。

详见 [使用手册](docs/SDK_USAGE.md)、[历史示例](examples/history_kline.py)、
[三市场复权](examples/history_adjust.py)。

## 市场与财务数据

实时市场包括 A 股、港股、美股、国内期货、外汇、海外期货、加密货币。
历史接口也可查询服务支持的期权等品种；市场权限、可用品种、周期与覆盖区间由服务决定。
请使用 `api.symbols` 中的代码，不要自行拼接别名。

交易所后缀、股票/指数/基金、期货/期权、外汇、加密货币的具体例子见
[市场品种分类与代码实例](docs/MARKET-SYMBOLS.md)。

财务接口：`get_finance()`、`get_finance_ttm()`、`get_finance_ratios()`、
`get_finance_pit()`。普通报表与 PIT 的 `query_type` 默认均为 `4`（三表），
`1/2/3` 分别选择利润表/资产负债表/现金流量表。A/HK/US 字段可能不同；PIT 当前
支持 A 股，港股、美股不可用时返回 `QueryError`。见 [财务示例](examples/finance_query.py)。

## 连接、状态与错误

`API` 懒连接，`with API(...)` 或首次订阅/查询时连接；也可显式 `connect()`。
需细化心跳、自动重连或受控直连时使用 `RtdataClient`。
默认服务发现、自动重连及订阅恢复，无需在 `on_connect` 中重复订阅。
节点迁移须服务端共同支持；在途查询中断会报错，不会承诺自动重做。

```python
print(api.last_subscribe_requested)  # 期望订阅
print(api.last_subscribe_confirmed)  # 当前连接已确认
print(api.last_subscribe_rejected)   # 服务端拒绝，不包括待映射代码
print(api.last_subscribe_warning)
print(api.realtime_status)
```

订阅确认是异步的，紧接 `subscribe()` 读取时可能尚未更新。
`on_token_status` / `token_status` 可观察账号状态；旧服务端不发送时为 `None`。
主要异常：`AuthenticationError`、`DiscoveryError`、`ConnectionError`、
`SymbolNotFoundError`、`QueryError`、`QueryTimeoutError`、`DisconnectedError`、
`RealtimeUnavailableError`、`ProtocolError`；都继承 `RtdataError`。

## 文档导航

- [完整使用手册与参数默认值](docs/SDK_USAGE.md)
- [市场品种分类与代码实例](docs/MARKET-SYMBOLS.md)
- [时间、夏令时与市场代码](docs/TIME-AND-MARKETS.md)
- [实时全字段、盘口与兼容性](docs/REALTIME-FULL-FIELDS.md)
- [Token 状态](docs/TOKEN_STATUS.md)
- [示例索引及运行方式](examples/README.md)
- [交付核对](DELIVERY.md) / [开发流程](DEVELOPMENT.md)
- [变更历史](CHANGELOG.md) / [已发布版本说明](docs/RELEASE-NOTES.md)
