# 示例运行指南

此目录跟随 main 源码。时间显示示例需要 `Quote/Kline.to_datetime()`，
**已发布 v0.3.7 安装包尚无该方法**。请在源码目录安装：

```bash
python -m pip install -e ".[market-time]"
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__); print(hasattr(rtdata.Quote, 'to_datetime'))"
```

## 配置与运行

Linux/macOS：

```bash
export RTDATA_TOKEN='your_token'
# 可选：覆盖默认服务发现地址，使用服务商给定的地址。
# export RTDATA_API_URL='https://discovery.example'
python examples/subscribe_realtime.py
```

Windows PowerShell：

```powershell
$env:RTDATA_TOKEN = 'your_token'
python examples/subscribe_realtime.py
```

也可以从仓库根目录运行 `python -m examples.subscribe_realtime`。
所有示例均在 `main()` 中才建立连接，导入不会订阅或查询；缺少 token 时在联网前退出。
示例之间不要共用一个会受并发会话限制的 token 同时连接。
`_common.py` 仅供示例共享配置、格式化和等待函数，不是 SDK 的公开接口。

按需要修改示例顶部的 `SYMBOLS`、查询区间和观察时长，不把真实 token 写入文件。
期货合约可能到期、不同服务目录不同，先确认 `api.symbols`。休市无推送或初始化快照
并不代表连接失败；协议 ready 也不保证每个品种都有活跃行情。

## 文件索引

| 文件 | 用途 |
| --- | --- |
| [basic_usage.py](basic_usage.py) | 最小订阅，监听 60 秒 |
| [subscribe_realtime.py](subscribe_realtime.py) | A/HK/US、外汇、加密货币、国内期货混合订阅；Ctrl+C 退出 |
| [history_kline.py](history_kline.py) | 日线/分钟线、市场时间、实际范围及错误处理 |
| [history_adjust.py](history_adjust.py) | A/HK/US 前后复权 |
| [history_v2.py](history_v2.py) | 默认历史协商及回退状态；不手工启用 V2 |
| [finance_query.py](finance_query.py) | 财务三表、TTM、比率及 PIT 支持边界 |
| [token_status.py](token_status.py) | 只认证，监听账号状态通知 |
| [session_rehome.py](session_rehome.py) | 被动观察节点迁移协商，不制造故障 |
| [diagnose_realtime_stall.py](diagnose_realtime_stall.py) | 每品种回调间隔、持续静默时间及公开 SDK 状态 |
| [test_subscribe.py](test_subscribe.py) | 旧路径兼容，转调多市场示例并监听 10 秒 |
| [test_subscribe2.py](test_subscribe2.py) | 旧路径兼容，转调多市场示例并监听 15 秒 |

`test_subscribe*.py` 是手动联网示例，不是自动化测试。CI 运行 `tests/` 中的离线测试，
不使用真实 token。

## 输出说明

- `to_datetime()` 默认按证券选择市场时区，输出带 UTC offset。混合市场不需分开 API；
  如需全部按北京时间显示，使用 `to_datetime("Asia/Shanghai")`。
- `N/A` 是缺失值，真实零保留为零；价格打印六位小数便于看外汇价差，但只是显示精度。
- `volume_raw` 及买卖量是原始整数，未擅自缩放。扩展行情的
  `volume_precision/book_volume_precision` 可从 `q.fields` 读取，不能假设所有市场同单位。
- 设置 `PRINT_FULL_FIELDS=True` 查看完整字段，仅用于少量样本，频繁打印会拖慢回调。
- `event_age_ms` 是当前 UTC 与行情时间差，包含数据年龄、时钟偏差等，
  **不是网络延迟**。盘口更新可能沿用成交时间，不能单靠它判定断流。
- 诊断中的 `silence_s/max_callback_gap_s` 使用单调时钟，分别是当前静默时间与最长
  回调空档（包含当前静默）。它们也包含客户端调度/消费时间，不能单独定位上游故障。
- 历史结果可能为空或只覆盖部分请求范围，以打印的实际起止为准。查询示例不写自动
  无限重试循环，也不假定缓存永久保持最新。

进一步参考：[使用手册](../docs/SDK_USAGE.md)、[时间](../docs/TIME-AND-MARKETS.md)、
[全字段说明](../docs/REALTIME-FULL-FIELDS.md)。
