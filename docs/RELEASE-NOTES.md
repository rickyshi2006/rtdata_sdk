## v0.4.0

- 新增七类市场的整类订阅，无需客户端准备本地品种目录。通过 `get_markets()`
  查看可用分类，`subscribe_market(s)` 返回可等待应用完成的操作句柄。
- 分类订阅与手工订阅去重合并，提供分类退订、重连恢复、问题分页及分类供数状态。
  权限、额度及实际可用品种由服务决定，订阅完成不等于已有行情。
- 自动协商普通无损实时块，保留完整字段、空值和严格序号检查，兼容原有逐条格式。
  大目录采用有界缓存；实时行情仍是最新快照流，不保证无损逐笔交付。
- 实验性数值解码默认关闭，独立原生可选包不随本次发布；普通用户无需编译器。
- 单品种订阅、历史查询、UTC 毫秒时间戳和 `Quote/Kline.to_datetime()` 用法不变。

下载 wheel 并使用随包 `SHA256SUMS` 校验后安装：

```sh
python -m pip install --upgrade "./rtdata-0.4.0-py3-none-any.whl[market-time]"
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__)"
```

`market-time` 会安装 IANA 时区数据，推荐 Windows 和精简环境使用。
升级后请重启 Python 或订阅进程；原调用方式不变，时间显示仍可使用：

```python
q.to_datetime()                 # 自动选择证券市场时区
q.to_datetime("Asia/Shanghai")  # 所有市场统一显示北京时间
k.to_datetime("UTC")            # 历史 K 线时间显示为 UTC
```

分类接口需要服务端支持，不会在旧服务端展开为全市场手工订阅。
详见随版本提供的 [分类订阅说明](MARKET-SUBSCRIPTIONS.md) 和分类订阅示例。

本次使用独立的 v0.4.0 标签及安装包，不替换任何旧版本产物。
