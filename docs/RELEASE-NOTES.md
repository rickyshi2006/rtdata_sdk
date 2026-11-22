## v0.4.1

- 分类订阅自动协商紧凑二进制身份分块和可选无损压缩，减少目录传输量。
  旧服务端继续使用原有身份分页，无需手工开关。
- 同连接退订后保留有界目录缓存，重订时复用；断线清空缓存。
  缓存不代表订阅权限，接纳结果仍以 `wait_applied()` 成功返回为准。
- 默认等待区分有效身份进展与停滞，同时保留固定总上限。
  显式传入 `wait_applied(timeout=...)` 时仍遵守调用者指定的期限。
- 初始缓存行情采用有界接收确认，降低弱网下立即退订的控制阻塞风险。
- 等待无损行情块握手完成后发送订阅，避免大目录同步干扰握手。
- 原有订阅、历史查询、全字段与序号校验接口不变；实时行情仍是最新快照流，
  不承诺逐笔无损交付。实验性数值解码默认关闭，普通安装无需编译器。

下载 wheel 并使用随包 `SHA256SUMS` 校验后安装：

```sh
python -m pip install --upgrade "./rtdata-0.4.1-py3-none-any.whl[market-time]"
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

本次使用独立的 v0.4.1 标签及安装包，不替换任何旧版本产物。
