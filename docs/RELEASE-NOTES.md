## v0.3.9

- 修复首次订阅与实时协议协商的先后顺序，避免首批行情过早使用旧格式。
  订阅调用仍为非阻塞，旧服务端保留自动回退。
- 严格全字段模式不再将初始等待就绪误报为行情中断；已接收行情的订阅
  发生不可用时仍报告错误，恢复后再次中断仍可通知。
- 原有订阅、历史查询接口、时间戳和 `Quote/Kline.to_datetime()` 用法不变，
  无需新增配置参数。源码包继续包含中文文档和配套示例。

下载 wheel 并使用随包 `SHA256SUMS` 校验后安装：

```sh
python -m pip install --upgrade "./rtdata-0.3.9-py3-none-any.whl[market-time]"
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__)"
```

`market-time` 会安装 IANA 时区数据，推荐 Windows 和精简环境使用。
升级后请重启 Python 或订阅进程；原调用方式不变，时间显示仍可使用：

```python
q.to_datetime()                 # 自动选择证券市场时区
q.to_datetime("Asia/Shanghai")  # 所有市场统一显示北京时间
k.to_datetime("UTC")            # 历史 K 线时间显示为 UTC
```

本次使用独立的 v0.3.9 标签及安装包，不替换任何旧版本产物。
