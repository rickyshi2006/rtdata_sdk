## v0.3.8

- 新增 `Quote.to_datetime()` 和 `Kline.to_datetime()`，默认按证券市场显示带时区的时间，
  支持显式指定时区；美股自动处理夏令时，不修改原 UTC 毫秒时间戳。
- 补齐多市场订阅、完整字段、空值处理、历史查询和连接关闭示例。
- 仓库文档统一为简体中文，新增市场分类、交易所后缀和具体品种代码手册。
- 源码包包含配套文档与示例；保持原有订阅、历史查询接口及默认协议协商行为。

下载 wheel 并使用随包 `SHA256SUMS` 校验后安装：

```sh
python -m pip install --upgrade "./rtdata-0.3.8-py3-none-any.whl[market-time]"
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__)"
```

`market-time` 会安装 IANA 时区数据，推荐 Windows 和精简环境使用。
升级后请重启 Python 或订阅进程；原调用方式不变，可按需采用新的时间显示方法：

```python
q.to_datetime()                 # 自动选择证券市场时区
q.to_datetime("Asia/Shanghai")  # 所有市场统一显示北京时间
k.to_datetime("UTC")            # 历史 K 线时间显示为 UTC
```

旧 v0.3.7 不包含 `to_datetime()`，需升级后使用；本次不替换任何旧版本安装包。
