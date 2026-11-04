## v0.3.7

- 修复自动重连时部分品种订阅可能丢失的问题。
- 品种映射暂缺时保留订阅意图，映射恢复后自动补订，不阻塞行情接收。
- 修复并发订阅确认错配和迟到确认恢复已取消订阅的问题。
- 断线期间可以取消订阅；明确区分等待品种映射与服务端拒绝。
- 保持现有订阅用法和行情字段不变，无需新增配置参数。

下载 wheel 并使用随包 `SHA256SUMS` 校验后安装：

```sh
python -m pip install --upgrade ./rtdata-0.3.7-py3-none-any.whl
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__)"
```

升级后请重启 Python 或订阅进程，并照常订阅所需品种。
