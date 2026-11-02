## v0.3.6

- 改进连接协商在网络延迟和重传情况下的稳定性。

- 统一扩展市场 schema 的内部命名，不改变字段、schema ID、哈希及通信协议。
- 移除已弃用的数据源专用构造参数及兼容处理；原调用如含此类参数，请直接删除，
  无需替换参数。未知构造参数现在会触发 `TypeError`。

- 实时行情默认自动获取扩展字段，现有连接和订阅代码无需增加参数。
- 默认覆盖当前服务端声明的全部市场，包括海外期货和加密货币；无需选择数据源。
- 每次登录自动识别可用格式；旧节点保持兼容，不向其发送不支持的市场编号。
- 新增字段通过 `quote.fields` 或同名属性读取，原有常用行情属性继续可用。
- 保留空值和数值精度，支持按品种提供的买卖档位信息。
- 改进历史行情时间处理、分页完整性和本地缓存。
- 保留旧协议兼容及显式协议选择。
- 历史数据默认协商并优先使用 V2，普通安装自动包含 Zstandard，无需额外参数。
- 首次历史查询等待正在进行的协商，旧服务端或协商超时仍可使用 V1。
- 改进断线、重连和关闭时的连接资源释放。

```sh
python -m pip install --upgrade ./rtdata-0.3.6-py3-none-any.whl
```

请使用随包 `SHA256SUMS` 校验下载文件。详见 `docs/REALTIME-FULL-FIELDS.md`。

如已安装同版本或版本号更高的包，请显式重装本次 wheel，然后重启 Python 或订阅进程：

```sh
python -m pip install --force-reinstall --no-cache-dir ./rtdata-0.3.6-py3-none-any.whl
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__)"
```
