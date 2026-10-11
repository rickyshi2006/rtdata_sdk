## v0.3.5

- 实时行情默认自动获取扩展字段，现有连接和订阅代码无需增加参数。
- 新增字段通过 `quote.fields` 或同名属性读取，原有常用行情属性继续可用。
- 保留空值和数值精度，支持按品种提供的买卖档位信息。
- 改进历史行情时间处理、分页完整性和本地缓存。
- 保留旧协议兼容及显式协议选择。

```sh
python -m pip install --upgrade ./rtdata-0.3.5-py3-none-any.whl
```

请使用随包 `SHA256SUMS` 校验下载文件。详见 `docs/REALTIME-FULL-FIELDS.md`。
