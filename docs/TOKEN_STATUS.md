# Token 状态通知

从 SDK `0.2.0` 开始，服务端可以通过独立的
`TOKEN_STATUS (0x42)` 控制消息推送 Token 状态。现有
`AUTH_RESPONSE` 格式保持不变。

```python
import rtdata

api = rtdata.API(token="your_token")

@api.on_token_status
def handle_token_status(status):
    print("status:", status.status)
    print("severity:", status.severity)
    print("expires_at:", status.expires_at)

try:
    api.connect()  # 先注册回调，再建立连接。
    print(api.token_status)  # 此时通知也可能尚未到达。
finally:
    api.close()
```

也可以直接读取最后一次状态：

```python
print(api.token_status)
print(api.token_expires_at)
```

状态值包括：

- `valid`
- `expiring`
- `expired`
- `disabled`
- `revoked`

未启用该通知的服务端不会发送此消息。SDK 在这种情况下仍可正常连接，
此时 `api.token_status` 为 `None`。

`expires_at` 是带 UTC 时区的 datetime；`expires_ms <= 0` 表示不过期，
此时 `expires_at/remaining_ms` 为 `None`。`remaining_ms` 使用通知里的服务端时间计算，
不是会随本地时钟不断递减的倒计时。未收到通知与永不过期是不同状态。

上面的片段仅演示注册与关闭，持续监听见
[`examples/token_status.py`](../examples/token_status.py)。事件回调应快速返回，
不要在回调里发起同步查询。
