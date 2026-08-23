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
