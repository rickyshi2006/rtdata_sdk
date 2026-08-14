# Changelog

## 0.3.0 - 2026-08-14

- 新增基于能力协商的 History V2 流式传输，采用列式差分编码和 Zstandard
  压缩，同时保留自动回退 History V1 的兼容路径。
- 新增有界解码队列、字节窗口流控、请求取消、严格流校验和精确时间范围
  分页检查。
- 新增可选依赖组 `history-v2`，基础 SDK 仍保持零外部依赖。
- 新增显式启用的安全会话迁移，支持签名 handoff ticket、定向服务发现、
  重连安全的节点选择和订阅自动恢复。
- 阻止失效或已被替代的连接错误完成重连及在途查询状态迁移。

## 0.2.0 - 2026-07-20

- Add additive `TOKEN_STATUS (0x42)` protocol support.
- Add `TokenStatus`, `on_token_status`, `token_status`, and token expiry accessors.
- Read gateway version and enabled protocol features from discovery responses.
- Preserve compatibility with gateways that do not support token status events.
- Prevent dead or superseded sockets from reporting a successful reconnect.
- Stop automatic reconnect after an expired, disabled, or revoked token status.
- Allow an explicit reconnect after the token is restored.
