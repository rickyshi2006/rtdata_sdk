# Changelog

## 0.3.2 - 2026-08-23

- 修正财务请求（包括 `API.get_finance_pit()` 和 `RtdataClient.get_finance_pit()`）的默认
  `query_type` 为 `4`（income + balance + cashflow），与 Cloud Gateway 协议一致；TTM
  和财务比率仍显式使用协议保留值 `0`。
- 补充 A 股、港股、美股财务报表、TTM、财务比率和 PIT 能力边界说明；PIT 仍仅对有
  `announcement_date` 的数据源可用。
- 更新 History V2、三市场复权、Token 状态和 session rehome 的使用文档与示例。

## 0.3.1 - 2026-08-15

- 使用服务发现且启用自动重连时，默认声明安全 session rehome 能力，使客户端在节点
  故障转移后自动返回账号首选节点。
- 保留 `session_rehome_advertise=False` 显式关闭方式；固定 `host:port` 或关闭自动重连
  的客户端仍不会声明迁移能力。

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
