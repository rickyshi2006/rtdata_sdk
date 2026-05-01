# rtdata SDK 变更摘要（0.1.3）

本次版本主要覆盖 2026-04-26 当天对 SDK 的一组实质性改动。

## 主要变化

- 历史数据本地缓存彻底切换为分段二进制缓存，不再保留 sqlite 方案
- 历史查询支持本地命中后直接返回，重复同区间请求显著加快
- 增强自动重连后的恢复流程，补齐重鉴权与恢复订阅
- SDK 记录并暴露 discovery 返回的 `node_id`
- SDK 连接日志改为仅显示 `node_id`，不输出节点 IP
- 对外 API 新增当前连接节点查询属性：
  - `current_node_id`
  - `current_host`
  - `current_port`
  - `current_endpoint`

## 版本

- `0.1.2` -> `0.1.3`

## 打包产物

- `dist/rtdata-0.1.3-py3-none-any.whl`
- `dist/rtdata-0.1.3.tar.gz`
