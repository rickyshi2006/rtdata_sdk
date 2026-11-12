# rtdata SDK 交付核对

## 源码与安装包分开核对

- 当前包版本号为 `0.3.9`，发布产物以 GitHub Release/tag 为准。
- 本版本修复首次订阅协商顺序和严格全字段模式冷启动误报，保留时间显示和配套示例。
- 后续 main 仍可能领先正式版本，交付必须匹配包版本、标签和示例。
- 用户安装说明以 [README](README.md) 为入口，参数与行为以
  [使用手册](docs/SDK_USAGE.md) 为准，避免在交付文档复制另一套过期接口说明。
- v0.3.9 使用新标签和新附件，不替换 v0.3.8 或更旧的 Release 产物。

## 安装与校验

正式交付使用审核过的不可变 wheel/sdist 及 `SHA256SUMS`：

```bash
python -m pip install --upgrade "./rtdata-0.3.9-py3-none-any.whl[market-time]"
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__)"
```

如需源码开发，在对应版本仓库目录安装，不要将其冒充旧 Release：

```bash
python -m pip install -e ".[market-time]"
python -c "import rtdata; print(rtdata.__version__, rtdata.__file__); print(hasattr(rtdata.Quote, 'to_datetime'))"
```

普通安装包含 Zstandard；Windows/精简系统的 IANA 时区数据可通过
`market-time` extra 安装。升级后重启 Python 或订阅进程。

## 随包材料

1. 已校验的 wheel、源码包和校验清单。
2. 与该包对应 tag 的文档和示例，不能混用更新 main 的示例。
3. 服务发现地址；token 通过独立渠道交付，不写入仓库、示例或共享日志。
4. [示例索引](examples/README.md)、[时间说明](docs/TIME-AND-MARKETS.md)、
   [市场品种与代码](docs/MARKET-SYMBOLS.md)、
   [实时字段](docs/REALTIME-FULL-FIELDS.md) 和 [Token 状态](docs/TOKEN_STATUS.md)。

源码包通过 `MANIFEST.in` 包含文档、示例和离线测试；wheel 是 SDK 安装产物，
不是完整示例仓库。构建输出 `dist/` 只能作为候选产物，不等于已发布。

## 功能验收清单

- 默认实时自动协商全字段，旧端兼容；缺失值是 `None`，不要当成零。
- 多市场代码使用服务目录；海外期货、加密货币不需要单独的客户端源选择参数。
- 时间显示按市场选择时区或显式统一时区，保留原 UTC 毫秒，不手工偏移。
- 行情是最新值优先的快照，不保证无损逐笔；重连恢复订阅，确认结果异步更新。
- 历史按实际返回区间验收，不假定一次调用完整覆盖全部请求范围。
- 缓存默认 `~/.rtdata/history_utc_v1/`；查询更正数据时可关闭缓存。
- 区分认证、发现、连接、查询拒绝、超时、断线和实时不可用异常。
- 财务字段按市场处理；PIT 支持范围见使用手册。
- 使用 `to_datetime()` 的客户端应确认安装 v0.3.8 或更高版本。

## 发布流程

遵循 [DEVELOPMENT](DEVELOPMENT.md)：先分支审查/测试，原目录本地 main 合并验证，
再同步相同 main。包发布另行批准，采用与版本一致的 tag，发布前更新
`CHANGELOG.md` 与 `docs/RELEASE-NOTES.md`，由 release workflow 构建校验。
不要覆盖旧 tag/附件来补充当前文档能力。
