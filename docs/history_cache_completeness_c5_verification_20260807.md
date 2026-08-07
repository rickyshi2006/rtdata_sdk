# 历史缓存完整性 C5 验证记录（2026-08-07）

## 结论

提交 `2cc75bd` 修复了范围查询固定请求 5,000 根后，把整个缺失时间区间错误
标记为已缓存的问题。修复同时覆盖启用和关闭本地缓存的公开 `get_kline()` 路径，
不改变 V1 协议和 SDK 公共 API。

范围查询现在按以下方式执行：

```text
请求最多 5000 根
  -> 校验返回时间戳位于请求范围且严格递增
  -> 以上一页最后时间戳 + 1ms 继续请求
  -> 直到服务端返回空页或到达范围末端
```

启用缓存时，每个成功页只登记从本页请求起点到最后一根时间戳之后 1ms 的已确认
前缀。后续页失败时，未获取区间仍保持 missing，不能被部分结果覆盖。

## 本地门禁

```text
python3 -m compileall -q rtdata tests
python3 -m unittest discover -s tests -v
```

结果：

```text
18/18 passed
total test time: 3.515s
```

新增 4 个回归用例覆盖：

- 关闭缓存时跨 5,000 行继续翻页，直到空页；
- 第二页失败时只保留已确认前缀，剩余区间仍为 missing；
- 空尾部不会被有数据的前缀错误覆盖；
- 非递增页在写缓存前明确失败。

## `.251` 隔离联合验证

联合组件：

```text
rtdata commit: 2cc75bd
rtdata archive SHA256:
d84406b9adf7363bfbb7d34204bf0ced9c31b6dbef5fd09394b3cb45b566f5e4
upcloud candidate: 9c31928
cloud gateway candidate: f9b8c2b
DDB: 127.0.0.1:8848
fixture: dfs://histperf_2608061115/kline
staging ports: 19480-19483
```

候选 SDK 通过独立 `PYTHONPATH` 加载，未替换 `.251` 已安装的正式 SDK。

### 关闭缓存的 5,001 行边界

```text
page 1: max_count=5000 rows=5000
page 2: max_count=5000 rows=1
page 3: max_count=5000 rows=0
returned_rows=5001
unique_timestamps=5001
strictly_increasing=true
api_complete_ms=69.232
```

该结果证明公开范围 API 不再在首个 5,000 行响应后静默结束。

### 启用缓存的重复查询

第一次查询：

```text
page rows=5000,1,0
returned_rows=5001
api_complete_ms=418.214
```

第二次相同查询：

```text
page rows=0
returned_rows=5001
same_timestamps=true
api_complete_ms=33.915
```

当前缓存不把没有分段文件的空尾部持久化为 coverage，因此第二次查询仍做一次空尾
探测。这是刻意保留的正确性优先边界：不会重取前 5,001 根，也不会把尚未证明的
区间标记为完整。空区间的可持久化表示可作为后续独立优化，不能与本修复混在一起。

## DDB / IPC 资源

| 时点 | DDB PID | fd | deleted mmap | deleted fd |
|---|---:|---:|---:|---:|
| baseline | 3607771 | 60 | 2 | 0 |
| staging active | 3607771 | 65 | 2 | 0 |
| staging stopped | 3607771 | 60 | 2 | 0 |

最终 upcloud 统计保持：

```text
connected=yes
reconnects=0
client_reconnects=0
server_recreate_attempts=0
```

DDB PID 未变化，staging 结束后 `19480-19483` 端口均已释放。

证据位于 `.251`：

```text
/home/Project/history-stream-test/results/histperf_2608061115/sdk-cache-2cc75bd/
```

## 测试期间暴露的既有边界

1. `auto_reconnect=False` 时，现有 `_connection.py` 接收循环不会运行，导致首次鉴权
   等待超时；联合验收使用 SDK 默认值 `auto_reconnect=True`。该问题与历史分页改动
   无关，但在依赖关闭自动重连前必须单独修复。
2. upcloud 主循环使用不可中断的 30 秒 sleep，本次 `SIGTERM` 后恰好略超 staging
   停止脚本的 30 秒等待上限。进程随后完成正常清理，第二次停止调用仅清理 cloud；
   没有使用强杀，也没有遗留端口或 DDB 资源。该停止延迟属于既有运行边界。

## 阶段结论

- 阶段 C 的 SDK 缓存完整性项已完成；
- 失败的部分结果不会再覆盖未获取区间；
- V1 请求和响应格式、旧公开 API、实时连接逻辑均未改变；
- 正式服务、正式配置及三个主工作目录均未修改。
