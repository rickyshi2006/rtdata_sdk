# rtdata SDK 交付说明

## 当前包信息

- 包名：`rtdata`
- 当前版本：`0.1.3`

本次没有提升版本号，但已重新打包，产物仍为 `0.1.3`。

## 当前交付产物

目录：

- `/home/Project/rtdata_sdk/dist/`

文件：

- `rtdata-0.1.3-py3-none-any.whl`
- `rtdata-0.1.3.tar.gz`

优先建议对外提供 `.whl`。

## 本次应向客户说明的关键点

### 1. 推荐连接方式

优先使用 HTTPS 服务发现：

```python
import rtdata

api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
)
```

不建议直接把云网关 TCP IP 暴露给客户。

### 2. 实时通道语义

实时行情是“最新值优先”的快照流，不是逐条事件流。

应明确告知客户：

- 中间某些快速变化可能被覆盖
- 如果本地消费太慢，服务端可能丢弃旧快照
- 极端情况下，服务端可能主动断开，SDK 会自动重连并恢复订阅
- 如果只关心当前最新值，应优先使用 `get_quote()`

### 3. 历史查询推荐写法

主推 `start/end` 时间范围语义：

```python
rows = api.get_history(
    "000001.SZ",
    period="1d",
    start="2015-01-01",
    end="2015-12-31",
)
```

兼容接口 `get_history_by_count()` 仍保留，但不建议新代码继续使用。

### 4. 本地历史缓存

当前 SDK 的历史缓存不是 sqlite，而是本地分段二进制缓存。

行为：

- 首次查询会回源服务器
- 再次查询相同区间时优先命中本地
- 缺口区间才回源

默认目录：

- `~/.rtdata/history_v1/`

关闭缓存：

```python
api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
    history_cache_enabled=False,
)
```

### 5. 查询失败语义

当前 SDK 会区分以下场景：

- `QueryError`
  服务端显式拒绝，例如：
  - `Market not allowed`
  - `History download busy, retry later`
  - `Upstream query channel unavailable`

- `QueryTimeoutError`
  等待响应超时

- `DisconnectedError`
  查询过程中连接断开

### 6. 订阅部分成功提示

如果某些 symbol 因权限、订阅数或服务端过滤被拒绝，SDK 会保留详细状态：

```python
print(api.last_subscribe_warning)
print(api.last_subscribe_requested)
print(api.last_subscribe_confirmed)
print(api.last_subscribe_rejected)
```

## 安装方式

### 安装 wheel

```bash
pip install rtdata-0.1.3-py3-none-any.whl
```

### 安装源码包

```bash
pip install rtdata-0.1.3.tar.gz
```

### 升级安装

```bash
pip install --upgrade rtdata-0.1.3-py3-none-any.whl
```

## 建议同时交付给客户的内容

建议一起提供：

1. wheel 文件
2. 一份最小示例代码
3. discovery 域名
4. token
5. 一份使用说明，至少覆盖：
   - 实时快照流语义
   - 历史缓存行为
   - 失败异常语义
   - 订阅部分成功的检查方式

## 最小示例

```python
import time
import rtdata

api = rtdata.API(
    token="your_token",
    api_url="https://api.fengv2ray.tk",
)

@api.on_quote
def on_quote(q):
    print(q.symbol, q.last)

api.subscribe(["rb2610.SHF"])

rows = api.get_history(
    "rb2610.SHF",
    period="1m",
    start="2026-04-29",
    end="2026-04-30",
)
print("history rows:", len(rows))

time.sleep(10)
api.close()
```

## 参考文档

- `README.md`
- `docs/SDK_USAGE.md`
- `examples/`
