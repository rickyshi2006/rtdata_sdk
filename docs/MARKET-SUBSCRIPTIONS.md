# 按市场分类订阅

SDK 0.4.0 提供以下接口。需要实际连接的服务端支持，
不支持时明确报错，不在客户端把目录展开成全市场手工订阅请求。

**客户端不需要准备本地市场品种目录。** 分类成员由服务端维护，订阅时自动下发
所需的代码与身份映射；客户端缓存不决定分类成员，也不是使用该接口的前置条件。

```python
from rtdata import API

api = API(token="your_token", api_url="https://discovery.example")
try:
    print(api.get_markets())
    sub = api.subscribe_markets(["cn_equity", "hk_equity"])
    print(sub.wait_applied(timeout=30))
    print(sub.status())
    print(sub.list_issues(limit=100))
    api.unsubscribe_market("hk_equity").wait_applied(timeout=30)
finally:
    api.close()
```

## 分类与语义

支持 `cn_equity`、`hk_equity`、`futures`、`us_equity`、`fx`、
`overseas_futures`、`crypto`。这是既有行情类别，不是交易所全覆盖承诺，
也不是纯股票类型筛选器。指数、基金、权证等以目录分类为准。
期货分类也可包含目录中的组合合约；保留服务端返回的完整代码，包括内部空格和
`&`，不要自行拆分为腿合约。单品种订阅与分类订阅重叠时只计一个成员。

- `subscribe_market("us_equity")` 与传入单个分类的 `subscribe_markets()` 等价。
- 分类订阅与手工 `subscribe()` 取去重并集；取消分类不会取消仍被手工订阅的品种。
- `unsubscribe()` 不传参数时清除全部分类与手工意图。
- 目录更新会调整成员；额度不足时首次请求整体拒绝，不静默截断。
  后续扩容可能保留原有合法成员并报告 `complete=False`，需要检查状态。
- 成功的意图在重连后恢复；未收到最终回执的操作标记 `disconnected`，
  不把不确定结果宣称成功。迁移到不支持分类的服务端不会展开为手工全量订阅。
- 新协议下手工订阅也返回操作句柄；单次最多 512 个代码且受消息字节上限约束。
  老服务端的手工订阅接口行为保持不变。
- 请求总量最多 64 个、编码后总量最多 1 MiB，包含正在发送的请求和恢复预留。
  大量手工意图在重连时逐批恢复，不一次创建全部恢复任务；分类意图不展开。
- 新品种身份可随有效目录自动加入，无需断线；身份重映射仍需重新验证。
  清空操作目前仍串行执行，不抢占正在应用的操作。

## 状态和问题分页

`wait_applied(timeout=30)` 等待订阅应用完成，不等待首条行情；失败抛出异常。
等待超时不意味着请求已取消，避免在超时后盲目重复提交。
`sub.status()` 是本次操作最后回执；`api.market_subscription_status` 包含自动目录更新。

`list_issues(limit=100, offset=0, catalog_revision=None)` 返回一页目录问题。
下一页使用 `next_offset`，并传回同一 `catalog_revision`，直到 `done=True`。
分页扫描可能返回空页但仍未结束。目录改变时应从头查询。

`sub.list_issues(scope="upstream", limit=100)` 查询当前已应用订阅的逐品种接纳状态，
返回 `pending`、`accepted`、`unavailable` 或 `unknown`，以及拒绝的 `reason`。
这类页面包含全部状态，不只失败项。下一页传 `offset=next_offset`、
`upstream_revision=上一页的 upstream_revision`、
`subscription_generation=上一页的 generation`；版本改变时从头查询。
`api.list_market_issues()` 支持相同参数。

操作汇总仍使用 `upstream_state="unknown"`，不会将某一页或某一批的结果当成全市场
覆盖。`accepted` 不代表已收到行情、交易活跃或交易所完整覆盖；需结合实际行情和
`realtime_status`。当前不会自动重试未接纳品种。

协议协商超过 10 秒、已发送的操作或身份屏障超过 35 秒会断开并清理未确定操作；
已有连接的接收循环检查期限，不为每个操作创建线程。业务等待超时和协议期限不同。

## 分类供数状态

`api.market_feed_status` 与 `market_subscription_status` 分开：前者描述当前已提交
分类成员的供数采样，后者描述订阅操作是否安装成功。支持的服务端会自动推送变化：

```python
@api.on_market_feed_status
def supply_changed(status):
    for row in status['markets']:
        print(row['category'], row['state'], row['available_count'], row['admitted_count'])
```

分类 `state`：`normal` 正常、`partial` 部分可用、`paused` 暂停、`pending` 尚待确认、
`empty` 没有已接纳成员。从非正常恢复到正常时，该行 `event='recovered'`；其余为
`updated`。`primary_count`、`backup_count`、`unavailable_count`、`unknown_count`
分别计数主源、备源、当前无可用源和尚未确认的成员。

已订阅分类没有可用备源时保持订阅，报告暂停；源恢复并有有效数据后恢复供数。
源连接成功或订阅 `accepted` 本身不算正常；没有首条数据但仍有候选源时为待确认。
市场休市、品种安静不会单凭长时间无更新就将主源判为中断。

统计只覆盖当前连接已接纳的分类成员（含同分类的手工重叠成员），不是交易所
完整覆盖率，也不保证客户端已收到全部数据。热缓存淘汰不撤销已确认的主源身份。
采样分片完成、变化后才推送；`atomic=False`、`sample_duration_ms` 表示不是逐笔原子快照。
源状态变化会重新采样，正常扫描结束后至少间隔一秒再扫描，不是通知时延保证。

订阅代次改变后状态先变为 `pending`，不会沿用旧代统计；连接断开后查询状态为
`disconnected`，重连重新协商。旧服务端不支持该扩展时为 `unsupported`。
回调运行在接收线程中，不能阻塞或调用 `wait_applied()` 等等待接口；连接中断通知
仍使用已有连接回调。此扩展不改变 `auto`／`required_v2` 的错误通知语义。

## 大目录与回调

实时水位最多跟踪 250000 个身份；16 MiB 热缓存按最近更新淘汰，淘汰不停止回调。
250000 是身份容量上限，不是单连接可订阅数量承诺；实际接纳还受账户额度和
服务端可用资源约束。资源不足会明确报错，不截断为部分成功。
16 MiB 是编码数据计费，不是 Python 对象的实际进程内存上限。
`realtime_statistics` 增加 `cache_evictions`。缓存未命中时 `get_quote()` 可能没有结果。
异步回调队列仍有界且可能丢弃旧快照；本接口不提供逐笔无损保证。
V2 不会仅因同品种的新行情已入缓存就跳过有效的旧队列项；来源、连接及身份
代次仍在回调前检查。退订后重订相同代码和序号，也不会重新放行旧回调。
不要在同步行情回调或接收线程中的通知回调调用阻塞等待接口。

行情和控制通知共用连接的接收处理。接收端积压时，分类状态更新及断线识别也可能
延迟；读取到的 `normal` 是最近处理的采样，不是此刻连接健康的独立证明。
大量客户端放在同一 Python 进程中还会竞争解释器执行时间。大规模压测应区分
单进程多客户端与独立进程客户端，并记录实际接收速率和通知延迟。
