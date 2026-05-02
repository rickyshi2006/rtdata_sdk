"""下载历史 K 线示例

查询 A 股日线数据。
主推荐接口为按 start/end 时间范围查询：
- 只有日期：自动扩展为当天 00:00:00 ~ 23:59:59.999
- 带时间：按精确时间截取
"""
import logging
from datetime import datetime
import rtdata

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

api = rtdata.API(token="your_token", api_url="https://api.fengv2ray.tk")

print("提示：查询是可靠请求-响应语义；若连接中断，当前查询会快速失败，调用方应自行重试。")


# ── 示例 1: 指定日期范围（日线） ─────────────────────────────
print("=" * 70)
print("示例 1: 中远海控 2025-12-01 ~ 2025-12-22 日线")
print("=" * 70)

klines = api.get_kline("601919.SH", period="1d",
                       start="2025-12-01", end="2025-12-22")

print(f"{'日期':<12}  {'开盘':>10}  {'最高':>10}  {'最低':>10}  "
      f"{'收盘':>10}  {'成交量':>12}  {'成交额':>16}")
print("-" * 90)
for k in klines:
    dt = datetime.fromtimestamp(k.timestamp / 1000).strftime("%Y-%m-%d")
    print(f"{dt:<12}  {k.open:>10.2f}  {k.high:>10.2f}  {k.low:>10.2f}  "
          f"{k.close:>10.2f}  {k.volume:>12,}  {k.turnover:>16,.0f}")


# ── 示例 2: 指定时间范围 ──────────────────────────────────
print()
print("=" * 70)
print("示例 2: 平安银行 2015-01 ~ 2015-06 日线")
print("=" * 70)

klines = api.get_kline("000001.SZ", period="1d",
                       start="2015-01-01", end="2015-07-01")

print(f"共 {len(klines)} 根K线")
if klines:
    first = datetime.fromtimestamp(klines[0].timestamp / 1000).strftime("%Y-%m-%d")
    last = datetime.fromtimestamp(klines[-1].timestamp / 1000).strftime("%Y-%m-%d")
    print(f"时间范围: {first} ~ {last}")
    for k in klines[:5]:
        dt = datetime.fromtimestamp(k.timestamp / 1000).strftime("%Y-%m-%d")
        print(f"  {dt}  O={k.open:.2f}  H={k.high:.2f}  L={k.low:.2f}  "
              f"C={k.close:.2f}  V={k.volume:,}")
    if len(klines) > 10:
        print(f"  ... 省略 {len(klines) - 10} 根 ...")
    for k in klines[-5:]:
        dt = datetime.fromtimestamp(k.timestamp / 1000).strftime("%Y-%m-%d")
        print(f"  {dt}  O={k.open:.2f}  H={k.high:.2f}  L={k.low:.2f}  "
              f"C={k.close:.2f}  V={k.volume:,}")


# ── 示例 3: 单日分钟线（纯日期自动扩展为全天） ────────────────
print()
print("=" * 70)
print("示例 3: 平安银行 2015-06-17 当天分钟线")
print("=" * 70)

klines = api.get_kline("000001.SZ", period="1m", start="2015-06-17", end="2015-06-17")
print(f"当天返回 {len(klines)} 根分钟K线")
if klines:
    print("前3根:")
    for k in klines[:3]:
        dt = datetime.fromtimestamp(k.timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  {dt} O={k.open:.2f} H={k.high:.2f} L={k.low:.2f} C={k.close:.2f}")


# ── 示例 4: 批量下载 ─────────────────────────────────────
print()
print("=" * 70)
print("示例 4: 批量下载 2016 年日线")
print("=" * 70)
print("说明：历史查询默认会写入本地分段二进制缓存；同样区间再次请求时会优先走本地。")

stocks = ["601398.SH", "601939.SH", "600036.SH"]  # 工行、建行、招行
for code in stocks:
    try:
        klines = api.get_kline(code, period="1d",
                               start="2016-01-01", end="2017-01-01",
                               timeout=60.0)
        if klines:
            closes = [k.close for k in klines]
            print(f"  {code}: {len(klines)} 根, "
                  f"收盘 [{min(closes):.2f}, {max(closes):.2f}]")
        else:
            print(f"  {code}: 无数据")
    except Exception as e:
        print(f"  {code}: 失败 - {e}")


api.close()
print("\n完成")
