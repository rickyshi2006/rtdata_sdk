"""订阅实时行情示例

订阅 A 股实时快照行情。
交易时间: 9:30-11:30, 13:00-15:00 (北京时间)
非交易时间连接正常但不会收到行情推送。

注意：实时流是最新值优先，不保证逐条完整回放；
若回调过慢，云网关可能主动断开，SDK 会自动重连。
"""
import time
import logging
from datetime import datetime, timezone
import rtdata
from rtdata import Quote

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

SYMBOLS = [
    "601919.SH",   # 中远海控
    # "EUR.JPY",   # 欧元/美元
    "rb2605.SHF",   # 螺纹钢期货
]

# 推荐使用 API + 服务发现，不要在示例里暴露具体网关地址
api = rtdata.API(token="your_token", api_url="https://api.fengv2ray.tk")

@api.on_quote
def on_quote(q: Quote):
    dt = datetime.fromtimestamp(q.timestamp / 1000, tz=timezone.utc)
    ts = dt.strftime("%H:%M:%S.%f")[:-3]
    print(f"  [{ts}] {q.symbol:<12s}  "
          f"last={q.last:.3f}  bid={q.bid:.3f}  "
          f"ask={q.ask:.3f}  vol={q.volume}")


@api.on_connect
def on_connect():
    print(">> Connected")


@api.on_disconnect
def on_disconnect(reason):
    print(f">> Disconnected: {reason}")


print(f"Subscribing: {SYMBOLS}")
api.connect()  # 先连接
api.subscribe(SYMBOLS)

# Ctrl+C to exit
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass

api.close()
print("Done")
