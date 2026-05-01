"""订阅实时行情示例

订阅 A 股实时快照行情。
交易时间: 9:30-11:30, 13:00-15:00 (北京时间)
非交易时间连接正常但不会收到行情推送。
"""
import time
import logging
from datetime import datetime, timezone
import rtdata
from rtdata import Quote, RtdataClient

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

SYMBOLS = [
    "601919.SH",   # 中远海控
    "EUR.JPY",   # 欧元/美元
    "rb2605.SHF",   # 螺纹钢期货
]

api = rtdata.API(token="python_04")
# 直接使用 RtdataClient 指定阿里云节点
# api = RtdataClient(
#     token="test_0000002",
#     host="151.145.72.82",
#     port=9100,
#     api_url=None  # 不使用服务发现
# )

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
