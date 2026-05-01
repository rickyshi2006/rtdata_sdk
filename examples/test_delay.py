import time
import logging
from datetime import datetime, timezone
import rtdata
from rtdata import Quote

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

SYMBOLS = ["601919.SH", "EUR.USD", "rb2605.SHF"]
api = rtdata.API(token="test_0000001", async_callbacks=True)

@api.on_quote
def on_quote(q: Quote):
    now = datetime.now(tz=timezone.utc)
    dt = datetime.fromtimestamp(q.timestamp / 1000, tz=timezone.utc)
    delay = (now - dt).total_seconds()
    ts = dt.strftime("%H:%M:%S.%f")[:-3]
    now_ts = now.strftime("%H:%M:%S.%f")[:-3]
    print(f"[NOW:{now_ts}] [DATA:{ts}] DELAY:{delay:.1f}s {q.symbol:<12s} last={q.last:.3f}")
    # 模拟慢消费。SDK 已异步分发回调，服务端实时流优先保最新值。
    time.sleep(0.2)

@api.on_connect
def on_connect():
    print(">> Connected")

print(f"Subscribing: {SYMBOLS}")
api.subscribe(SYMBOLS)

try:
    time.sleep(10)
except KeyboardInterrupt:
    pass

api.close()
