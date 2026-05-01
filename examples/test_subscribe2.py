import time
import logging
from datetime import datetime, timezone
import rtdata
from rtdata import Quote

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

SYMBOLS = [
    "601919.SH",
    "EUR.USD",
    "rb2605.SHF",
]

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
api.subscribe(SYMBOLS)

try:
    time.sleep(15)
except KeyboardInterrupt:
    pass

api.close()
print("Done")
