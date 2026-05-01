import time
from datetime import datetime, timezone
import rtdata
from rtdata import Quote

api = rtdata.API(token="python_05")

@api.on_quote
def on_quote(q: Quote):
    now = datetime.now(tz=timezone.utc)
    data_time = datetime.fromtimestamp(q.timestamp / 1000, tz=timezone.utc)
    latency_ms = (now - data_time).total_seconds() * 1000
    
    now_str = now.strftime("%H:%M:%S.%f")[:-3]
    data_str = data_time.strftime("%H:%M:%S.%f")[:-3]
    
    print(f"[{now_str}] {q.symbol:<12s} data_time={data_str} latency={latency_ms:.0f}ms")

@api.on_connect
def on_connect():
    print(">> Connected")

print("Subscribing to 601919.SH, EUR.USD, rb2605.SHF")
api.subscribe(["601919.SH", "EUR.USD", "rb2605.SHF"])

try:
    time.sleep(20)
except KeyboardInterrupt:
    pass

api.close()
