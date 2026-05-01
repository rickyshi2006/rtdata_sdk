"""rtdata SDK 基本用法

说明：实时订阅是最新值优先的快照流，回调应尽量轻量。
若需要做慢处理，请将 quote 投递到你自己的队列/线程池。
"""
import time
import rtdata
from rtdata import Quote

api = rtdata.API(token="your_token", api_url="https://api.fengv2ray.tk")


@api.on_quote
def on_quote(quote: Quote):
    print(f"  {quote.symbol}: last={quote.last:.4f}, vol={quote.volume}")


api.subscribe(["601919.SH", "000001.SZ"])

try:
    time.sleep(60)
except KeyboardInterrupt:
    pass

api.close()
