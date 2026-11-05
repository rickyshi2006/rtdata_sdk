"""观察各品种回调空档，不把行情时间差误判为网络延迟。

只读取 SDK 公共状态，每 5 秒汇总，不逐帧打印以免诊断本身拖慢回调。
event_age_ms 是本机 UTC 与行情标签之差，包含数据年龄和时钟差，非网络 RTT。
callback_gap_s 才是本进程观察到的回调空档；休市也会产生长空档。
"""

import logging
import threading
import time
from datetime import datetime, timezone

import rtdata

if __package__:
    from ._common import connection_options, format_quote, require_datetime
else:
    from _common import connection_options, format_quote, require_datetime


SYMBOLS = ["AAPL", "EUR.USD", "BTCUSDT.BINANCE", "BTCUSDT.OKX"]
MONITOR_INTERVAL = 5.0
OBSERVE_SECONDS = 300.0  # 0: 持续观察。


class Observations:
    def __init__(self):
        self._lock = threading.Lock()
        self._rows = {}

    def record(self, quote):
        now = time.monotonic()
        with self._lock:
            previous = self._rows.get(quote.symbol)
            count, gap = (0, 0.0) if previous is None else (
                previous[0], max(previous[2], now - previous[1])
            )
            self._rows[quote.symbol] = (count + 1, now, gap, quote)

    def snapshot(self):
        with self._lock:
            return dict(self._rows)


def report(api, observations, started):
    now = time.monotonic()
    utc_now = datetime.now(timezone.utc)
    print(
        f"[{utc_now.isoformat(timespec='milliseconds')}] "
        f"connected={api.is_connected} phase={api.realtime_status.phase} "
        f"statistics={dict(api.realtime_statistics)}"
    )
    print(
        f"confirmed={api.last_subscribe_confirmed} rejected={api.last_subscribe_rejected} "
        f"warning={api.last_subscribe_warning!r}"
    )
    rows = observations.snapshot()
    for symbol in SYMBOLS:
        item = rows.get(symbol)
        if item is None:
            print(f"  {symbol}: no callback, observed_s={now - started:.1f}")
            continue
        count, last_recv, max_gap, quote = item
        silence = now - last_recv
        event_age_ms = int(utc_now.timestamp() * 1000) - quote.timestamp
        print(
            f"  callbacks={count} silence_s={silence:.1f} "
            f"max_callback_gap_s={max(max_gap, silence):.1f} event_age_ms={event_age_ms} "
            f"{format_quote(quote)}"
        )


def main():
    require_datetime()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    api = rtdata.API(**connection_options())
    observations = Observations()
    api.on_quote(observations.record)
    try:
        api.subscribe(SYMBOLS)
        started = time.monotonic()
        while True:
            time.sleep(MONITOR_INTERVAL)
            report(api, observations, started)
            if OBSERVE_SECONDS > 0 and time.monotonic() - started >= OBSERVE_SECONDS:
                break
    except KeyboardInterrupt:
        pass
    finally:
        api.close()


if __name__ == "__main__":
    main()
