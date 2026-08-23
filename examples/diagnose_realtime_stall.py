"""诊断实时订阅是否发生客户端本地停读/停回调。

输出包含:
- recv: 客户端本地收到并执行回调的时间
- quote: 行情包内时间戳
- lag_ms: recv 与 quote 的差值
- monitor: SDK 内部已收消息数、行情数、回调队列长度、最新缓存年龄
"""

import logging
import threading
import time
from datetime import datetime, timezone

import rtdata
from rtdata import Quote


TOKEN = "your_token"
API_URL = "https://api.fengv2ray.tk"
SYMBOLS = ["601919.SH"]
SYNC_CALLBACKS = False
CALLBACK_QUEUE_SIZE = 1000
MONITOR_INTERVAL = 1.0


def fmt_quote_ts(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%H:%M:%S.%f")[:-3]


def fmt_recv_ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    api = rtdata.API(
        token=TOKEN,
        api_url=API_URL,
        async_callbacks=not SYNC_CALLBACKS,
        callback_queue_size=CALLBACK_QUEUE_SIZE,
    )

    stop_event = threading.Event()

    @api.on_quote
    def on_quote(q: Quote):
        recv_ms = int(time.time() * 1000)
        lag_raw_ms = recv_ms - int(q.timestamp)
        lag_beijing_adj_ms = recv_ms - (int(q.timestamp) - 8 * 3600 * 1000)
        print(
            f"[recv={fmt_recv_ts()}] [quote={fmt_quote_ts(int(q.timestamp))}] "
            f"[lag_raw_ms={lag_raw_ms:>8d}] [lag_bj_adj_ms={lag_beijing_adj_ms:>6d}] {q.symbol:<10s} "
            f"last={q.last:.3f} bid={q.bid:.3f} ask={q.ask:.3f} vol={q.volume}",
            flush=True,
        )

    @api.on_connect
    def on_connect():
        print(f">> Connected recv={fmt_recv_ts()}", flush=True)

    @api.on_disconnect
    def on_disconnect(reason):
        print(f">> Disconnected recv={fmt_recv_ts()} reason={reason}", flush=True)

    def monitor_loop():
        client = api._client
        while not stop_event.wait(MONITOR_INTERVAL):
            conn = client._conn
            is_connected = bool(conn and conn.connected)
            callback_qsize = client._callback_queue.qsize() if client._async_callbacks else 0
            with client._stats_lock:
                messages_received = client._messages_received
                quotes_received = client._quotes_received
                quotes_dropped = client._quotes_dropped
            subscribed = client.get_subscribed_symbols()
            latest_desc = 'none'
            if SYMBOLS:
                latest = client.get_quote(SYMBOLS[0])
                if latest is not None:
                    age_ms = int(time.time() * 1000) - int(latest.timestamp)
                    latest_desc = f'{latest.symbol}@{fmt_quote_ts(int(latest.timestamp))}/raw_age={age_ms}ms/bj_adj_age={age_ms + 8 * 3600 * 1000}ms'
            print(
                f"[monitor recv={fmt_recv_ts()}] connected={is_connected} "
                f"subs={subscribed} msgs={messages_received} quotes={quotes_received} "
                f"dropped={quotes_dropped} cb_q={callback_qsize} latest={latest_desc}",
                flush=True,
            )

    t = threading.Thread(target=monitor_loop, name='diag-monitor', daemon=True)
    t.start()

    print(f'Subscribing: {SYMBOLS}', flush=True)
    api.connect()
    api.subscribe(SYMBOLS)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        api.close()
        print('Done', flush=True)


if __name__ == '__main__':
    main()
