"""诊断实时订阅是否发生客户端本地停读/停回调。

用法示例:
    python examples/diagnose_realtime_stall.py --token test-02 --symbols 601919.SH
    python examples/diagnose_realtime_stall.py --token test-02 --symbols 601919.SH rb2605.SHF

输出包含:
- recv: 客户端本地收到并执行回调的时间
- quote: 行情包内时间戳
- lag_ms: recv 与 quote 的差值
- monitor: SDK 内部已收消息数、行情数、回调队列长度、最新缓存年龄
"""

import argparse
import logging
import threading
import time
from datetime import datetime, timezone

import rtdata
from rtdata import Quote


def fmt_quote_ts(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%H:%M:%S.%f")[:-3]


def fmt_recv_ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', default='test-03')
    parser.add_argument('--symbols', nargs='+', default=['601919.SH'])
    parser.add_argument('--api-url', default=rtdata.api.DEFAULT_API_URL)
    parser.add_argument('--sync-callbacks', action='store_true', help='禁用异步回调队列，直接在收包线程执行回调')
    parser.add_argument('--callback-queue-size', type=int, default=1000)
    parser.add_argument('--monitor-interval', type=float, default=1.0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    api = rtdata.API(
        token=args.token,
        api_url=args.api_url,
        async_callbacks=not args.sync_callbacks,
        callback_queue_size=args.callback_queue_size,
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
        while not stop_event.wait(args.monitor_interval):
            conn = client._conn
            is_connected = bool(conn and conn.connected)
            callback_qsize = client._callback_queue.qsize() if client._async_callbacks else 0
            with client._stats_lock:
                messages_received = client._messages_received
                quotes_received = client._quotes_received
                quotes_dropped = client._quotes_dropped
            subscribed = client.get_subscribed_symbols()
            latest_desc = 'none'
            if args.symbols:
                latest = client.get_quote(args.symbols[0])
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

    print(f'Subscribing: {args.symbols}', flush=True)
    api.connect()
    api.subscribe(args.symbols)

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
