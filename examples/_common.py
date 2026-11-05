"""示例共用辅助函数；导入时不建立连接，也不读取凭据。"""

import os
import time

import rtdata


def connection_options():
    token = os.environ.get("RTDATA_TOKEN", "").strip()
    if not token:
        raise SystemExit("Set RTDATA_TOKEN before running this example.")
    return {
        "token": token,
        "api_url": os.environ.get("RTDATA_API_URL", "https://api.vrrv.ccwu.cc"),
    }


def require_datetime():
    if not hasattr(rtdata.Quote, "to_datetime") or not hasattr(rtdata.Kline, "to_datetime"):
        raise SystemExit(
            "This example needs current main source (to_datetime is not in the "
            "published v0.3.7 wheel). See examples/README.md."
        )


def fmt_price(value):
    return "N/A" if value is None else f"{value:.6f}"


def fmt_value(value):
    return "N/A" if value is None else str(value)


def format_quote(q):
    dt = q.to_datetime().isoformat(timespec="milliseconds")
    bids, asks = q.bid_prices, q.ask_prices
    bid_sizes, ask_sizes = q.bid_volumes, q.ask_volumes
    bid = bids[0] if bids else q.bid
    ask = asks[0] if asks else q.ask
    bid_size = bid_sizes[0] if bid_sizes else None
    ask_size = ask_sizes[0] if ask_sizes else None
    return (
        f"[{dt}] {q.symbol} V{q.wire_version} last={fmt_price(q.last)} "
        f"volume_raw={fmt_value(q.volume)} "
        f"bid1={fmt_price(bid)}/{fmt_value(bid_size)} "
        f"ask1={fmt_price(ask)}/{fmt_value(ask_size)} "
        f"book_volume_precision={fmt_value(q.fields.get('book_volume_precision'))}"
    )


def wait(seconds):
    if seconds > 0:
        time.sleep(seconds)
    else:
        while True:
            time.sleep(1)
