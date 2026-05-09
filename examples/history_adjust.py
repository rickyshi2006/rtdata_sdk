"""历史复权示例

对同一只 A 股股票分别查询不复权、前复权、后复权日线，
便于快速核对复权参数是否生效。
"""
from datetime import datetime

import rtdata


TOKEN = "your_token"
API_URL = "https://api.fengv2ray.tk"
SYMBOL = "000001.SZ"
START = "2015-01-01"
END = "2015-03-31"


def fmt_row(row):
    trade_date = datetime.fromtimestamp(row.timestamp / 1000).strftime("%Y-%m-%d")
    return (
        f"{trade_date} "
        f"O={row.open:.4f} H={row.high:.4f} L={row.low:.4f} C={row.close:.4f}"
    )


with rtdata.API(token=TOKEN, api_url=API_URL, history_cache_enabled=False) as api:
    rows_none = api.get_kline(SYMBOL, period="1d", start=START, end=END, adjust="none")
    rows_forward = api.get_kline(SYMBOL, period="1d", start=START, end=END, adjust="forward")
    rows_backward = api.get_kline(SYMBOL, period="1d", start=START, end=END, adjust="backward")

    print(f"symbol={SYMBOL} range={START}~{END}")
    print(f"none rows={len(rows_none)}")
    print(f"forward rows={len(rows_forward)}")
    print(f"backward rows={len(rows_backward)}")

    if rows_none and rows_forward and rows_backward:
        sample_indexes = sorted({0, min(5, len(rows_none) - 1), len(rows_none) - 1})
        print("\n对比样例:")
        for idx in sample_indexes:
            print("-" * 72)
            print("none    ", fmt_row(rows_none[idx]))
            print("forward ", fmt_row(rows_forward[idx]))
            print("backward", fmt_row(rows_backward[idx]))
