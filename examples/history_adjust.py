"""A 股、港股和美股历史复权示例。

账号需要具备相应市场权限。
"""

from datetime import datetime

import rtdata


TOKEN = "your_token"
API_URL = "https://api.fengv2ray.tk"

CASES = (
    ("A 股", "000001.SZ", "2025-06-09", "2025-06-16"),
    ("港股", "00700.HK", "2025-06-09", "2025-06-12"),
    ("美股", "AAPL.US", "2020-08-26", "2020-09-02"),
)


def fmt_row(row) -> str:
    trade_time = datetime.fromtimestamp(row.timestamp / 1000).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return (
        f"{trade_time} O={row.open:.4f} H={row.high:.4f} "
        f"L={row.low:.4f} C={row.close:.4f}"
    )


def main() -> None:
    with rtdata.API(
        token=TOKEN,
        api_url=API_URL,
        history_cache_enabled=False,
    ) as api:
        for market, symbol, start, end in CASES:
            result = {
                adjust: api.get_kline(
                    symbol,
                    period="1d",
                    start=start,
                    end=end,
                    adjust=adjust,
                )
                for adjust in ("none", "forward", "backward")
            }

            print(f"\n{market} symbol={symbol} range={start}~{end}")
            for adjust, rows in result.items():
                sample = fmt_row(rows[0]) if rows else "no data"
                print(f"  {adjust:<8} rows={len(rows):>5} first={sample}")


if __name__ == "__main__":
    main()
