"""A 股、港股和美股历史复权示例。

用法:
    python examples/history_adjust.py --token your_token

需要网关配置相应市场的 K 线表和复权因子表，token 也必须拥有市场权限。
"""

import argparse
from datetime import datetime

import rtdata


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
    parser = argparse.ArgumentParser(description="查询 A/HK/US 历史复权 K 线")
    parser.add_argument("--token", required=True, help="客户端 token")
    parser.add_argument(
        "--api-url",
        default=rtdata.api.DEFAULT_API_URL,
        help="服务发现 API 地址",
    )
    args = parser.parse_args()

    with rtdata.API(
        token=args.token,
        api_url=args.api_url,
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
