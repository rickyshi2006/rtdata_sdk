"""A 股、港股和美股历史复权示例。

账号需要具备相应市场权限。
"""

import rtdata


if __package__:
    from ._common import connection_options, require_datetime
else:
    from _common import connection_options, require_datetime


CASES = (
    ("A 股", "000001.SZ", "2025-06-09", "2025-06-16"),
    ("港股", "00700.HK", "2025-06-09", "2025-06-12"),
    ("美股", "AAPL", "2020-08-26", "2020-09-02"),
)


def fmt_row(row) -> str:
    trade_time = row.to_datetime().isoformat(timespec="milliseconds")
    return (
        f"{trade_time} O={row.open:.4f} H={row.high:.4f} "
        f"L={row.low:.4f} C={row.close:.4f}"
    )


def main() -> None:
    require_datetime()
    with rtdata.API(
        **connection_options(),
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
                first = fmt_row(rows[0]) if rows else "no data"
                last = rows[-1].to_datetime().isoformat() if rows else "no data"
                print(f"  {adjust:<8} rows={len(rows):>5} first={first} last={last}")


if __name__ == "__main__":
    main()
