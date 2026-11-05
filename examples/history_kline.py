"""历史日线与分钟线：使用证券市场时间，输出实际返回区间。

时间范围不保证全部覆盖；可用周期和数据范围以服务返回为准。
"""

import logging

import rtdata

if __package__:
    from ._common import connection_options, require_datetime
else:
    from _common import connection_options, require_datetime


SYMBOL = "000001.SZ"
CASES = (
    ("1d", "2026-10-01", "2026-10-30"),
    ("1m", "2026-11-02 09:30:00", "2026-11-02 10:00:00"),
)
ADJUST = "none"
TIMEOUT = 60.0


def show_rows(rows):
    print(f"rows={len(rows)}")
    if not rows:
        return
    print("actual range:", rows[0].to_datetime().isoformat(), rows[-1].to_datetime().isoformat())
    for k in rows[:3]:
        print(
            f"  {k.to_datetime().isoformat()} "
            f"O={k.open:.4f} H={k.high:.4f} L={k.low:.4f} C={k.close:.4f} "
            f"V={k.volume} amount={k.turnover} open_interest={k.open_interest}"
        )


def main():
    require_datetime()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    with rtdata.API(**connection_options()) as api:
        for period, start, end in CASES:
            print(f"{SYMBOL} {period} requested={start} ~ {end} adjust={ADJUST}")
            try:
                rows = api.get_kline(
                    SYMBOL, period=period, start=start, end=end, adjust=ADJUST, timeout=TIMEOUT,
                )
            except rtdata.QueryTimeoutError as exc:
                print(f"Query timed out: {exc}")
            except rtdata.DisconnectedError as exc:
                print(f"Query interrupted: {exc}")
                break
            except rtdata.QueryError as exc:
                print(f"Query failed: {exc}")
            else:
                show_rows(rows)


if __name__ == "__main__":
    main()
