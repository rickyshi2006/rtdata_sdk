"""History V2 能力协商、查询和自动回退状态示例。

普通安装默认包含 History V2，无需额外协议开关。
时间显示使用当前 main 的 to_datetime()，安装见 examples/README.md。
"""

import rtdata


if __package__:
    from ._common import connection_options, require_datetime
else:
    from _common import connection_options, require_datetime

SYMBOL = "000001.SZ"
PERIOD = "1d"
START = "2025-06-02"
END = "2025-06-17"
ADJUST = "none"


def main() -> None:
    require_datetime()
    with rtdata.API(
        **connection_options(),
        history_cache_enabled=False,
    ) as api:
        rows = api.get_kline(
            SYMBOL,
            period=PERIOD,
            start=START,
            end=END,
            adjust=ADJUST,
            timeout=60.0,
        )
        print(
            "history capability:",
            f"state={api.history_capability_state}",
            f"eligible={api.history_v2_eligible}",
            f"fallback={api.history_capability_fallback_reason!r}",
        )
        print(
            f"symbol={SYMBOL} period={PERIOD} adjust={ADJUST} "
            f"rows={len(rows)}"
        )
        if rows:
            print("actual range:", rows[0].to_datetime().isoformat(), rows[-1].to_datetime().isoformat())


if __name__ == "__main__":
    main()
