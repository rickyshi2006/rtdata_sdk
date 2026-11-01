"""History V2 能力协商、查询和自动回退状态示例。

普通安装已默认包含 History V2 支持：
    pip install rtdata-0.3.6-py3-none-any.whl
"""

import rtdata


TOKEN = "your_token"
API_URL = "https://api.vrrv.ccwu.cc"
SYMBOL = "000001.SZ"
PERIOD = "1d"
START = "2025-06-02"
END = "2025-06-17"
ADJUST = "none"


def main() -> None:
    with rtdata.API(
        token=TOKEN,
        api_url=API_URL,
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
            print(f"timestamp range: {rows[0].timestamp} ~ {rows[-1].timestamp}")


if __name__ == "__main__":
    main()
