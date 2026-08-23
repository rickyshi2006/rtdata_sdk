"""History V2 能力协商、查询和自动回退状态示例。

运行前安装带 History V2 支持的 SDK：
    pip install "rtdata-0.3.2-py3-none-any.whl[history-v2]"
"""

import rtdata


TOKEN = "your_token"
API_URL = "https://api.fengv2ray.tk"
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
        history_v2_advertise=True,
        history_v2_default=True,
    ) as api:
        print(
            "history capability:",
            f"state={api.history_capability_state}",
            f"eligible={api.history_v2_eligible}",
            f"fallback={api.history_capability_fallback_reason!r}",
        )
        rows = api.get_kline(
            SYMBOL,
            period=PERIOD,
            start=START,
            end=END,
            adjust=ADJUST,
            timeout=60.0,
        )
        print(
            f"symbol={SYMBOL} period={PERIOD} adjust={ADJUST} "
            f"rows={len(rows)}"
        )
        if rows:
            print(f"timestamp range: {rows[0].timestamp} ~ {rows[-1].timestamp}")


if __name__ == "__main__":
    main()
