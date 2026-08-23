"""History V2 能力协商、查询和自动回退状态示例。

用法:
    pip install "rtdata-0.3.2-py3-none-any.whl[history-v2]"
    python examples/history_v2.py --token your_token
"""

import argparse

import rtdata


def main() -> None:
    parser = argparse.ArgumentParser(description="运行 History V2 历史查询")
    parser.add_argument("--token", required=True, help="客户端 token")
    parser.add_argument(
        "--api-url",
        default=rtdata.api.DEFAULT_API_URL,
        help="服务发现 API 地址",
    )
    parser.add_argument("--symbol", default="000001.SZ")
    parser.add_argument("--period", default="1d")
    parser.add_argument("--start", default="2025-06-02")
    parser.add_argument("--end", default="2025-06-17")
    parser.add_argument(
        "--adjust",
        choices=("none", "forward", "backward"),
        default="none",
    )
    args = parser.parse_args()

    with rtdata.API(
        token=args.token,
        api_url=args.api_url,
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
            args.symbol,
            period=args.period,
            start=args.start,
            end=args.end,
            adjust=args.adjust,
            timeout=60.0,
        )
        print(
            f"symbol={args.symbol} period={args.period} adjust={args.adjust} "
            f"rows={len(rows)}"
        )
        if rows:
            print(f"timestamp range: {rows[0].timestamp} ~ {rows[-1].timestamp}")


if __name__ == "__main__":
    main()
