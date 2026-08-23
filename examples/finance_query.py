"""A 股、港股和美股财务数据示例。

用法:
    python examples/finance_query.py --token your_token

覆盖普通财务报表、单表查询、TTM、财务比率和 PIT。港股、美股财务字段与 A 股不同，
应按服务端返回的实际字段读取；源数据没有 announcement_date 时不支持 PIT。
"""

import argparse
from typing import Any, Dict

import rtdata
from rtdata import FinanceData, QueryError


SECURITIES = (
    ("A 股", "600519.SH"),
    ("港股", "00700.HK"),
    ("美股", "AAPL.US"),
)


def statement_counts(data: Dict[str, Any]) -> str:
    counts = []
    for name in ("income", "balance", "cashflow"):
        value = data.get(name)
        counts.append(f"{name}={len(value) if isinstance(value, list) else 0}")
    return ", ".join(counts)


def show(title: str, result: FinanceData) -> None:
    market = result.data.get("market", "")
    print(
        f"{title}: code={result.stock_code} period={result.report_period} "
        f"market={market} {statement_counts(result.data)}"
    )


def try_show(title: str, query) -> None:
    try:
        show(title, query())
    except QueryError as exc:
        print(f"{title} unavailable: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="查询 A/HK/US 财务数据")
    parser.add_argument("--token", required=True, help="客户端 token")
    parser.add_argument(
        "--api-url",
        default=rtdata.api.DEFAULT_API_URL,
        help="服务发现 API 地址",
    )
    parser.add_argument("--report-period", default="2025-12-31")
    parser.add_argument("--pit-date", default="2025-12-31")
    args = parser.parse_args()

    with rtdata.API(token=args.token, api_url=args.api_url) as api:
        for market, code in SECURITIES:
            print(f"\n== {market} {code} ==")

            # 默认 query_type=4：income + balance + cashflow。
            try_show(
                "all statements",
                lambda: api.get_finance(
                    code, report_period=args.report_period
                ),
            )
            try_show(
                "income only",
                lambda: api.get_finance(
                    code,
                    report_period=args.report_period,
                    query_type=1,
                ),
            )

            try:
                ttm = api.get_finance_ttm(code, as_of_date=args.report_period)
                print(f"TTM fields={len(ttm.data)}")
            except QueryError as exc:
                print(f"TTM unavailable: {exc}")
            try:
                ratios = api.get_finance_ratios(
                    code, report_period=args.report_period
                )
                print(f"ratios fields={len(ratios.data)}")
            except QueryError as exc:
                print(f"ratios unavailable: {exc}")

            # v0.3.2 起默认 query_type=4，与网关 PIT 协议一致。
            try_show(
                "PIT",
                lambda: api.get_finance_pit(code, trade_date=args.pit_date),
            )


if __name__ == "__main__":
    main()
