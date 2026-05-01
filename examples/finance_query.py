"""获取财务数据示例

查询 A 股财务报表:
  get_finance()        利润表 / 资产负债表 / 现金流量表
  get_finance_ttm()    TTM（滚动12个月）
  get_finance_pit()    Point-in-Time（时点）
  get_finance_ratios() 财务比率
"""
import logging
import rtdata
from rtdata import FinanceData

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

api = rtdata.API(token="your_token", api_url="https://api.fengv2ray.tk")


def show(title: str, fd: FinanceData, n: int = 15):
    """格式化打印财务数据"""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"  股票: {fd.stock_code}  报告期: {fd.report_period}")
    print(f"{'=' * 60}")

    skip = {"stock_code", "security_name", "report_period", "report_type",
            "statement_type", "ann_date", "announcement_date",
            "actual_announcement_date", "data_source", "update_time"}

    # 处理三表格式：income/balance/cashflow 是数组
    has_statements = False
    for section, label in [("income", "利润表"), ("balance", "资产负债表"),
                           ("cashflow", "现金流量表")]:
        if section in fd.data and isinstance(fd.data[section], list) and fd.data[section]:
            has_statements = True
            print(f"  {label}:")
            row = fd.data[section][0]
            items = [(k, v) for k, v in sorted(row.items())
                     if k not in skip and v is not None]
            limit = n if section == "income" else 5
            for k, v in items[:limit]:
                val = f"{v:>20,.2f}" if isinstance(v, (int, float)) else f"{v!s:>20}"
                print(f"    {k:<35} {val}")
            if len(items) > limit:
                print(f"    ... 共 {len(items)} 个字段")
            print()

    # 处理扁平格式（TTM / Ratios 等）
    if not has_statements:
        items = [(k, v) for k, v in sorted(fd.data.items())
                 if k not in skip and v is not None]
        if items:
            for k, v in items[:n]:
                val = f"{v:>20,.4f}" if isinstance(v, float) else f"{v!s:>20}"
                print(f"    {k:<35} {val}")
            if len(items) > n:
                print(f"    ... 共 {len(items)} 个字段")
        else:
            print("    (无数据)")


# ── 示例 1: 利润表 ────────────────────────────────────────
print("\n>> 示例 1: 贵州茅台 利润表")
try:
    show("利润表", api.get_finance("600519.SH", report_period="2024-12-31"))
except Exception as e:
    print(f"  失败: {e}")

# ── 示例 2: TTM 数据 ──────────────────────────────────────
print("\n>> 示例 2: 平安银行 TTM")
try:
    show("TTM", api.get_finance_ttm("000001.SZ", as_of_date="2024-12-31"))
except Exception as e:
    print(f"  失败: {e}")

# ── 示例 3: Point-in-Time 数据 ────────────────────────────
print("\n>> 示例 3: 中国平安 PIT")
try:
    show("PIT", api.get_finance_pit("601318.SH", trade_date="2025-05-01", query_type=4))
except Exception as e:
    print(f"  失败: {e}")

# ── 示例 4: 财务比率 ──────────────────────────────────────
print("\n>> 示例 4: 贵州茅台 财务比率")
try:
    show("财务比率", api.get_finance_ratios("600519.SH",
                                         report_period="2024-12-31"))
except Exception as e:
    print(f"  失败: {e}")

# ── 示例 5: 批量查询 ──────────────────────────────────────
print("\n>> 示例 5: 批量查询")
print("=" * 60)
for code in ["601398.SH", "601939.SH", "600036.SH", "600519.SH"]:
    try:
        fd = api.get_finance(code, report_period="2024-12-31")
        d = fd.data
        # 从 income 数组中获取数据
        if 'income' in d and len(d['income']) > 0:
            income = d['income'][0]
            rev = income.get("total_operating_revenue", "N/A")
            np_ = income.get("net_profit", "N/A")
        else:
            rev = d.get("total_revenue", d.get("revenue", "N/A"))
            np_ = d.get("net_profit", d.get("net_income", "N/A"))
        rev_s = f"{rev:,.0f}" if isinstance(rev, (int, float)) else str(rev)
        np_s = f"{np_:,.0f}" if isinstance(np_, (int, float)) else str(np_)
        print(f"  {code}  营收: {rev_s:>20}  净利润: {np_s:>20}")
    except Exception as e:
        print(f"  {code}  失败: {e}")

api.close()
print("\n完成")
