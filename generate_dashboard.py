"""
generate_dashboard.py
=====================
从 gas_prices.csv 读取最新数据，注入到 dashboard.html 的 DATA 对象中。

时效规则（与 scraper.py print_summary 完全一致）:
  GasWizard tomorrow  : price_date == 明天  且 scraped_at 在今天
  GasWizard today     : price_date == 今天  （历史实价，不限抓取时间）
  GasWizard yesterday : price_date == 昨天  （历史实价，不限抓取时间）
  Stockr today/tmr    : price_date 精确匹配 且 scraped_at 在今天 且 price 非空
  CityNews            : price_date >= 今天  且 scraped_at 在今天

用法:
  python generate_dashboard.py
  python generate_dashboard.py --csv gas_prices.csv --html dashboard.html
"""

import csv
import json
import re
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path


# ── 时效检查 ───────────────────────────────────────────────────────────────

def scraped_today(s: str) -> bool:
    if not s:
        return False
    try:
        return datetime.fromisoformat(s).date() == date.today()
    except ValueError:
        return False


def to_float(s) -> float | None:
    if s == "" or s is None:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


# ── CSV 读取 ───────────────────────────────────────────────────────────────

def load_csv(csv_path: Path) -> list[dict]:
    if not csv_path.exists():
        return []
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def get_row(rows: list[dict], source: str, price_date: str,
            require_fresh: bool = False) -> dict | None:
    """
    从 rows 中找 source+price_date 匹配的最新一条记录。
    require_fresh=True 时额外要求 scraped_at 在今天。
    """
    matched = [r for r in rows
               if r["source"] == source and r["price_date"] == price_date]
    if not matched:
        return None
    r = max(matched, key=lambda x: x["scraped_at"])
    if require_fresh and not scraped_today(r["scraped_at"]):
        return None
    return r


# ── 数据加载 ───────────────────────────────────────────────────────────────

def load_data(csv_path: Path) -> dict:
    rows = load_csv(csv_path)
    today_s     = date.today().isoformat()
    yesterday_s = (date.today() - timedelta(days=1)).isoformat()
    tomorrow_s  = (date.today() + timedelta(days=1)).isoformat()

    # ── GasWizard ─────────────────────────────────────────────────────────
    def gw(target, fresh=False):
        r = get_row(rows, "gaswizard", target, require_fresh=fresh)
        if not r:
            return None
        return {
            "date":        r["price_date"],
            "label":       r["label"],
            "regular":     to_float(r["regular"]),
            "regular_chg": to_float(r["regular_chg"]),
            "premium":     to_float(r["premium"]),
            "premium_chg": None,   # CSV 未单独存储 premium_chg
            "diesel":      to_float(r["diesel"]),
            "diesel_chg":  None,
            "day_name":    "",     # CSV 未存储 day_name，前端自动省略
        }

    # ── Stockr ────────────────────────────────────────────────────────────
    def sk(target):
        r = get_row(rows, "stockr", target, require_fresh=True)
        if r and to_float(r["regular"]) is not None:
            return to_float(r["regular"])
        return None

    # ── CityNews ──────────────────────────────────────────────────────────
    cn_rows = [r for r in rows
               if r["source"] == "citynews"
               and r["price_date"] >= today_s
               and r.get("label", "") != "history"]
    cn_row = None
    if cn_rows:
        best = max(cn_rows, key=lambda x: x["scraped_at"])
        if scraped_today(best["scraped_at"]):
            cn_row = best

    # CityNews history: label=='history', newest 3 days excluding today
    hist_rows = sorted(
        [r for r in rows
         if r["source"] == "citynews"
         and r.get("label") == "history"
         and r["price_date"] < today_s],
        key=lambda x: x["price_date"],
        reverse=True
    )[:3]

    return {
        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "citynews": {
            "date":            cn_row["price_date"]              if cn_row else None,
            "direction":       cn_row["direction"]               if cn_row else None,
            "direction_cents": to_float(cn_row["regular_chg"])   if cn_row else None,
            "summary":         cn_row["summary"]                 if cn_row else None,
            "predicted_price": to_float(cn_row["regular"])       if cn_row else None,
            "history": [{"date": r["price_date"], "price": to_float(r["regular"])}
                        for r in hist_rows if to_float(r["regular"]) is not None],
        },
        "stockr": {
            "today":         sk(today_s),
            "tomorrow":      sk(tomorrow_s),
            "today_date":    today_s,
            "tomorrow_date": tomorrow_s,
        },
        "gaswizard": {
            "tomorrow":  gw(tomorrow_s, fresh=True),
            "today":     gw(today_s,    fresh=False),
            "yesterday": gw(yesterday_s,fresh=False),
        },
    }


# ── HTML 注入 ──────────────────────────────────────────────────────────────

def inject_data(html_path: Path, data: dict) -> None:
    html = html_path.read_text(encoding="utf-8")
    json_str = json.dumps(data, ensure_ascii=False, indent=2)
    new_html = re.sub(
        r"(const DATA = )(\{.*?\})(;)",
        r"\g<1>" + json_str + r"\g<3>",
        html, count=1, flags=re.DOTALL,
    )
    if new_html == html:
        print("⚠ 未找到 DATA 注入点，请检查 dashboard.html")
        return
    html_path.write_text(new_html, encoding="utf-8")
    print(f"✓ dashboard.html 已更新 ({html_path.resolve()})")


# ── 摘要打印 ───────────────────────────────────────────────────────────────

def print_summary(data: dict) -> None:
    today = date.today().isoformat()
    cn, sk, gw = data["citynews"], data["stockr"], data["gaswizard"]
    print(f"\n{'─'*54}")
    print(f"  数据时效检查  ({today})")
    print(f"{'─'*54}")
    if cn["summary"]:
        print(f"  CityNews   ✓  {cn['summary']}")
    else:
        print(f"  CityNews   ✗  今日无有效预测 → 显示「暂无预测」")
    td_s = f"{sk['today']} ¢/L"    if sk["today"]    is not None else "✗ 暂无"
    tm_s = f"{sk['tomorrow']} ¢/L" if sk["tomorrow"] is not None else "✗ 暂无"
    print(f"  Stockr     today={td_s}  tomorrow={tm_s}")
    for lbl, note in [("tomorrow","今天抓取"), ("today","date匹配"), ("yesterday","date匹配")]:
        d = gw.get(lbl)
        if d:
            print(f"  GasWizard  {lbl:9s} ✓  {d['date']}  普通={d['regular']}")
        else:
            print(f"  GasWizard  {lbl:9s} ✗  暂无 ({note}) → 显示「暂无预测」")
    print(f"{'─'*54}\n")


# ── 主入口 ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",  default="gas_prices.csv")
    parser.add_argument("--html", default="dashboard.html")
    args = parser.parse_args()

    csv_path  = Path(args.csv)
    html_path = Path(args.html)

    if not csv_path.exists():
        print(f"✗ CSV 不存在: {csv_path}")
        return
    if not html_path.exists():
        print(f"✗ HTML 不存在: {html_path}")
        return

    data = load_data(csv_path)
    print_summary(data)
    inject_data(html_path, data)


if __name__ == "__main__":
    main()
