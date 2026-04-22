"""
query.py  —  数据库查询工具
=============================
快速查看已收集的油价历史数据。

用法:
  python query.py              # 显示所有摘要
  python query.py --history    # 显示历史记录
  python query.py --export     # 导出 CSV
"""

import sqlite3
import csv
import argparse
from pathlib import Path
from datetime import date

DB_PATH = Path("gas_prices.db")


def show_latest(conn):
    print("\n" + "═" * 65)
    print(f"  多伦多油价数据库  —  查询日期: {date.today()}")
    print("═" * 65)

    # GasWizard 最近7天
    print("\n【GasWizard 最近价格（cents/L）】")
    rows = conn.execute("""
        SELECT price_date, label, regular, regular_chg, premium, diesel
        FROM gaswizard_prices
        ORDER BY price_date DESC, label
        LIMIT 10
    """).fetchall()
    if rows:
        print(f"  {'日期':12s} {'标签':10s} {'普通':8s} {'涨跌':6s} {'高级':8s} {'柴油':8s}")
        print(f"  {'-'*12} {'-'*10} {'-'*8} {'-'*6} {'-'*8} {'-'*8}")
        for pd_, lbl, reg, rchg, pre, die in rows:
            chg = f"{rchg:+.0f}¢" if rchg is not None else " —"
            print(f"  {pd_:12s} {lbl:10s} {str(reg):8s} {chg:6s} {str(pre):8s} {str(die):8s}")

    # Stockr 预测
    print("\n【Stockr 预测记录（最近5条）】")
    rows = conn.execute("""
        SELECT price_date, direction, direction_cents, predicted_price,
               substr(summary, 1, 60)
        FROM stockr_predictions
        ORDER BY scraped_at DESC LIMIT 5
    """).fetchall()
    if rows:
        for pd_, direc, cents, price, summ in rows:
            cents_str = f"{cents:+.1f}¢" if cents is not None else "N/A"
            print(f"  {pd_}: {direc} {cents_str}  预测={price}")
            if summ:
                print(f"    摘要: {summ}...")
    else:
        print("  （暂无数据）")

    # Stockr 历史月度
    print("\n【Stockr 月度历史均价】")
    rows = conn.execute("""
        SELECT month_label, avg_price FROM stockr_history
        ORDER BY month_label DESC LIMIT 12
    """).fetchall()
    if rows:
        for label, price in rows:
            print(f"  {label:20s}  {price} cents/L")
    else:
        print("  （暂无数据）")

    # CityNews 预测
    print("\n【CityNews 预测记录（最近5条）】")
    rows = conn.execute("""
        SELECT price_date, direction, direction_cents, predicted_price,
               substr(summary, 1, 60)
        FROM citynews_predictions
        ORDER BY scraped_at DESC LIMIT 5
    """).fetchall()
    if rows:
        for pd_, direc, cents, price, summ in rows:
            cents_str = f"{cents:+.1f}¢" if cents is not None else "N/A"
            print(f"  {pd_}: {direc} {cents_str}  预测={price}")
            if summ:
                print(f"    摘要: {summ}...")
    else:
        print("  （暂无数据）")

    # CityNews 历史
    print("\n【CityNews 历史记录（最近15条）】")
    rows = conn.execute("""
        SELECT price_date, price FROM citynews_history
        ORDER BY price_date DESC LIMIT 15
    """).fetchall()
    if rows:
        for pd_, price in rows:
            print(f"  {pd_}  {price} cents/L")
    else:
        print("  （暂无数据）")

    print("\n" + "═" * 65)


def export_csv(conn):
    tables = [
        ("gaswizard_prices", "export_gaswizard.csv"),
        ("stockr_predictions", "export_stockr_pred.csv"),
        ("stockr_history", "export_stockr_hist.csv"),
        ("citynews_predictions", "export_citynews_pred.csv"),
        ("citynews_history", "export_citynews_hist.csv"),
    ]
    for table, fname in tables:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            continue
        cols = [d[0] for d in conn.execute(f"SELECT * FROM {table} LIMIT 0").description]
        with open(fname, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)
        print(f"已导出: {fname}  ({len(rows)} 行)")


def main():
    parser = argparse.ArgumentParser(description="查询油价数据库")
    parser.add_argument("--history", action="store_true", help="显示完整历史")
    parser.add_argument("--export", action="store_true", help="导出 CSV")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print("数据库不存在，请先运行 scraper.py")
        return

    conn = sqlite3.connect(DB_PATH)
    if args.export:
        export_csv(conn)
    else:
        show_latest(conn)
    conn.close()


if __name__ == "__main__":
    main()
