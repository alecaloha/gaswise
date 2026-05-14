"""
Toronto Gas Price Scraper
=========================
GasWizard : 照搬 deep.py  (requests + html.parser + CSS class)
Stockr    : 照搬 seep.py  (Playwright + inner_text + 正则)
CityNews  : 正则解析预测句（原有稳定逻辑）

运行:
  pip install requests playwright beautifulsoup4 lxml
  playwright install chromium
  python scraper.py
"""

import asyncio
import csv
import re
import os
import time
import random
import logging
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

# 1. 设置环境变量（Windows 和 Linux 都认这个变量）
os.environ['TZ'] = 'America/Toronto'

# 2. 核心修正逻辑
if hasattr(time, 'tzset'):
    # 这行代码只在 Linux (GitHub Actions) 上运行
    # 它会强迫 Python 丢弃 UTC，改用多伦多时间
    time.tzset() 
else:
    # 这行在 Windows 上运行，Windows 不支持 tzset
    # 但通常 Windows 本地时间已经是正确的，所以跳过即可
    pass

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

CSV_PATH = Path("gas_prices.csv")
CSV_COLUMNS = [
    "scraped_at", "source", "price_date", "label",
    "regular", "premium", "diesel", "regular_chg",
    "direction", "summary",
]


# ═══════════════════════════════════════════════════════════════════════════
#  CSV 读写
# ═══════════════════════════════════════════════════════════════════════════

def _load_csv():
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _save_csv(rows):
    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)


def upsert_row(new_row: dict):
    """同 source+price_date 只保留最新一条。"""
    rows = _load_csv()
    key  = (new_row["source"], new_row["price_date"])
    rows = [r for r in rows if (r["source"], r["price_date"]) != key]
    filled = {col: new_row.get(col, "") for col in CSV_COLUMNS}
    rows.append(filled)
    rows.sort(key=lambda r: (r["price_date"], r["source"]), reverse=True)
    _save_csv(rows)


def _val(s):
    if s == "" or s is None:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return s


# ═══════════════════════════════════════════════════════════════════════════
#  来源 1: gaswizard.ca
#  照搬 deep.py 逻辑（已验证可正确抓取）
# ═══════════════════════════════════════════════════════════════════════════

URL_GASWIZARD        = "https://gaswizard.trustyalec.workers.dev"
URL_GASWIZARD_DIRECT = "https://gaswizard.ca/gas-prices/toronto/"


def _parse_price(price_text: str):
    """照搬 deep.py 的 parse_price，修复 Â¢ 乱码。"""
    price_text = price_text.replace("Â¢", "¢").replace("\xa2", "¢")

    price_m = re.search(r"([\d.]+)", price_text)
    price   = float(price_m.group(1)) if price_m else None

    change_m   = re.search(r"\(([^)]+)\)", price_text)
    change_raw = change_m.group(1).strip() if change_m else "n/c"
    change_raw = re.sub(r"<[^>]+>", "", change_raw).strip()

    if re.search(r"n/c|unchanged", change_raw, re.IGNORECASE) or change_raw == "0":
        return price, 0, "unchanged"

    sign_m = re.search(r"([+-]?\d+)", change_raw)
    if sign_m:
        chg = int(sign_m.group(1))
        return price, chg, "up" if chg > 0 else "down"

    return price, 0, "unchanged"


def scrape_gaswizard():
    """照搬 deep.py 的完整抓取和解析逻辑。"""
    log.info("→ 抓取 gaswizard.ca ...")
    now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = date.today()

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    soup = None
    for url in [URL_GASWIZARD, URL_GASWIZARD_DIRECT]:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            log.info(f"  GasWizard OK: {url} ({len(resp.text)}b)")
            break
        except Exception as e:
            log.warning(f"  GasWizard {url}: {e}")

    if not soup:
        log.error("  GasWizard 所有 URL 失败")
        return

    price_ul = soup.find("ul", class_=lambda c: c and "single-city-prices" in c)
    if not price_ul:
        reg_elem = soup.find(string=re.compile(r"Regular"))
        if reg_elem:
            price_ul = reg_elem.find_parent("ul")

    if not price_ul:
        log.warning("  GasWizard: 未找到 ul.single-city-prices")
        return

    items = price_ul.find_all("li", recursive=False)
    log.info(f"  GasWizard 找到 {len(items)} 个 li")
    written = 0

    for li in items:
        date_div = li.find("div", class_="datetext")
        if date_div:
            date_str = date_div.get_text(strip=True)
        else:
            date_span = li.find("span", class_="datetext")
            if date_span:
                date_str = date_span.get_text(strip=True)
            else:
                date_text = li.find(string=re.compile(
                    r"\b(?:January|February|March|April|May|June|July|August|"
                    r"September|October|November|December)\s+\d{1,2},\s+\d{4}\b"
                ))
                date_str = date_text.strip() if date_text else None

        if not date_str:
            continue

        try:
            price_date = datetime.strptime(date_str, "%B %d, %Y").date()
        except ValueError:
            log.warning(f"  GasWizard 日期解析失败: {date_str!r}")
            continue

        delta = (price_date - today).days
        if   delta > 0:   label = "tomorrow"
        elif delta == 0:  label = "today"
        elif delta == -1: label = "yesterday"
        else:             continue

        fuel_types = li.find_all("div", class_="fueltype")
        if not fuel_types:
            fuel_types = [fc for fc in li.find_all("div", recursive=True)
                          if fc.find("div", class_="fueltitle")]

        data = {"regular": None, "premium": None, "diesel": None,
                "regular_chg": None, "direction": None}

        for fuel in fuel_types:
            title_div = fuel.find("div", class_="fueltitle")
            if not title_div:
                continue
            fuel_name = title_div.get_text(strip=True).lower()
            price_div = fuel.find("div", class_="fuelprice")
            if not price_div:
                continue
            price_text = price_div.get_text(strip=True)
            log.info(f"  油品: {fuel_name}, 价格: {price_text!r}")
            price_val, chg_val, direction = _parse_price(price_text)

            if fuel_name == "regular":
                data["regular"]     = price_val
                data["regular_chg"] = chg_val
                data["direction"]   = direction
            elif fuel_name == "premium":
                data["premium"] = price_val
            elif fuel_name == "diesel":
                data["diesel"] = price_val

        if any(v is None for v in [data["regular"], data["premium"], data["diesel"]]):
            log.warning(f"  GasWizard [{label}]: 部分油品缺失，跳过")
            continue

        upsert_row({
            "scraped_at":  now,
            "source":      "gaswizard",
            "price_date":  price_date.isoformat(),
            "label":       label,
            "regular":     data["regular"],
            "premium":     data["premium"],
            "diesel":      data["diesel"],
            "regular_chg": data["regular_chg"],
            "direction":   data["direction"],
            "summary":     "",
        })
        log.info(f"  GasWizard [{label}] {price_date}: 普通={data['regular']}({data['regular_chg']:+d}¢)")
        written += 1

    log.info(f"  GasWizard 完成，写入 {written} 条")


# ═══════════════════════════════════════════════════════════════════════════
#  来源 2: stockr.net
#  照搬 seep.py 逻辑（Playwright + inner_text + 正则）
# ═══════════════════════════════════════════════════════════════════════════

URL_STOCKR        = "https://stockr.trustyalec.workers.dev"
URL_STOCKR_DIRECT = "https://stockr.net/Toronto/GasPrice.aspx"


async def _fetch_stockr_playwright():
    """照搬 seep.py 的 fetch_stockr_prices 抓取部分。"""
    from playwright.async_api import async_playwright

    for url in [URL_STOCKR, URL_STOCKR_DIRECT]:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page    = await browser.new_page()
                await page.set_extra_http_headers({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                })
                await page.goto(url, wait_until="networkidle")
                try:
                    await page.wait_for_selector("text=Today", timeout=10000)
                except Exception:
                    pass
                text = await page.inner_text("body")
                await browser.close()
                if "Today" in text:
                    log.info(f"  Stockr Playwright OK: {url}")
                    return text
        except Exception as e:
            log.warning(f"  Stockr Playwright {url}: {e}")

    return None


def _parse_stockr(text: str):
    """
    分别匹配 Today 和 Tomorrow（Tomorrow 价格可能为空）。
    修复：seep.py 原正则要求 Today+Tomorrow 同时匹配，
    当 Tomorrow 无价格时整体失败。改为各自独立匹配。
    """
    TODAY_RE = re.compile(
        r"Today\s+([\d.]+)\s+"
        r"([A-Za-z]+\s+[A-Za-z]+\s+\d+,\s+\d{4})",
        re.DOTALL
    )
    TMR_RE = re.compile(
        r"Tomorrow\s+([\d.]+)\s+"
        r"([A-Za-z]+\s+[A-Za-z]+\s+\d+,\s+\d{4})",
        re.DOTALL
    )

    def parse_date(ds):
        for fmt in ("%A %B %d, %Y", "%A %b %d, %Y", "%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(ds.strip(), fmt).date()
            except ValueError:
                continue
        return None

    tm = TODAY_RE.search(text)
    mm = TMR_RE.search(text)

    if not tm:
        log.warning("  Stockr: 页面中未找到 Today 价格")
        log.warning(repr(text[:300]))
        return []

    today_price = float(tm.group(1))
    today_date  = parse_date(tm.group(2))
    if not today_date:
        log.warning(f"  Stockr Today 日期解析失败: {tm.group(2)!r}")
        return []

    today_sys = date.today()
    results   = []

    # Today
    if today_date >= today_sys:
        label = "today" if today_date == today_sys else "tomorrow"
        results.append((today_date, label, today_price, 0, "unchanged"))

    # Tomorrow（可选）
    if mm:
        tmr_price = float(mm.group(1))
        tmr_date  = parse_date(mm.group(2))
        if tmr_date and tmr_date >= today_sys:
            # 计算涨跌（seep.py 逻辑）
            price_change = tmr_price - today_price
            if price_change > 0:
                direction, regular_chg = "up", int(price_change)
            elif price_change < 0:
                direction, regular_chg = "down", int(price_change)
            else:
                direction, regular_chg = "unchanged", 0
            # 更新 Today 的涨跌信息
            results = [(pd, lbl, pv, regular_chg, direction) for pd, lbl, pv, _, _ in results]
            lbl = "today" if tmr_date == today_sys else "tomorrow"
            results.append((tmr_date, lbl, tmr_price, regular_chg, direction))
            log.info(f"  Stockr Tomorrow: {tmr_price}¢ @ {tmr_date}")
    else:
        log.info("  Stockr: 今日无 Tomorrow 预测（正常，每天11am后发布）")

    return results


def scrape_stockr():
    """照搬 seep.py 的完整抓取和解析逻辑。"""
    log.info("→ 抓取 stockr.net ...")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    text = asyncio.run(_fetch_stockr_playwright())
    if not text:
        log.error("  Stockr: Playwright 失败")
        return

    results = _parse_stockr(text)
    if not results:
        log.warning("  Stockr: 未提取到价格数据")
        return

    for pd, label, price, chg, direction in results:
        upsert_row({
            "scraped_at":  now,
            "source":      "stockr",
            "price_date":  pd.isoformat(),
            "label":       label,
            "regular":     price,
            "premium":     "", "diesel": "",
            "regular_chg": chg,
            "direction":   direction,
            "summary":     "Gas price prediction from Stockr",
        })
        log.info(f"  Stockr [{label}] {pd}: {price}¢ ({chg:+d} {direction})")

    log.info("  Stockr 完成")


# ═══════════════════════════════════════════════════════════════════════════
#  来源 3: toronto.citynews.ca（原有稳定逻辑）
# ═══════════════════════════════════════════════════════════════════════════

URL_CITYNEWS = "https://toronto.citynews.ca/toronto-gta-gas-prices/"


def _decode_content(content: bytes) -> str:
    import gzip as _gz, zlib as _zl
    try:
        return _gz.decompress(content).decode("utf-8", errors="replace")
    except Exception:
        pass
    try:
        return _zl.decompress(content, -_zl.MAX_WBITS).decode("utf-8", errors="replace")
    except Exception:
        pass
    return content.decode("utf-8", errors="replace")


def scrape_citynews():
    log.info("→ 抓取 toronto.citynews.ca ...")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    session = requests.Session()
    session.headers.update(headers)

    log.info("  先访问首页获取 Cloudflare cookie ...")
    try:
        session.get("https://toronto.citynews.ca/", timeout=15)
    except Exception:
        pass
    time.sleep(random.uniform(3, 6))

    try:
        resp = session.get(URL_CITYNEWS, timeout=20)
        resp.raise_for_status()
        html = _decode_content(resp.content)
    except Exception as e:
        log.warning(f"  CityNews 抓取失败: {e}")
        return

    now       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    soup      = BeautifulSoup(html, "lxml")
    page_text = soup.get_text(" ", strip=True)

    direction = direction_cents = predicted_price = price_date = None

    CHANGE_RE    = re.compile(r"expected\s+to\s+(rise|fall|increase|decrease|drop|jump)\s+([\d.]+)\s*cent", re.IGNORECASE)
    UNCHANGED_RE = re.compile(r"(remain\s+unchanged|no\s+change|holding\s+at|unchanged)", re.IGNORECASE)
    DATE_RE      = re.compile(r"on\s+(January|February|March|April|May|June|July|August|September|October|November|December)\.?\s+(\d{1,2}),?\s+(202\d)", re.IGNORECASE)
    AVG_RE       = re.compile(r"(?:average\s+of|holding\s+at(?:\s+an\s+average\s+of)?)\s+(1[3-9]\d\.?\d?)\s*cent", re.IGNORECASE)

    cm = CHANGE_RE.search(page_text)
    um = UNCHANGED_RE.search(page_text)
    dm = DATE_RE.search(page_text)
    am = AVG_RE.search(page_text)

    if cm:
        word = cm.group(1).lower()
        direction = ("up" if word in ("rise","increase","jump") else
                     "down" if word in ("fall","decrease","drop") else "unchanged")
        direction_cents = float(cm.group(2))
        if direction == "down":
            direction_cents = -direction_cents
    elif um:
        direction, direction_cents = "unchanged", 0.0

    if am:
        predicted_price = float(am.group(1))
    else:
        fb = re.search(r"\bat\s+(1[3-9]\d\.?\d?)\s*cent", page_text, re.IGNORECASE)
        if fb:
            predicted_price = float(fb.group(1))

    if dm:
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                price_date = datetime.strptime(
                    f"{dm.group(1)} {dm.group(2)} {dm.group(3)}", fmt
                ).date().isoformat()
                break
            except ValueError:
                pass

    if price_date is None:
        price_date = (date.today() + timedelta(days=1)).isoformat()

    summary = ""
    if predicted_price is not None and direction is not None:
        try:
            pd_obj     = datetime.strptime(price_date, "%Y-%m-%d").date()
            date_short = f"{pd_obj.month}.{pd_obj.day}"
        except Exception:
            date_short = price_date
        if direction == "unchanged":
            summary = f"预测{date_short}日均价{predicted_price} cent(s)/litre，维持不变"
        else:
            dir_word = "上涨" if direction == "up" else "下降"
            summary  = f"预测{date_short}日均价{predicted_price} cent(s)/litre，{dir_word}{abs(direction_cents):.1f}¢"

    log.info(f"  CityNews: {summary or '解析失败'}")

    upsert_row({
        "scraped_at":  now, "source": "citynews",
        "price_date":  price_date, "label": "prediction",
        "regular":     "" if predicted_price is None else predicted_price,
        "premium":     "", "diesel": "",
        "regular_chg": "" if direction_cents is None else direction_cents,
        "direction":   direction or "", "summary": summary,
    })
    log.info(f"  CityNews 已写入: {price_date} price={predicted_price} dir={direction}")

    DATE_FMTS = ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y")
    hist_count = 0
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in row.find_all(["td","th"])]
            if len(cells) < 2:
                continue
            pm = re.search(r"\b(1[3-9]\d(?:\.\d)?)\b", cells[-1])
            if not pm:
                continue
            for fmt in DATE_FMTS:
                try:
                    d = datetime.strptime(cells[0].strip(), fmt).date()
                    upsert_row({
                        "scraped_at": now, "source": "citynews",
                        "price_date": d.isoformat(), "label": "history",
                        "regular": float(pm.group(1)), "premium": "", "diesel": "",
                        "regular_chg": "", "direction": "", "summary": "",
                    })
                    hist_count += 1
                    break
                except ValueError:
                    pass

    log.info(f"  CityNews 历史: {hist_count} 条")
    log.info("  CityNews 完成")


# ═══════════════════════════════════════════════════════════════════════════
#  汇总显示
# ═══════════════════════════════════════════════════════════════════════════

def print_summary():
    today_s     = date.today().isoformat()
    yesterday_s = (date.today() - timedelta(days=1)).isoformat()
    tomorrow_s  = (date.today() + timedelta(days=1)).isoformat()
    rows        = _load_csv()

    def best(source, price_date):
        matched = [r for r in rows if r["source"] == source and r["price_date"] == price_date]
        return max(matched, key=lambda x: x["scraped_at"]) if matched else None

    def fmt_chg(v):
        if v is None or v == "": return ""
        try:
            fv = float(v)
            if fv == 0: return " (n/c)"
            return f" ({fv:+.0f}¢)"
        except (ValueError, TypeError):
            return ""

    print("\n" + "═" * 56)
    print(f"  多伦多油价汇总  —  {today_s}")
    print("═" * 56)

    print("\n【GasWizard】")
    for lbl, tgt, need_fresh in [
        ("明天", tomorrow_s,  True),
        ("今天", today_s,     False),
        ("昨天", yesterday_s, False),
    ]:
        r = best("gaswizard", tgt)
        if not r:
            print(f"  {lbl}  暂无数据")
            continue
        if need_fresh and r["scraped_at"][:10] < yesterday_s:
            print(f"  {lbl}  暂无预测（数据过期）")
            continue
        print(f"  {lbl}  {r['price_date']}  普通={_val(r['regular'])}{fmt_chg(_val(r['regular_chg']))}  高级={_val(r['premium'])}  柴油={_val(r['diesel'])}")

    print("\n【Stockr】")
    for lbl, tgt in [("今天", today_s), ("明天", tomorrow_s)]:
        r = best("stockr", tgt)
        if r and _val(r["regular"]) is not None:
            print(f"  {lbl}  {r['price_date']}  {_val(r['regular'])} cents/L")
        else:
            print(f"  {lbl}  暂无预测")

    print("\n【CityNews】")
    cn_pred = [r for r in rows
               if r["source"] == "citynews"
               and r.get("label") == "prediction"
               and r["price_date"] >= today_s]
    if cn_pred:
        r     = max(cn_pred, key=lambda x: x["scraped_at"])
        delta = (datetime.strptime(r["price_date"], "%Y-%m-%d").date() - date.today()).days
        lbl   = "今天" if delta == 0 else "明天" if delta == 1 else r["price_date"]
        print(f"  预测对象: {lbl}  {r['price_date']}")
        print(f"  {r['summary'] or '（无摘要）'}")
    else:
        print("  暂无预测")

    total  = len(rows)
    gw_cnt = sum(1 for r in rows if r["source"] == "gaswizard")
    sk_cnt = sum(1 for r in rows if r["source"] == "stockr")
    cn_cnt = sum(1 for r in rows if r["source"] == "citynews")
    print(f"\n【CSV 记录】总={total}  GasWizard={gw_cnt}  Stockr={sk_cnt}  CityNews={cn_cnt}")
    print("═" * 56 + "\n")


# ═══════════════════════════════════════════════════════════════════════════
#  主入口
# ═══════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 50)
    log.info(f"开始抓取  {datetime.now()}")
    log.info("=" * 50)
    scrape_gaswizard()
    scrape_stockr()
    scrape_citynews()
    print_summary()
    log.info(f"完成。数据存入 {CSV_PATH.resolve()}")


if __name__ == "__main__":
    main()
