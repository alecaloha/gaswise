"""
Toronto Gas Price Scraper
=========================
抓取多个来源的多伦多油价预测，写入单一 CSV 文件。

来源:
  1. gaswizard.ca        — 明天/今天/昨天  Regular / Premium / Diesel
  2. stockr.net          — 今天/明天 Regular 预测价
  3. toronto.citynews.ca — 预测方向、均价、摘要

运行方式:
  pip install requests beautifulsoup4 lxml
  python scraper.py

输出文件: gas_prices.csv（自动创建/追加，同 source+price_date 自动去重覆盖）

CSV 列说明:
  scraped_at   抓取时间 YYYY-MM-DD HH:MM:SS
  source       gaswizard / stockr / citynews
  price_date   价格对应日期 YYYY-MM-DD
  label        tomorrow / today / yesterday（仅 gaswizard）
  regular      普通汽油 cents/L
  premium      高级汽油 cents/L（仅 gaswizard，其余空）
  diesel       柴油 cents/L（仅 gaswizard，其余空）
  regular_chg  普通油涨跌量（仅 gaswizard）
  direction    up / down / unchanged（仅 citynews）
  summary      预测摘要（仅 citynews）
"""

import csv
import re
import time
import random
import logging
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ── 日志 ──────────────────────────────────────────────────────────────────
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
#  CSV 读写层（替代 SQLite）
# ═══════════════════════════════════════════════════════════════════════════

def _load_csv() -> list[dict]:
    """读取 CSV，返回行列表。文件不存在时返回空列表。"""
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _save_csv(rows: list[dict]) -> None:
    """将行列表写回 CSV（覆盖整个文件）。"""
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def upsert_row(new_row: dict) -> None:
    """
    插入或更新一条记录。
    去重键: (source, price_date) — 同来源同日期只保留最新一条。
    """
    rows = _load_csv()
    key = (new_row["source"], new_row["price_date"])
    # 过滤掉相同键的旧行
    rows = [r for r in rows if (r["source"], r["price_date"]) != key]
    # 补齐缺失列为空字符串
    filled = {col: new_row.get(col, "") for col in CSV_COLUMNS}
    rows.append(filled)
    # 按 price_date 降序、source 排序，保持文件整洁
    rows.sort(key=lambda r: (r["price_date"], r["source"]), reverse=True)
    _save_csv(rows)


def read_rows(source: str = None, price_date: str = None) -> list[dict]:
    """
    读取记录，可按 source 和/或 price_date 过滤。
    """
    rows = _load_csv()
    if source:
        rows = [r for r in rows if r["source"] == source]
    if price_date:
        rows = [r for r in rows if r["price_date"] == price_date]
    return rows


def _val(s: str):
    """把 CSV 空字符串转为 None，数字字符串转为 float。"""
    if s == "" or s is None:
        return None
    try:
        return float(s)
    except ValueError:
        return s


# ═══════════════════════════════════════════════════════════════════════════
#  时效检查
# ═══════════════════════════════════════════════════════════════════════════

def _scraped_today(scraped_at_str: str) -> bool:
    if not scraped_at_str:
        return False
    try:
        return datetime.fromisoformat(scraped_at_str).date() == date.today()
    except ValueError:
        return False


# ═══════════════════════════════════════════════════════════════════════════
#  HTTP 工具
# ═══════════════════════════════════════════════════════════════════════════

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]


def make_session(referer: str = "https://www.google.com/") -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Cache-Control": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-User": "?1",
        "DNT": "1",
    })
    return s


def fetch(url: str, session: requests.Session,
          verify_ssl: bool = True, retries: int = 3) -> BeautifulSoup | None:
    for attempt in range(1, retries + 1):
        try:
            time.sleep(random.uniform(1.5, 4.0))
            resp = session.get(url, timeout=20, verify=verify_ssl, allow_redirects=True)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except requests.exceptions.HTTPError as e:
            log.warning(f"[{attempt}/{retries}] HTTP 错误 {e} — {url}")
            if e.response.status_code in (403, 429, 500):
                session.headers["User-Agent"] = random.choice(UA_POOL)
                time.sleep(random.uniform(10, 20))
        except requests.exceptions.RequestException as e:
            log.warning(f"[{attempt}/{retries}] 请求失败 {e} — {url}")
            time.sleep(random.uniform(5, 10))
    log.error(f"所有重试失败: {url}")
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  来源 1: gaswizard.ca
# ═══════════════════════════════════════════════════════════════════════════

URL_GASWIZARD = "https://gaswizard.ca/gas-prices/toronto/"


def scrape_gaswizard():
    """
    抓取 GasWizard，写入 CSV。
    每次抓取到的 price_date 行会覆盖 CSV 中同日期的旧记录。
    """
    log.info("→ 抓取 gaswizard.ca ...")
    session = make_session()
    soup = fetch(URL_GASWIZARD, session)
    if not soup:
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = date.today()

    # 找含 Regular/Premium 的 <ul>
    price_items = []
    for ul in soup.find_all("ul"):
        items = ul.find_all("li", recursive=False)
        if items and "Regular" in items[0].get_text() and "Premium" in items[0].get_text():
            price_items = items
            break

    for li in price_items[:2]:
        li_text = li.get_text(" ", strip=True)

        date_m = re.search(
            r"(\w+day)\s*[-–]\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
            r"\s+(\d{1,2}),\s*(\d{4})",
            li_text,
        )
        if not date_m:
            continue
        try:
            price_date = datetime.strptime(
                f"{date_m.group(2)} {date_m.group(3)} {date_m.group(4)}", "%b %d %Y"
            ).date()
        except ValueError:
            continue

        delta = (price_date - today).days
        if delta > 0:
            label = "tomorrow"
        elif delta == 0:
            label = "today"
        elif delta == -1:
            label = "yesterday"
        else:
            label = f"{abs(delta)}_days_ago"

        def extract_fuel(keyword):
            m = re.search(rf"{keyword}\s+([\d.]+)\s*\(([+-]?\d+)\s*[¢\xa2]\)", li_text)
            if m:
                return float(m.group(1)), float(m.group(2))
            m = re.search(rf"{keyword}\s+([\d.]+)\s*\(n/c\)", li_text, re.IGNORECASE)
            if m:
                return float(m.group(1)), 0.0
            m = re.search(rf"{keyword}\s+([\d.]+)", li_text)
            if m:
                return float(m.group(1)), None
            return None, None

        reg, reg_chg = extract_fuel("Regular")
        pre, _       = extract_fuel("Premium")
        die, _       = extract_fuel("Diesel")

        row = {
            "scraped_at":  now,
            "source":      "gaswizard",
            "price_date":  price_date.isoformat(),
            "label":       label,
            "regular":     "" if reg is None else reg,
            "premium":     "" if pre is None else pre,
            "diesel":      "" if die is None else die,
            "regular_chg": "" if reg_chg is None else reg_chg,
            "direction":   "",
            "summary":     "",
        }
        upsert_row(row)
        log.info(f"  GasWizard [{label}] {price_date}: 普通={reg}({reg_chg}¢) 高级={pre} 柴油={die}")

    log.info("  GasWizard 完成")


# ═══════════════════════════════════════════════════════════════════════════
#  来源 2: stockr.net
# ═══════════════════════════════════════════════════════════════════════════

URL_STOCKR_FALLBACKS = [
    "https://www.stockr.net/Toronto/GasPrice.aspx",
    "https://stockr.net/Toronto/GasPrice.aspx",
    "http://www.stockr.net/Toronto/GasPrice.aspx",
    "https://www.stockr.net/toronto/gasprice.aspx",
]


def scrape_stockr():
    """
    抓取 Stockr，只写 Regular 价格（today / tomorrow）到 CSV。
    """
    log.info("→ 抓取 stockr.net ...")
    session = make_session()
    soup = None
    for url_try in URL_STOCKR_FALLBACKS:
        log.info(f"  尝试 URL: {url_try}")
        soup = fetch(url_try, session, verify_ssl=False, retries=2)
        if soup:
            log.info(f"  成功: {url_try}")
            break
    if not soup:
        log.error("  Stockr 所有 URL 均失败，跳过。")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today_iso = date.today().isoformat()
    page_text = soup.get_text(" ", strip=True)

    # ── Today 价格 ────────────────────────────────────────────────────────
    today_price = None
    today_date_str = None

    m = re.search(
        r"(1[3-9]\d\.\d)\s+"
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+"
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{1,2}),?\s+(202\d)",
        page_text,
    )
    if m:
        today_price = float(m.group(1))
        try:
            today_date_str = datetime.strptime(
                f"{m.group(3)} {m.group(4)} {m.group(5)}", "%B %d %Y"
            ).date().isoformat()
        except ValueError:
            today_date_str = today_iso

    if today_price is None:
        for selector in ["#ctl00_ContentPlaceHolder1_lblTodayPrice", "#lblTodayPrice", "h1", "h2"]:
            el = soup.select_one(selector)
            if el:
                m2 = re.search(r"(1[3-9]\d\.\d)", el.get_text())
                if m2:
                    today_price = float(m2.group(1))
                    today_date_str = today_iso
                    break

    if today_price is None:
        for tag in soup.find_all(["span", "div", "td", "h1", "h2", "h3", "p"]):
            if tag.find(["span", "div", "p", "h1", "h2", "h3", "td"]):
                continue
            txt = tag.get_text(strip=True)
            m3 = re.fullmatch(r"(1[3-9]\d\.\d)", txt)
            if m3:
                today_price = float(m3.group(1))
                today_date_str = today_iso
                break

    log.info(f"  Stockr today: {today_price} ({today_date_str})")

    # 页面日期必须等于今天，否则页面尚未更新
    if today_price is not None and today_date_str:
        if today_date_str != today_iso:
            log.warning(f"  Stockr 页面日期={today_date_str} ≠ 今天={today_iso}，跳过（页面尚未更新）")
        else:
            upsert_row({
                "scraped_at": now, "source": "stockr",
                "price_date": today_date_str, "label": "today",
                "regular": today_price, "premium": "", "diesel": "",
                "regular_chg": "", "direction": "", "summary": "",
            })

    # ── Tomorrow 价格（仅关键词明确出现时写入）────────────────────────────
    tm = re.search(r"[Tt]omorrow[^\d]{0,60}?(1[3-9]\d\.\d)", page_text)
    if tm:
        tomorrow_price = float(tm.group(1))
        tmr_date = (date.today() + timedelta(days=1)).isoformat()
        upsert_row({
            "scraped_at": now, "source": "stockr",
            "price_date": tmr_date, "label": "tomorrow",
            "regular": tomorrow_price, "premium": "", "diesel": "",
            "regular_chg": "", "direction": "", "summary": "",
        })
        log.info(f"  Stockr tomorrow: {tomorrow_price} ({tmr_date})")
    else:
        log.info("  Stockr tomorrow: 暂无数据")

    log.info("  Stockr 完成")


# ═══════════════════════════════════════════════════════════════════════════
#  来源 3: toronto.citynews.ca
# ═══════════════════════════════════════════════════════════════════════════

URL_CITYNEWS = "https://toronto.citynews.ca/toronto-gta-gas-prices/"


def scrape_citynews():
    """
    抓取 CityNews，写入预测价格、方向、摘要到 CSV。

    实测页面格式:
      "__En-Pro__ tells CityNews that prices are expected to fall 7 cent(s)
       at 12:01am on April 19, 2026 to an average of 174.9 cent(s)/litre"
    """
    log.info("→ 抓取 toronto.citynews.ca ...")
    session = make_session()

    log.info("  先访问首页以获取 Cloudflare cookie ...")
    fetch("https://toronto.citynews.ca/", session)
    time.sleep(random.uniform(3, 6))

    soup = fetch(URL_CITYNEWS, session)
    if not soup:
        log.warning("  citynews 抓取失败（Cloudflare 拦截）。如需绕过，请改用 Playwright。")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    page_text = soup.get_text(" ", strip=True)

    # ── 核心正则：方向 + 变化量 ───────────────────────────────────────────
    direction = None
    direction_cents = None
    predicted_price = None
    price_date = None

    CHANGE_RE = re.compile(
        r"expected\s+to\s+(rise|fall|increase|decrease|drop|jump)\s+([\d.]+)\s*cent",
        re.IGNORECASE,
    )
    UNCHANGED_RE = re.compile(
        r"(remain\s+unchanged|no\s+change|holding\s+at|unchanged)",
        re.IGNORECASE,
    )
    DATE_RE = re.compile(
        r"on\s+(January|February|March|April|May|June|July|August|"
        r"September|October|November|December|"
        r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"\.?\s+(\d{1,2}),?\s+(202\d)",
        re.IGNORECASE,
    )
    AVG_RE = re.compile(
        r"(?:average\s+of|holding\s+at(?:\s+an\s+average\s+of)?)\s+(1[3-9]\d\.?\d?)\s*cent",
        re.IGNORECASE,
    )

    cm = CHANGE_RE.search(page_text)
    um = UNCHANGED_RE.search(page_text)
    dm = DATE_RE.search(page_text)
    am = AVG_RE.search(page_text)

    if cm:
        word = cm.group(1).lower()
        direction = "up"   if word in ("rise", "increase", "jump") else \
                    "down" if word in ("fall", "decrease", "drop") else "unchanged"
        direction_cents = float(cm.group(2))
        if direction == "down":
            direction_cents = -direction_cents
    elif um:
        direction = "unchanged"
        direction_cents = 0.0

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

    # ── 格式化 summary ────────────────────────────────────────────────────
    summary = ""
    if predicted_price is not None and direction is not None:
        try:
            pd_obj = datetime.strptime(price_date, "%Y-%m-%d").date()
            date_short = f"{pd_obj.month}.{pd_obj.day}"
        except Exception:
            date_short = price_date
        if direction == "unchanged":
            summary = f"预测{date_short}日均价{predicted_price} cent(s)/litre，维持不变"
        else:
            dir_word = "上涨" if direction == "up" else "下降"
            summary = (f"预测{date_short}日均价{predicted_price} cent(s)/litre，"
                       f"{dir_word}{abs(direction_cents):.1f}¢")

    log.info(f"  CityNews: {summary or '解析失败'}")

    upsert_row({
        "scraped_at":  now,
        "source":      "citynews",
        "price_date":  price_date,
        "label":       "",
        "regular":     "" if predicted_price is None else predicted_price,
        "premium":     "",
        "diesel":      "",
        "regular_chg": "" if direction_cents is None else direction_cents,
        "direction":   direction or "",
        "summary":     summary,
    })

    # ── 历史价格：也写入 CSV（citynews 历史条目，label 为空）────────────────
    DATE_FMTS = ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y")
    hist_count = 0
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            pm = re.search(r"\b(1[3-9]\d(?:\.\d)?)\b", cells[-1])
            if not pm:
                continue
            price_val = float(pm.group(1))
            for fmt in DATE_FMTS:
                try:
                    d = datetime.strptime(cells[0].strip(), fmt).date()
                    upsert_row({
                        "scraped_at": now, "source": "citynews",
                        "price_date": d.isoformat(), "label": "history",
                        "regular": price_val, "premium": "", "diesel": "",
                        "regular_chg": "", "direction": "", "summary": "",
                    })
                    hist_count += 1
                    break
                except ValueError:
                    pass

    DATE_TEXT_RE = re.compile(
        r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+"
        r"\d{1,2},?\s+202\d)[^0-9]{0,20}(1[3-9]\d(?:\.\d)?)",
        re.IGNORECASE,
    )
    for line in page_text.splitlines():
        for m in DATE_TEXT_RE.finditer(line):
            date_str = m.group(1).strip().rstrip(",")
            price_val = float(m.group(2))
            for fmt in DATE_FMTS:
                try:
                    d = datetime.strptime(date_str, fmt).date()
                    upsert_row({
                        "scraped_at": now, "source": "citynews",
                        "price_date": d.isoformat(), "label": "history",
                        "regular": price_val, "premium": "", "diesel": "",
                        "regular_chg": "", "direction": "", "summary": "",
                    })
                    hist_count += 1
                    break
                except ValueError:
                    pass

    log.info(f"  CityNews 历史记录已存: {hist_count} 条")
    log.info("  CityNews 完成")


# ═══════════════════════════════════════════════════════════════════════════
#  汇总显示
# ═══════════════════════════════════════════════════════════════════════════

def print_summary():
    """
    读取 CSV，按时效规则显示今日数据。
    与 generate_dashboard.py 保持完全一致的时效判断逻辑。
    """
    today_s     = date.today().isoformat()
    yesterday_s = (date.today() - timedelta(days=1)).isoformat()
    tomorrow_s  = (date.today() + timedelta(days=1)).isoformat()

    rows = _load_csv()

    def get(source, price_date, require_fresh=False):
        matched = [r for r in rows
                   if r["source"] == source and r["price_date"] == price_date]
        if not matched:
            return None
        r = max(matched, key=lambda x: x["scraped_at"])
        if require_fresh and not _scraped_today(r["scraped_at"]):
            return None
        return r

    print("\n" + "═" * 56)
    print(f"  多伦多油价汇总  —  {today_s}")
    print("═" * 56)

    # GasWizard
    print("\n【GasWizard】")
    for lbl, tgt, fresh in [("tomorrow", tomorrow_s, True),
                             ("today",    today_s,    False),
                             ("yesterday",yesterday_s,False)]:
        r = get("gaswizard", tgt, fresh)
        if r:
            reg  = _val(r["regular"])
            chg  = _val(r["regular_chg"])
            pre  = _val(r["premium"])
            die  = _val(r["diesel"])
            def fc(v):
                if v is None: return ""
                if v == 0:    return " (n/c)"
                return f" ({v:+.0f}¢)"
            print(f"  {lbl:10s} {r['price_date']}  "
                  f"普通={reg}{fc(chg)}  高级={pre}  柴油={die}")
        else:
            print(f"  {lbl:10s} 暂无预测")

    # Stockr
    print("\n【Stockr】")
    for lbl, tgt in [("today", today_s), ("tomorrow", tomorrow_s)]:
        r = get("stockr", tgt, require_fresh=True)
        if r and _val(r["regular"]) is not None:
            print(f"  {lbl:10s} {r['price_date']}  {_val(r['regular'])} cents/L")
        else:
            print(f"  {lbl:10s} 暂无预测")

    # CityNews
    print("\n【CityNews】")
    cn_rows = [r for r in rows
               if r["source"] == "citynews"
               and r["price_date"] >= today_s
               and r["label"] != "history"]
    if cn_rows:
        r = max(cn_rows, key=lambda x: x["scraped_at"])
        if _scraped_today(r["scraped_at"]):
            print(f"  {r['summary'] or '（无摘要）'}")
        else:
            print("  暂无预测")
    else:
        print("  暂无预测")

    # CSV 记录总数
    total = len(rows)
    gw_cnt = sum(1 for r in rows if r["source"] == "gaswizard")
    sk_cnt = sum(1 for r in rows if r["source"] == "stockr")
    cn_cnt = sum(1 for r in rows if r["source"] == "citynews")
    print(f"\n【CSV 记录】总计={total}  GasWizard={gw_cnt}  Stockr={sk_cnt}  CityNews={cn_cnt}")
    print("═" * 56 + "\n")


# ═══════════════════════════════════════════════════════════════════════════
#  主入口
# ═══════════════════════════════════════════════════════════════════════════

def main():
    import sys

    log.info("=" * 50)
    log.info(f"开始抓取  {datetime.now()}")
    log.info("=" * 50)

    scrape_gaswizard()
    scrape_stockr()
    scrape_citynews()

    print_summary()
    log.info(f"全部完成。数据已存入 {CSV_PATH.resolve()}")


if __name__ == "__main__":
    main()
