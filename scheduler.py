"""
scheduler.py  —  每日自动抓取调度器
=====================================
每天下午 1:00 自动运行一次抓取（stockr 在下午1-2点更新预测）。

用法:
  python scheduler.py           # 前台运行
  nohup python scheduler.py &   # 后台运行（Linux/macOS）

Windows 任务计划也可以直接调用 scraper.py，不需要此脚本。
"""

import time
import logging
import subprocess
import sys
from datetime import datetime

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

# 每天触发时间（24小时制）
TRIGGER_HOUR = 13
TRIGGER_MINUTE = 15


def run_scraper():
    log.info("▶ 开始每日抓取任务 ...")
    result = subprocess.run(
        [sys.executable, "scraper.py"],
        capture_output=False,
    )
    if result.returncode == 0:
        log.info("✓ 抓取成功完成")
    else:
        log.error(f"✗ 抓取失败，退出码: {result.returncode}")
        # 尝试 Playwright 备用
        log.info("  尝试 Playwright 备用方案 ...")
        subprocess.run([sys.executable, "scraper_playwright.py"])


def main():
    log.info(f"调度器启动，每天 {TRIGGER_HOUR:02d}:{TRIGGER_MINUTE:02d} 运行抓取")
    last_run_date = None

    while True:
        now = datetime.now()
        today = now.date()

        if (
            now.hour == TRIGGER_HOUR
            and now.minute >= TRIGGER_MINUTE
            and last_run_date != today
        ):
            run_scraper()
            last_run_date = today

        # 每分钟检查一次
        time.sleep(60)


if __name__ == "__main__":
    main()
