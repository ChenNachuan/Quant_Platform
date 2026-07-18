# -*- coding: utf-8 -*-
"""
股票1分钟K线数据增量拉取（按月获取，避免 OOM）

功能说明：
1. 增量拉取：跳过已完整下载的历史月份，只拉取当前月和失败月
2. 断点续传：每月写入 _SUCCESS 标记，下次运行自动跳过
3. 失败记录：失败的股票代码落盘到 failed_codes/ 目录，便于针对性重试
4. 安全批处理：query_kline 返回非 dict 时按 code 列拆分，避免数据丢失
5. 精确结束日期：当月数据只拉到今天，不请求未来交易日
6. 防除零保护：空代码列表不会导致运行时错误

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_min1.py
"""

import os
import logging
from pathlib import Path


from fetch_utils import fetch_months

STOCK_DATA_DIR = Path("/data/stock/market_data/min1")
FAILED_CODES_DIR = STOCK_DATA_DIR / "failed_codes"
HIST_CODE_PATH = "/data/stock/"

START_YEAR, START_MONTH = 2025, 1
BATCH_SIZE = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    import AmazingData as ad
    from AmazingData import BaseData, MarketData

    username = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = os.getenv("PORT")

    ad.login(username=username, password=password, host=host, port=int(port))
    logger.info(f"已登录 ({host}:{port})")

    try:
        base_data = BaseData()
        calendar = base_data.get_calendar()
        market_data = MarketData(calendar)

        # 获取股票代码（当前活跃）
        codes = base_data.get_code_list(security_type="EXTRA_STOCK_A_SH_SZ")
        logger.info(f"获取到 {len(codes)} 个股票代码")

        fetch_months(
            market_data=market_data,
            codes=codes,
            period=ad.constant.Period.min1.value,
            data_dir=STOCK_DATA_DIR,
            failed_dir=FAILED_CODES_DIR,
            start_year=START_YEAR,
            start_month=START_MONTH,
            batch_size=BATCH_SIZE,
        )

    finally:
        try:
            ad.logout(username=os.getenv("USER"))
            logger.info("已登出")
        except Exception:
            pass


if __name__ == "__main__":
    main()
