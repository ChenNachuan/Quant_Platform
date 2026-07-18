# -*- coding: utf-8 -*-
"""
股票日线数据增量拉取（按月获取，避免 OOM）

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_daily.py
"""

import os
import logging
from pathlib import Path
from datetime import datetime


from fetch_utils import fetch_months

STOCK_DAILY_DIR = Path("/data/stock/market_data/1d")
FAILED_CODES_DIR = STOCK_DAILY_DIR / "failed_codes"
HIST_CODE_PATH = "/data/stock/"

START_YEAR, START_MONTH = 2013, 1
BATCH_SIZE = 500

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

        # 获取股票代码
        Path(HIST_CODE_PATH).mkdir(parents=True, exist_ok=True)
        now = datetime.now()
        hist = base_data.get_hist_code_list(
            security_type="EXTRA_STOCK_A_SH_SZ",
            start_date=20130101,
            end_date=int(now.strftime("%Y%m%d")),
            local_path=HIST_CODE_PATH,
        )
        codes = hist[max(hist.keys())] if isinstance(hist, dict) else list(hist)
        logger.info(f"获取到 {len(codes)} 个股票代码")

        fetch_months(
            market_data=market_data,
            codes=codes,
            period=ad.constant.Period.day.value,
            data_dir=STOCK_DAILY_DIR,
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
