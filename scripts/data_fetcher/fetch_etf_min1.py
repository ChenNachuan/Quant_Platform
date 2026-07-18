# -*- coding: utf-8 -*-
"""
ETF 1分钟K线数据增量拉取（按月获取，避免 OOM）

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_etf_min1.py
"""

import os
import logging
from pathlib import Path


from fetch_utils import fetch_months

ETF_DATA_DIR = Path("/data/etf/market_data/min1")
FAILED_CODES_DIR = ETF_DATA_DIR / "failed_codes"

START_YEAR, START_MONTH = 2025, 1
BATCH_SIZE = 50

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

        # 获取ETF代码
        codes = base_data.get_code_list(security_type="EXTRA_ETF")
        logger.info(f"获取到 {len(codes)} 个ETF代码")

        fetch_months(
            market_data=market_data,
            codes=codes,
            period=ad.constant.Period.min1.value,
            data_dir=ETF_DATA_DIR,
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
