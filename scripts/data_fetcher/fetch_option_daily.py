# -*- coding: utf-8 -*-
"""
期权日线数据增量拉取（按月获取，避免 OOM）

支持的期权类型：
- ETF期权（上交所 + 深交所）

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_option_daily.py
"""

import os
import logging
from pathlib import Path


from fetch_utils import fetch_months

OPTION_DATA_DIR = Path("/data/option/market_data/1d")
FAILED_CODES_DIR = OPTION_DATA_DIR / "failed_codes"

START_YEAR, START_MONTH = 2020, 1
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

        # 获取期权代码
        codes = base_data.get_option_code_list(security_type="EXTRA_ETF_OP")
        logger.info(f"获取到 {len(codes)} 个期权合约代码")

        fetch_months(
            market_data=market_data,
            codes=codes,
            period=ad.constant.Period.day.value,
            data_dir=OPTION_DATA_DIR,
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
