# -*- coding: utf-8 -*-
"""
期货日线数据增量拉取（按月获取，避免 OOM）

支持的交易所：
- CFFEX: 中金所
- SHFE: 上期所
- DCE: 大商所
- CZCE: 郑商所
- INE: 上海国际能源交易中心

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_future_daily.py
"""

import os
import logging
from pathlib import Path
from datetime import datetime

import time

from fetch_utils import fetch_months

FUTURE_DAILY_DIR = Path("/data/future/market_data/1d")
FAILED_CODES_DIR = FUTURE_DAILY_DIR / "failed_codes"
HIST_CODE_PATH = "/data/future/hist_code/"

START_YEAR, START_MONTH = 2010, 4
BATCH_SIZE = 200

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

        # 获取期货历史代码
        Path(HIST_CODE_PATH).mkdir(parents=True, exist_ok=True)
        now = datetime.now()
        logger.info("正在获取历史代码表（可能需要几分钟）...")
        t0 = time.time()
        codes = base_data.get_hist_code_list(
            security_type="EXTRA_FUTURE",
            start_date=START_YEAR * 10000 + START_MONTH * 100 + 1,
            end_date=int(now.strftime("%Y%m%d")),
            local_path=HIST_CODE_PATH,
        )
        t1 = time.time()
        if isinstance(codes, dict):
            codes = codes[max(codes.keys())]
        logger.info(f"获取到 {len(codes)} 个期货合约代码 | 耗时 {t1 - t0:.1f}s")

        fetch_months(
            market_data=market_data,
            codes=codes,
            period=ad.constant.Period.day.value,
            data_dir=FUTURE_DAILY_DIR,
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
