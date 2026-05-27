# -*- coding: utf-8 -*-
"""
获取股票基础信息（上市日期、退市日期、板块等）

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_basic.py
"""

import os
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

STOCK_BASIC_DIR = Path("/data/stock/basedata/stock_basic")
HIST_CODE_PATH = "/data/stock/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    import AmazingData as ad
    from AmazingData import BaseData, InfoData

    username = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = os.getenv("PORT")

    ad.login(username=username, password=password, host=host, port=int(port))
    logger.info(f"已登录 ({host}:{port})")

    try:
        base_data = BaseData()

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

        # 获取基础信息
        info_data = InfoData()
        stock_basic = info_data.get_stock_basic(codes)
        logger.info(f"返回类型: {type(stock_basic)}")

        if isinstance(stock_basic, dict):
            # dict[code -> DataFrame]，合并
            dfs = [
                df
                for df in stock_basic.values()
                if hasattr(df, "shape") and not df.empty
            ]
            if dfs:
                stock_basic = pd.concat(dfs, ignore_index=True)
            else:
                stock_basic = pd.DataFrame()

        if hasattr(stock_basic, "shape") and not stock_basic.empty:
            logger.info(
                f"shape: {stock_basic.shape}, cols: {list(stock_basic.columns)}"
            )
            STOCK_BASIC_DIR.mkdir(parents=True, exist_ok=True)
            out = STOCK_BASIC_DIR / "stock_basic.parquet"
            stock_basic.to_parquet(out, engine="pyarrow", index=False)
            logger.info(f"写入 {out}: {len(stock_basic)} 行")
        else:
            logger.warning("未获取到数据")

    finally:
        try:
            ad.logout(username=os.getenv("USER"))
        except Exception:
            pass


if __name__ == "__main__":
    main()
