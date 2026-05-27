# -*- coding: utf-8 -*-
"""
获取历史证券信息（涨跌停、ST、除权除息等）

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_status.py
"""

import os
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

STOCK_STATUS_DIR = Path("/data/stock/basedata/status")
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
        info_data = InfoData()
        STOCK_STATUS_DIR.mkdir(parents=True, exist_ok=True)

        # 按年拉取，避免单次请求过大
        now = datetime.now()
        total_rows = 0

        for year in range(2013, now.year + 1):
            begin = year * 10000 + 101
            end = (
                year * 10000 + 1231 if year < now.year else int(now.strftime("%Y%m%d"))
            )

            logger.info(f"[{year}] 拉取 {begin}~{end} ...")

            all_code_list = base_data.get_hist_code_list(
                security_type="EXTRA_STOCK_A_SH_SZ",
                start_date=begin,
                end_date=20131231,
                local_path=str(HIST_CODE_PATH),
            )
            logger.info(f"[{year}] 代码数: {len(all_code_list)}")

            if not all_code_list:
                continue

            result = info_data.get_history_stock_status(
                all_code_list,
                str(STOCK_STATUS_DIR),
                is_local=True,
                begin_date=begin,
                end_date=end,
            )

            if isinstance(result, dict):
                dfs = [
                    df
                    for df in result.values()
                    if hasattr(df, "shape") and not df.empty
                ]
                if dfs:
                    merged = pd.concat(dfs, ignore_index=True)

                    out = STOCK_STATUS_DIR / f"year={year}.parquet"
                    merged.to_parquet(out, engine="pyarrow", index=False)
                    total_rows += len(merged)
                    logger.info(f"[{year}] 写入 {out}: {len(merged)} 行")
                else:
                    logger.warning(f"[{year}] 未获取到数据")
            else:
                logger.warning(f"[{year}] 返回类型: {type(result)}")

        logger.info(f"\n完成 | 总写入: {total_rows} 行")

    finally:
        try:
            ad.logout(username=os.getenv("USER"))
        except Exception:
            pass


if __name__ == "__main__":
    main()
