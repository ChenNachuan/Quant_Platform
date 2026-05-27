# -*- coding: utf-8 -*-
"""
获取后复权因子

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_backward_factor.py
"""

import os
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

OUTPUT_DIR = Path("/data/stock/basedata/backward_factor")
HIST_CODE_PATH = "/data/stock/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    import AmazingData as ad
    from AmazingData import BaseData

    username = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = os.getenv("PORT")

    ad.login(username=username, password=password, host=host, port=int(port))
    logger.info(f"已登录 ({host}:{port})")

    try:
        base_data = BaseData()
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # 获取全部代码
        HIST_CODE_PATH_STR = str(HIST_CODE_PATH)
        now = datetime.now()
        all_code_list = base_data.get_hist_code_list(
            security_type="EXTRA_STOCK_A_SH_SZ",
            start_date=20130101,
            end_date=int(now.strftime("%Y%m%d")),
            local_path=HIST_CODE_PATH_STR,
        )
        logger.info(f"代码数: {len(all_code_list)}")

        if not all_code_list:
            logger.warning("无代码，退出")
            return

        # 获取后复权因子（全量）
        result = base_data.get_backward_factor(
            all_code_list,
            local_path=HIST_CODE_PATH_STR,
            is_local=True,
        )
        logger.info(f"返回类型: {type(result).__name__}")

        if isinstance(result, pd.DataFrame) and not result.empty:
            logger.info(f"DataFrame shape: {result.shape}, index: {result.index.name}")

            # 按年拆分保存
            if result.index is not None and len(result.index) > 0:
                # index 是日期
                dates = pd.to_datetime(result.index)
                total_rows = 0

                for year in range(2013, now.year + 1):
                    mask = dates.year == year
                    if not mask.any():
                        continue
                    yearly = result.loc[mask]
                    out = OUTPUT_DIR / f"year={year}.parquet"
                    yearly.to_parquet(out, engine="pyarrow")
                    total_rows += len(yearly)
                    logger.info(f"[{year}] 写入 {out}: {len(yearly)} 行")

                logger.info(f"\n完成 | 总写入: {total_rows} 行")
            else:
                logger.warning("DataFrame index 为空")

        elif isinstance(result, dict):
            dfs = [
                df for df in result.values() if hasattr(df, "shape") and not df.empty
            ]
            if dfs:
                merged = pd.concat(dfs, ignore_index=True)
                logger.info(f"合并后 shape: {merged.shape}")
                out = OUTPUT_DIR / "backward_factor.parquet"
                merged.to_parquet(out, engine="pyarrow", index=False)
                logger.info(f"写入 {out}: {len(merged)} 行")
        else:
            logger.warning(f"未获取到数据, type={type(result)}")

    finally:
        try:
            ad.logout(username=os.getenv("USER"))
        except Exception:
            pass


if __name__ == "__main__":
    main()
