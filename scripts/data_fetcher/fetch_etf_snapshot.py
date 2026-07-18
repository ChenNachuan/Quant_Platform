# -*- coding: utf-8 -*-
"""
ETF Level-1快照数据全量拉取（按月获取，避免 OOM）

docker run --rm \
  -v ./data_lake/etf:/data/etf \
  --env-file ~/AmazingData/docker/.env \
  --entrypoint python3 \
  docker-amazingdata \
  /app/scripts/data_fetcher/fetch_etf_snapshot.py
"""

import os
import logging
from pathlib import Path
from calendar import monthrange

import time
import pandas as pd

# ETF快照数据目录
ETF_DATA_DIR = Path("/data/etf/snapshot/1d")

# 数据起始日期
START_YEAR, START_MONTH = 2020, 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def month_range(start_year, start_month, end_year, end_month):
    """生成 (year, month, begin_date_int, end_date_int) 序列"""
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        _, days_in_month = monthrange(y, m)
        begin = y * 10000 + m * 100 + 1
        end = y * 10000 + m * 100 + days_in_month
        yield y, m, begin, end
        m += 1
        if m > 12:
            m = 1
            y += 1


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

        # 按月拉取
        ETF_DATA_DIR.mkdir(parents=True, exist_ok=True)
        total_rows = 0

        for y, m, begin, end in month_range(START_YEAR, START_MONTH, 2026, 6):
            logger.info(f"[{y}-{m:02d}] 拉取 {begin}~{end} ...")
            try:
                t0 = time.time()
                BATCH_SIZE = 100
                result = {}
                for i in range(0, len(codes), BATCH_SIZE):
                    batch = codes[i : i + BATCH_SIZE]
                    batch_result = market_data.query_snapshot(
                        batch,
                        begin_date=begin,
                        end_date=end,
                    )
                    if isinstance(batch_result, dict):
                        result.update(batch_result)
                    else:
                        result[batch[0]] = batch_result
                t1 = time.time()

                frames = []
                for code, df in result.items():
                    if df is None or df.empty:
                        continue

                    # 处理时间列
                    if "trade_time" in df.columns:
                        df.rename(columns={"trade_time": "timestamp"}, inplace=True)
                    elif "timestamp" not in df.columns:
                        if isinstance(df.index, pd.DatetimeIndex):
                            df = df.reset_index()

                    if "timestamp" not in df.columns:
                        logger.warning(f"[{code}] 缺少时间列，跳过")
                        continue

                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)
                    if df["timestamp"].dt.tz is not None:
                        df["timestamp"] = (
                            df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
                        )
                    df["timestamp"] = df["timestamp"].astype("datetime64[ns]")

                    # 过滤到当月
                    mask = (df["timestamp"].dt.year == y) & (
                        df["timestamp"].dt.month == m
                    )
                    df = df[mask]
                    if df.empty:
                        continue

                    if "code" not in df.columns:
                        df["code"] = code

                    frames.append(df)

                if frames:
                    new_data = pd.concat(frames, ignore_index=True)

                    year_dir = ETF_DATA_DIR / f"year={y}"
                    year_dir.mkdir(parents=True, exist_ok=True)
                    out = year_dir / f"month={m:02d}.parquet"

                    if out.exists():
                        existing = pd.read_parquet(out)
                        merged = pd.concat(
                            [existing, new_data], ignore_index=True
                        ).drop_duplicates(subset=["code", "timestamp"], keep="last")
                    else:
                        merged = new_data.drop_duplicates(
                            subset=["code", "timestamp"], keep="last"
                        )

                    merged.to_parquet(out, engine="pyarrow", index=False)
                    month_rows = len(new_data)
                else:
                    month_rows = 0

                t2 = time.time()
                total_rows += month_rows
                logger.info(
                    f"[{y}-{m:02d}] 写入 {month_rows} 行 | "
                    f"网络 {t1 - t0:.1f}s | 处理 {t2 - t1:.1f}s"
                )

            except Exception as e:
                logger.warning(f"[{y}-{m:02d}] 失败: {e}")

        logger.info(f"\n完成 | 总写入: {total_rows} 行")

    finally:
        try:
            ad.logout(username=os.getenv("USER"))
        except Exception:
            pass


if __name__ == "__main__":
    main()
