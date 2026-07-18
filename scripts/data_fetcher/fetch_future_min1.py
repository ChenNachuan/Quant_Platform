# -*- coding: utf-8 -*-
"""
期货1分钟K线数据全量拉取（按天获取，按月存储，便于观察进度）

支持的交易所：
- CFFEX: 中金所
- SHFE: 上期所
- DCE: 大商所
- CZCE: 郑商所
- INE: 上海国际能源交易中心

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_future_min1.py
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta

import time
import pandas as pd

from fetch_utils import safe_parse_batch_result

# 期货数据目录
FUTURE_DATA_DIR = Path("/data/future/market_data/1min")
# 历史代码表本地缓存路径
HIST_CODE_PATH = "/data/future/hist_code/"

# 数据起始日期（期货数据从2010年4月开始）
START_YEAR, START_MONTH, START_DAY = 2010, 4, 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def each_day(start_date, end_date):
    """生成每天的 (date_int, year, month, day) 序列"""
    d = start_date
    while d <= end_date:
        yield d.year * 10000 + d.month * 100 + d.day, d.year, d.month, d.day
        d += timedelta(days=1)


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
        logger.info("正在获取历史代码表...")
        t0 = time.time()
        codes = base_data.get_hist_code_list(
            security_type="EXTRA_FUTURE",
            start_date=START_YEAR * 10000 + START_MONTH * 100 + START_DAY,
            end_date=int(datetime.now().strftime("%Y%m%d")),
            local_path=HIST_CODE_PATH,
        )
        t1 = time.time()
        if isinstance(codes, dict):
            codes = codes[max(codes.keys())]
        logger.info(f"获取到 {len(codes)} 个期货合约代码 | 耗时 {t1 - t0:.1f}s")

        # 按天拉取，按月存储
        FUTURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
        start_date = datetime(START_YEAR, START_MONTH, START_DAY)
        end_date = datetime.now()

        total_rows = 0
        total_days = 0
        month_frames = []  # 当月累积数据
        cur_month_key = None  # (year, month)

        for date_int, y, m, d in each_day(start_date, end_date):
            # 跳过非交易日
            if date_int not in calendar:
                continue

            # 月份切换时，写入上月数据并清空缓存
            month_key = (y, m)
            if cur_month_key is not None and month_key != cur_month_key:
                if month_frames:
                    new_data = pd.concat(month_frames, ignore_index=True)
                    py, pm = cur_month_key
                    year_dir = FUTURE_DATA_DIR / f"year={py}"
                    year_dir.mkdir(parents=True, exist_ok=True)
                    out = year_dir / f"month={pm:02d}.parquet"

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
                    logger.info(
                        f"=== 月份 {cur_month_key[0]}-{cur_month_key[1]:02d} "
                        f"写入完成，共 {len(merged)} 行 ==="
                    )
                month_frames = []

            cur_month_key = month_key

            # 拉取当天数据（分批调用，避免单次请求过大）
            t0 = time.time()
            try:
                BATCH_SIZE = 10
                MAX_RETRIES = 3
                RETRY_DELAY = 5
                result = {}
                num_batches = (len(codes) + BATCH_SIZE - 1) // BATCH_SIZE
                for i in range(0, len(codes), BATCH_SIZE):
                    batch = codes[i : i + BATCH_SIZE]
                    batch_idx = i // BATCH_SIZE
                    for attempt in range(MAX_RETRIES):
                        try:
                            batch_result = market_data.query_kline(
                                batch,
                                begin_date=date_int,
                                end_date=date_int,
                                period=ad.constant.Period.min1.value,
                            )
                            parsed = safe_parse_batch_result(batch_result, batch)
                            if parsed:
                                result.update(parsed)
                            break
                        except Exception as e:
                            if attempt < MAX_RETRIES - 1:
                                logger.warning(
                                    f"  批次 {batch_idx + 1}/{num_batches} 重试 {attempt + 1}: {e}"
                                )
                                time.sleep(RETRY_DELAY)
                            else:
                                logger.error(
                                    f"  批次 {batch_idx + 1}/{num_batches} 失败: {e}"
                                )
                    # 批次间休息，避免服务端限流
                    if batch_idx < num_batches - 1:
                        time.sleep(1)
                t1 = time.time()

                day_rows = 0
                for code, df in result.items():
                    if df is None or df.empty:
                        continue

                    # 处理时间列
                    if "kline_time" in df.columns:
                        df.rename(columns={"kline_time": "timestamp"}, inplace=True)
                    elif "trade_time" in df.columns:
                        df.rename(columns={"trade_time": "timestamp"}, inplace=True)
                    elif "timestamp" not in df.columns:
                        if isinstance(df.index, pd.DatetimeIndex):
                            df = df.reset_index()

                    if "timestamp" not in df.columns:
                        continue

                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)
                    if df["timestamp"].dt.tz is not None:
                        df["timestamp"] = (
                            df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
                        )
                    df["timestamp"] = df["timestamp"].astype("datetime64[ns]")

                    # 过滤到当天（防止返回相邻日期数据）
                    mask = (
                        (df["timestamp"].dt.year == y)
                        & (df["timestamp"].dt.month == m)
                        & (df["timestamp"].dt.day == d)
                    )
                    df = df[mask]
                    if df.empty:
                        continue

                    if "code" not in df.columns:
                        df["code"] = code

                    day_rows += len(df)
                    month_frames.append(df)

                t2 = time.time()
                total_rows += day_rows
                total_days += 1
                logger.info(
                    f"[{y}-{m:02d}-{d:02d}] {day_rows} 行 | "
                    f"网络 {t1 - t0:.1f}s | 处理 {t2 - t1:.1f}s | "
                    f"累计 {total_days} 天 {total_rows} 行"
                )

            except Exception as e:
                logger.warning(f"[{y}-{m:02d}-{d:02d}] 失败: {e}")

            # 每天之间休息 1 秒，避免服务端限流
            time.sleep(1)

        # 写入最后一个月
        if month_frames:
            new_data = pd.concat(month_frames, ignore_index=True)
            py, pm = cur_month_key
            year_dir = FUTURE_DATA_DIR / f"year={py}"
            year_dir.mkdir(parents=True, exist_ok=True)
            out = year_dir / f"month={pm:02d}.parquet"

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
            logger.info(f"=== 月份 {py}-{pm:02d} 写入完成，共 {len(merged)} 行 ===")

        logger.info(f"\n全部完成 | 总天数: {total_days} | 总行数: {total_rows}")

    finally:
        try:
            ad.logout(username=os.getenv("USER"))
        except Exception:
            pass


if __name__ == "__main__":
    main()
