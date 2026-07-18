# -*- coding: utf-8 -*-
"""
股票Level-1快照数据增量拉取（按月获取，避免 OOM）

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_snapshot.py
"""

import os
import logging
from pathlib import Path
from datetime import datetime

import time
import pandas as pd

from fetch_utils import (
    month_range,
    is_month_complete,
    mark_month_complete,
    safe_parse_batch_result,
    save_failed_codes,
    load_failed_codes,
    clear_failed_codes,
)

STOCK_DATA_DIR = Path("/data/stock/snapshot/1d")
FAILED_CODES_DIR = STOCK_DATA_DIR / "failed_codes"

START_YEAR, START_MONTH = 2020, 1
BATCH_SIZE = 100

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
        codes = base_data.get_code_list(security_type="EXTRA_STOCK_A_SH_SZ")
        logger.info(f"获取到 {len(codes)} 个股票代码")

        if not codes:
            logger.error("未获取到任何股票代码，退出")
            return

        now = datetime.now()
        end_year, end_month = now.year, now.month
        today_int = int(now.strftime("%Y%m%d"))
        logger.info(
            f"数据范围: {START_YEAR}-{START_MONTH:02d} 到 {end_year}-{end_month:02d} (今天: {today_int})"
        )

        STOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)
        FAILED_CODES_DIR.mkdir(parents=True, exist_ok=True)
        total_rows = 0
        total_batches = 0
        failed_batches = 0
        skipped_months = 0

        for y, m, begin, end in month_range(
            START_YEAR, START_MONTH, end_year, end_month, today_int
        ):
            is_current_month = (y, m) == (end_year, end_month)
            if not is_current_month and is_month_complete(STOCK_DATA_DIR, y, m):
                skipped_months += 1
                continue

            logger.info(f"[{y}-{m:02d}] 拉取 {begin}~{end} ...")
            month_start_time = time.time()

            try:
                # 优先重试上次失败的代码
                result = {}
                batch_failed_codes = []
                prev_failed = load_failed_codes(FAILED_CODES_DIR, y, m)
                already_fetched = set()

                if prev_failed:
                    retry_batches = (len(prev_failed) + BATCH_SIZE - 1) // BATCH_SIZE
                    logger.info(
                        f"[{y}-{m:02d}] 优先重试 {len(prev_failed)} 个历史失败代码，分 {retry_batches} 批次"
                    )
                    for i in range(0, len(prev_failed), BATCH_SIZE):
                        batch = prev_failed[i : i + BATCH_SIZE]
                        batch_idx = i // BATCH_SIZE
                        total_batches += 1
                        try:
                            batch_result = market_data.query_snapshot(
                                batch,
                                begin_date=begin,
                                end_date=end,
                            )
                            parsed = safe_parse_batch_result(batch_result, batch)
                            if not parsed:
                                failed_batches += 1
                                batch_failed_codes.extend(batch)
                            else:
                                result.update(parsed)
                                already_fetched.update(parsed.keys())
                        except Exception as e:
                            logger.warning(f"  重试批次失败: {e}")
                            failed_batches += 1
                            batch_failed_codes.extend(batch)
                        if batch_idx < retry_batches - 1:
                            time.sleep(1)

                # 全量拉取，跳过已重试的代码
                remaining_codes = [c for c in codes if c not in already_fetched]
                num_batches = (len(remaining_codes) + BATCH_SIZE - 1) // BATCH_SIZE
                logger.info(
                    f"[{y}-{m:02d}] 全量拉取 {len(remaining_codes)} 个股票（跳过已重试 {len(already_fetched)} 个），分 {num_batches} 批次处理"
                )

                for i in range(0, len(remaining_codes), BATCH_SIZE):
                    batch = remaining_codes[i : i + BATCH_SIZE]
                    batch_idx = i // BATCH_SIZE
                    total_batches += 1

                    try:
                        batch_result = market_data.query_snapshot(
                            batch,
                            begin_date=begin,
                            end_date=end,
                        )
                        parsed = safe_parse_batch_result(batch_result, batch)
                        if not parsed:
                            failed_batches += 1
                            batch_failed_codes.extend(batch)
                            continue
                        result.update(parsed)
                    except Exception as e:
                        logger.warning(f"  批次 {batch_idx + 1} 失败: {e}")
                        failed_batches += 1
                        batch_failed_codes.extend(batch)
                        continue

                    if batch_idx < num_batches - 1:
                        time.sleep(1)

                # 更新失败代码记录
                if batch_failed_codes:
                    save_failed_codes(FAILED_CODES_DIR, y, m, batch_failed_codes)
                else:
                    clear_failed_codes(FAILED_CODES_DIR, y, m)

                # 处理数据
                t1 = time.time()
                frames = []
                for code, df in result.items():
                    if df is None or df.empty:
                        continue

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

                    mask = (df["timestamp"].dt.year == y) & (
                        df["timestamp"].dt.month == m
                    )
                    df = df[mask]
                    if df.empty:
                        continue

                    if "code" not in df.columns:
                        df["code"] = code

                    frames.append(df)

                # 保存数据
                if frames:
                    new_data = pd.concat(frames, ignore_index=True)

                    year_dir = STOCK_DATA_DIR / f"year={y}"
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
                    f"网络 {t1 - month_start_time:.1f}s | 处理 {t2 - t1:.1f}s"
                )

                if not is_current_month and not batch_failed_codes and month_rows > 0:
                    mark_month_complete(STOCK_DATA_DIR, y, m)

            except Exception as e:
                logger.error(f"[{y}-{m:02d}] 失败: {e}", exc_info=True)

        logger.info(f"\n{'=' * 60}")
        logger.info(
            f"完成 | 总写入: {total_rows} 行 | 跳过已完成月份: {skipped_months}"
        )
        logger.info(f"总批次数: {total_batches} | 失败批次: {failed_batches}")
        if total_batches > 0:
            logger.info(
                f"成功率: {(total_batches - failed_batches) / total_batches * 100:.1f}%"
            )
        logger.info(f"{'=' * 60}")

    finally:
        try:
            ad.logout(username=os.getenv("USER"))
            logger.info("已登出")
        except Exception:
            pass


if __name__ == "__main__":
    main()
