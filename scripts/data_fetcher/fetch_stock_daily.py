# -*- coding: utf-8 -*-
"""
股票日线数据全量拉取（按月获取，避免 OOM）

docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_daily.py
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from calendar import monthrange

import pandas as pd

STOCK_DAILY_DIR = Path("/data/stock/market_data/1d")
HIST_CODE_PATH = "/data/stock/"

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

        # 按月拉取
        STOCK_DAILY_DIR.mkdir(parents=True, exist_ok=True)
        total_rows = 0

        for y, m, begin, end in month_range(2013, 1, 2026, 4):
            logger.info(f"[{y}-{m:02d}] 拉取 {begin}~{end} ...")
            try:
                result = market_data.query_kline(
                    codes,
                    begin_date=begin,
                    end_date=end,
                    period=ad.constant.Period.day.value,
                )
                if not isinstance(result, dict):
                    result = {codes[0]: result}

                month_rows = 0
                for code, df in result.items():
                    if df is None or df.empty:
                        continue

                    if "kline_time" in df.columns:
                        df.rename(columns={"kline_time": "timestamp"}, inplace=True)
                    elif "timestamp" not in df.columns:
                        if isinstance(df.index, pd.DatetimeIndex):
                            df = df.reset_index()
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

                    year_dir = STOCK_DAILY_DIR / f"year={y}"
                    year_dir.mkdir(parents=True, exist_ok=True)
                    out = year_dir / f"month={m:02d}.parquet"

                    if out.exists():
                        existing = pd.read_parquet(out)
                        df = pd.concat(
                            [existing, df], ignore_index=True
                        ).drop_duplicates(subset=["code", "timestamp"], keep="last")

                    df.to_parquet(out, engine="pyarrow", index=False)
                    month_rows += len(df)

                total_rows += month_rows
                logger.info(f"[{y}-{m:02d}] 写入 {month_rows} 行")

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
