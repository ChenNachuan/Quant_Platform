# -*- coding: utf-8 -*-
"""Fetch report-period 1-minute bars for all listed TL contracts.

Run from the project root:
docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_tl_min1.py
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pandas as pd

from fetch_utils import (
    _process_and_flush,
    fetch_batch_with_retry,
    is_month_complete,
    mark_month_complete,
    month_range,
)

DATA_DIR = Path("/data/future/market_data/1min_tl")
TL_CONTRACTS = [
    "TL2306.CFE",
    "TL2309.CFE",
    "TL2312.CFE",
    "TL2403.CFE",
    "TL2406.CFE",
    "TL2409.CFE",
    "TL2412.CFE",
    "TL2503.CFE",
    "TL2506.CFE",
    "TL2509.CFE",
    "TL2512.CFE",
    "TL2603.CFE",
    "TL2606.CFE",
    "TL2609.CFE",
]
ACTIVE_MONTHS = {
    "TL2306.CFE": ((2023, 4), (2023, 6)),
    "TL2309.CFE": ((2023, 4), (2023, 9)),
    "TL2312.CFE": ((2023, 4), (2023, 12)),
    "TL2403.CFE": ((2023, 6), (2024, 3)),
    "TL2406.CFE": ((2023, 9), (2024, 6)),
    "TL2409.CFE": ((2023, 12), (2024, 9)),
    "TL2412.CFE": ((2024, 3), (2024, 12)),
    "TL2503.CFE": ((2024, 6), (2025, 3)),
    "TL2506.CFE": ((2024, 9), (2025, 6)),
    "TL2509.CFE": ((2024, 12), (2025, 9)),
    "TL2512.CFE": ((2025, 3), (2025, 12)),
    "TL2603.CFE": ((2025, 6), (2025, 12)),
    "TL2606.CFE": ((2025, 9), (2025, 12)),
    "TL2609.CFE": ((2025, 12), (2025, 12)),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def active_contracts(year: int, month: int) -> list[str]:
    current = (int(year), int(month))
    return [
        code
        for code in TL_CONTRACTS
        if ACTIVE_MONTHS[code][0] <= current <= ACTIVE_MONTHS[code][1]
    ]


def fetch_daily_by_month(market_data, calendar, period) -> None:
    """Use daily API calls to avoid long-running monthly 1-minute queries."""
    end_date = 20251231
    for year, month, begin, end in month_range(2023, 4, 2025, 12, end_date):
        if is_month_complete(DATA_DIR, year, month):
            continue
        codes = active_contracts(year, month)
        logger.info("[%d-%02d] %d active contracts", year, month, len(codes))
        frames_by_code: dict[str, list[pd.DataFrame]] = {code: [] for code in codes}
        failed_days: list[int] = []
        for day in pd.date_range(str(begin), str(end)):
            day_int = int(day.strftime("%Y%m%d"))
            if day_int not in calendar:
                continue
            result = fetch_batch_with_retry(
                market_data=market_data,
                batch=codes,
                begin=day_int,
                end=day_int,
                period=period,
                batch_idx=0,
                total_batches=1,
                max_retries=3,
                retry_delay=2,
                request_timeout=30,
            )
            if not result:
                failed_days.append(day_int)
                continue
            for code, frame in result.items():
                if frame is not None and not frame.empty:
                    frames_by_code.setdefault(code, []).append(frame)
            time.sleep(0.1)

        combined = {
            code: pd.concat(frames, ignore_index=True)
            for code, frames in frames_by_code.items()
            if frames
        }
        output = DATA_DIR / f"year={year}" / f"month={month:02d}.parquet"
        rows = _process_and_flush(combined, year, month, output)
        if failed_days:
            raise RuntimeError(f"{year}-{month:02d} failed trading days: {failed_days}")
        if rows <= 0:
            raise RuntimeError(f"{year}-{month:02d} returned no TL minute bars")
        mark_month_complete(DATA_DIR, year, month)
        logger.info("[%d-%02d] completed with %d rows", year, month, rows)


def main() -> None:
    import AmazingData as ad
    from AmazingData import BaseData, MarketData

    username = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    ad.login(username=username, password=password, host=host, port=int(port))
    logger.info("AmazingData login succeeded")
    try:
        calendar = BaseData().get_calendar()
        market_data = MarketData(calendar)
        fetch_daily_by_month(
            market_data=market_data,
            calendar=calendar,
            period=ad.constant.Period.min1.value,
        )
    finally:
        try:
            ad.logout(username=username)
            logger.info("AmazingData logout succeeded")
        except Exception:
            logger.exception("AmazingData logout failed")


if __name__ == "__main__":
    main()
