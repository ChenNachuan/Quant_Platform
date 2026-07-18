"""Aggregate local CFFEX tick snapshots into report-period TL 1-minute bars."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TICK_ROOT = PROJECT_ROOT / "data_lake/future/market_data/tick_l2"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data_lake/future/market_data/1min_tl_rebuilt"


def month_keys() -> list[tuple[int, int]]:
    months = pd.period_range("2023-04", "2025-12", freq="M")
    return [(period.year, period.month) for period in months]


def aggregate_month(paths: list[str]) -> pd.DataFrame:
    query = """
        WITH ticks AS (
            SELECT
                code,
                timestamp,
                trading_date,
                last_price,
                greatest(
                    coalesce(
                        volume - lag(volume) OVER (
                            PARTITION BY trading_date, code ORDER BY timestamp
                        ),
                        volume
                    ),
                    0
                ) AS volume_delta,
                greatest(
                    coalesce(
                        turnover - lag(turnover) OVER (
                            PARTITION BY trading_date, code ORDER BY timestamp
                        ),
                        turnover
                    ),
                    0
                ) AS amount_delta
            FROM read_parquet(?)
            WHERE upper(code) LIKE 'TL%'
              AND last_price > 0
              AND CAST(timestamp AS TIME) >= TIME '09:30:00'
              AND CAST(timestamp AS TIME) <= TIME '15:15:00'
        )
        SELECT
            code || '.CFE' AS code,
            date_trunc('minute', timestamp) + INTERVAL '1 minute' AS timestamp,
            arg_min(last_price, timestamp) AS open,
            max(last_price) AS high,
            min(last_price) AS low,
            arg_max(last_price, timestamp) AS close,
            CAST(sum(volume_delta) AS BIGINT) AS volume,
            sum(amount_delta) AS amount
        FROM ticks
        GROUP BY code, date_trunc('minute', timestamp)
        ORDER BY timestamp, code
    """
    connection = duckdb.connect()
    try:
        return connection.execute(query, [paths]).fetchdf()
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tick-root", type=Path, default=DEFAULT_TICK_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    for year, month in month_keys():
        output_dir = args.output_root / f"year={year}"
        output = output_dir / f"month={month:02d}.parquet"
        success = output_dir / f"month={month:02d}_SUCCESS"
        if output.exists() and success.exists() and not args.force:
            print(f"[{year}-{month:02d}] skip completed month")
            continue
        paths = sorted(
            str(path)
            for path in (args.tick_root / f"year={year}" / f"month={month:02d}").glob(
                "CFFEX_*.parquet"
            )
        )
        if not paths:
            raise FileNotFoundError(f"No CFFEX tick files for {year}-{month:02d}")
        bars = aggregate_month(paths)
        if bars.empty:
            raise ValueError(f"No TL ticks found for {year}-{month:02d}")
        bars["year"] = year
        output_dir.mkdir(parents=True, exist_ok=True)
        bars.to_parquet(output, index=False)
        success.touch()
        print(
            f"[{year}-{month:02d}] {len(paths)} days, "
            f"{bars['code'].nunique()} contracts, {len(bars)} bars"
        )


if __name__ == "__main__":
    main()
