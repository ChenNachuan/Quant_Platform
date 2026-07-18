"""Utilities for constructing a volume-dominant adjusted futures series."""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PRICE_COLUMNS = ["open", "high", "low", "close", "vwap"]


def _contract_symbol(code: str) -> str:
    match = re.match(r"([A-Za-z]+)", str(code))
    return match.group(1).upper() if match else ""


def load_contract_bars(
    data_root: str | Path,
    symbol: str,
    start: str,
    end: str,
    contract_multiplier: float = 10_000.0,
) -> pd.DataFrame:
    """Load one product's raw contract bars with DuckDB predicate pushdown."""
    data_root = Path(data_root)
    paths = sorted(str(path) for path in data_root.glob("year=*/month=*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No monthly parquet files found under {data_root}")

    con = duckdb.connect()
    try:
        frame = con.execute(
            """
            SELECT code, timestamp, open, high, low, close, volume, amount
            FROM read_parquet(?)
            WHERE upper(regexp_extract(code, '^([A-Za-z]+)', 1)) = ?
              AND timestamp >= ? AND timestamp < ?
            ORDER BY timestamp, code
            """,
            [paths, symbol.upper(), pd.Timestamp(start), pd.Timestamp(end)],
        ).fetchdf()
    finally:
        con.close()
    if frame.empty:
        raise ValueError(f"No {symbol} bars found in {data_root} from {start} to {end}")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    frame["code"] = frame["code"].astype(str)
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(frame["amount"], errors="coerce")
    inferred_vwap = amount / (frame["volume"] * float(contract_multiplier))
    frame["vwap"] = inferred_vwap.where(
        np.isfinite(inferred_vwap) & (inferred_vwap > 0), frame["close"]
    )
    frame["date"] = frame["timestamp"].dt.date
    return frame


def build_volume_dominant_series(raw_bars: pd.DataFrame) -> pd.DataFrame:
    """Select each day's most active contract and ratio-adjust at rolls.

    The roll ratio uses the incoming contract's previous-trading-day close,
    matching the report's stated proportional adjustment convention.
    """
    required = {"code", "timestamp", "date", "volume", *PRICE_COLUMNS}
    missing = required.difference(raw_bars.columns)
    if missing:
        raise ValueError(f"Missing columns for dominant series: {sorted(missing)}")

    daily_volume = (
        raw_bars.groupby(["date", "code"], observed=True)["volume"]
        .sum()
        .rename("daily_volume")
        .reset_index()
    )
    dominant = daily_volume.loc[
        daily_volume.groupby("date", observed=True)["daily_volume"].idxmax()
    ].sort_values("date")
    dominant_by_date = dominant.set_index("date")["code"]

    daily_close = (
        raw_bars.sort_values("timestamp")
        .groupby(["date", "code"], observed=True)["close"]
        .last()
    )
    dates = list(dominant_by_date.index)
    pieces: list[pd.DataFrame] = []
    scale = 1.0
    previous_code: str | None = None
    previous_adjusted_close: float | None = None

    for index, date in enumerate(dates):
        code = dominant_by_date.loc[date]
        day = raw_bars[(raw_bars["date"] == date) & (raw_bars["code"] == code)].copy()
        if day.empty:
            continue
        if previous_code is not None and code != previous_code:
            previous_date = dates[index - 1]
            incoming_previous_close = daily_close.get((previous_date, code), np.nan)
            if np.isfinite(incoming_previous_close) and incoming_previous_close > 0:
                scale = float(previous_adjusted_close / incoming_previous_close)
            else:
                scale = float(previous_adjusted_close / day["open"].iloc[0])
        day["adjustment_factor"] = scale
        day["dominant_code"] = code
        for column in PRICE_COLUMNS:
            day[column] = day[column].astype(float) * scale
        previous_adjusted_close = float(day["close"].iloc[-1])
        previous_code = code
        pieces.append(day)

    if not pieces:
        raise ValueError("Dominant-contract selection produced no bars")
    result = pd.concat(pieces, ignore_index=True).sort_values("timestamp")
    return result.set_index("timestamp", drop=True)


__all__ = ["build_volume_dominant_series", "load_contract_bars"]
