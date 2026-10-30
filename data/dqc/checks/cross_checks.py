"""DQC cross-table checks K06–K08: daily vs minute consistency.

Each function takes ``(daily_df, minute_df, **params)`` and returns
``pd.Series[bool]`` indexed by ``(ticker, trade_date)`` where **True = failure**.

These are the only rules that require two frames in one call.  Ingest-time vs
weekly-batch scheduling is S9-T3's concern (KB pin K5).
"""

from __future__ import annotations

import pandas as pd


def check_daily_vs_minute_close(
    daily_df: pd.DataFrame,
    minute_df: pd.DataFrame,
    tolerance: float = 0.01,
    **_,
) -> pd.Series:
    """K06: daily close must equal last minute close within *tolerance*."""
    last_min = (
        minute_df.sort_values("timestamp")
        .groupby(["ticker", "trade_date"])["close"]
        .last()
        .rename("minute_last_close")
    )
    merged = daily_df.set_index(["ticker", "trade_date"])[["close"]].join(
        last_min, how="inner"
    )
    diff = (merged["close"] - merged["minute_last_close"]).abs()
    return (diff > tolerance).rename("failed")


def check_daily_vs_minute_volume(
    daily_df: pd.DataFrame,
    minute_df: pd.DataFrame,
    max_rel_diff: float = 0.001,
    **_,
) -> pd.Series:
    """K07: daily volume must equal sum of minute volumes (rel diff <= *max_rel_diff*)."""
    min_sum = (
        minute_df.groupby(["ticker", "trade_date"])["volume"]
        .sum()
        .rename("minute_vol_sum")
    )
    merged = daily_df.set_index(["ticker", "trade_date"])[["volume"]].join(
        min_sum, how="inner"
    )
    denom = merged["volume"].abs().replace(0, 1)
    rel = (merged["volume"] - merged["minute_vol_sum"]).abs() / denom
    return (rel > max_rel_diff).rename("failed")


def check_daily_vs_minute_hl(
    daily_df: pd.DataFrame,
    minute_df: pd.DataFrame,
    **_,
) -> pd.Series:
    """K08: daily high >= max(minute high) and daily low <= min(minute low)."""
    agg = minute_df.groupby(["ticker", "trade_date"]).agg(
        minute_high=("high", "max"),
        minute_low=("low", "min"),
    )
    merged = daily_df.set_index(["ticker", "trade_date"])[["high", "low"]].join(
        agg, how="inner"
    )
    high_fail = merged["high"] < merged["minute_high"]
    low_fail = merged["low"] > merged["minute_low"]
    return (high_fail | low_fail).rename("failed")
