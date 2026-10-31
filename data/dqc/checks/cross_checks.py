"""DQC cross-table checks K06–K08: daily vs minute consistency.

Each function takes ``(daily_df, minute_df, **params)`` and returns
``pd.Series[bool]`` indexed by ``(ticker, trade_date)`` where **True = failure**.

These are the only rules that require two frames in one call.  Ingest-time vs
weekly-batch scheduling is S9-T3's concern (KB pin K5).

All three use ``how="left"`` on the daily frame so a ticker-day with no minute
bars is flagged as a failure rather than silently dropped — except when the
daily row is suspended (``suspend == True``), in which case zero minute bars
is the expected state and the row is exempted.

The minute timestamp column is ``datetime`` (schema name); ``timestamp`` is
accepted as a legacy fallback.
"""

from __future__ import annotations

import pandas as pd


def _dt_col(df: pd.DataFrame) -> str | None:
    for c in ("datetime", "timestamp"):
        if c in df.columns:
            return c
    return None


def _suspended_mask(daily_df: pd.DataFrame) -> pd.Series:
    """Boolean mask of suspended daily rows (all-False when no suspend column)."""
    if "suspend" in daily_df.columns:
        return daily_df["suspend"].fillna(False).astype(bool)
    return pd.Series(False, index=daily_df.index)


def check_daily_vs_minute_close(
    daily_df: pd.DataFrame,
    minute_df: pd.DataFrame,
    tolerance: float = 0.01,
    **_,
) -> pd.Series:
    """K06: daily close must equal last minute close within *tolerance*."""
    ts = _dt_col(minute_df) or "datetime"
    last_min = (
        minute_df.sort_values(ts)
        .groupby(["ticker", "trade_date"])["close"]
        .last()
        .rename("minute_last_close")
    )
    merged = daily_df.set_index(["ticker", "trade_date"])[["close"]].join(
        last_min, how="left"
    )
    diff = (merged["close"] - merged["minute_last_close"]).abs()
    missing = merged["minute_last_close"].isna()
    failed = missing | (diff > tolerance)
    # Exempt suspended daily rows (no minute bars is expected).
    sus = _suspended_mask(daily_df)
    sus.index = daily_df.set_index(["ticker", "trade_date"]).index
    return (failed & ~sus.reindex(failed.index, fill_value=False)).rename("failed")


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
        min_sum, how="left"
    )
    denom = merged["volume"].abs().replace(0, 1)
    rel = (merged["volume"] - merged["minute_vol_sum"]).abs() / denom
    missing = merged["minute_vol_sum"].isna()
    failed = missing | (rel > max_rel_diff)
    sus = _suspended_mask(daily_df)
    sus.index = daily_df.set_index(["ticker", "trade_date"]).index
    return (failed & ~sus.reindex(failed.index, fill_value=False)).rename("failed")


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
        agg, how="left"
    )
    high_fail = merged["high"] < merged["minute_high"]
    low_fail = merged["low"] > merged["minute_low"]
    missing = merged["minute_high"].isna() | merged["minute_low"].isna()
    failed = missing | high_fail | low_fail
    sus = _suspended_mask(daily_df)
    sus.index = daily_df.set_index(["ticker", "trade_date"]).index
    return (failed & ~sus.reindex(failed.index, fill_value=False)).rename("failed")
