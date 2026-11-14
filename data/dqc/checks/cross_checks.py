"""DQC 跨表检查 K06–K08（日线与分钟线一致性）。

函数签名统一为 ``(daily_df, minute_df, **params) -> pd.Series[bool]``，
索引为 ``(ticker, trade_date)``，``True`` 表示校验失败。

均以日线表为左表做 ``left`` 连接：无分钟线对应时判为失败，但停牌日
（``suspend == True``）无分钟线属正常状态，予以豁免。分钟时间戳优先取
``datetime`` 列，兼容旧列名 ``timestamp``。
"""

from __future__ import annotations

import pandas as pd


def _dt_col(df: pd.DataFrame) -> str | None:
    for c in ("datetime", "timestamp"):
        if c in df.columns:
            return c
    return None


def _suspended_mask(daily_df: pd.DataFrame) -> pd.Series:
    """返回日线停牌掩码（无 ``suspend`` 列时全为 False）。"""
    if "suspend" in daily_df.columns:
        return daily_df["suspend"].fillna(False).astype(bool)
    return pd.Series(False, index=daily_df.index)


def check_daily_vs_minute_close(
    daily_df: pd.DataFrame,
    minute_df: pd.DataFrame,
    tolerance: float = 0.01,
    **_,
) -> pd.Series:
    """K06：日线收盘价与当日最后一根分钟线收盘价偏差须 <= *tolerance*。"""
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
    # 豁免停牌日（无分钟线属正常）
    sus = _suspended_mask(daily_df)
    sus.index = daily_df.set_index(["ticker", "trade_date"]).index
    return (failed & ~sus.reindex(failed.index, fill_value=False)).rename("failed")


def check_daily_vs_minute_volume(
    daily_df: pd.DataFrame,
    minute_df: pd.DataFrame,
    max_rel_diff: float = 0.001,
    **_,
) -> pd.Series:
    """K07：日线成交量与当日分钟线成交量之和相对偏差须 <= *max_rel_diff*。"""
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
    """K08：日线最高价 >= 分钟线最大高价，且日线最低价 <= 分钟线最小低价。"""
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
