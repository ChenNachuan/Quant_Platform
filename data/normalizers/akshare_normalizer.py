"""AKShare normalizer — format conversion only (C3).

Maps AKShare's Chinese column names to the unified bars_daily schema.

Edge mappings (S2):
- Suspension: suspended stocks return no row — ``suspend=False`` for present rows.
- Missing values: NaN in OHLC preserved as-is, never fabricated.
- CA events: ``adjfactor=1.0`` (raw contract: ``adjust=""`` means unadjusted prices).
  The §21.3 bug (adjfactor forced to 1 when suspend=1) does not apply here
  because AKShare raw data carries no adjfactor column at all.
"""

from __future__ import annotations

import pandas as pd

from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

# AKShare stock_zh_a_hist → unified column names
_COLUMN_MAP: dict[str, str] = {
    "日期": "trade_date",
    "股票代码": "ticker",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
    "换手率": "turnover",
}

_NUMERIC_COLS = ("open", "high", "low", "close", "volume", "amount", "turnover")


def normalize(asset_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """Format conversion only: column name mapping, type fixes.

    Parameters
    ----------
    asset_type : str
        Must be ``"stock"`` (the only type AKShare adapter currently fetches).
    raw_df : pd.DataFrame
        Raw AKShare frame with Chinese column names.
    """
    if asset_type != "stock":
        raise ValueError(f"unsupported asset_type: {asset_type!r}")

    df = raw_df.rename(columns=_COLUMN_MAP).copy()

    # Type conversion: dates and numerics
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Edge: suspension — AKShare omits suspended rows entirely.
    df["suspend"] = False

    # Edge: CA events — raw contract (adjust=""), so adjfactor is identity.
    df["adjfactor"] = 1.0

    # Edge: limit flags not available from this API.
    df["is_limit_up"] = False
    df["is_limit_down"] = False

    # DQC adds real quality_flag later; start clean.
    df["quality_flag"] = 0

    return df.reindex(columns=UNIFIED_COLUMNS)
