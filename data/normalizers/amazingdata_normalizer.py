"""AmazingData normalizer — format conversion only (C3).

Maps AmazingData SDK columns to the unified bars_daily schema.

Edge mappings (S2):
- Suspension: ``suspend`` flag from source is preserved.  The §21.3 bug
  (``DLS_Basedata_US`` writes ``adjfactor=1`` when ``suspend==1``) is
  explicitly avoided: the normalizer never overwrites a source-provided
  ``adjfactor`` based on the suspend flag.
- Missing values: NaN in OHLC preserved as-is, never fabricated.
- CA events: ``adjfactor`` from ``NAV_ADJ_FACTOR`` is mapped verbatim.
  When absent (kline-only fetch), defaults to 1.0.
"""

from __future__ import annotations

import pandas as pd

from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

# AmazingData kline / fund_nav → unified column names
_COLUMN_MAP: dict[str, str] = {
    "code": "ticker",
    "kline_time": "trade_date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "amount": "amount",
    "adjfactor": "adjfactor",
    "NAV_ADJ_FACTOR": "adjfactor",
    "suspend": "suspend",
}

_NUMERIC_COLS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjfactor",
    "turnover",
)


def normalize(asset_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """Format conversion only: column name mapping, type fixes.

    Parameters
    ----------
    asset_type : str
        Must be ``"stock"`` or ``"option"``.
    raw_df : pd.DataFrame
        Raw AmazingData frame (kline output, optionally merged with
        ``NAV_ADJ_FACTOR`` / suspend flag).
    """
    if asset_type not in ("stock", "option"):
        raise ValueError(f"unsupported asset_type: {asset_type!r}")

    # Fix 4: coalesce adjfactor + NAV_ADJ_FACTOR before renaming to avoid
    # duplicate columns (both map to "adjfactor" in _COLUMN_MAP).
    df = raw_df.copy()
    if "adjfactor" in df.columns and "NAV_ADJ_FACTOR" in df.columns:
        df["adjfactor"] = df["adjfactor"].fillna(df["NAV_ADJ_FACTOR"])
        df = df.drop(columns=["NAV_ADJ_FACTOR"])
    elif "NAV_ADJ_FACTOR" in df.columns:
        df = df.rename(columns={"NAV_ADJ_FACTOR": "adjfactor"})

    df = df.rename(columns=_COLUMN_MAP).copy()

    # Type conversion: dates and numerics
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Edge: suspension — source provides explicit flag; default False if absent.
    if "suspend" not in df.columns:
        df["suspend"] = False
    else:
        df["suspend"] = df["suspend"].fillna(False).astype(bool)

    # Edge: CA events — NAV_ADJ_FACTOR merged by adapter; default 1.0 if absent.
    # §21.3 bug guard: NEVER overwrite source adjfactor based on suspend value.
    # If the source says adjfactor=0.85 on a suspended day, we keep 0.85.
    if "adjfactor" not in df.columns:
        df["adjfactor"] = 1.0

    # Edge: limit flags not available from kline.
    df["is_limit_up"] = False
    df["is_limit_down"] = False

    # DQC adds real quality_flag later; start clean.
    df["quality_flag"] = 0

    return df.reindex(columns=UNIFIED_COLUMNS)
