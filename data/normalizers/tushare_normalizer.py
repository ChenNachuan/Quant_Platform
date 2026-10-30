"""Tushare normalizer — format conversion only (C3).

Maps Tushare Pro API columns to the unified bars_daily schema.

Edge mappings (S2):
- Suspension: ``suspend_d`` marks halted days; absence means active.
  If the adapter merged a ``suspend`` column it is mapped; otherwise ``False``.
- Missing values: NaN in OHLC preserved as-is, never fabricated.
- CA events: ``adj_factor`` from the ``adj_factor`` API is mapped to
  ``adjfactor``.  When absent (raw daily-only fetch), defaults to 1.0.
  The §21.3 bug (adjfactor forced to 1 when suspend=1) is avoided: the
  normalizer never overwrites a source-provided adjfactor based on suspend.

Unit conversion (Tushare raw → storage schema units):
- ``vol`` is in lots (手) → ``volume`` in shares (股): ×100.
- ``amount`` is in thousands (千元) → ``amount`` in yuan (元): ×1000.
"""

from __future__ import annotations

import pandas as pd

from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

# Tushare pro.daily() / adj_factor() / suspend_d() → unified column names
_COLUMN_MAP: dict[str, str] = {
    "ts_code": "ticker",
    "trade_date": "trade_date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "vol": "volume",
    "amount": "amount",
    "adj_factor": "adjfactor",
    "suspend_d": "suspend",
}

#: When the frame carries both ``suspend_d`` and ``suspend``, coalesce before
#: rename so we do not get two columns mapped to the same target.
_SUSPEND_COALESCE_PRIMARY = "suspend_d"

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
    """Format conversion only: column name mapping, type fixes, unit conversion.

    Parameters
    ----------
    asset_type : str
        Must be ``"stock"``.
    raw_df : pd.DataFrame
        Raw Tushare frame (``pro.daily`` output, optionally merged with
        ``adj_factor`` / ``suspend_d``).
    """
    if asset_type != "stock":
        raise ValueError(f"unsupported asset_type: {asset_type!r}")

    df = raw_df.copy()
    # Fix (optional finding #52): coalesce dual suspend columns before rename.
    # suspend_d is the authoritative source; suspend is the pre-merged fallback.
    if _SUSPEND_COALESCE_PRIMARY in df.columns and "suspend" in df.columns:
        # Coerce suspend_d tokens to bool, preserving NaN so the suspend
        # fallback can fill them (filling NaN→False first would block fallback).
        sd = df["suspend_d"]
        if sd.dtype == object or sd.dtype.name == "string":
            coerced = sd.map({"S": True, " ": False})
        else:
            coerced = sd.astype("boolean")
        df["suspend_d"] = coerced.fillna(df["suspend"].astype(bool))
        df = df.drop(columns=["suspend"])
    df = df.rename(columns=_COLUMN_MAP)

    # Type conversion: dates and numerics
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Unit conversion: Tushare vol is lots (手), amount is thousands (千元).
    # Fix 2: NaN-preserving Int64 cast — int64 raises IntCastingNaNError on NaN.
    if "volume" in df.columns:
        df["volume"] = (df["volume"] * 100).round().astype("Int64")
    if "amount" in df.columns:
        df["amount"] = df["amount"] * 1000.0

    # Edge: suspension — suspend_d merged by adapter.
    # Fix 3: suspend_d carries 'S' (suspended) / ' ' (active) strings.
    if "suspend" not in df.columns:
        df["suspend"] = False
    elif df["suspend"].dtype == object or df["suspend"].dtype.name == "string":
        # Semantic coercion: 'S'→True, ' '→False (suspend_d API contract)
        df["suspend"] = (
            df["suspend"].map({"S": True, " ": False}).fillna(False).astype(bool)
        )
    else:
        df["suspend"] = df["suspend"].fillna(False).astype(bool)

    # Edge: CA events — adj_factor merged by adapter; default 1.0 if absent.
    # §21.3: never overwrite source adjfactor based on suspend value.
    if "adjfactor" not in df.columns:
        df["adjfactor"] = 1.0

    # Edge: limit flags not available from this API.
    df["is_limit_up"] = False
    df["is_limit_down"] = False

    # DQC adds real quality_flag later; start clean.
    df["quality_flag"] = 0

    return df.reindex(columns=UNIFIED_COLUMNS)
