"""Tonghuashun (Fuyao / StraightFlush) normalizer — format conversion only (C3).

Maps Fuyao REST API columns (``PriceBarItem`` / ``PriceSnapshotItem``) to the
unified ``bars_daily`` schema per ``docs/data_source/StrightFlush/``:

* ``thscode`` -> ``ticker`` (already ``NNNNNN.SH/SZ/BJ``; bare codes qualified
  via :data:`core.asset_rules.PREFIX_EXCHANGE`).
* ``date_ms`` (Unix ms in ``Asia/Shanghai``) -> ``trade_date`` (``datetime.date``).
* ``open_price`` / ``high_price`` / ``low_price`` / ``close_price`` -> OHLC.
* ``volume`` -> ``volume`` (already shares 股; NaN-preserving ``Int64``).
* **Important unit/name distinction**: Fuyao ``PriceBarItem.turnover`` is
  traded value (成交额 in CNY 元), NOT turnover rate (换手率)! Mapped to
  ``amount``, whereas ``turnover_ratio_pct`` (from snapshot endpoints when
  present) maps to ``turnover``.
* CA events: raw adapter explicitly passes ``adjust="none"``; ``adjfactor``
  defaults to ``1.0`` when absent and is never overwritten based on ``suspend``.
"""

from __future__ import annotations

import pandas as pd

from core.asset_rules import PREFIX_EXCHANGE
from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

_SUPPORTED_ASSET_TYPES = frozenset({"stock", "etf", "index"})

_COLUMN_MAP: dict[str, str] = {
    "thscode": "ticker",
    "open_price": "open",
    "high_price": "high",
    "low_price": "low",
    "close_price": "close",
    "volume": "volume",
    "turnover": "amount",  # Fuyao 'turnover' is 成交额 in yuan, NOT turnover rate
    "turnover_ratio_pct": "turnover",
    "adjfactor": "adjfactor",
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


def _qualify_ticker(code: str) -> str:
    val = str(code).strip()
    if "." in val:
        return val
    prefix = val[:2]
    exchange = PREFIX_EXCHANGE.get(prefix)
    if exchange is None:
        raise ValueError(f"cannot qualify ticker {val!r}: unknown prefix {prefix!r}")
    return f"{val}.{exchange}"


def normalize(asset_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """Format conversion only: Fuyao columns/date_ms -> unified bars_daily."""
    if asset_type not in _SUPPORTED_ASSET_TYPES:
        raise ValueError(f"unsupported asset_type: {asset_type!r}")

    df = raw_df.copy()

    if "date_ms" in df.columns:
        df["trade_date"] = (
            pd.to_datetime(
                pd.to_numeric(df["date_ms"], errors="coerce"), unit="ms", utc=True
            )
            .dt.tz_convert("Asia/Shanghai")
            .dt.date
        )
        df = df.drop(columns=["date_ms"])
    elif "date" in df.columns and "trade_date" not in df.columns:
        df = df.rename(columns={"date": "trade_date"})

    # Prevent collision if a test/custom frame already contains 'amount' alongside
    # Fuyao 'turnover'.
    if "amount" in df.columns and "turnover" in df.columns:
        df["amount"] = df["amount"].fillna(df["turnover"])
        df = df.drop(columns=["turnover"])

    df = df.rename(columns=_COLUMN_MAP)

    if "ticker" in df.columns and len(df) > 0:
        df["ticker"] = df["ticker"].astype(str).map(_qualify_ticker)

    if "trade_date" in df.columns and len(df) > 0:
        non_null = df["trade_date"].dropna()
        if len(non_null) > 0 and isinstance(non_null.iloc[0], str):
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = df["volume"].round().astype("Int64")

    if "suspend" not in df.columns:
        df["suspend"] = False
    else:
        df["suspend"] = df["suspend"].fillna(False).astype(bool)

    if "adjfactor" not in df.columns:
        df["adjfactor"] = 1.0

    df["is_limit_up"] = False
    df["is_limit_down"] = False
    df["quality_flag"] = 0

    return df.reindex(columns=UNIFIED_COLUMNS)
