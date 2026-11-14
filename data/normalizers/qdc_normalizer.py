"""QDCData normalizer — format conversion only (C3).

Maps QDCData raw columns (``cn_daily_full``, ``cn_etf_1d``, etc.) to the
unified ``bars_daily`` schema per ``docs/data_source/QDCData/`` and
``docs/design/data/QDC_ingestion_contract.md``:

* Ticker conversion: ``.XSHG -> .SH``, ``.XSHE -> .SZ``, ``.XBSE -> .BJ``;
  bare 6-digit codes are qualified via :data:`core.asset_rules.PREFIX_EXCHANGE`.
* Date conversion: ``ts`` (Unix ms ``BIGINT``) is converted via
  ``Asia/Shanghai`` timezone before extracting ``.dt.date`` (never naive UTC).
* CA events: ``hfq_factor`` / ``adj_factor`` / ``adjfactor`` preserved verbatim
  (defaults to ``1.0`` when absent, never overwritten based on ``suspend``).
* ``volume``: ``cn_daily_full`` and ``cn_etf_1d`` report shares (股) directly;
  cast with NaN-preserving ``Int64``.
"""

from __future__ import annotations

import pandas as pd

from core.asset_rules import PREFIX_EXCHANGE
from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

_SUPPORTED_ASSET_TYPES = frozenset({"stock", "etf", "index", "convertible"})

_EXCHANGE_SUFFIX_MAP: dict[str, str] = {
    ".XSHG": ".SH",
    ".XSHE": ".SZ",
    ".XBSE": ".BJ",
}

_COLUMN_MAP: dict[str, str] = {
    "symbol": "ticker",
    "ts_code": "ticker",
    "code": "ticker",
    "ts": "trade_date",
    "trade_day": "trade_date",
    "d": "trade_date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "amount": "amount",
    "hfq_factor": "adjfactor",
    "adj_factor": "adjfactor",
    "adjfactor": "adjfactor",
    "turnover": "turnover",
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


def _normalize_qdc_ticker(value: str) -> str:
    val = str(value).strip()
    for src_suffix, dst_suffix in _EXCHANGE_SUFFIX_MAP.items():
        if val.endswith(src_suffix):
            return val[: -len(src_suffix)] + dst_suffix
    if "." in val:
        return val
    prefix = val[:2]
    exchange = PREFIX_EXCHANGE.get(prefix)
    if exchange is None:
        raise ValueError(f"cannot qualify ticker {val!r}: unknown prefix {prefix!r}")
    return f"{val}.{exchange}"


def normalize(asset_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """Format conversion only: QDC columns/suffixes/ts -> unified bars_daily."""
    if asset_type not in _SUPPORTED_ASSET_TYPES:
        raise ValueError(f"unsupported asset_type: {asset_type!r}")

    df = raw_df.copy()

    # Coalesce multiple possible date columns before renaming so duplicate targets
    # are avoided (prefer explicit trade_date / trade_day over ts only if ts absent).
    if "ts" in df.columns:
        df["trade_date"] = (
            pd.to_datetime(
                pd.to_numeric(df["ts"], errors="coerce"), unit="ms", utc=True
            )
            .dt.tz_convert("Asia/Shanghai")
            .dt.date
        )
        drop_extra = [c for c in ("ts", "trade_day", "d") if c in df.columns]
        if drop_extra:
            df = df.drop(columns=drop_extra)

    # Coalesce ticker source columns (prefer symbol -> ts_code -> code)
    for alt in ("symbol", "ts_code", "code"):
        if alt in df.columns:
            if alt != "ticker":
                df = df.rename(columns={alt: "ticker"})
            break

    # Coalesce adjfactor columns (prefer hfq_factor -> adj_factor -> adjfactor)
    if "hfq_factor" in df.columns:
        if "adj_factor" in df.columns:
            df["hfq_factor"] = df["hfq_factor"].fillna(df["adj_factor"])
            df = df.drop(columns=["adj_factor"])
        if "adjfactor" in df.columns:
            df["hfq_factor"] = df["hfq_factor"].fillna(df["adjfactor"])
            df = df.drop(columns=["adjfactor"])
        df = df.rename(columns={"hfq_factor": "adjfactor"})
    elif "adj_factor" in df.columns:
        if "adjfactor" in df.columns:
            df["adj_factor"] = df["adj_factor"].fillna(df["adjfactor"])
            df = df.drop(columns=["adjfactor"])
        df = df.rename(columns={"adj_factor": "adjfactor"})

    df = df.rename(
        columns={
            k: v
            for k, v in _COLUMN_MAP.items()
            if k in df.columns and v not in df.columns
        }
    )

    if "ticker" in df.columns and len(df) > 0:
        df["ticker"] = df["ticker"].astype(str).map(_normalize_qdc_ticker)

    if "trade_date" in df.columns and len(df) > 0:
        if not pd.api.types.is_object_dtype(df["trade_date"]) or isinstance(
            df["trade_date"].dropna().iloc[0]
            if len(df["trade_date"].dropna())
            else None,
            str,
        ):
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
