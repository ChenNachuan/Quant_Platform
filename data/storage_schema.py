"""Storage schema contract — the 11 core tables as pandera DataFrameSchemas.

Pure schema definitions, no IO (red line 3).  This module defines *what a
table looks like*; write paths (T2) and read paths (T3) consume these schemas.

Field-level authority: ``docs/archive/OPEN_QUESTIONS.md`` §47.
Settled rulings on QUANT-12 override stale wording in the design docs:

1. ``quality_flag`` is an ``int64`` bitmask with 5 bits (§47.15 wins over the
   ``INT8 (0/1/2)`` line in §55.2, which is the :class:`QualitySeverity` scale).
2. Calendar exchanges = 9, sourced from :data:`core.asset_rules.CALENDAR_EXCHANGES`.
   The "6 exchanges" line in the docs misses INE and GFEX.
3. ``bars_daily`` holds stock + ETF + LOF together.  Table selection is by
   family (equity / futures / continuous), not by the 6-member ``AssetType``.

Schema evolution: **add-column only**.  New columns must be ``nullable=True``
so that ``union_by_name=true`` reads of pre-addition files still validate.
``strict=True`` rejects unexpected columns on write.

Implementation decisions (QUANT-12 §5, documented in the PR description):

- ``trade_date`` datetime→date conversion happens **before** validation
  (``coerce=True``): the schema asserts the stored type (``date``).
- Dates are ``datetime.date`` / Parquet ``DATE``, **not** VARCHAR.
- Schema changes are recorded in ``schema_changelog.jsonl`` alongside git log.
- Factor value column is named per-factor via :func:`factor_schema`;
  ``SCHEMA["factor"]`` uses the canonical placeholder ``"value"``.
- Factor DuckDB view names: ``factor_{sanitized}`` — non-alphanumerics → ``_``.
"""

from __future__ import annotations

import re

import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

from core.asset_rules import validate_equity_ticker, validate_futures_ticker
from core.types import QualityFlag

# ---------------------------------------------------------------------------
# quality_flag — int64 bitmask, 5 bits, CLEAN = 0 (§47.15)
# ---------------------------------------------------------------------------
QUALITY_FLAG_BITS: dict[str, int] = {
    "C02_COVERAGE": int(QualityFlag.C02_COVERAGE),
    "C05_NULL_RATE": int(QualityFlag.C05_NULL_RATE),
    "L04_BEYOND_LIMIT": int(QualityFlag.L04_BEYOND_LIMIT),
    "L07_ADJFACTOR_RANGE": int(QualityFlag.L07_ADJFACTOR_RANGE),
    "K01_OVERNIGHT_GAP": int(QualityFlag.K01_OVERNIGHT_GAP),
}
CLEAN: int = int(QualityFlag.CLEAN)
QUALITY_FLAG_MAX: int = 31  # 5 bits → value < 32

# ---------------------------------------------------------------------------
# Shared checks
# ---------------------------------------------------------------------------
_equity_ticker_check = Check(
    validate_equity_ticker, element_wise=True, name="equity_ticker",
)
_futures_ticker_check = Check(
    validate_futures_ticker, element_wise=True, name="futures_ticker",
)
_quality_flag_checks = [
    Check.ge(0, name="quality_flag_non_negative"),
    Check.lt(QUALITY_FLAG_MAX + 1, name="quality_flag_within_5_bits"),
]
_ohlcv_checks = [Check.gt(0, name="positive")]
_non_negative_checks = [Check.ge(0, name="non_negative")]
_event_type_check = Check.isin(
    ["dividend", "split", "rights", "bonus"], name="event_type",
)
_classification_check = Check.isin(
    ["citics", "sw"], name="classification",
)


# ---------------------------------------------------------------------------
# Directory / partition conventions (relative to ``data_platform/``)
# ---------------------------------------------------------------------------
# Dual-track partitioning (§47.14): market / fundamental tables use one file
# per day (``{YYYY-MM-DD}.parquet``); reference / continuous use full or
# per-symbol files; factors use year-partitioned files under a version dir.
# No Hive partitioning — flat names, ``hive_partitioning=false``.
PARTITION_PATHS: dict[str, str] = {
    "bars_daily": "asset/bars_daily/{date}.parquet",
    "bars_minute": "asset/bars_minute/{date}.parquet",
    "bars_daily_futures": "asset/bars_daily_futures/{date}.parquet",
    "tick_futures_l2": "asset/tick_futures_l2/{date}.parquet",
    "fundamental": "asset/fundamental/{date}.parquet",
    "factor": "factors/{name}/v{version}/{year}.parquet",
    "calendar": "asset/calendar/{exchange}.parquet",
    "contract_specs": "asset/reference/contract_specs.parquet",
    "industry": "asset/reference/industry.parquet",
    "corporate_actions": "asset/reference/corporate_actions.parquet",
    "continuous": "asset/continuous/{symbol}.parquet",
}

# Table family for selection (gap #12 ruling): equity tables serve stock + ETF
# + LOF together; the 6-member ``AssetType`` is for fee / limit rules, not for
# picking a table.
TABLE_FAMILY: dict[str, str] = {
    "bars_daily": "equity",
    "bars_minute": "equity",
    "bars_daily_futures": "futures",
    "tick_futures_l2": "futures",
    "fundamental": "equity",
    "factor": "equity",
    "calendar": "reference",
    "contract_specs": "futures",
    "industry": "equity",
    "corporate_actions": "equity",
    "continuous": "continuous",
}


# ---------------------------------------------------------------------------
# Pandera schemas — 11 tables
# ---------------------------------------------------------------------------

def _table(columns: dict, **kwargs) -> DataFrameSchema:
    """Build a table schema with the standard coerce / strict policy."""
    return DataFrameSchema(columns, coerce=True, strict=True, **kwargs)


# --- 1. bars_daily — 14 cols, PK(ticker, trade_date) ------------------------
BARS_DAILY_SCHEMA = _table({
    "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
    "trade_date": Column(pa.Date, nullable=False),
    "open": Column(float, checks=_ohlcv_checks, nullable=False),
    "high": Column(float, checks=_ohlcv_checks, nullable=False),
    "low": Column(float, checks=_ohlcv_checks, nullable=False),
    "close": Column(float, checks=_ohlcv_checks, nullable=False),
    "volume": Column(int, checks=_non_negative_checks, nullable=False),
    "amount": Column(float, checks=_non_negative_checks, nullable=False),
    "adjfactor": Column(float, checks=_ohlcv_checks, nullable=False),
    "turnover": Column(float, checks=_non_negative_checks, nullable=True),
    "suspend": Column(bool, nullable=False),
    "is_limit_up": Column(bool, nullable=False),
    "is_limit_down": Column(bool, nullable=False),
    "quality_flag": Column(int, checks=_quality_flag_checks, nullable=False),
})

# --- 2. bars_minute — 10 cols, no adjfactor, PK(ticker, trade_date, datetime)
BARS_MINUTE_SCHEMA = _table({
    "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
    "trade_date": Column(pa.Date, nullable=False),
    "datetime": Column(pa.DateTime, nullable=False),
    "open": Column(float, checks=_ohlcv_checks, nullable=False),
    "high": Column(float, checks=_ohlcv_checks, nullable=False),
    "low": Column(float, checks=_ohlcv_checks, nullable=False),
    "close": Column(float, checks=_ohlcv_checks, nullable=False),
    "volume": Column(int, checks=_non_negative_checks, nullable=False),
    "amount": Column(float, checks=_non_negative_checks, nullable=False),
    "quality_flag": Column(int, checks=_quality_flag_checks, nullable=False),
})

# --- 3. bars_daily_futures — 11 cols, settle/open_interest, no adjfactor ----
BARS_DAILY_FUTURES_SCHEMA = _table({
    "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
    "trade_date": Column(pa.Date, nullable=False),
    "open": Column(float, checks=_ohlcv_checks, nullable=False),
    "high": Column(float, checks=_ohlcv_checks, nullable=False),
    "low": Column(float, checks=_ohlcv_checks, nullable=False),
    "close": Column(float, checks=_ohlcv_checks, nullable=False),
    "settle": Column(float, checks=_ohlcv_checks, nullable=False),
    "volume": Column(int, checks=_non_negative_checks, nullable=False),
    "amount": Column(float, checks=_non_negative_checks, nullable=False),
    "open_interest": Column(int, checks=_non_negative_checks, nullable=False),
    "quality_flag": Column(int, checks=_quality_flag_checks, nullable=False),
})

# --- 4. tick_futures_l2 — 28 cols (Phase 2 reserved) ------------------------
# bid_price_1~5 / bid_volume_1~5 / ask_price_1~5 / ask_volume_1~5 are five
# independent columns each, not list-typed.  7 base + 20 book + quality_flag.
_tick_base: dict = {
    "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
    "trade_date": Column(pa.Date, nullable=False),
    "datetime": Column(pa.DateTime, nullable=False),
    "last_price": Column(float, checks=_ohlcv_checks, nullable=False),
    "volume": Column(int, checks=_non_negative_checks, nullable=False),
    "amount": Column(float, checks=_non_negative_checks, nullable=False),
    "open_interest": Column(int, checks=_non_negative_checks, nullable=False),
}
_tick_book: dict = {}
for _i in range(1, 6):
    _tick_book[f"bid_price_{_i}"] = Column(float, checks=_non_negative_checks, nullable=False)
    _tick_book[f"bid_volume_{_i}"] = Column(int, checks=_non_negative_checks, nullable=False)
    _tick_book[f"ask_price_{_i}"] = Column(float, checks=_non_negative_checks, nullable=False)
    _tick_book[f"ask_volume_{_i}"] = Column(int, checks=_non_negative_checks, nullable=False)

TICK_FUTURES_L2_SCHEMA = _table({
    **_tick_base,
    **_tick_book,
    "quality_flag": Column(int, checks=_quality_flag_checks, nullable=False),
})

# --- 5. fundamental — 11 cols, valuation ratios nullable -------------------
FUNDAMENTAL_SCHEMA = _table({
    "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
    "trade_date": Column(pa.Date, nullable=False),
    "total_market_cap": Column(float, checks=_non_negative_checks, nullable=False),
    "float_market_cap": Column(float, checks=_non_negative_checks, nullable=False),
    "pe_ttm": Column(float, nullable=True),
    "pe_lyr": Column(float, nullable=True),
    "pb": Column(float, nullable=True),
    "ps_ttm": Column(float, nullable=True),
    "total_shares": Column(int, checks=_non_negative_checks, nullable=False),
    "float_shares": Column(int, checks=_non_negative_checks, nullable=False),
    "quality_flag": Column(int, checks=_quality_flag_checks, nullable=False),
})


# --- 6. factor — 3 cols, value column named per-factor ---------------------
def factor_schema(value_column: str = "value") -> DataFrameSchema:
    """Schema for one factor's long-format value table.

    Columns: ``ticker``, ``date``, *value_column* (nullable).
    The value column is named after the factor (issue QUANT-12: "3 cols
    ticker/date/{name}").  ``SCHEMA["factor"]`` uses the canonical placeholder
    ``"value"``; T2/T3 should call this function with the real factor name.
    """
    return _table({
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "date": Column(pa.Date, nullable=False),
        value_column: Column(float, nullable=True),
    })


# Canonical placeholder for the SCHEMA registry.
FACTOR_SCHEMA = factor_schema()

# --- 7. calendar — 1 col, per-exchange file -------------------------------
CALENDAR_SCHEMA = _table({
    "trade_date": Column(pa.Date, nullable=False),
})

# --- 8. contract_specs — 12 cols, futures contract metadata ---------------
CONTRACT_SPECS_SCHEMA = _table({
    "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
    "symbol": Column(str, nullable=False),
    "exchange": Column(str, nullable=False),
    "contract_multiplier": Column(float, checks=_ohlcv_checks, nullable=False),
    "price_tick": Column(float, checks=_ohlcv_checks, nullable=False),
    "list_date": Column(pa.Date, nullable=False),
    "expire_date": Column(pa.Date, nullable=False),
    "delivery_month": Column(str, nullable=False),
    "price_limit_pct": Column(float, checks=_ohlcv_checks, nullable=False),
    "margin_rate": Column(float, checks=_ohlcv_checks, nullable=True),
    "trading_hours": Column(str, nullable=False),
    "is_active": Column(bool, nullable=False),
})

# --- 9. industry — 6 cols, l3 nullable ------------------------------------
INDUSTRY_SCHEMA = _table({
    "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
    "industry_l1": Column(str, nullable=False),
    "industry_l2": Column(str, nullable=False),
    "industry_l3": Column(str, nullable=True),
    "classification": Column(str, checks=_classification_check, nullable=False),
    "effective_date": Column(pa.Date, nullable=False),
})

# --- 10. corporate_actions — 10 cols, PK(ticker, ex_date, event_type) -----
CORPORATE_ACTIONS_SCHEMA = _table({
    "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
    "ex_date": Column(pa.Date, nullable=False),
    "event_type": Column(str, checks=_event_type_check, nullable=False),
    "announce_date": Column(pa.Date, nullable=True),
    "record_date": Column(pa.Date, nullable=True),
    "detail": Column(str, nullable=False),
    "cash_dividend": Column(float, checks=_non_negative_checks, nullable=True),
    "stock_dividend": Column(float, checks=_non_negative_checks, nullable=True),
    "rights_price": Column(float, checks=_non_negative_checks, nullable=True),
    "rights_ratio": Column(float, checks=_non_negative_checks, nullable=True),
})

# --- 11. continuous — 11 cols, per-symbol file, no quality_flag -----------
CONTINUOUS_SCHEMA = _table({
    "trade_date": Column(pa.Date, nullable=False),
    "dominant_contract": Column(str, checks=_futures_ticker_check, nullable=False),
    "open": Column(float, checks=_ohlcv_checks, nullable=False),
    "high": Column(float, checks=_ohlcv_checks, nullable=False),
    "low": Column(float, checks=_ohlcv_checks, nullable=False),
    "close": Column(float, checks=_ohlcv_checks, nullable=False),
    "volume": Column(int, checks=_non_negative_checks, nullable=False),
    "amount": Column(float, checks=_non_negative_checks, nullable=False),
    "open_interest": Column(int, checks=_non_negative_checks, nullable=False),
    "settle": Column(float, checks=_ohlcv_checks, nullable=False),
    "adjfactor": Column(float, checks=_ohlcv_checks, nullable=False),
})


# ---------------------------------------------------------------------------
# SCHEMA registry — exactly 11 tables, no more, no fewer
# ---------------------------------------------------------------------------
SCHEMA: dict[str, DataFrameSchema] = {
    "bars_daily": BARS_DAILY_SCHEMA,
    "bars_minute": BARS_MINUTE_SCHEMA,
    "bars_daily_futures": BARS_DAILY_FUTURES_SCHEMA,
    "tick_futures_l2": TICK_FUTURES_L2_SCHEMA,
    "fundamental": FUNDAMENTAL_SCHEMA,
    "factor": FACTOR_SCHEMA,
    "calendar": CALENDAR_SCHEMA,
    "contract_specs": CONTRACT_SPECS_SCHEMA,
    "industry": INDUSTRY_SCHEMA,
    "corporate_actions": CORPORATE_ACTIONS_SCHEMA,
    "continuous": CONTINUOUS_SCHEMA,
}


# ---------------------------------------------------------------------------
# Factor DuckDB view naming (QUANT-12 §5 decision)
# ---------------------------------------------------------------------------
_non_alnum = re.compile(r"[^0-9a-zA-Z_]")


def factor_view_name(factor_name: str) -> str:
    """Return the DuckDB view name for *factor_name*.

    Rule: ``factor_{sanitized}`` where every non-``[0-9a-zA-Z_]`` character
    becomes ``_``; if the sanitized name starts with a digit, prepend ``_``.
    The same sanitization applies to the value-column alias inside the view.
    """
    sanitized = _non_alnum.sub("_", factor_name)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return f"factor_{sanitized}"
