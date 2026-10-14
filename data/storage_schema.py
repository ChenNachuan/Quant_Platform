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

Schema evolution: **add-column only**.  To keep pre-addition frames passing
validation, a new column must set **both** ``nullable=True`` (values may be
null when ``union_by_name=true`` fills them) **and** ``required=False`` (the
column may be entirely absent from older frames).  ``strict=True`` rejects
unexpected columns on write.

Implementation decisions (QUANT-12 §5, documented in the PR description):

- ``trade_date`` datetime→date conversion happens **before** validation via
  a custom parser that accepts only datetime64 / date / string dates and
  rejects integer input (which pandera would treat as epoch seconds).
  Datetime columns likewise reject tz-aware timestamps (design uses naive
  Beijing time).  Numeric columns use ``coerce=False`` so non-integral floats
  in integer columns raise instead of silently truncating.
- Dates are ``datetime.date`` / Parquet ``DATE``, **not** VARCHAR.
- Schema changes are recorded in ``schema_changelog.jsonl`` alongside git log.
- Factor value column is named per-factor via :func:`factor_schema`;
  ``SCHEMA["factor"]`` uses the canonical placeholder ``"value"``.
- Factor DuckDB view names: ``factor_{sanitized}`` — non-alphanumerics → ``_``.
  Use :class:`FactorViewRegistry` to detect collisions between distinct factor
  names that sanitize to the same view.
"""

from __future__ import annotations

import re

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema, Parser

from core.asset_rules import EXCHANGE_MAP, validate_equity_ticker, validate_futures_ticker
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


def _quality_flag_column() -> Column:
    """Shared quality_flag column: int64 bitmask, 5 bits, not-null."""
    return Column(
        int,
        checks=[
            Check.ge(0, name="quality_flag_non_negative"),
            Check.lt(QUALITY_FLAG_MAX + 1, name="quality_flag_within_5_bits"),
        ],
        nullable=False,
    )


# ---------------------------------------------------------------------------
# Safe date / datetime coercion (code-review findings 1 & 2)
# ---------------------------------------------------------------------------
def _safe_date_parser(series: pd.Series) -> pd.Series:
    """Convert to ``datetime.date``, rejecting integer and tz-aware input.

    Pandera's built-in ``coerce=True`` treats integer values as epoch
    seconds, so ``20240115`` silently becomes ``date(1970, 1, 1)``.  This
    parser accepts only datetime64 / date / string and rejects everything
    else loudly.
    """
    if series.dtype.kind in "iub":
        raise ValueError(
            f"integer/boolean input to date column not allowed (got dtype {series.dtype}); "
            "pass datetime64, date, or ISO string"
        )
    if getattr(series.dtype, "tz", None) is not None:
        raise ValueError("tz-aware input to date column not allowed; use naive dates")
    try:
        converted = pd.to_datetime(series, errors="raise")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"cannot parse date column: {exc}") from exc
    return converted.dt.date


def _safe_datetime_parser(series: pd.Series) -> pd.Series:
    """Convert to naive ``datetime64``, rejecting integer and tz-aware input.

    The design uses naive Beijing time.  Pandera's built-in coercion silently
    shifts tz-aware timestamps (e.g. ``09:31+0800`` → ``01:31``).
    """
    if series.dtype.kind in "iub":
        raise ValueError(
            f"integer/boolean input to datetime column not allowed (got dtype {series.dtype})"
        )
    if getattr(series.dtype, "tz", None) is not None:
        raise ValueError(
            "tz-aware timestamps not allowed (design uses naive Beijing time)"
        )
    return pd.to_datetime(series, errors="raise")


def _date_column(nullable: bool = False) -> Column:
    """Date column with safe coercion (rejects int / tz-aware)."""
    return Column(
        pa.Date,
        coerce=False,
        parsers=Parser(_safe_date_parser),
        nullable=nullable,
    )


def _datetime_column(nullable: bool = False) -> Column:
    """Datetime column with safe coercion (rejects int / tz-aware)."""
    return Column(
        pa.DateTime,
        coerce=False,
        parsers=Parser(_safe_datetime_parser),
        nullable=nullable,
    )


# ---------------------------------------------------------------------------
# Shared checks
# ---------------------------------------------------------------------------
_equity_ticker_check = Check(
    validate_equity_ticker, element_wise=True, name="equity_ticker",
)
_futures_ticker_check = Check(
    validate_futures_ticker, element_wise=True, name="futures_ticker",
)
_ohlcv_checks = [Check.gt(0, name="positive")]
_non_negative_checks = [Check.ge(0, name="non_negative")]
_event_type_check = Check.isin(
    ["dividend", "split", "rights", "bonus"], name="event_type",
)
_classification_check = Check.isin(
    ["citics", "sw"], name="classification",
)
_exchange_check = Check.isin(
    sorted(EXCHANGE_MAP), name="exchange_code",
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

def _table(columns: dict, unique: list | None = None, **kwargs) -> DataFrameSchema:
    """Build a table schema with the standard coerce / strict / unique policy.

    Schema-level coercion is **off**.  Date/datetime columns carry safe
    parsers individually (§5 decision 1).  *unique* lists column names whose
    combination must be unique (the table's PK per §47.14).
    """
    return DataFrameSchema(
        columns, coerce=False, strict=True, unique=unique or [], **kwargs,
    )


# --- 1. bars_daily — 14 cols, PK(ticker, trade_date) ------------------------
BARS_DAILY_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "trade_date": _date_column(),
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
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

# --- 2. bars_minute — 10 cols, no adjfactor, PK(ticker, trade_date, datetime)
BARS_MINUTE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "datetime": _datetime_column(),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "datetime"],
)

# --- 3. bars_daily_futures — 11 cols, settle/open_interest, no adjfactor ----
BARS_DAILY_FUTURES_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "settle": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "open_interest": Column(int, checks=_non_negative_checks, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

# --- 4. tick_futures_l2 — 28 cols (Phase 2 reserved) ------------------------
# bid_price_1~5 / bid_volume_1~5 / ask_price_1~5 / ask_volume_1~5 are five
# independent columns each, not list-typed.  7 base + 20 book + quality_flag.
_tick_base: dict = {
    "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
    "trade_date": _date_column(),
    "datetime": _datetime_column(),
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

TICK_FUTURES_L2_SCHEMA = _table(
    {
        **_tick_base,
        **_tick_book,
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "datetime"],
)

# --- 5. fundamental — 11 cols, valuation ratios nullable -------------------
FUNDAMENTAL_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "total_market_cap": Column(float, checks=_non_negative_checks, nullable=False),
        "float_market_cap": Column(float, checks=_non_negative_checks, nullable=False),
        "pe_ttm": Column(float, nullable=True),
        "pe_lyr": Column(float, nullable=True),
        "pb": Column(float, nullable=True),
        "ps_ttm": Column(float, nullable=True),
        "total_shares": Column(int, checks=_non_negative_checks, nullable=False),
        "float_shares": Column(int, checks=_non_negative_checks, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)


# --- 6. factor — 3 cols, value column named per-factor ---------------------
_FACTOR_RESERVED_COLUMNS = frozenset({"ticker", "date"})


def factor_schema(value_column: str = "value") -> DataFrameSchema:
    """Schema for one factor's long-format value table.

    Columns: ``ticker``, ``date``, *value_column* (nullable).
    The value column is named after the factor (issue QUANT-12: "3 cols
    ticker/date/{name}").  ``SCHEMA["factor"]`` uses the canonical placeholder
    ``"value"``; T2/T3 should call this function with the real factor name.

    Raises ``ValueError`` when *value_column* collides with the fixed
    ``ticker`` / ``date`` columns (code-review finding 5).
    """
    if value_column in _FACTOR_RESERVED_COLUMNS:
        raise ValueError(
            f"value_column {value_column!r} collides with a fixed column name; "
            f"reserved names are {sorted(_FACTOR_RESERVED_COLUMNS)}"
        )
    return _table(
        {
            "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
            "date": _date_column(),
            value_column: Column(float, nullable=True),
        },
        unique=["ticker", "date"],
    )


# Canonical placeholder for the SCHEMA registry.
FACTOR_SCHEMA = factor_schema()

# --- 7. calendar — 1 col, per-exchange file -------------------------------
CALENDAR_SCHEMA = _table(
    {"trade_date": _date_column()},
    unique=["trade_date"],
)

# --- 8. contract_specs — 12 cols, futures contract metadata ---------------
CONTRACT_SPECS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
        "symbol": Column(str, nullable=False),
        "exchange": Column(str, checks=_exchange_check, nullable=False),
        "contract_multiplier": Column(float, checks=_ohlcv_checks, nullable=False),
        "price_tick": Column(float, checks=_ohlcv_checks, nullable=False),
        "list_date": _date_column(),
        "expire_date": _date_column(),
        "delivery_month": Column(str, nullable=False),
        "price_limit_pct": Column(float, checks=_ohlcv_checks, nullable=False),
        "margin_rate": Column(float, checks=_ohlcv_checks, nullable=True),
        "trading_hours": Column(str, nullable=False),
        "is_active": Column(bool, nullable=False),
    },
    unique=["ticker"],
)

# --- 9. industry — 6 cols, l3 nullable ------------------------------------
INDUSTRY_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "industry_l1": Column(str, nullable=False),
        "industry_l2": Column(str, nullable=False),
        "industry_l3": Column(str, nullable=True),
        "classification": Column(str, checks=_classification_check, nullable=False),
        "effective_date": _date_column(),
    },
    unique=["ticker"],
)

# --- 10. corporate_actions — 10 cols, PK(ticker, ex_date, event_type) -----
CORPORATE_ACTIONS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "ex_date": _date_column(),
        "event_type": Column(str, checks=_event_type_check, nullable=False),
        "announce_date": _date_column(nullable=True),
        "record_date": _date_column(nullable=True),
        "detail": Column(str, nullable=False),
        "cash_dividend": Column(float, checks=_non_negative_checks, nullable=True),
        "stock_dividend": Column(float, checks=_non_negative_checks, nullable=True),
        "rights_price": Column(float, checks=_non_negative_checks, nullable=True),
        "rights_ratio": Column(float, checks=_non_negative_checks, nullable=True),
    },
    unique=["ticker", "ex_date", "event_type"],
)

# --- 11. continuous — 11 cols, per-symbol file, no quality_flag -----------
CONTINUOUS_SCHEMA = _table(
    {
        "trade_date": _date_column(),
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
    },
    unique=["trade_date"],
)


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

    This is a pure function and does **not** detect collisions; use
    :class:`FactorViewRegistry` when registering views at runtime.
    """
    sanitized = _non_alnum.sub("_", factor_name)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return f"factor_{sanitized}"


class FactorViewRegistry:
    """Collision-safe registry mapping factor names to DuckDB view names.

    ``factor_view_name`` sanitizes names, so ``alpha.1`` and ``alpha_1``
    both map to ``factor_alpha_1``.  Registering both through this
    registry raises ``ValueError`` instead of silently overwriting.
    """

    def __init__(self) -> None:
        self._views: dict[str, str] = {}  # view_name -> factor_name

    def register(self, factor_name: str) -> str:
        """Register *factor_name* and return its view name.

        Raises ``ValueError`` on collision with a different factor name.
        """
        view = factor_view_name(factor_name)
        existing = self._views.get(view)
        if existing is not None and existing != factor_name:
            raise ValueError(
                f"factor view name collision: {existing!r} and {factor_name!r} "
                f"both map to {view!r}"
            )
        self._views[view] = factor_name
        return view

    @property
    def views(self) -> dict[str, str]:
        return dict(self._views)
