"""Shared helpers for the temporary bootstrap fetchers (QUANT-35).

Not an S9 SourceAdapter — no DQC, no quarantine, no cross_check.  The shape is
always: pull from tushare -> align to ``data/storage_schema.py`` -> call
``write_parquet_atomic``.  ``quality_flag`` is constant 0 (CLEAN).

Data root is ``data_platform/``, the root ``PARTITION_PATHS`` are relative to
(``data/storage_schema.py`` "Directory / partition conventions") and the root
``storage_ops.cleanup_expired`` / ``check_capacity`` default to.  Override with
``--data-root`` for tests; do not hardcode anything else.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

# Bootstrap fetchers run as `python scripts/bootstrap/fetch_*.py`, which puts
# this directory on sys.path but not the repo root the `data` package lives in.
# Adding it here means every fetcher works when invoked directly.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from data.storage_ops import write_parquet_atomic  # noqa: E402
from data.storage_schema import PARTITION_PATHS, SCHEMA  # noqa: E402

#: Root every ``PARTITION_PATHS`` template is relative to.  Authority:
#: ``data/storage_schema.py`` (partition conventions) and ``data/storage_ops.py``
#: (``cleanup_expired(root="data_platform")`` / ``check_capacity(data_root=...)``).
DATA_ROOT = Path("data_platform")

#: Where ``TUSHARE_TOKEN`` may live.  Read, never printed, never committed.
_TOKEN_FILES = (Path(".env"), Path("config/secrets"))

#: Columns filled with a constant when the source cannot supply them.  A
#: missing column outside this map must be nullable in the schema (filled with
#: nulls) or the frame is refused — fabricating a price is worse than failing.
_OPTIONAL_DEFAULTS: dict[str, object] = {
    "quality_flag": 0,
    "suspend": False,
    "is_limit_up": False,
    "is_limit_down": False,
    "turnover": float("nan"),
}

#: tushare ``ts_code`` shape: ``{6 digits}.{SH|SZ|BJ}`` — identical to the
#: repo ticker (see core.asset_rules._equity_re).
_TS_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)\Z")


# ---------------------------------------------------------------------------
# tushare init
# ---------------------------------------------------------------------------
def init_tushare():
    """Return a tushare pro client authenticated from ``.env`` / ``config/secrets``.

    The token is read into memory and handed to the SDK; it is never logged.
    """
    token = _read_token()
    if not token:
        raise RuntimeError(
            "TUSHARE_TOKEN not found in .env or config/secrets — "
            "copy .env.example to .env and fill it in"
        )
    import tushare as ts

    ts.set_token(token)
    return ts.pro_api()


def _read_token() -> str | None:
    env = os.environ.get("TUSHARE_TOKEN")
    if env:
        return env.strip()
    for path in _TOKEN_FILES:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() == "TUSHARE_TOKEN":
                return value.strip().strip("'\"")
    return None


# ---------------------------------------------------------------------------
# ticker format (AC-A3)
# ---------------------------------------------------------------------------
def ticker_convert(ts_code: str) -> str:
    """tushare ``ts_code`` -> repo ``ticker``.

    The two formats are **the same string**: ``{6 digits}.{SH|SZ|BJ}``.  This
    is therefore the identity map, kept as a function so a future divergence
    has exactly one place to change.  See README "ticker 格式".
    """
    if not isinstance(ts_code, str) or _TS_CODE_RE.match(ts_code) is None:
        raise ValueError(f"not a tushare ts_code: {ts_code!r}")
    return ts_code


# ---------------------------------------------------------------------------
# schema alignment
# ---------------------------------------------------------------------------
def align_schema(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Drop unknown columns, fill missing ones, coerce dtypes to *table_name*.

    Column set and order come from ``SCHEMA[table_name]`` — this function never
    invents column names.  Missing columns are filled from
    ``_OPTIONAL_DEFAULTS`` when known, with nulls when the schema column is
    nullable, and refused otherwise.
    """
    schema = SCHEMA[table_name]
    out = pd.DataFrame(index=df.index)

    for name, col in schema.columns.items():
        if name in df.columns:
            out[name] = df[name]
        elif name in _OPTIONAL_DEFAULTS:
            out[name] = _OPTIONAL_DEFAULTS[name]
        elif col.nullable or not col.required:
            out[name] = pd.NA
        else:
            raise ValueError(
                f"{table_name}: required column {name!r} missing from source frame "
                f"(have {sorted(df.columns)}); refusing to fabricate it"
            )

    for name in schema.columns:
        out[name] = _coerce_column(out[name], schema.columns[name], name, table_name)
    return out.reset_index(drop=True)


def _coerce_column(series: pd.Series, col, name: str, table_name: str) -> pd.Series:
    # pandera wraps the declared type in a DataType object; `str()` is the
    # stable spelling ("int64", "float64", "bool", "str", "date").
    kind = str(col.dtype)
    if kind == "bool":
        return series.astype("boolean").fillna(False).astype(bool)
    if kind.startswith("int"):
        return _to_int64(series, name, table_name)
    if kind.startswith("float"):
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if kind == "str":
        return series.astype("string").astype(object)
    # date / datetime columns: the schema's own parser does the strict work at
    # validation time, so all we do here is make them parseable.  Keep ISO
    # strings as strings and leave datetimes alone.
    return series


def _to_int64(series: pd.Series, name: str, table_name: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    non_null = numeric.dropna()
    if not non_null.empty and not (non_null == non_null.round()).all():
        raise ValueError(
            f"{table_name}.{name}: non-integral float in integer column "
            f"(e.g. {non_null.iloc[0]!r}); refusing to truncate"
        )
    return (
        numeric.astype("Int64").astype("int64") if not numeric.isna().any() else numeric
    )


# ---------------------------------------------------------------------------
# AmazingData staging -> schema frames
# ---------------------------------------------------------------------------
# ``amazing_pull.py`` drops the source's own frames under
# ``data_platform/raw/amazingdata/``.  These helpers are the whole of the
# source -> schema mapping, so the unit conversions live in exactly one place
# per source.  Measured units (``task/probe_amazing_quant35.log``): AmazingData
# ``volume`` is 股 and ``amount`` is 元, i.e. already the storage unit — unlike
# tushare (手 / 千元) there is no scaling here.  ``FLOAT_SHARE`` is 万份.


def frame_from_amazing_calendar(cal: pd.DataFrame) -> pd.DataFrame:
    """Staging ``calendar.parquet`` -> the one-column calendar table."""
    if "cal_date" not in cal.columns:
        raise ValueError(
            f"amazing calendar staging missing cal_date: {list(cal.columns)}"
        )
    return pd.DataFrame({"trade_date": _ymd_int_to_date(cal["cal_date"])})


def frame_from_amazing_kline(
    kline: pd.DataFrame,
    nav: pd.DataFrame,
    share: pd.DataFrame,
) -> pd.DataFrame:
    """Staging kline + fund_nav + fund_share -> bars_daily column names.

    ``adjfactor`` comes from ``get_fund_nav``'s ``NAV_ADJ_FACTOR`` — the only
    adjustment source this vendor exposes.  It is written **as measured**, not
    renormalized: ``data_api._apply_adjustment`` divides by ``adj_latest`` /
    ``adj_first``, so both ``qfq`` and ``hfq`` are invariant to its scale.

    ``turnover`` follows AC-A2: ``amount / float_market_cap``, with the cap
    being ``FLOAT_SHARE`` (万份 -> 份) priced at that day's close.  When the
    share table has no row for a day the turnover is NaN, never 0.

    ``get_dividend`` / ``get_right_issue`` return 0 rows for the golden ETFs
    over 1990-2025, and kline carries no limit prices, so ``suspend`` and both
    limit flags are written ``False`` — a **source limitation, not a market
    fact**.  Do not reason about price limits from these columns.
    """
    for name, frame in (("kline", kline), ("fund_nav", nav), ("fund_share", share)):
        if frame is None or frame.empty:
            raise ValueError(f"amazing staging {name} is empty")

    bars = pd.DataFrame(
        {
            "ticker": kline["code"].map(ticker_convert),
            "trade_date": pd.to_datetime(kline["kline_time"]).dt.date,
            "open": pd.to_numeric(kline["open"], errors="coerce").astype("float64"),
            "high": pd.to_numeric(kline["high"], errors="coerce").astype("float64"),
            "low": pd.to_numeric(kline["low"], errors="coerce").astype("float64"),
            "close": pd.to_numeric(kline["close"], errors="coerce").astype("float64"),
            "volume": pd.to_numeric(kline["volume"], errors="coerce"),
            "amount": pd.to_numeric(kline["amount"], errors="coerce").astype("float64"),
        }
    )

    factors = _amazing_series(nav, "MARKET_CODE", "PRICE_DATE", "NAV_ADJ_FACTOR")
    bars["adjfactor"] = _lookup(bars, factors, "adjfactor")
    if bars["adjfactor"].isna().any():
        missing = bars.loc[bars["adjfactor"].isna(), "trade_date"].head(5).tolist()
        raise ValueError(
            f"NAV_ADJ_FACTOR missing for {int(bars['adjfactor'].isna().sum())} bar "
            f"day(s), e.g. {missing}; adjfactor is non-nullable and is not fabricated"
        )

    float_shares = _amazing_series(share, "MARKET_CODE", "CHANGE_DATE", "FLOAT_SHARE")
    cap = _lookup(bars, float_shares, "float_share") * 1e4 * bars["close"]
    bars["turnover"] = bars["amount"] / cap

    bars["suspend"] = False
    bars["is_limit_up"] = False
    bars["is_limit_down"] = False
    bars["quality_flag"] = 0
    return bars


def _amazing_series(
    frame: pd.DataFrame, code_col: str, date_col: str, value_col: str
) -> pd.Series:
    """Key a staging column by ``(ticker, trade_date)`` for lookup."""
    if not {code_col, date_col, value_col}.issubset(frame.columns):
        raise ValueError(
            f"amazing staging missing {[code_col, date_col, value_col]}: "
            f"{list(frame.columns)}"
        )
    keyed = pd.DataFrame(
        {
            "ticker": frame[code_col].map(ticker_convert),
            "trade_date": _ymd_int_to_date(frame[date_col]),
            "value": pd.to_numeric(frame[value_col], errors="coerce"),
        }
    )
    dupes = keyed.duplicated(subset=["ticker", "trade_date"]).sum()
    if dupes:
        raise ValueError(
            f"amazing staging has {dupes} duplicate (ticker, trade_date) rows"
        )
    return keyed.set_index(["ticker", "trade_date"])["value"]


def _lookup(bars: pd.DataFrame, keyed: pd.Series, label: str) -> pd.Series:
    index = pd.MultiIndex.from_arrays([bars["ticker"], bars["trade_date"]])
    values = keyed.reindex(index)
    values.index = bars.index
    return values.astype("float64").rename(label)


def _ymd_int_to_date(series: pd.Series) -> pd.Series:
    """``20250228`` int / str -> ``datetime.date``."""
    text = pd.to_numeric(series, errors="coerce")
    if text.isna().any():
        raise ValueError(
            f"non-numeric YYYYMMDD in amazing staging: {series.head(3).tolist()}"
        )
    return pd.to_datetime(text.astype("int64").astype(str), format="%Y%m%d").dt.date


# ---------------------------------------------------------------------------
# write paths
# ---------------------------------------------------------------------------
def table_path(
    table_name: str,
    data_root: str | os.PathLike[str] | None = None,
    **fmt: object,
) -> Path:
    """Compose ``data_root / PARTITION_PATHS[table_name]`` with *fmt*."""
    root = Path(data_root) if data_root is not None else DATA_ROOT
    template = PARTITION_PATHS[table_name]
    return root / template.format(**fmt)


def write_daily(
    df: pd.DataFrame,
    table_name: str,
    data_root: str | os.PathLike[str] | None = None,
) -> list[Path]:
    """Align *df*, split on ``trade_date``, write one parquet file per day."""
    aligned = align_schema(df, table_name)
    written: list[Path] = []
    if aligned.empty:
        return written
    if "trade_date" not in aligned.columns:
        raise ValueError(f"{table_name}: write_daily needs a trade_date column")
    for day, group in aligned.groupby("trade_date", sort=True):
        path = table_path(table_name, data_root, date=_iso(day))
        write_parquet_atomic(group.reset_index(drop=True), path, table=table_name)
        written.append(path)
    return written


def write_reference(
    df: pd.DataFrame,
    table_name: str,
    data_root: str | os.PathLike[str] | None = None,
    **fmt: object,
) -> Path:
    """Align *df* and write the whole table to its single reference file."""
    aligned = align_schema(df, table_name)
    path = table_path(table_name, data_root, **fmt)
    write_parquet_atomic(aligned, path, table=table_name)
    return path


def _iso(day: object) -> str:
    if hasattr(day, "isoformat"):
        text = day.isoformat()
        return text[:10]
    return str(day)[:10]


def yyyymmdd(value: str) -> str:
    """Accept ``YYYYMMDD`` or ``YYYY-MM-DD``; return the tushare ``YYYYMMDD``."""
    text = str(value).replace("-", "").strip()
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"not a date (YYYYMMDD or YYYY-MM-DD): {value!r}")
    return text
