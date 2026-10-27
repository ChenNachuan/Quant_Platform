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
