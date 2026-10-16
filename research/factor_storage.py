"""Physical persistence for factor values — per-factor-one-table on disk.

Layout (factor_storage.md §61)::

    data_platform/factors/{factor_name}/v{version}/{year}.parquet
    data_platform/factors/{factor_name}/v{version}/metadata.json

Stores ``Factor.compute()`` **raw** output — ``NaN`` is suspension / missing
and is kept, never filled.  Winsorising, filling and neutralising belong to
S5's ``PostProcessor`` and happen at consumption time, not here.

This module is write / read only.  No post-processing (S5 ``PostProcessor``),
no scheduling (``FactorDAG``), no market bars (data domain), no governance
(S11).  Semantic version directories coexist: ``save_factor_full`` overwrites
the *current* version directory and leaves sibling versions alone.

Column contract — exactly three columns, matching
:func:`research.factor_interface.validate_output`:

==========  ==================  ==========================================
column      type                notes
==========  ==================  ==========================================
date        VARCHAR ``YYYYMMDD``  lexicographic order == time order
ticker      VARCHAR ``000001.SZ`` exchange-suffixed, JOINs against bars
{factor}    DOUBLE              raw value, NaN kept
==========  ==================  ==========================================
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from research.factor_interface import validate_output

__all__ = [
    "load_factor",
    "load_factor_matrix",
    "save_factor_append",
    "save_factor_full",
    "write_metadata",
]

#: Default root of the factor tree.
DEFAULT_BASE_DIR: str = "data_platform/factors"

#: Parquet codec — factor_storage.md pins zstd level 3.
COMPRESSION: str = "zstd"
COMPRESSION_LEVEL: int = 3

#: metadata.json field names, in write order (factor_storage.md §61).
METADATA_FIELDS: tuple[str, ...] = (
    "factor_name",
    "version",
    "description",
    "category",
    "frequency",
    "asset_types",
    "created_at",
    "updated_at",
    "depends_on",
    "depends_hash",
    "date_range",
    "row_count",
    "file_count",
    "compression",
)

_KEY_COLS: tuple[str, ...] = ("date", "ticker")


# ---------------------------------------------------------------------------
# Path and scalar helpers
# ---------------------------------------------------------------------------


def _safe_component(value: str, what: str) -> str:
    """Reject path separators / traversal in a factor name or version."""
    s = str(value)
    if not s or s in {".", ".."} or any(sep in s for sep in ("/", "\\")):
        raise ValueError(f"invalid {what} {value!r}")
    return s


def _version_dir(base_dir: str, name: str, version: str) -> Path:
    """``{base_dir}/{name}/v{version}`` — one directory per factor version."""
    return (
        Path(base_dir)
        / _safe_component(name, "factor name")
        / f"v{_safe_component(version, 'version')}"
    )


def _to_date_str(value: Any) -> str:
    """Normalise one date scalar to ``YYYYMMDD``."""
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    s = str(value)
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s.replace("-", "")
    return s


def _as_date_str(series: pd.Series) -> pd.Series:
    """Normalise the date column to ``YYYYMMDD`` strings.

    Datetime dtypes and ISO ``YYYY-MM-DD`` strings are both folded onto the
    eight-digit form so that lexicographic order is time order — the property
    :func:`load_factor` relies on for its ``[start, end]`` clip.
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.dt.strftime("%Y%m%d")
    if (
        series.dtype == object
        and len(series)
        and isinstance(series.iloc[0], (date, datetime))
    ):
        return series.map(_to_date_str)
    out = series.astype(str)
    if len(out) and out.str.fullmatch(r"\d{4}-\d{2}-\d{2}").all():
        out = out.str.replace("-", "", regex=False)
    return out


def _normalise(result: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    """Validate the compute() contract and fold dtypes onto the on-disk schema.

    Returns a new frame — the caller's ``result`` is never mutated.  The value
    column is coerced to float64 (DOUBLE) with NaN left in place; nothing here
    fills, winsorises or standardises.
    """
    validate_output(factor_name, result)
    out = result.copy()
    out["date"] = _as_date_str(out["date"])
    out["ticker"] = out["ticker"].astype(str)
    out[factor_name] = out[factor_name].astype("float64")
    return out


def _depends_hash(depends_on: Iterable[str]) -> str:
    """12-hex fingerprint of the ``depends_on()`` **declaration**.

    Not the same object as :func:`research.factor_runtime.compute_depends_hash`
    — that one fingerprints the input ``data`` dict and is what keys the
    runtime cache.  :func:`write_metadata`'s fixed signature carries no
    ``data``, so the declaration is all it can pin; a changed dependency set
    still changes this hash (and, per the versioning rule, also bumps
    ``version``).
    """
    raw = "|".join(sorted(str(d) for d in depends_on))
    return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()[:12]


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    df.to_parquet(
        path,
        compression=COMPRESSION,
        compression_level=COMPRESSION_LEVEL,
        index=False,
    )


# ---------------------------------------------------------------------------
# write_metadata
# ---------------------------------------------------------------------------


def write_metadata(out_dir, factor, result) -> None:
    """Write ``metadata.json`` describing a factor version directory.

    Identity fields come from *factor* (``name / version / description /
    category / frequency / asset_types / depends_on``).  ``depends_hash`` is
    the declaration fingerprint of ``depends_on()`` — see :func:`_depends_hash`
    for why it is not the runtime cache key's data fingerprint.

    ``date_range`` / ``row_count`` / ``file_count`` describe the parquet files
    **currently in *out_dir***, so after an append the counts are the
    directory's rather than the batch's.  *result* (the frame just recorded)
    supplies them only when the directory holds no parquet yet.

    ``created_at`` is preserved across rewrites; ``updated_at`` is now.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    meta_path = out_path / "metadata.json"

    previous: dict[str, Any] = {}
    if meta_path.exists():
        try:
            previous = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            previous = {}

    # Coverage from what is on disk; fall back to the recorded frame.
    files = sorted(out_path.glob("*.parquet"))
    if files:
        dates = pd.concat(
            [pd.read_parquet(p, columns=["date"]) for p in files], ignore_index=True
        )["date"].astype(str)
        row_count = int(len(dates))
        date_range: list[str] | None = [dates.min(), dates.max()]
    else:
        row_count = int(len(result))
        if row_count:
            rdates = result["date"].astype(str)
            date_range = [rdates.min(), rdates.max()]
        else:
            date_range = None

    depends_on = list(factor.depends_on())
    now = datetime.now(timezone.utc).isoformat()

    meta: dict[str, Any] = {
        "factor_name": factor.name,
        "version": factor.version,
        "description": getattr(factor, "description", ""),
        "category": getattr(factor, "category", ""),
        "frequency": getattr(factor, "frequency", ""),
        "asset_types": list(getattr(factor, "asset_types", []) or []),
        "created_at": previous.get("created_at", now),
        "updated_at": now,
        "depends_on": depends_on,
        "depends_hash": _depends_hash(depends_on),
        "date_range": date_range,
        "row_count": row_count,
        "file_count": len(files),
        "compression": COMPRESSION,
    }
    meta_path.write_text(
        json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# save
# ---------------------------------------------------------------------------


def save_factor_append(factor, result, base_dir="data_platform/factors") -> None:
    """Append *result* to the factor's current version directory.

    Incremental daily production: rows are split by the year prefix of
    ``date``, each year file is concatenated with what is already on disk and
    then ``drop_duplicates(["date", "ticker"])`` keeps the **first** row —
    values already stored win over the incoming batch.  Corrections go through
    :func:`save_factor_full`, never through a mixed append (factor_storage.md
    write-mode trigger table).
    """
    out_dir = _version_dir(base_dir, factor.name, factor.version)
    out_dir.mkdir(parents=True, exist_ok=True)
    data = _normalise(result, factor.name)

    if len(data):
        data = data.assign(_year=data["date"].str[:4])
        for year, chunk in data.groupby("_year", sort=True):
            path = out_dir / f"{year}.parquet"
            chunk = chunk.drop(columns="_year")
            if path.exists():
                chunk = pd.concat([pd.read_parquet(path), chunk], ignore_index=True)
            chunk = (
                chunk.drop_duplicates(["date", "ticker"])
                .sort_values(["date", "ticker"], kind="mergesort")
                .reset_index(drop=True)
            )
            _write_parquet(chunk, path)
        data = data.drop(columns=["_year"])

    write_metadata(out_dir, factor, data)


def save_factor_full(factor, result, base_dir="data_platform/factors") -> None:
    """Write *result* as the complete contents of the current version directory.

    Unlike :func:`save_factor_append` there is no concatenation: stale year
    files are removed first so the directory is exactly *result* — a narrowed
    recompute does not leave earlier years behind.  Sibling version directories
    are untouched; a new version is a new directory.  Duplicate ``(date,
    ticker)`` keys in *result* are collapsed to the first row before writing,
    so a buggy ``compute()`` cannot persist ambiguous keys.
    """
    out_dir = _version_dir(base_dir, factor.name, factor.version)
    out_dir.mkdir(parents=True, exist_ok=True)
    data = _normalise(result, factor.name)

    for stale in out_dir.glob("*.parquet"):
        stale.unlink()

    if len(data):
        data = data.assign(_year=data["date"].str[:4])
        for year, chunk in data.groupby("_year", sort=True):
            chunk = chunk.drop(columns="_year")
            chunk = (
                chunk.drop_duplicates(["date", "ticker"])
                .sort_values(["date", "ticker"], kind="mergesort")
                .reset_index(drop=True)
            )
            _write_parquet(chunk, out_dir / f"{year}.parquet")
        data = data.drop(columns=["_year"])

    write_metadata(out_dir, factor, data)


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


def load_factor(
    name, start, end, version="1.0", base_dir="data_platform/factors"
) -> pd.DataFrame:
    """Read one factor's raw values over ``[start, end]`` (inclusive).

    Only the year files that can overlap the window are read; rows are then
    clipped with plain string comparison on ``YYYYMMDD``.  Returns an empty
    frame with the contract columns when the window holds no data.  A missing
    version directory is an error — a typo'd factor name should not look like
    "no data in range".
    """
    out_dir = _version_dir(base_dir, name, version)
    if not out_dir.is_dir():
        raise FileNotFoundError(f"factor version directory not found: {out_dir}")

    columns = ["date", "ticker", name]
    start_s, end_s = _to_date_str(start), _to_date_str(end)

    frames: list[pd.DataFrame] = []
    for year in range(int(start_s[:4]), int(end_s[:4]) + 1):
        path = out_dir / f"{year}.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))

    if not frames:
        return pd.DataFrame(columns=columns)

    df = pd.concat(frames, ignore_index=True)
    df["date"] = _as_date_str(df["date"])
    df = df.loc[(df["date"] >= start_s) & (df["date"] <= end_s), columns]
    return df.sort_values(list(_KEY_COLS), kind="mergesort").reset_index(drop=True)


def load_factor_matrix(
    names: list[str],
    start,
    end,
    version="1.0",
    base_dir="data_platform/factors",
) -> pd.DataFrame:
    """Build the logical wide table ``(date, ticker, f1, f2, ...)``.

    One :func:`load_factor` per name, outer-joined on ``(date, ticker)`` —
    the cross-factor frame a combiner or evaluator wants, matching the
    company's GetViewer shape.  Missing values for a factor that has no row on
    a key stay ``NaN``; nothing is filled here.
    """
    if not names:
        raise ValueError("names must be a non-empty list of factor names")

    frames = [
        load_factor(n, start, end, version, base_dir).set_index(list(_KEY_COLS))
        for n in names
    ]
    wide = pd.concat(frames, axis=1, join="outer").reset_index()
    return wide.sort_values(list(_KEY_COLS), kind="mergesort").reset_index(drop=True)
