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

Column contract — three columns, and the **types come from**
:func:`data.storage_schema.factor_schema`, which is the landed authority on
table structure.  This module does not keep a private copy of the column
list; ``data/`` is read-only here, and importing it from ``research/`` is the
allowed direction.

==========  ==================  ==========================================
column      type                notes
==========  ==================  ==========================================
date        Parquet ``DATE``    ``date32``; joins ``load_bars`` on ``date``
ticker      VARCHAR             exchange-suffixed (``000001.SZ``)
{factor}    DOUBLE              raw value, NaN kept
==========  ==================  ==========================================

Human ruling 2026-10-18 (option A): the issue's VARCHAR ``YYYYMMDD`` row is
superseded by ``data/storage_schema.py``, which types every date column
``pa.Date`` and registers this exact path as ``PARTITION_PATHS["factor"]`` —
one file path, one type.  Consequences worth stating: ``2024/01/05`` is legal
input and means 2024-01-05, ``NaT`` is refused by the column type rather than
persisted as the string ``"NaT"``, and the year partition is derived with
``dt.year`` because ``str[:4]`` is exactly where a bad value used to hide.

In memory the date column is ``datetime.date`` — the same object
``pd.read_parquet`` returns for ``date32``, so a factor frame and a bars frame
join without conversion.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from data.storage_ops import write_parquet_atomic
from data.storage_schema import factor_schema

__all__ = [
    "load_factor",
    "load_factor_matrix",
    "save_factor_append",
    "save_factor_full",
    "write_metadata",
]

#: Default root of the factor tree.
DEFAULT_BASE_DIR: str = "data_platform/factors"

#: Parquet codec — factor_storage.md pins zstd level 3.  Passed through to
#: :func:`data.storage_ops.write_parquet_atomic`, which owns the tmp+rename.
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


def _to_date(value: Any) -> date:
    """Normalise one date scalar to ``datetime.date``.

    Accepts the same forms the schema's parser does (``date`` / ``datetime`` /
    parseable string, including ``2024/01/05`` and ``YYYYMMDD``) and refuses
    the rest.  Used for the ``[start, end]`` read window.
    """
    ts = pd.to_datetime(
        pd.Series([value], dtype=object), format="mixed", errors="raise"
    ).iloc[0]
    if pd.isna(ts):
        raise ValueError(f"date bound must not be missing: {value!r}")
    return ts.date()


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


def _write_year(chunk: pd.DataFrame, path: Path, factor_name: str) -> None:
    """Persist one year file through the atomic writer.

    :func:`data.storage_ops.write_parquet_atomic` owns the whole crash-safety
    story: validate against :func:`factor_schema`, then ``FileLock`` → write
    ``{path}.tmp`` → ``fsync`` → ``os.replace``.  A crash or disk-full leaves
    neither a half-written target nor a stray ``.tmp``.
    """
    write_parquet_atomic(
        chunk,
        path,
        table="factor",
        value_column=factor_name,
        compression=COMPRESSION,
        compression_level=COMPRESSION_LEVEL,
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
    supplies them only when the directory holds no parquet yet.  ``date_range``
    is ISO ``YYYY-MM-DD`` — the natural serialisation of a ``DATE`` column.

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
        )["date"]
        row_count = int(len(dates))
        date_range: list[str] | None = [str(dates.min()), str(dates.max())]
    else:
        row_count = int(len(result))
        if row_count:
            rdates = pd.to_datetime(result["date"], format="mixed")
            date_range = [str(rdates.min().date()), str(rdates.max().date())]
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


def _normalise(result: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    """Fold *result* onto the on-disk contract and reject what it will not take.

    The column set is read off :func:`factor_schema` so it cannot drift from
    the ``data/`` authority.  Dates are parsed to ``datetime64`` here — that
    is what makes the year split a ``dt.year`` rather than a ``str[:4]`` —
    duplicate ``(date, ticker)`` rows are collapsed to the first (the
    semantics ``save_factor_full`` was reviewed on), and then
    :func:`factor_schema` is run as the gate: ``NaT`` is refused by
    ``nullable=False``, unparseable strings raise, and the ``(ticker, date)``
    primary key is enforced.

    The returned frame keeps ``datetime64`` dates for partitioning;
    :func:`data.storage_ops.write_parquet_atomic` re-validates and narrows to
    ``DATE`` on the way to disk.  The caller's ``result`` is never mutated.
    """
    schema = factor_schema(factor_name)
    expected = list(schema.columns)
    if set(result.columns) != set(expected):
        raise ValueError(
            f"compute() must return columns {expected}, got {list(result.columns)}"
        )

    out = result.copy()
    out["date"] = pd.to_datetime(out["date"], format="mixed", errors="raise")
    out["ticker"] = out["ticker"].astype(str)
    out[factor_name] = out[factor_name].astype("float64")
    out = (
        out.drop_duplicates(["date", "ticker"])
        .sort_values(["date", "ticker"], kind="mergesort")
        .reset_index(drop=True)
    )

    schema.validate(out)  # gate only — the frame below keeps datetime64
    return out


def save_factor_append(factor, result, base_dir="data_platform/factors") -> None:
    """Append *result* to the factor's current version directory.

    Incremental daily production: rows are split by the year of their parsed
    ``date`` (``dt.year``), each year file is concatenated with what is already
    on disk and then ``drop_duplicates(["date", "ticker"])`` keeps the **first**
    row — values already stored win over the incoming batch.  Corrections go
    through :func:`save_factor_full`, never through a mixed append
    (factor_storage.md write-mode trigger table).

    *result* is validated before any directory is created, so a rejected write
    leaves no empty version directory behind — :func:`load_factor` reports
    "not found" rather than a silent empty frame.  Each year file is written
    through :func:`data.storage_ops.write_parquet_atomic`, so a failed write
    never truncates what was already stored.
    """
    data = _normalise(result, factor.name)
    out_dir = _version_dir(base_dir, factor.name, factor.version)
    out_dir.mkdir(parents=True, exist_ok=True)

    if len(data):
        data = data.assign(_year=data["date"].dt.year)
        for year, chunk in data.groupby("_year", sort=True):
            path = out_dir / f"{year}.parquet"
            chunk = chunk.drop(columns="_year")
            if path.exists():
                stored = pd.read_parquet(path)
                stored["date"] = pd.to_datetime(stored["date"], format="mixed")
                chunk = pd.concat([stored, chunk], ignore_index=True)
            chunk = (
                chunk.drop_duplicates(["date", "ticker"])
                .sort_values(["date", "ticker"], kind="mergesort")
                .reset_index(drop=True)
            )
            _write_year(chunk, path, factor.name)
        data = data.drop(columns=["_year"])

    write_metadata(out_dir, factor, data)


def save_factor_full(factor, result, base_dir="data_platform/factors") -> None:
    """Write *result* as the complete contents of the current version directory.

    Unlike :func:`save_factor_append` there is no concatenation: the directory
    becomes exactly *result* — a narrowed recompute does not leave earlier
    years behind.  Sibling version directories are untouched; a new version is
    a new directory.  Duplicate ``(date, ticker)`` keys in *result* are
    collapsed to the first row before writing, so a buggy ``compute()`` cannot
    persist ambiguous keys.

    *result* is validated before any directory is created, so a rejected write
    leaves no empty version directory behind.  Stale year files are removed
    **after** every replacement has been written: a crash, disk-full or kill
    mid-loop therefore leaves the previous years intact rather than a subset
    that :func:`load_factor` would report as complete history.
    """
    data = _normalise(result, factor.name)
    out_dir = _version_dir(base_dir, factor.name, factor.version)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: set[Path] = set()
    if len(data):
        data = data.assign(_year=data["date"].dt.year)
        for year, chunk in data.groupby("_year", sort=True):
            chunk = chunk.drop(columns="_year")
            path = out_dir / f"{year}.parquet"
            _write_year(chunk, path, factor.name)
            written.add(path)
        data = data.drop(columns=["_year"])

    # Only now — every replacement exists — is a year file allowed to go.
    for stale in sorted(out_dir.glob("*.parquet")):
        if stale not in written:
            stale.unlink()

    write_metadata(out_dir, factor, data)


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


def load_factor(
    name, start, end, version="1.0", base_dir="data_platform/factors"
) -> pd.DataFrame:
    """Read one factor's raw values over ``[start, end]`` (inclusive).

    Only the year files that can overlap the window are read; rows are then
    clipped on the parsed dates.  ``date`` comes back as ``datetime.date`` —
    the ``date32`` convention, matching :func:`data.data_api.load_bars` so the
    two join without conversion.  Returns an empty frame with the contract
    columns when the window holds no data.  A missing version directory is an
    error — a typo'd factor name should not look like "no data in range".
    """
    out_dir = _version_dir(base_dir, name, version)
    if not out_dir.is_dir():
        raise FileNotFoundError(f"factor version directory not found: {out_dir}")

    columns = ["date", "ticker", name]
    start_d, end_d = _to_date(start), _to_date(end)

    frames: list[pd.DataFrame] = []
    for year in range(start_d.year, end_d.year + 1):
        path = out_dir / f"{year}.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))

    if not frames:
        return pd.DataFrame(columns=columns)

    df = pd.concat(frames, ignore_index=True)
    parsed = pd.to_datetime(df["date"], format="mixed")
    df = df.loc[
        (parsed >= pd.Timestamp(start_d)) & (parsed <= pd.Timestamp(end_d)), columns
    ]
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
