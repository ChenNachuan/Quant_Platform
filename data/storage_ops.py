"""Storage operations — atomic writes, locking, lifecycle cleanup, capacity.

Runtime behaviour of the storage layer.  Structure (what a table looks like)
lives in :mod:`data.storage_schema`; this module never redefines columns,
types, nullability or checks (QUANT-13 §0).  Every write path calls
``SCHEMA[table].validate(df)`` before touching disk — a validation failure
means **do not write**.

Three-phase atomic write (``lock -> tmp -> rename``)::

    with FileLock(path):                 # phase 1: declare intent
        df.to_parquet(tmp, ...)          # phase 2: write {path}.tmp
        os.replace(tmp, path)            # phase 3: atomic commit

An exception at any point leaves neither a half-written target nor a stray
``.tmp``: the tmp is removed on the error path and the target is only ever
created by the final ``os.replace``.

Field authority: ``docs/design/data/storage_ops.md`` (§48–§51).
"""

from __future__ import annotations

import fcntl
import os
import re
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import yaml

from data.storage_schema import (
    CLEAN,
    PARTITION_PATHS,
    QUALITY_FLAG_BITS,
    QUALITY_FLAG_MAX,
    SCHEMA,
    TABLE_FAMILY,
    factor_schema,
    factor_view_name,
)

__all__ = [
    "CLEAN",
    "PARTITION_PATHS",
    "QUALITY_FLAG_BITS",
    "QUALITY_FLAG_MAX",
    "TABLE_FAMILY",
    "PARQUET_WRITE_CONFIG",
    "ConcurrentWriteError",
    "DiskFullError",
    "FileLock",
    "write_parquet_atomic",
    "normalize_missing_floats",
    "parse_date_from_filename",
    "cleanup_stale_tmp",
    "cleanup_stale_locks",
    "cleanup_expired",
    "load_lifecycle",
    "check_capacity",
    "check_daily_growth",
    "factor_schema",
    "factor_view_name",
]

# ---------------------------------------------------------------------------
# Write configuration (§48) — one dict, no per-table overrides
# ---------------------------------------------------------------------------
PARQUET_WRITE_CONFIG: dict[str, Any] = dict(
    compression="zstd",
    row_group_size=65536,
    use_dictionary=True,
    write_statistics=True,
    index=False,
)

# Capacity tiers (§51): free > 50 GB OK; 10–50 GB WARNING; ≤ 10 GB CRITICAL.
CAPACITY_OK_BYTES = 50 * 1024**3
CAPACITY_WARNING_BYTES = 10 * 1024**3

# Daily-growth anomaly baseline (§51): normal day is ~16 MB; > 100 MB is a
# duplicate-ingestion / bloat signal.  Single global baseline (§5 decision 9).
GROWTH_BASELINE_BYTES = 100 * 1024**2

# Stale-residue threshold for .tmp / .lock sweepers (§48).  Single threshold
# for all tables (§5 decision 1).
STALE_MAX_AGE_HOURS = 1

_TMP_SUFFIX = ".tmp"
_LOCK_SUFFIX = ".lock"


# ---------------------------------------------------------------------------
# Errors (§5 decision 5 — defined beside the code that raises them)
# ---------------------------------------------------------------------------
class ConcurrentWriteError(OSError):
    """Another writer already holds the lock for this target.

    Concurrent writes to the same day/table are a configuration error, not a
    normal condition to coordinate (§50).  Subclasses :class:`OSError` so
    callers that just want "the write failed" can catch one type.
    """


class DiskFullError(OSError):
    """Free space has crossed the CRITICAL tier; the write is refused.

    Recovery is operational, not automatic (§5 decision 7): run the cleanup
    sweepers, then retry by hand.  No auto-retry loop is attempted here.
    """


# ---------------------------------------------------------------------------
# FileLock — flock LOCK_EX | LOCK_NB (§48.6)
# ---------------------------------------------------------------------------
class FileLock:
    """Exclusive, non-blocking advisory lock on a target path.

    ``flock(LOCK_EX | LOCK_NB)``: if another writer holds the lock the enter
    raises :class:`ConcurrentWriteError` immediately (§50).  No retry —
    see §5 decision 6.

    The lock file is removed on exit.  Crash residue is swept by
    :func:`cleanup_stale_locks`, not by this class (§5 decision 4).
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._target = Path(path)
        self._lock_path = self._target.with_name(self._target.name + _LOCK_SUFFIX)
        self._fd: int | None = None

    @property
    def lock_path(self) -> Path:
        return self._lock_path

    def __enter__(self) -> FileLock:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        self._fd = fd
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            self._fd = None
            raise ConcurrentWriteError(
                f"another writer holds the lock for {self._target}"
            ) from exc
        return self

    def __exit__(self, *exc: object) -> None:
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        try:
            os.remove(self._lock_path)
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# Missing-float normalization (Review note carried into T2)
# ---------------------------------------------------------------------------
def _is_float_dtype(dtype: Any) -> bool:
    return str(dtype).startswith("float")


def _normalize_for_schema(df: pd.DataFrame, schema: Any) -> pd.DataFrame:
    """Replace ``None`` in the schema's float columns with ``NaN``.

    A column of all-``None`` becomes ``object`` dtype in pandas, which then
    fails the whole schema type check.  Float column names are read off the
    schema — this module does not name columns itself (§0).
    """
    out = df.copy()
    for name, col in schema.columns.items():
        if name not in out.columns:
            continue
        if not _is_float_dtype(col.dtype):
            continue
        series = out[name]
        if series.dtype == object:
            out[name] = series.where(series.notna(), float("nan")).astype("float64")
    return out


def normalize_missing_floats(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Normalize missing floats against ``SCHEMA[table]``.

    Public entry for the 11 registered tables.  Factor tables with a named
    value column are normalized inside :func:`write_parquet_atomic` against
    the resolved per-factor schema instead.
    """
    return _normalize_for_schema(df, SCHEMA[table])


# ---------------------------------------------------------------------------
# Schema resolution — T1 is the only authority (§0)
# ---------------------------------------------------------------------------
_FIXED_FACTOR_COLUMNS = ("ticker", "date")


def _resolve_schema(table: str, value_column: str | None = None):
    """Return the pandera schema for *table*.

    Standard tables resolve through ``SCHEMA``; factor tables may name their
    value column via :func:`factor_schema`.  ``value_column`` values that
    collide with the factor table's fixed columns are refused here rather
    than left to the silent dict-key overwrite inside ``factor_schema``
    (QUANT-13 finding; the one-line guard in ``storage_schema.py`` is a
    must-ask-back because this ticket forbids editing that file).
    """
    if table not in SCHEMA:
        raise KeyError(f"unknown table {table!r}; known: {sorted(SCHEMA)}")
    if table == "factor" and value_column is not None:
        if value_column in _FIXED_FACTOR_COLUMNS:
            raise ValueError(
                f"factor value_column {value_column!r} collides with a fixed "
                f"column; factor_schema would silently overwrite it"
            )
        schema = factor_schema(value_column)
        if len(schema.columns) != 3:
            raise ValueError(
                f"factor value_column {value_column!r} produced "
                f"{len(schema.columns)} columns, expected 3"
            )
        return schema
    return SCHEMA[table]


# ---------------------------------------------------------------------------
# Atomic write (§1) — lock → tmp → rename
# ---------------------------------------------------------------------------
def write_parquet_atomic(
    df: pd.DataFrame,
    path: str | os.PathLike[str],
    *,
    table: str,
    value_column: str | None = None,
    check_disk: bool = True,
    **parquet_kwargs: Any,
) -> None:
    """Write *df* to *path* atomically, after schema validation.

    1. Resolve the T1 schema and ``validate`` — failure means **do not write**.
    2. Normalize missing floats to ``NaN`` first (they would otherwise become
       ``object`` dtype and fail the type check).
    3. ``FileLock`` → write ``{path}.tmp`` → ``os.replace`` onto the target.

    A crash mid-write leaves neither a half-written target nor a stray
    ``.tmp``: the tmp is unlinked on the error path, and the target is only
    ever created by the final ``os.replace``.

    ``table`` names a key of :data:`data.storage_schema.SCHEMA`.  For factor
    tables pass ``value_column`` to select the per-factor value column.
    """
    target = Path(path)
    schema = _resolve_schema(table, value_column=value_column)

    # Missing floats must be NaN or the column turns object and the type
    # check fails.  Normalize against the *resolved* schema so a factor table
    # with a named value column is handled too (Review note for T2).
    prepared = _normalize_for_schema(df, schema)
    # Validation failure propagates; nothing has been written yet.
    validated = schema.validate(prepared)

    if check_disk:
        status = check_capacity(target.parent if target.parent.exists() else target)
        if status == "CRITICAL":
            raise DiskFullError(
                f"free space is at the CRITICAL tier; refusing to write {target}"
            )

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + _TMP_SUFFIX)

    with FileLock(target):
        try:
            kwargs = {**PARQUET_WRITE_CONFIG, **parquet_kwargs}
            validated.to_parquet(tmp, **kwargs)
            os.replace(tmp, target)
        except BaseException:
            # Never leave a half-written file or a stray .tmp behind.
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            raise


# ---------------------------------------------------------------------------
# Filename date parsing (§51) — used by the lifecycle sweeper
# ---------------------------------------------------------------------------
_DATE_PATTERNS = (
    re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(\d{8})(?!\d)"),
    re.compile(r"(?<!\d)(\d{4})(?!\d)"),
)


def parse_date_from_filename(name: str) -> date | None:
    """Return the date encoded in *name*, or ``None`` if there is none.

    Accepts ``YYYY-MM-DD``, ``YYYYMMDD`` and a bare ``YYYY`` year token (the
    factor year-partition shape).  Names with no date — ``contract_specs.parquet``
    and other full-file reference tables — return ``None`` and are never
    expired (§5 decision 8: a leftover file is cheap, a wrong delete is not).
    """
    stem = Path(name).name
    for pattern, kind in (
        (_DATE_PATTERNS[0], "iso"),
        (_DATE_PATTERNS[1], "compact"),
        (_DATE_PATTERNS[2], "year"),
    ):
        match = pattern.search(stem)
        if not match:
            continue
        token = match.group(1)
        try:
            if kind == "iso":
                return date.fromisoformat(token)
            if kind == "compact":
                return datetime.strptime(token, "%Y%m%d").date()
            return date(int(token), 1, 1)
        except ValueError:
            # A date-shaped token that is not a real date is a malformed
            # name — do not fall through and guess a year from it.
            return None
    return None


# ---------------------------------------------------------------------------
# Residue sweepers — .tmp and .lock left by a crashed writer (§48)
# ---------------------------------------------------------------------------
def _iter_files(root: Path) -> Iterator[Path]:
    if not root.exists():
        return
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            yield Path(dirpath) / filename


def _is_stale(path: Path, max_age_hours: float, now: datetime | None) -> bool:
    reference = now if now is not None else datetime.now()
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
    except FileNotFoundError:
        return False
    return (reference - mtime) > timedelta(hours=max_age_hours)


def cleanup_stale_tmp(
    root: str | os.PathLike[str],
    max_age_hours: float = STALE_MAX_AGE_HOURS,
    *,
    now: datetime | None = None,
    dry_run: bool = False,
) -> list[Path]:
    """Remove ``.tmp`` residue older than *max_age_hours*.

    These are crashed-writer leftovers, not expiring data — ``cleanup_expired``
    is the other sweeper (§48: 残留 vs 过期).  A live write's tmp is younger
    than the write itself, so a single one-hour threshold is safe for every
    table (§5 decision 1).
    """
    root = Path(root)
    removed: list[Path] = []
    for path in _iter_files(root):
        if not path.name.endswith(_TMP_SUFFIX):
            continue
        if not _is_stale(path, max_age_hours, now):
            continue
        removed.append(path)
        if not dry_run:
            path.unlink(missing_ok=True)
    return removed


def cleanup_stale_locks(
    root: str | os.PathLike[str],
    max_age_hours: float = STALE_MAX_AGE_HOURS,
    *,
    now: datetime | None = None,
    dry_run: bool = False,
) -> list[Path]:
    """Remove ``.lock`` residue older than *max_age_hours*.

    ``flock`` releases on process exit, so a stale lock file is purely
    cosmetic.  Kept separate from :func:`cleanup_stale_tmp` on purpose (§5
    decision 4): deleting a live lock breaks mutual exclusion, so it must
    never share a code path with a safe delete.
    """
    root = Path(root)
    removed: list[Path] = []
    for path in _iter_files(root):
        if not path.name.endswith(_LOCK_SUFFIX):
            continue
        if not _is_stale(path, max_age_hours, now):
            continue
        removed.append(path)
        if not dry_run:
            path.unlink(missing_ok=True)
    return removed


# ---------------------------------------------------------------------------
# Lifecycle cleanup — driven by data/lifecycle.yaml (§3)
# ---------------------------------------------------------------------------
def load_lifecycle(config_path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    """Load the retention policy from ``data/lifecycle.yaml``.

    The YAML file is the single source of retention truth; nothing is
    hard-coded here.  ``pyyaml`` is currently a transitive dependency of the
    stack — declaring it explicitly is a follow-up (see the PR description).
    """
    if config_path is None:
        config_path = Path(__file__).with_name("lifecycle.yaml")
    with open(config_path, encoding="utf-8") as handle:
        policy = yaml.safe_load(handle)
    if not isinstance(policy, dict) or "policies" not in policy:
        raise ValueError(f"lifecycle config at {config_path} has no 'policies' key")
    return policy


def _match_policy(rel_parts: tuple[str, ...], policies: list[dict]) -> dict:
    """Return the longest-prefix policy matching *rel_parts*, or the default."""
    best: dict | None = None
    best_len = -1
    for policy in policies:
        prefix = str(policy.get("prefix", "")).strip("/")
        if not prefix:
            continue
        prefix_parts = tuple(prefix.split("/"))
        if rel_parts[: len(prefix_parts)] == prefix_parts and len(prefix_parts) > best_len:
            best = policy
            best_len = len(prefix_parts)
    return best if best is not None else {}


def cleanup_expired(
    root: str | os.PathLike[str] = "data_platform",
    *,
    dry_run: bool = True,
    config_path: str | os.PathLike[str] | None = None,
    today: date | None = None,
) -> list[Path]:
    """Delete files older than their lifecycle.yaml retention window.

    ``dry_run`` defaults to **True**: a bare call reports and deletes nothing,
    so there is no "call it and it deletes" path.  Production callers pass
    ``dry_run=False`` explicitly (§3).

    Rules with ``retention: permanent`` are skipped.  Files whose name has no
    parseable date are kept.  ``.tmp`` / ``.lock`` residue is the job of
    :func:`cleanup_stale_tmp` / :func:`cleanup_stale_locks`, not this one.

    Returns the paths that were deleted (``dry_run=False``) or would be
    (``dry_run=True``).
    """
    root = Path(root)
    policy_doc = load_lifecycle(config_path)
    policies = list(policy_doc.get("policies", []))
    default_policy = {"retention": policy_doc.get("default", "permanent")}
    cutoff_today = today if today is not None else date.today()

    removed: list[Path] = []
    for path in _iter_files(root):
        if path.name.endswith((_TMP_SUFFIX, _LOCK_SUFFIX)):
            continue
        try:
            rel = path.relative_to(root)
        except ValueError:
            continue
        policy = _match_policy(rel.parts, policies) or default_policy
        if str(policy.get("retention", "permanent")) == "permanent":
            continue
        days = policy.get("days")
        if days is None:
            continue
        file_date = parse_date_from_filename(path.name)
        if file_date is None:
            continue
        if file_date < cutoff_today - timedelta(days=int(days)):
            removed.append(path)
            if not dry_run:
                path.unlink(missing_ok=True)
    return removed


# ---------------------------------------------------------------------------
# Capacity tiers (§51)
# ---------------------------------------------------------------------------
def check_capacity(data_root: str | os.PathLike[str] = "data_platform") -> str:
    """Return the free-space tier for the filesystem holding *data_root*.

    ``OK`` above 50 GB free, ``WARNING`` at 10–50 GB, ``CRITICAL`` at or
    below 10 GB.  ``CRITICAL`` is the "refuse the write" tier.
    """
    target = Path(data_root)
    probe = target if target.exists() else target.parent
    if not probe.exists():
        probe = Path.cwd()
    free = shutil.disk_usage(probe).free
    if free > CAPACITY_OK_BYTES:
        return "OK"
    if free > CAPACITY_WARNING_BYTES:
        return "WARNING"
    return "CRITICAL"


def check_daily_growth(
    data_root: str | os.PathLike[str] = "data_platform",
    *,
    today: date | None = None,
    baseline_bytes: int = GROWTH_BASELINE_BYTES,
) -> dict[str, Any]:
    """Return today's data volume against the single 100 MB/day baseline.

    Volume is the total size of files whose *filename date* equals *today* —
    the same date the partition is named after, so a rewrite of an existing
    day does not count as new data.  Exceeding the baseline is an anomaly
    signal (duplicate ingestion / bloat), not a write refusal (§51).
    """
    root = Path(data_root)
    day = today if today is not None else date.today()
    total = 0
    count = 0
    for path in _iter_files(root):
        if parse_date_from_filename(path.name) != day:
            continue
        try:
            total += path.stat().st_size
        except FileNotFoundError:
            continue
        count += 1
    return {
        "date": day,
        "bytes": total,
        "files": count,
        "baseline_bytes": baseline_bytes,
        "exceeded": total > baseline_bytes,
    }
