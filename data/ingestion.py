"""Multi-source ingestion arbitration (QUANT-40, S10-T1).

Priority, cross-source comparison and gap-fill marking per
``docs/design/data/ingestion.md`` (settled details in
``docs/archive/OPEN_QUESTIONS.md`` §41).  Core rule: the asset layer is
written from **one** source; backups only cross-check and backfill gaps —
never mix-write, so lineage stays traceable to a single source and batch.

Scope of this module: arbitration and comparison.  Fetching is
:mod:`data.source_adapter` (called from the ingest pipeline, never rewritten
here); DQC is ``data/dqc/``; landing primitives are :mod:`data.storage_ops`
and :mod:`data.raw`.  Comparison runs on **post-Normalizer** frames — raw
column names and units differ per source and are not comparable.

Implementation-time rulings (recorded in the QUANT-40 PR description):

* ``should_cross_check`` fires when the primary source's ingest has just
  completed and every backup's raw batch has arrived (§41.4 default).  A
  backup's own ingest never triggers, and an asset type with no backup never
  cross-checks.
* ``raw_exists`` is raw landing **file presence** (:func:`data.raw.raw_path`),
  not watermark inspection — watermark formats are subclass-defined and the
  platform never parses them.
* ``NUMERIC_FIELDS`` (§41 open item) is defined here from the storage
  schemas: the compared numeric columns per asset type.  ``option_basic`` and
  ``reference`` have no cross-check list yet (option_basic is single-source;
  reference is per-table and lands with T2/T3).
* The ``pe_pb`` tolerance class covers the valuation ratios ``pe_ttm`` /
  ``pe_lyr`` / ``pb`` / ``ps_ttm`` — same cross-source definition-difference
  rationale as PE/PB.
* ``field_override`` sources must appear in the same entry's ``priority`` —
  an override naming a source we do not fetch is a config error.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from data.raw import raw_path
from data.storage_ops import DiskFullError, FileLock, check_capacity
from data.storage_schema import PARTITION_PATHS
from core.config import DATA_ROOT

__all__ = [
    "SOURCE_PRIORITY_PATH",
    "NUMERIC_FIELDS",
    "TOLERANCE_CLASS",
    "TOLERANCE_CLASSES",
    "MismatchDetail",
    "CrossCheckResult",
    "ResponseLevel",
    "load_source_priority",
    "load_tolerances",
    "get_source_priority",
    "get_primary_source",
    "get_field_overrides",
    "tolerance_for",
    "cross_source_check",
    "response_level",
    "respond",
    "backfill_source_tag",
    "write_asset",
    "raw_exists",
    "should_cross_check",
]

logger = logging.getLogger(__name__)

SOURCE_PRIORITY_PATH = DATA_ROOT / "config" / "source_priority.yaml"

# ---------------------------------------------------------------------------
# Field classification (§41 open item: NUMERIC_FIELDS was referenced from
# three design docs and defined nowhere — defined here, from the storage
# schemas).  Only numeric columns participate; flags (suspend / is_limit_up /
# quality_flag) are not cross-compared.
# ---------------------------------------------------------------------------
NUMERIC_FIELDS: dict[str, tuple[str, ...]] = {
    "bars_daily": (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "adjfactor",
        "turnover",
    ),
    "bars_minute": ("open", "high", "low", "close", "volume", "amount"),
    "fundamental": (
        "total_market_cap",
        "float_market_cap",
        "pe_ttm",
        "pe_lyr",
        "pb",
        "ps_ttm",
        "total_shares",
        "float_shares",
    ),
    "option_basic": (),  # single source — nothing to cross-check
    "reference": (),  # per-table lists land with the reference slice
}

# Field name -> tolerance class.  Anything not listed falls to "default".
TOLERANCE_CLASS: dict[str, str] = {
    "open": "price",
    "high": "price",
    "low": "price",
    "close": "price",
    "pre_close": "price",
    "volume": "volume",
    "amount": "amount",
    "adjfactor": "adjfactor",
    "pe_ttm": "pe_pb",
    "pe_lyr": "pe_pb",
    "pb": "pe_pb",
    "ps_ttm": "pe_pb",
}

# The five tolerance classes of §41.3.  Values live in source_priority.yaml
# (cross_check.tolerances); the comparison mode is fixed here — price and
# adjfactor are absolute, everything else is relative to the primary value.
TOLERANCE_CLASSES: tuple[str, ...] = (
    "price",
    "volume",
    "amount",
    "adjfactor",
    "pe_pb",
    "default",
)
_ABSOLUTE_CLASSES = frozenset({"price", "adjfactor"})

# Tiered response bands (§41.5): <0.1% silent record, 0.1–1% WARNING,
# >1% ERROR.  Any adjfactor mismatch is at least WARNING whatever the band.
ResponseLevel = Literal["silent", "WARNING", "ERROR"]
SILENT_BAND_MAX = 0.001
ERROR_BAND_MIN = 0.01


# ---------------------------------------------------------------------------
# source_priority.yaml loading
# ---------------------------------------------------------------------------
def load_source_priority(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Load and validate the source arbitration table.

    Validation: every entry exposes a ``priority`` list (directly, or per
    sub-table for ``reference``), and every ``field_override`` source appears
    in the same entry's ``priority`` list.  Raises ``ValueError`` otherwise.
    """
    cfg = yaml.safe_load(Path(path or SOURCE_PRIORITY_PATH).read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError(f"{SOURCE_PRIORITY_PATH}: top level must be a mapping")
    for asset_type, entry in cfg.items():
        if asset_type == "cross_check":
            continue
        _validate_entry(str(asset_type), entry)
    return cfg


def _validate_entry(asset_type: str, entry: Any) -> None:
    if not isinstance(entry, dict):
        raise ValueError(f"{asset_type}: entry must be a mapping")
    if isinstance(entry.get("priority"), list):
        _validate_table(asset_type, entry)
        return
    for sub, sub_entry in entry.items():
        if not isinstance(sub_entry, dict) or not isinstance(
            sub_entry.get("priority"), list
        ):
            raise ValueError(f"{asset_type}.{sub}: needs a priority list")
        _validate_table(f"{asset_type}.{sub}", sub_entry)


def _validate_table(name: str, entry: dict) -> None:
    priority = entry["priority"]
    if not all(isinstance(s, str) and s for s in priority):
        raise ValueError(f"{name}: priority must be a list of source names")
    overrides = entry.get("field_override") or {}
    if not isinstance(overrides, dict):
        raise ValueError(f"{name}: field_override must be a mapping")
    for field, source in overrides.items():
        if source not in priority:
            raise ValueError(
                f"{name}: field_override {field!r} names {source!r}, "
                f"which is not in priority {priority}"
            )


def _entry_for(cfg: dict, asset_type: str, sub_type: str | None) -> dict:
    entry = cfg[asset_type]
    if sub_type is not None:
        entry = entry[sub_type]
    if not isinstance(entry, dict) or not isinstance(entry.get("priority"), list):
        raise ValueError(f"{asset_type}: per-table entry — pass sub_type")
    return entry


def load_tolerances(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, float]:
    """Return the five-tier tolerance table from ``cross_check.tolerances``."""
    cfg = load_source_priority(path)
    section = cfg.get("cross_check") or {}
    tolerances = section.get("tolerances") or {}
    missing = [c for c in TOLERANCE_CLASSES if c not in tolerances]
    if missing:
        raise ValueError(f"cross_check.tolerances missing classes: {missing}")
    return {c: float(tolerances[c]) for c in TOLERANCE_CLASSES}


def get_source_priority(
    asset_type: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> list[str]:
    """Priority-ordered source list for *asset_type* (first = primary)."""
    cfg = load_source_priority(path)
    return list(_entry_for(cfg, asset_type, sub_type)["priority"])


def get_primary_source(
    asset_type: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> str:
    """The one source allowed to write the asset layer for *asset_type*."""
    priority = get_source_priority(asset_type, sub_type, path=path)
    if not priority:
        raise ValueError(f"no source assigned for {asset_type}/{sub_type or '-'}")
    return priority[0]


def get_field_overrides(
    asset_type: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """Field -> source overrides for *asset_type* (``{}`` when none)."""
    cfg = load_source_priority(path)
    entry = _entry_for(cfg, asset_type, sub_type)
    return dict(entry.get("field_override") or {})


# ---------------------------------------------------------------------------
# Tolerances (five tiers, §41.3)
# ---------------------------------------------------------------------------
def tolerance_for(
    field: str, tolerances: Mapping[str, float] | None = None
) -> tuple[float, str]:
    """``(value, mode)`` for *field*; mode is ``absolute`` or ``relative``.

    Falls back to the ``default`` class for unlisted fields, and to
    :func:`load_tolerances` when no table is passed.
    """
    cls = TOLERANCE_CLASS.get(field, "default")
    table = dict(tolerances) if tolerances is not None else load_tolerances()
    value = float(table[cls] if cls in table else table["default"])
    mode = "absolute" if cls in _ABSOLUTE_CLASSES else "relative"
    return value, mode


# ---------------------------------------------------------------------------
# Cross-check dataclasses (ingestion.md interface signatures)
# ---------------------------------------------------------------------------
@dataclass
class MismatchDetail:
    """Per-field mismatch aggregate (not per-ticker detail)."""

    field: str
    mismatch_count: int
    mismatch_rate: float  # mismatch_count / matched_rows
    max_abs_diff: float
    sample_tickers: list[str]  # first 5 mismatching tickers


@dataclass
class CrossCheckResult:
    """One cross-check run over a (trade_date, asset_type) cross-section.

    ``total_rows`` is the outer-join size and ``matched_rows`` the tickers
    both sources carry; ``missing_in_primary`` counts tickers only the backup
    has, ``missing_in_secondary`` tickers only the primary has.
    """

    asset_type: str
    trade_date: str
    primary_source: str
    secondary_source: str
    total_rows: int
    matched_rows: int
    mismatched_fields: dict[str, MismatchDetail]
    missing_in_primary: int
    missing_in_secondary: int

    @property
    def mismatch_rate(self) -> float:
        """Worst per-field mismatch rate (``0.0`` when everything matched)."""
        if not self.mismatched_fields:
            return 0.0
        return max(d.mismatch_rate for d in self.mismatched_fields.values())


# ---------------------------------------------------------------------------
# cross_source_check
# ---------------------------------------------------------------------------
def cross_source_check(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    asset_type: str,
    trade_date: str | date | None = None,
    tolerances: Mapping[str, float] | None = None,
    *,
    secondary_source: str | None = None,
) -> CrossCheckResult:
    """Compare two post-Normalizer cross-sections of *asset_type*.

    ``df_a`` is the primary-side frame (the one that owns the asset layer),
    ``df_b`` the backup side.  Rows join on ``[ticker, trade_date]`` (or
    ``[ticker]`` alone when neither frame carries ``trade_date``).  Numeric
    fields are compared per :data:`NUMERIC_FIELDS` with the five-tier
    tolerance table; a pair mismatches past the tier's tolerance, or when the
    value is present on one side only (both sides null matches).

    ``primary_source`` comes from ``source_priority.yaml`` (the authority for
    who owns the asset layer); ``secondary_source`` is passed in, falling back
    to ``df_b.attrs["source"]``.
    """
    if "ticker" not in df_a.columns or "ticker" not in df_b.columns:
        raise ValueError("both frames need a 'ticker' join column")
    fields = NUMERIC_FIELDS.get(asset_type)
    if fields is None:
        raise KeyError(
            f"no NUMERIC_FIELDS entry for {asset_type!r} — define it in "
            "data/ingestion.py before cross-checking it"
        )
    join_keys = (
        ["ticker", "trade_date"]
        if "trade_date" in df_a.columns and "trade_date" in df_b.columns
        else ["ticker"]
    )
    if df_a.duplicated(join_keys).any() or df_b.duplicated(join_keys).any():
        raise ValueError(f"duplicate join keys {join_keys} in input frames")

    merged = df_a.merge(
        df_b, on=join_keys, how="outer", suffixes=("_a", "_b"), indicator=True
    )
    both = merged[merged["_merge"] == "both"]
    matched_rows = int(len(both))

    mismatched: dict[str, MismatchDetail] = {}
    for field in fields:
        col_a, col_b = f"{field}_a", f"{field}_b"
        if col_a not in both.columns or col_b not in both.columns:
            continue  # only fields both sources deliver are comparable
        a, b = both[col_a], both[col_b]
        na, nb = a.isna(), b.isna()
        valid = ~(na | nb)
        one_missing = na ^ nb
        diff = (a - b).abs()
        tol, mode = tolerance_for(field, tolerances)
        if mode == "absolute":
            bad = valid & (diff > tol)
        else:
            base = a.abs().clip(lower=1e-9)
            bad = valid & ((diff / base) > tol)
        bad = bad | one_missing
        count = int(bad.sum())
        if not count:
            continue
        mismatched[field] = MismatchDetail(
            field=field,
            mismatch_count=count,
            mismatch_rate=(count / matched_rows) if matched_rows else 0.0,
            max_abs_diff=float(diff[valid].max()) if bool(valid.any()) else 0.0,
            sample_tickers=list(both.loc[bad, "ticker"].head(5)),
        )

    return CrossCheckResult(
        asset_type=asset_type,
        trade_date=_as_date_str(
            trade_date if trade_date is not None else _frame_trade_date(df_a, df_b)
        ),
        primary_source=_primary_source_or(df_a, asset_type),
        secondary_source=secondary_source or str(df_b.attrs.get("source", "secondary")),
        total_rows=int(len(merged)),
        matched_rows=matched_rows,
        mismatched_fields=mismatched,
        missing_in_primary=int((merged["_merge"] == "right_only").sum()),
        missing_in_secondary=int((merged["_merge"] == "left_only").sum()),
    )


def _frame_trade_date(df_a: pd.DataFrame, df_b: pd.DataFrame) -> Any:
    for df in (df_a, df_b):
        if "trade_date" in df.columns and len(df):
            return df["trade_date"].iloc[0]
    return ""


def _as_date_str(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value) if value is not None else ""


def _primary_source_or(df_a: pd.DataFrame, asset_type: str) -> str:
    try:
        return get_primary_source(asset_type)
    except (KeyError, ValueError):
        return str(df_a.attrs.get("source", "primary"))


# ---------------------------------------------------------------------------
# Tiered response (§41.5)
# ---------------------------------------------------------------------------
def response_level(mismatch_rate: float) -> ResponseLevel:
    """Band for a mismatch rate: <0.1% silent, 0.1–1% WARNING, >1% ERROR."""
    if mismatch_rate > ERROR_BAND_MIN:
        return "ERROR"
    if mismatch_rate >= SILENT_BAND_MAX:
        return "WARNING"
    return "silent"


def respond(result: CrossCheckResult) -> ResponseLevel:
    """Decide (and log) the response for one cross-check result.

    The worst per-field band wins.  An adjfactor mismatch is **always** at
    least ``WARNING`` — its effect on every downstream adjustment makes the
    tiered bands' silent suppression inapplicable to it.  A result with zero
    overlapping rows is inconclusive, not clean: it warns and returns
    ``WARNING`` before the bands are consulted.
    """
    if result.matched_rows == 0 and result.total_rows:
        logger.warning(
            "cross-check %s %s: no overlapping tickers between %s and %s "
            "(%d rows unmatched) — comparison inconclusive, not clean",
            result.asset_type,
            result.trade_date,
            result.primary_source,
            result.secondary_source,
            result.total_rows,
        )
        return "WARNING"
    level = response_level(result.mismatch_rate)
    adj = result.mismatched_fields.get("adjfactor")
    if adj is not None and adj.mismatch_count > 0 and level == "silent":
        level = "WARNING"
    detail = "; ".join(
        f"{f} {d.mismatch_rate:.2%} (n={d.mismatch_count}, "
        f"max_diff={d.max_abs_diff:.6f}, e.g. {d.sample_tickers})"
        for f, d in sorted(result.mismatched_fields.items())
    )
    msg = (
        f"cross-check {result.asset_type} {result.trade_date}: "
        f"{result.primary_source} vs {result.secondary_source} — "
        f"worst mismatch {result.mismatch_rate:.2%}"
        + (f"; {detail}" if detail else "; all fields matched")
    )
    if level == "ERROR":
        logger.error(msg)
    elif level == "WARNING":
        logger.warning(msg)
    else:
        logger.info(msg)
    return level


# ---------------------------------------------------------------------------
# Fallback backfill marking (§41.6)
# ---------------------------------------------------------------------------
def backfill_source_tag(backup: str) -> str:
    """Lineage tag for a gap-fill batch: ``"{backup}:backfill"``."""
    return f"{backup}:backfill"


def write_asset(
    df: pd.DataFrame,
    asset_type: str,
    trade_date: str | date,
    source_tag: str,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    target_path: str | os.PathLike[str] | None = None,
) -> Path:
    """Land one batch on the asset layer with lineage in Parquet metadata.

    ``source_tag`` and ``ingest_time`` go into the file's key-value metadata —
    **never a data column** (§41.6): the asset schema stays stable and
    ``pq.read_metadata`` is enough for audits.  ``source_tag`` is the plain
    source name for a normal ingest, :func:`backfill_source_tag` for a backup
    gap-fill.

    Schema validation is DQC's step 7 (``ingest_with_dqc``) — this is the
    tagged landing step the fallback path runs after that gate, written
    atomically (``FileLock`` + tmp → fsync → ``os.replace`` → parent-dir
    fsync, the :mod:`data.raw` pattern; ``write_parquet_atomic`` cannot
    carry schema metadata) and refusing at the CRITICAL disk tier
    (:func:`~data.storage_ops.check_capacity`, like the platform writer).
    """
    path = (
        Path(target_path)
        if target_path is not None
        else _landing_path(asset_type, trade_date, data_root)
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    meta = dict(table.schema.metadata or {})
    meta[b"source_tag"] = str(source_tag).encode("utf-8")
    meta[b"ingest_time"] = datetime.now().isoformat(timespec="seconds").encode("utf-8")
    table = table.replace_schema_metadata(meta)

    if check_capacity(path.parent if path.parent.exists() else path) == "CRITICAL":
        raise DiskFullError(
            f"free space is at the CRITICAL tier; refusing to write {path}"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with FileLock(path):
        try:
            pq.write_table(table, tmp, compression="zstd")
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, path)
        except BaseException:
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            raise

        # fsync parent dir for rename durability
        try:
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            logger.warning("wrote %s but could not fsync %s", path, path.parent)
    return path


def _landing_path(
    asset_type: str, trade_date: str | date, data_root: str | os.PathLike[str]
) -> Path:
    template = PARTITION_PATHS.get(asset_type)
    if template is None or "{date}" not in template:
        raise NotImplementedError(
            f"landing layout for {asset_type!r} is not a date partition; "
            f"pass target_path explicitly (layouts: {PARTITION_PATHS})"
        )
    return Path(data_root) / template.format(date=_as_date_str(trade_date))


# ---------------------------------------------------------------------------
# Cross-check trigger hook (§41.4)
# ---------------------------------------------------------------------------
def raw_exists(
    source: str,
    asset_type: str,
    trade_date: str | date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> bool:
    """Has *source*'s raw batch for (*asset_type*, *trade_date*) landed?

    Ruling (§41 open item: file scan vs watermark): raw landing **file
    presence** — watermarks are subclass-defined and never parsed, so a file
    either landed or it did not.
    """
    if isinstance(trade_date, str):
        trade_date = date.fromisoformat(trade_date)
    return raw_path(source, asset_type, trade_date, data_root).exists()


def should_cross_check(
    source: str,
    asset_type: str,
    trade_date: str | date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> bool:
    """Hook: is this the moment to cross-check *asset_type* on *trade_date*?

    Fires only when the **primary** source's ingest just completed (a backup
    ingest does not write the asset layer, so it never triggers) **and** every
    backup's raw batch has arrived (§41.4).  An asset type with no backup
    never cross-checks, and one the YAML does not arbitrate (``factor``,
    futures, per-table ``reference``) answers ``False`` instead of raising.
    *source* is the source whose ingest just finished.
    """
    try:
        priority = get_source_priority(asset_type)
    except (KeyError, ValueError):
        # Asset type not arbitrated here (factor, futures, per-table reference)
        return False
    if not priority or source != priority[0]:
        return False
    backups = priority[1:]
    if not backups:
        return False
    return all(
        raw_exists(backup, asset_type, trade_date, data_root=data_root)
        for backup in backups
    )
