"""Data revision — detect, apply, propagate impact (QUANT-42, S10-T3).

Sources overwrite history silently and never flag a correction, so the
platform has to find revisions itself, write them back safely, and tell the
downstream what is now stale.  Three steps per ``docs/design/data/data_revision.md``:

* **detect** — re-pull the last *lookback* trade dates and diff row by row
  against the asset layer (:func:`detect_corrections`).  Nothing else finds a
  revision: there is no source-side correction marker to trust.
* **apply** — append the correction log, *then* re-run the full ingest
  pipeline (:func:`apply_corrections`).  Overwrite, never version: the platform
  always wants the latest and most accurate values, and the log is the audit.
* **propagate** — classify severity, record the impact, mark affected factors
  stale (:func:`assess_correction_impact`, :func:`mark_factors_stale`).
  **Nothing is recomputed here** — the rerun decision is a human's, made from
  the severity.

Boundaries (issue 边界): no data versioning, no automatic cascade into factor /
backtest recomputation, and no writes to ``data_lake/``.  This module is the
detection / logging / marking code path only.

``data.ingestion.daily_ingest`` resolves at call time (:func:`_daily_ingest`)
so this module stays importable on a tree where the ingest pipeline has not
landed yet, and so a unit test can stand in a double without patching an
import graph.  When it is genuinely absent the call raises a :class:`RuntimeError`
that names it, rather than an opaque :class:`ImportError` from mid-write.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

__all__ = [
    "CORRECTION_TOLERANCES",
    "CORRECTIONS_ROOT",
    "IMPACT_LOG_NAME",
    "DEFAULT_LOOKBACK",
    "QUARTERLY_LOOKBACK",
    "MEDIUM_REL_DIFF",
    "TOTAL_CHANGE_REL_DIFF",
    "ALERT_SEVERITIES",
    "SEVERITY_LOW",
    "SEVERITY_MEDIUM",
    "SEVERITY_HIGH",
    "STORAGE_TO_SOURCE_ASSET",
    "FieldChange",
    "CorrectionRecord",
    "detect_corrections",
    "compare_frames",
    "apply_corrections",
    "assess_correction_impact",
    "mark_factors_stale",
    "correction_log_path",
    "impact_log_path",
]

logger = logging.getLogger(__name__)

#: Per-field comparison tolerances (§45.2).  A source-side float / unit wobble
#: at or under these thresholds is not a correction and must not alert.
CORRECTION_TOLERANCES: dict[str, float] = {
    "close": 0.01,
    "volume": 100,
    "amount": 1000,
    "adjfactor": 1e-6,
}

CORRECTIONS_ROOT = "corrections"
IMPACT_LOG_NAME = "impact_log.jsonl"

#: Weekly scan window, in trade dates (§45.2).
DEFAULT_LOOKBACK = 30
#: Quarterly full-scan window, in trade dates — the long-tail fallback for
#: revisions that arrive after the weekly window has moved on (§45.2).
QUARTERLY_LOOKBACK = 250

#: ``rel_diff`` recorded when one side of a comparison is absent — a value that
#: appears from nothing, or vanishes, is a total change of that field.  Finite
#: on purpose so the correction log stays strict JSON, and always clears
#: ``MEDIUM_REL_DIFF`` (100% is not a minor correction).
TOTAL_CHANGE_REL_DIFF = 1.0

#: Relative difference above which a correction is MEDIUM (§45.4).
MEDIUM_REL_DIFF = 0.01

SEVERITY_LOW = "LOW"
SEVERITY_MEDIUM = "MEDIUM"
SEVERITY_HIGH = "HIGH"

#: Alerting floor.  Only MEDIUM and above raise a warning — a LOW correction is
#: noise, and alerting on all of them drowns the real ones (§45.4).
ALERT_SEVERITIES = frozenset({SEVERITY_MEDIUM, SEVERITY_HIGH})

#: Storage table name (``PARTITION_PATHS`` key) → the source-domain asset type
#: the adapters and normalizers accept (``TushareAdapter._ASSET_TYPES`` and
#: friends — ``"stock"``, ``"option"``).  The two domains are not
#: interchangeable: ``get_primary_source`` and the asset layer are keyed by
#: table name (``bars_daily``), while ``adapter.fetch`` / ``normalize`` reject
#: anything outside the source domain.  Today's fetch + normalize pair serves
#: the unified bars_daily schema only; extend this map when another table gets
#: a source mapping.
STORAGE_TO_SOURCE_ASSET: dict[str, str] = {
    "bars_daily": "stock",
}


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldChange:
    """One field of one ticker that moved past its tolerance.

    ``asset_type`` and ``trade_date`` locate the change so
    :func:`apply_corrections` can group a scan's flat list into one
    :class:`CorrectionRecord` per asset/date and name the log file it writes.
    """

    ticker: str
    field: str
    old_value: float
    new_value: float
    abs_diff: float
    rel_diff: float
    asset_type: str = ""
    trade_date: date | None = None


@dataclass(frozen=True)
class CorrectionRecord:
    """All changes found for one ``asset_type`` / ``trade_date``.

    Serialised to ``corrections/{asset_type}/{date}.jsonl`` — one JSON object
    per change, appended (same-day corrections accumulate, §45.3).
    """

    asset_type: str
    trade_date: date
    detected_at: datetime
    changes: tuple[FieldChange, ...] = ()


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def correction_log_path(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> Path:
    """``{data_root}/corrections/{asset_type}/{date}.jsonl`` (§45.3)."""
    date_str = trade_date.strftime("%Y-%m-%d")
    return Path(data_root) / CORRECTIONS_ROOT / asset_type / f"{date_str}.jsonl"


def impact_log_path(
    data_root: str | os.PathLike[str] = "data_platform",
) -> Path:
    """``{data_root}/corrections/impact_log.jsonl`` (§45.4)."""
    return Path(data_root) / CORRECTIONS_ROOT / IMPACT_LOG_NAME


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def _now() -> datetime:
    """Clock seam — tests never call ``datetime.now()`` directly (conftest)."""
    return datetime.now()


def _source_asset_type(asset_type: str) -> str:
    """Translate a storage table name into the adapter/normalizer domain.

    ``detect_corrections`` speaks storage tables (``bars_daily``) because
    ``get_primary_source``, the asset layer and ``depends_on`` all do, while
    ``adapter.fetch`` / ``normalize`` validate against their source-domain
    ``_ASSET_TYPES`` (``"stock"``, ``"option"``).  No single input satisfies
    both without this translation.
    """
    try:
        return STORAGE_TO_SOURCE_ASSET[asset_type]
    except KeyError:
        raise ValueError(
            f"no source asset type for storage table {asset_type!r} "
            f"(mapped: {sorted(STORAGE_TO_SOURCE_ASSET)}); add its entry to "
            "STORAGE_TO_SOURCE_ASSET before scanning it"
        ) from None


def _fetch_normalized(asset_type: str, trade_date: date) -> Any:
    """Re-pull one trade date from the primary source, post-Normalizer.

    *asset_type* is the storage table name; the adapter and normalizer are
    handed its source-domain translation (:data:`STORAGE_TO_SOURCE_ASSET`)
    because passing the table name through is rejected by both.

    I/O seam: the source layer lands with the ingest pipeline, and a unit test
    stands in a frame here rather than opening a connection.
    """
    from data.adapters import get_adapter
    from data.ingestion import get_primary_source
    from data.normalizers import get_normalizer

    source = get_primary_source(asset_type)  # storage domain
    source_asset_type = _source_asset_type(asset_type)  # adapter domain
    adapter = get_adapter(source)
    try:
        result = adapter.fetch(source_asset_type, trade_date)
        # get_normalizer() returns the normalize function itself (see
        # data/normalizers/__init__.py and the sibling call in dqc/report.py).
        return get_normalizer(source)(source_asset_type, result.df)
    finally:
        adapter.close()


def _load_local(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> Any:
    """Read the asset layer's existing batch for one trade date."""
    import pandas as pd

    from data.storage_schema import PARTITION_PATHS

    template = PARTITION_PATHS.get(asset_type)
    if template is None or "{date}" not in template:
        raise NotImplementedError(
            f"no date-partitioned layout for {asset_type!r} ({PARTITION_PATHS})"
        )
    path = Path(data_root) / template.format(date=trade_date.strftime("%Y-%m-%d"))
    if not path.exists():
        return None
    return pd.read_parquet(path)


def _rel_diff(old: float, new: float) -> float:
    """``abs_diff / |old|``, floored so a zero baseline is not a division blow-up."""
    return abs(new - old) / max(abs(old), 1e-9)


def _is_missing(value: Any) -> bool:
    """True for NaN / None / NaT — a side that has no value to compare."""
    import pandas as pd

    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _jsonable(value: float) -> float | None:
    """NaN / Inf → ``null`` so the JSONL logs stay strict JSON."""
    return value if math.isfinite(value) else None


def compare_frames(
    df_old: Any,
    df_new: Any,
    asset_type: str,
    trade_date: date,
) -> list[FieldChange]:
    """Row-diff two normalised frames against :data:`CORRECTION_TOLERANCES`.

    An **outer** merge on ``(ticker, trade_date)`` with an indicator, so two
    classes of real correction are not silently dropped:

    * **one-sided rows** — a ticker the source backfilled (``right_only``) or
      removed (``left_only``);
    * **NaN ↔ value** — a field that was missing and got filled in, or cleared.

    Both are recorded as a total change of that field
    (:data:`TOTAL_CHANGE_REL_DIFF`).  Two populated values are compared against
    the field's tolerance as usual.  Only the four tolerance fields are
    compared; a field either frame does not carry is skipped rather than
    treated as a zero.  Duplicated ``(ticker, trade_date)`` keys are refused
    before the merge — one row per key on both sides, as in
    ``data.ingestion.cross_source_check``.
    """
    import pandas as pd

    old = pd.DataFrame(df_old)
    new = pd.DataFrame(df_new)
    if old.empty and new.empty:
        return []

    key = ["ticker", "trade_date"]
    if any(c not in old.columns or c not in new.columns for c in key):
        raise ValueError(f"frames must carry {key} to be compared")
    # One row per key on each side, refused like the sibling comparison
    # (data.ingestion.cross_source_check): a duplicated key fans the outer
    # merge out into a cross product and would record several FieldChanges
    # for the same ticker/field.
    if old.duplicated(key).any() or new.duplicated(key).any():
        raise ValueError(f"duplicate join keys {key} in input frames")

    # `indicator=` (not `indicator=True`) names the column explicitly: the
    # default `_merge` is invisible to `itertuples`, which renames `_`-prefixed
    # fields.  Outer is the point — inner would drop exactly the rows below.
    merged = old.merge(
        new, on=key, suffixes=("_old", "_new"), how="outer", indicator="merge_state"
    )
    changes: list[FieldChange] = []

    for field_name, tol in CORRECTION_TOLERANCES.items():
        old_col, new_col = f"{field_name}_old", f"{field_name}_new"
        if old_col not in merged.columns or new_col not in merged.columns:
            continue
        for row in merged.itertuples(index=False):
            old_raw = getattr(row, old_col)
            new_raw = getattr(row, new_col)
            old_missing = _is_missing(old_raw)
            new_missing = _is_missing(new_raw)
            if old_missing and new_missing:
                continue

            if old_missing or new_missing:
                # One side has no value: a backfill, a removal, or a fill.
                present = new_raw if old_missing else old_raw
                changes.append(
                    FieldChange(
                        ticker=str(row.ticker),
                        field=field_name,
                        old_value=float("nan") if old_missing else float(old_raw),
                        new_value=float("nan") if new_missing else float(new_raw),
                        abs_diff=abs(float(present)),
                        rel_diff=TOTAL_CHANGE_REL_DIFF,
                        asset_type=asset_type,
                        trade_date=trade_date,
                    )
                )
                continue

            old_value = float(old_raw)
            new_value = float(new_raw)
            abs_diff = abs(new_value - old_value)
            if abs_diff <= tol:
                continue
            changes.append(
                FieldChange(
                    ticker=str(row.ticker),
                    field=field_name,
                    old_value=old_value,
                    new_value=new_value,
                    abs_diff=abs_diff,
                    rel_diff=_rel_diff(old_value, new_value),
                    asset_type=asset_type,
                    trade_date=trade_date,
                )
            )
    return changes


def detect_corrections(
    asset_type: str,
    lookback: int = 30,
    *,
    data_root: str | os.PathLike[str] = "data_platform",
) -> list[FieldChange]:
    """Re-pull the last *lookback* trade dates and diff against the asset layer.

    The weekly scan uses ``lookback=DEFAULT_LOOKBACK`` (30 trade dates); the
    quarterly fallback passes ``lookback=QUARTERLY_LOOKBACK`` (250).  Dates
    missing on either side are skipped — a gap is not a correction.

    ``data_root`` names the tree to read (asset layer and calendar), defaulting
    as :func:`apply_corrections` / :func:`assess_correction_impact` /
    :func:`mark_factors_stale` do, so a scan and a write can be pointed at the
    same non-default tree instead of detection silently reading another.
    """
    from data.calendar_adjustment import TradingCalendar

    cal = TradingCalendar("sse", data_root=data_root)
    end = cal.prev(date.today())
    start = cal.prev(end, lookback)
    changes: list[FieldChange] = []
    for trade_date in cal.range(start, end):
        fresh = _fetch_normalized(asset_type, trade_date)
        local = _load_local(asset_type, trade_date, data_root)
        if fresh is None or local is None:
            continue
        changes.extend(compare_frames(local, fresh, asset_type, trade_date))
    return changes


# ---------------------------------------------------------------------------
# Apply — log first, then overwrite through the ingest pipeline
# ---------------------------------------------------------------------------


def _group_changes(
    changes: Sequence[FieldChange], detected_at: datetime
) -> list[CorrectionRecord]:
    """One record per ``(asset_type, trade_date)``, order preserved."""
    buckets: dict[tuple[str, date], list[FieldChange]] = {}
    for change in changes:
        if not change.asset_type or change.trade_date is None:
            raise ValueError(
                f"FieldChange needs asset_type and trade_date to be applied: {change!r}"
            )
        buckets.setdefault((change.asset_type, change.trade_date), []).append(change)
    return [
        CorrectionRecord(
            asset_type=key[0],
            trade_date=key[1],
            detected_at=detected_at,
            changes=tuple(items),
        )
        for key, items in buckets.items()
    ]


def _append_correction_log(
    record: CorrectionRecord, data_root: str | os.PathLike[str]
) -> Path:
    """Append one JSON line per change.  Creates the directory when missing."""
    path = correction_log_path(record.asset_type, record.trade_date, data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for change in record.changes:
            fh.write(
                json.dumps(
                    {
                        "detected_at": record.detected_at.isoformat(),
                        "asset_type": record.asset_type,
                        "trade_date": str(record.trade_date),
                        "ticker": change.ticker,
                        "field": change.field,
                        "old": _jsonable(change.old_value),
                        "new": _jsonable(change.new_value),
                        "abs_diff": _jsonable(change.abs_diff),
                        "rel_diff": _jsonable(change.rel_diff),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    return path


def _daily_ingest(source: str, asset_type: str, trade_date: date) -> None:
    """Call the full ingest pipeline (§40.9).

    Resolved at call time on purpose: ``daily_ingest`` lands with the ingest
    pipeline work, and importing it at module load would make this module
    unimportable on a tree where it is absent.  The order guarantee in
    :func:`apply_corrections` is that this runs *after* the correction log is
    on disk.

    When the pipeline is not on the tree this raises :class:`RuntimeError`
    naming the missing function — never a bare :class:`ImportError` from
    inside a write sequence, and never a silent skip.  A correction must
    overwrite through ``daily_ingest`` (DQC, raw and watermark all have to see
    the corrected batch); a direct Parquet write is not a fallback.
    """
    try:
        from data.ingestion import daily_ingest
    except ImportError as exc:
        raise RuntimeError(
            "data.ingestion.daily_ingest is not available on this tree; "
            "apply_corrections must overwrite through the full ingest pipeline "
            "and will not write the asset layer directly. Land the ingest "
            "pipeline (QUANT-40 daily_ingest) before applying corrections."
        ) from exc

    daily_ingest(source=source, asset_type=asset_type, trade_date=trade_date)


def apply_corrections(
    changes: Sequence[FieldChange],
    *,
    data_root: str | os.PathLike[str] = "data_platform",
) -> list[CorrectionRecord]:
    """Group *changes*, log each record, then re-ingest to overwrite.

    Order is the audit guarantee and is asserted by the tests: the correction
    log is appended **before** ``daily_ingest`` overwrites the asset layer.  A
    crash between the two leaves a log describing a write that did not happen —
    the safe direction.  The reverse loses the record of a write that did.

    The overwrite goes through the complete ``daily_ingest`` pipeline rather
    than a Parquet rewrite, so DQC, the raw layer and the watermark all see the
    corrected batch (§45.3).
    """
    from data.ingestion import get_primary_source

    records = _group_changes(list(changes), _now())
    applied: list[CorrectionRecord] = []
    for record in records:
        _append_correction_log(record, data_root)  # 1. log first
        _daily_ingest(
            source=get_primary_source(record.asset_type),
            asset_type=record.asset_type,
            trade_date=record.trade_date,
        )  # 2. then overwrite
        logger.warning(
            "Correction applied: %s %s, %d field(s) changed",
            record.asset_type,
            record.trade_date,
            len(record.changes),
        )
        applied.append(record)
    return applied


# ---------------------------------------------------------------------------
# Impact propagation — classify, record, mark stale.  Never recompute.
# ---------------------------------------------------------------------------


def _classify(record: CorrectionRecord) -> tuple[str, str, dict[str, Any]]:
    """Severity, reason, and the impact payload written to the log."""
    changes = list(record.changes)
    max_rel_diff = max((c.rel_diff for c in changes), default=0.0)
    impact: dict[str, Any] = {
        "trade_date": str(record.trade_date),
        "asset_type": record.asset_type,
        "num_changes": len(changes),
        "max_rel_diff": max_rel_diff,
        "affected_tickers": sorted({c.ticker for c in changes}),
    }

    if any(c.field == "adjfactor" for c in changes):
        return (
            SEVERITY_HIGH,
            "adjfactor changed, all price-based factors affected",
            impact,
        )
    if max_rel_diff > MEDIUM_REL_DIFF:
        return (
            SEVERITY_MEDIUM,
            f"price change > 1% for {len(changes)} field(s)",
            impact,
        )
    return SEVERITY_LOW, "minor correction, likely negligible", impact


def assess_correction_impact(
    record: CorrectionRecord,
    *,
    data_root: str | os.PathLike[str] = "data_platform",
) -> str:
    """Classify *record*, append ``impact_log.jsonl``, and warn on MEDIUM+.

    Returns the severity string: ``HIGH`` = any ``adjfactor`` change (every
    price-based factor is affected), ``MEDIUM`` = ``max_rel_diff > 0.01``,
    ``LOW`` = everything else (§45.4).  Only MEDIUM and above alert.
    """
    severity, reason, impact = _classify(record)
    impact["severity"] = severity
    impact["reason"] = reason

    path = impact_log_path(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(impact, ensure_ascii=False) + "\n")

    if severity in ALERT_SEVERITIES:
        logger.warning("Correction impact %s: %s", severity, reason)
    else:
        logger.info("Correction impact %s: %s", severity, reason)
    return severity


def mark_factors_stale(
    record: CorrectionRecord,
    *,
    data_root: str | os.PathLike[str] = "data_platform",
) -> list[str]:
    """Mark factors that depend on the touched table as stale (§45.4).

    Writes ``stale_since`` into each affected factor's ``metadata.json``
    (§62.3 — the registry is the factor's own metadata, not a side file) and
    returns the affected factor names.  **Does not recompute anything**:
    ``recompute --stale`` is a human decision taken from the severity.
    """
    factors_dir = Path(data_root) / "factors"
    if not factors_dir.is_dir():
        return []

    touched = {record.asset_type}
    affected: list[str] = []
    for meta_path in sorted(factors_dir.glob("*/v*/metadata.json")):
        try:
            meta: dict[str, Any] = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.warning("unreadable factor metadata: %s", meta_path)
            continue

        depends_on = set(meta.get("depends_on") or [])
        if not (depends_on & touched):
            continue

        name = str(meta.get("factor_name") or meta_path.parent.parent.name)
        meta["stale_since"] = str(record.trade_date)
        meta_path.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        affected.append(name)
        logger.info("Factor %r marked stale since %s", name, record.trade_date)
    return affected
