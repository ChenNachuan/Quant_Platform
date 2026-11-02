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
import graph.
"""

from __future__ import annotations

import json
import logging
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
    "ALERT_SEVERITIES",
    "SEVERITY_LOW",
    "SEVERITY_MEDIUM",
    "SEVERITY_HIGH",
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

#: Relative difference above which a correction is MEDIUM (§45.4).
MEDIUM_REL_DIFF = 0.01

SEVERITY_LOW = "LOW"
SEVERITY_MEDIUM = "MEDIUM"
SEVERITY_HIGH = "HIGH"

#: Alerting floor.  Only MEDIUM and above raise a warning — a LOW correction is
#: noise, and alerting on all of them drowns the real ones (§45.4).
ALERT_SEVERITIES = frozenset({SEVERITY_MEDIUM, SEVERITY_HIGH})


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


def _fetch_normalized(asset_type: str, trade_date: date) -> Any:
    """Re-pull one trade date from the primary source, post-Normalizer.

    I/O seam: the source layer lands with the ingest pipeline, and a unit test
    stands in a frame here rather than opening a connection.
    """
    from data.adapters import get_adapter
    from data.ingestion import get_primary_source
    from data.normalizers import get_normalizer

    source = get_primary_source(asset_type)
    adapter = get_adapter(source)
    try:
        result = adapter.fetch(asset_type, trade_date)
        return get_normalizer(source).normalize(asset_type, result.df)
    finally:
        adapter.close()


def _load_local(asset_type: str, trade_date: date) -> Any:
    """Read the asset layer's existing batch for one trade date."""
    import pandas as pd

    from data.storage_schema import PARTITION_PATHS

    template = PARTITION_PATHS.get(asset_type)
    if template is None or "{date}" not in template:
        raise NotImplementedError(
            f"no date-partitioned layout for {asset_type!r} ({PARTITION_PATHS})"
        )
    path = Path("data_platform") / template.format(date=trade_date.strftime("%Y-%m-%d"))
    if not path.exists():
        return None
    return pd.read_parquet(path)


def _rel_diff(old: float, new: float) -> float:
    """``abs_diff / |old|``, floored so a zero baseline is not a division blow-up."""
    return abs(new - old) / max(abs(old), 1e-9)


def compare_frames(
    df_old: Any,
    df_new: Any,
    asset_type: str,
    trade_date: date,
) -> list[FieldChange]:
    """Row-diff two normalised frames against :data:`CORRECTION_TOLERANCES`.

    Only the four tolerance fields are compared; a field either frame does
    not carry is skipped rather than treated as a zero.
    """
    import pandas as pd

    old = pd.DataFrame(df_old)
    new = pd.DataFrame(df_new)
    if old.empty or new.empty:
        return []

    key = ["ticker", "trade_date"]
    if any(c not in old.columns or c not in new.columns for c in key):
        raise ValueError(f"frames must carry {key} to be compared")

    merged = old.merge(new, on=key, suffixes=("_old", "_new"), how="inner")
    changes: list[FieldChange] = []

    for field_name, tol in CORRECTION_TOLERANCES.items():
        old_col, new_col = f"{field_name}_old", f"{field_name}_new"
        if old_col not in merged.columns or new_col not in merged.columns:
            continue
        pair = merged[key + [old_col, new_col]].dropna()
        for row in pair.itertuples(index=False):
            old_value = float(getattr(row, old_col))
            new_value = float(getattr(row, new_col))
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


def detect_corrections(asset_type: str, lookback: int = 30) -> list[FieldChange]:
    """Re-pull the last *lookback* trade dates and diff against the asset layer.

    The weekly scan uses ``lookback=DEFAULT_LOOKBACK`` (30 trade dates); the
    quarterly fallback passes ``lookback=QUARTERLY_LOOKBACK`` (250).  Dates
    missing on either side are skipped — a gap is not a correction.
    """
    from data.calendar_adjustment import TradingCalendar

    cal = TradingCalendar("sse")
    end = cal.prev(date.today())
    start = cal.prev(end, lookback)
    changes: list[FieldChange] = []
    for trade_date in cal.range(start, end):
        fresh = _fetch_normalized(asset_type, trade_date)
        local = _load_local(asset_type, trade_date)
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
                        "old": change.old_value,
                        "new": change.new_value,
                        "abs_diff": change.abs_diff,
                        "rel_diff": change.rel_diff,
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
    """
    from data.ingestion import daily_ingest

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
