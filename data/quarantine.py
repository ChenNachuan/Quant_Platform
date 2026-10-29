"""Quarantine — rejected batches with repair paths (S5).

``quarantine/{asset_type}/{date}.parquet`` + ``.rejection.json`` — rejected
batches are saved (not dropped) for T+30 auto-cleanup.

Repair paths (S5, no DQC in this module):
- **A** (source broken): fix upstream, ``fetch(force=True)`` re-downloads.
- **B** (rules too strict): ``load_quarantine`` → ``recheck`` with a
  caller-supplied validator.  DQC rules/engine are S9-T2/S9-T3.
- **C** (don't fix): T+30 auto-clean via ``cleanup_quarantine``.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from data.storage_ops import PARQUET_WRITE_CONFIG, FileLock

__all__ = [
    "save_quarantine",
    "load_quarantine",
    "recheck",
    "cleanup_quarantine",
    "quarantine_path",
    "rejection_path",
    "QUARANTINE_ROOT",
]

logger = logging.getLogger(__name__)

QUARANTINE_ROOT = "quarantine"


def quarantine_path(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> Path:
    """Return the quarantine parquet path."""
    date_str = trade_date.strftime("%Y-%m-%d")
    return Path(data_root) / QUARANTINE_ROOT / asset_type / f"{date_str}.parquet"


def rejection_path(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> Path:
    """Return the rejection metadata JSON path."""
    date_str = trade_date.strftime("%Y-%m-%d")
    return Path(data_root) / QUARANTINE_ROOT / asset_type / f"{date_str}.rejection.json"


def save_quarantine(
    df: pd.DataFrame,
    asset_type: str,
    trade_date: date,
    rejection_meta: dict[str, Any],
    data_root: str | os.PathLike[str] = "data_platform",
) -> tuple[Path, Path]:
    """Save a rejected batch to quarantine (parquet + rejection metadata).

    Parameters
    ----------
    df : pd.DataFrame
        Normalized DataFrame (the one that DQC rejected).
    rejection_meta : dict
        Must carry: data_date, asset_type, reject_time, batch_status,
        total_rows, rows_would_drop, drop_rate, hard_rules_triggered[].
    """
    target = quarantine_path(asset_type, trade_date, data_root)
    meta_path = rejection_path(asset_type, trade_date, data_root)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Write parquet atomically
    tmp = target.with_name(target.name + ".tmp")
    with FileLock(target):
        try:
            df.to_parquet(tmp, **PARQUET_WRITE_CONFIG)
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, target)
        except BaseException:
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            raise

    # Write rejection metadata atomically
    meta_tmp = meta_path.with_name(meta_path.name + ".tmp")
    meta_tmp.write_text(
        json.dumps(rejection_meta, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    os.replace(meta_tmp, meta_path)

    return target, meta_path


def load_quarantine(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    """Load a quarantined batch. Returns ``(df, rejection_meta)`` or ``None``.

    Repair path B: the caller passes the returned df to :func:`recheck`
    with a new validator.
    """
    target = quarantine_path(asset_type, trade_date, data_root)
    meta_path = rejection_path(asset_type, trade_date, data_root)
    if not target.exists():
        return None
    df = pd.read_parquet(target)
    meta: dict[str, Any] = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("corrupt rejection metadata %s", meta_path)
    return df, meta


def recheck(
    df: pd.DataFrame,
    validator: Any,
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> dict[str, Any]:
    """Re-validate a quarantined batch with a caller-supplied *validator*.

    Repair path B.  The *validator* is a callable ``(df) -> dict`` returning
    a verdict dict (e.g. ``{"passed": bool, "reasons": [...]}``).  DQC
    rules/engine are S9-T2/S9-T3 — this is the entry point only.

    Returns the validator's verdict dict.  Does NOT write to asset/ or
    delete from quarantine — the caller decides based on the verdict.
    """
    verdict = validator(df)
    logger.info("recheck %s/%s: %s", asset_type, trade_date, verdict)
    return verdict


def cleanup_quarantine(
    data_root: str | os.PathLike[str] = "data_platform",
    *,
    dry_run: bool = True,
    today: date | None = None,
) -> list[Path]:
    """Delete expired quarantine files (S4: 30d from lifecycle.yaml).

    Thin wrapper over ``storage_ops.cleanup_expired`` — no days literal in
    Python.  Covers repair path C (T+30 auto-clean).
    """
    from data.storage_ops import cleanup_expired

    q_root = Path(data_root) / QUARANTINE_ROOT
    if not q_root.exists():
        return []
    removed = cleanup_expired(
        root=data_root,
        dry_run=dry_run,
        today=today,
    )
    return [p for p in removed if q_root in p.parents or p.parent == q_root]
