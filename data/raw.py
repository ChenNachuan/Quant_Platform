"""Raw layer — original columns/values/types, zstd parquet landing (S3).

``raw/{source}/{asset_type}/{date}.parquet`` — one file per day per source.
Raw is written **before** DQC so the evidence chain is complete even for
rejected batches.

Cannot reuse ``storage_ops.write_parquet_atomic`` (it validates
``SCHEMA[table]`` and raises ``KeyError`` on an unknown table).  Reuses
``FileLock`` + ``PARQUET_WRITE_CONFIG`` (already zstd) + tmp→fsync→os.replace;
skips schema validation.  Does NOT add a raw table to ``storage_schema.py``.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd

from data.storage_ops import PARQUET_WRITE_CONFIG, FileLock

__all__ = ["save_raw", "load_raw", "raw_path", "cleanup_raw", "RAW_ROOT"]

logger = logging.getLogger(__name__)

RAW_ROOT = "raw"


def raw_path(
    source: str,
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> Path:
    """Return the raw landing path for *source* / *asset_type* / *trade_date*."""
    date_str = trade_date.strftime("%Y-%m-%d")
    return Path(data_root) / RAW_ROOT / source / asset_type / f"{date_str}.parquet"


def save_raw(
    df: pd.DataFrame,
    source: str,
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> Path:
    """Write *df* to the raw landing path atomically (zstd, no schema validation).

    Reuses ``FileLock`` + ``PARQUET_WRITE_CONFIG`` + tmp→fsync→os.replace.
    Raw is source-original columns/values/types — no schema check (S3).
    """
    target = raw_path(source, asset_type, trade_date, data_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")

    with FileLock(target):
        try:
            df.to_parquet(tmp, **PARQUET_WRITE_CONFIG)
            # fsync for durability (QUANT-15 #5)
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

        # fsync parent dir for rename durability
        try:
            dir_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            logger.warning("wrote %s but could not fsync %s", target, target.parent)

    return target


def load_raw(
    source: str,
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = "data_platform",
) -> pd.DataFrame | None:
    """Read the raw file for *source* / *asset_type* / *trade_date*.

    Returns ``None`` if the file does not exist.
    """
    path = raw_path(source, asset_type, trade_date, data_root)
    if not path.exists():
        return None
    return pd.read_parquet(path)


def cleanup_raw(
    data_root: str | os.PathLike[str] = "data_platform",
    *,
    dry_run: bool = True,
    today: date | None = None,
) -> list[Path]:
    """Delete expired raw ``.parquet`` files (S4: 365d from lifecycle.yaml).

    Thin wrapper over ``storage_ops.cleanup_expired`` — no days literal in
    Python.  ``.watermarks.json`` survives because ``parse_date_from_filename``
    returns ``None`` for it (ac-2 asserts this explicitly).
    """
    from data.storage_ops import cleanup_expired

    raw_root = Path(data_root) / RAW_ROOT
    if not raw_root.exists():
        return []
    removed = cleanup_expired(
        root=data_root,
        dry_run=dry_run,
        today=today,
    )
    # Filter to raw/ only — cleanup_expired operates on the whole tree.
    return [p for p in removed if raw_root in p.parents or p.parent == raw_root]
