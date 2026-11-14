"""隔离区管理 — 持久化被 DQC 拦截的批次并提供修复路径。

存储格式：``quarantine/{asset_type}/{date}.parquet`` 与 ``.rejection.json``，供 T+30 自动清理或人工修复：
- 路径 A（上游源修复）：修复源站后强制重拉；
- 路径 B（规则过严）：``load_quarantine`` 读出后由调用方传入校验器执行 ``recheck``；
- 路径 C（放弃修复）：通过 ``cleanup_quarantine`` 按生命周期策略过期清理。
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from data.storage_ops import PARQUET_WRITE_CONFIG, FileLock, cleanup_expired
from core.config import DATA_ROOT

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
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """返回隔离区 Parquet 文件路径。"""
    date_str = trade_date.strftime("%Y-%m-%d")
    return Path(data_root) / QUARANTINE_ROOT / asset_type / f"{date_str}.parquet"


def rejection_path(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """返回拦截元数据 JSON 文件路径。"""
    date_str = trade_date.strftime("%Y-%m-%d")
    return Path(data_root) / QUARANTINE_ROOT / asset_type / f"{date_str}.rejection.json"


def save_quarantine(
    df: pd.DataFrame,
    asset_type: str,
    trade_date: date,
    rejection_meta: dict[str, Any],
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> tuple[Path, Path]:
    """原子写入被拦截批次的数据帧与拦截元数据。

    Parameters
    ----------
    df : pd.DataFrame
        经标准化后被 DQC 拦截的数据帧。
    rejection_meta : dict
        须包含 data_date、asset_type、reject_time、batch_status、total_rows、rows_would_drop、drop_rate、hard_rules_triggered。
    """
    target = quarantine_path(asset_type, trade_date, data_root)
    meta_path = rejection_path(asset_type, trade_date, data_root)
    target.parent.mkdir(parents=True, exist_ok=True)

    tmp = target.with_name(target.name + ".tmp")
    meta_tmp = meta_path.with_name(meta_path.name + ".tmp")
    with FileLock(target):
        try:
            # 数据文件原子写：tmp -> fsync -> replace
            df.to_parquet(tmp, **PARQUET_WRITE_CONFIG)
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, target)

            # 元数据文件原子写：同锁内 tmp -> fsync -> replace
            meta_tmp.write_text(
                json.dumps(rejection_meta, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            fd = os.open(meta_tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(meta_tmp, meta_path)

            # 目录 fsync 确保持久化重命名
            fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except BaseException:
            for f in (tmp, meta_tmp):
                try:
                    os.remove(f)
                except FileNotFoundError:
                    pass
            raise

    return target, meta_path


def load_quarantine(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    """读取隔离区批次，返回 ``(df, rejection_meta)``，不存在时返回 ``None``。"""
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
) -> dict[str, Any]:
    """使用调用方提供的 *validator* 重检隔离区批次，仅返回判定结果字典，不主动写资产层或删隔离文件。"""
    verdict = validator(df)
    logger.info("recheck %s/%s: %s", asset_type, trade_date, verdict)
    return verdict


def cleanup_quarantine(
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    *,
    dry_run: bool = True,
    today: date | None = None,
    config_path: str | os.PathLike[str] | None = None,
) -> list[Path]:
    """清理过期隔离区文件（依据 ``lifecycle.yaml`` 策略，通过 ``subdir=quarantine`` 限定清理子树）。"""
    q_root = Path(data_root) / QUARANTINE_ROOT
    if not q_root.exists():
        return []
    return cleanup_expired(
        root=data_root,
        subdir=QUARANTINE_ROOT,
        dry_run=dry_run,
        today=today,
        config_path=config_path,
    )
