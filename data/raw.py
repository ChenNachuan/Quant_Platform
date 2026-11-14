"""原始层存储 — 保留源站原始列、值与类型的 zstd Parquet 落盘。

路径规范：``raw/{source}/{asset_type}/{date}.parquet``。原始层必须在 DQC 前写入，确保被拦截批次同样保留证据链。
不走 ``storage_ops.write_parquet_atomic``（跳过资产层 schema 校验），仅复用 ``FileLock`` 与 ``PARQUET_WRITE_CONFIG`` 原子写入。
"""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd

from data.storage_ops import PARQUET_WRITE_CONFIG, FileLock, cleanup_expired
from core.config import DATA_ROOT

__all__ = ["save_raw", "load_raw", "raw_path", "cleanup_raw", "RAW_ROOT"]

logger = logging.getLogger(__name__)

RAW_ROOT = "raw"


def raw_path(
    source: str,
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """返回 ``source / asset_type / trade_date`` 对应的原始层文件路径。"""
    date_str = trade_date.strftime("%Y-%m-%d")
    return Path(data_root) / RAW_ROOT / source / asset_type / f"{date_str}.parquet"


def save_raw(
    df: pd.DataFrame,
    source: str,
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """以 zstd 压缩原子写入原始 DataFrame（不做 schema 校验，仅复用文件锁与临时文件替换）。"""
    target = raw_path(source, asset_type, trade_date, data_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")

    with FileLock(target):
        try:
            df.to_parquet(tmp, **PARQUET_WRITE_CONFIG)
            # 文件内容刷盘确保持久性
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

        # 父目录刷盘确保重命名持久化
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
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> pd.DataFrame | None:
    """读取指定原始层文件，不存在时返回 ``None``。"""
    path = raw_path(source, asset_type, trade_date, data_root)
    if not path.exists():
        return None
    return pd.read_parquet(path)


def cleanup_raw(
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    *,
    dry_run: bool = True,
    today: date | None = None,
    config_path: str | os.PathLike[str] | None = None,
) -> list[Path]:
    """清理过期原始层 ``.parquet`` 文件（依据 ``lifecycle.yaml`` 策略，仅清理 ``raw`` 子树并保留 ``.watermarks.json``）。"""
    raw_root = Path(data_root) / RAW_ROOT
    if not raw_root.exists():
        return []
    return cleanup_expired(
        root=data_root,
        subdir=RAW_ROOT,
        dry_run=dry_run,
        today=today,
        config_path=config_path,
    )
