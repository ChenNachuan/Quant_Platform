"""
坚果云数据同步桥 — 任务协议定义

两端 (Mac publisher / Windows worker) 共享的契约:
- TaskRequest: 任务 JSON 格式
- 原子写入工具: 防止坚果云同步半写文件
- 带重试的读取: 防止坚果云同步锁
"""

import json
import os
import time
import logging
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class TaskRequest:
    """任务请求，序列化为 JSON 写入 trans/requests/"""
    task_id: str           # "task_{timestamp}"
    provider: str          # "amazingdata" / "akshare"
    data_type: str         # "stock_1d_kdata" / "stock_adj_factor"
    entity_ids: list[str]  # ["stock_sz_000001", ...]
    start_timestamp: str   # "2026-05-01"
    end_timestamp: str     # "2026-05-15"
    status: str = field(default="pending")  # pending / processing / done / failed

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, data: str) -> "TaskRequest":
        d = json.loads(data)
        return cls(**d)

    def to_dict(self) -> dict:
        return asdict(self)


def atomic_write_json(path: Path, data: dict) -> None:
    """原子写入 JSON: 先写 .tmp 再 os.rename，坚果云不会同步半写文件"""
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.rename(tmp_path, path)
    logger.debug(f"原子写入 JSON: {path}")


def atomic_write_parquet(path: Path, df: pd.DataFrame) -> None:
    """原子写入 Parquet: 先写 .tmp 再 os.rename"""
    tmp_path = path.with_suffix(".tmp")
    df.to_parquet(tmp_path, index=False)
    os.rename(tmp_path, path)
    logger.debug(f"原子写入 Parquet: {path}")


def safe_read_json(path: Path, retries: int = 5, delay: float = 1.0) -> Optional[dict]:
    """
    带重试的 JSON 读取，防止坚果云同步锁或文件尚未完整。
    返回 None 表示读取失败。
    """
    for attempt in range(retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            if attempt < retries - 1:
                logger.debug(f"读取 {path} 失败 (尝试 {attempt + 1}/{retries}): {e}")
                time.sleep(delay)
            else:
                logger.warning(f"读取 {path} 最终失败: {e}")
                return None
