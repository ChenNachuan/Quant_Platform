"""各数据源标准化器（仅做字段映射、单位换算与类型规范，不做 DQC 清洗）。

各子模块暴露 ``normalize(asset_type, raw_df) -> df``，输出对齐统一的 ``bars_daily`` 表结构。
"""

from __future__ import annotations

import importlib
from typing import Callable

import pandas as pd

__all__ = ["get_normalizer", "UNIFIED_COLUMNS"]

# 统一输出列（含由 DQC 阶段填充的 quality_flag 占位列）
UNIFIED_COLUMNS: list[str] = [
    "ticker",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjfactor",
    "turnover",
    "suspend",
    "is_limit_up",
    "is_limit_down",
    "quality_flag",
]

_NORMALIZER_MODULES: dict[str, str] = {
    "akshare": "data.normalizers.akshare_normalizer",
    "tushare": "data.normalizers.tushare_normalizer",
    "amazingdata": "data.normalizers.amazingdata_normalizer",
    "qdc": "data.normalizers.qdc_normalizer",
    "straightflush": "data.normalizers.straightflush_normalizer",
}


def get_normalizer(source: str) -> Callable[[str, pd.DataFrame], pd.DataFrame]:
    """返回指定 *source* 对应的 ``normalize`` 函数；未知数据源抛出 ``ValueError``。"""
    module_path = _NORMALIZER_MODULES.get(source)
    if module_path is None:
        raise ValueError(f"no normalizer for source: {source!r}")
    module = importlib.import_module(module_path)
    return module.normalize
