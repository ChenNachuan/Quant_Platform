"""Per-source normalizers — format conversion only (C3).

Each module exposes ``normalize(asset_type, raw_df) -> df`` that maps
source-native columns to the unified bars_daily schema.  Dispatch is by
source name: ``data/normalizers/{source}_normalizer.py``.

No DQC, no cleaning — only column-name mapping and type fixes.
"""

from __future__ import annotations

import importlib
from typing import Callable

import pandas as pd

__all__ = ["get_normalizer", "UNIFIED_COLUMNS"]

# Unified output columns (BARS_DAILY_SCHEMA minus quality_flag, which DQC adds)
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
}


def get_normalizer(source: str) -> Callable[[str, pd.DataFrame], pd.DataFrame]:
    """Return the ``normalize`` function for *source*.

    Raises ``ValueError`` for unknown source names.
    """
    module_path = _NORMALIZER_MODULES.get(source)
    if module_path is None:
        raise ValueError(f"no normalizer for source: {source!r}")
    module = importlib.import_module(module_path)
    return module.normalize
