"""AKShare 标准化器（仅格式转换）。

边界处理：
- 停牌：AKShare 不返回停牌日记录，因此现有行均设为 ``suspend=False``。
- 缺失值：原样保留 OHLC 中的 NaN，不做填充。
- 复权因子：适配器固定拉取 ``adjust=""`` 未复权价格且无复权因子列，统一设为 ``adjfactor=1.0``。
"""

from __future__ import annotations

import pandas as pd

from data.normalizers import (
    SOURCE_COLUMN_MAP,
    _coerce_and_finalize_bars,
    _qualify_prefix_ticker,
)

__all__ = ["normalize"]


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 AKShare 原始中文列名 DataFrame 转换为统一 ``bars_daily`` 格式。"""
    if dataset != "stock":
        raise ValueError(f"unsupported dataset: {dataset!r}")

    df = raw_df.rename(columns=SOURCE_COLUMN_MAP["akshare"]).copy()

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).map(_qualify_prefix_ticker)

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    return _coerce_and_finalize_bars(df)
