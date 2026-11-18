"""AmazingData 标准化器（仅格式转换与单位换算）。

边界处理：
- 停牌：保留源数据 ``suspend`` 标志；严禁因 ``suspend==True`` 而覆写源端提供的 ``adjfactor``。
- 缺失值：原样保留 OHLC 中的 NaN，不做填充。
- 复权因子：优先采用 ``NAV_ADJ_FACTOR`` 并原样保留；若无复权字段则默认 ``1.0``。
- 指数成交量：AmazingData ``EXTRA_INDEX_A`` 日线对上交所（``.SH``）指数按手回报，
  需 ``* 100`` 转为股；深交所（``.SZ``）/北交所（``.BJ``）按股回报保持原值，最终统一转为可空 ``Int64``。
"""

from __future__ import annotations

import pandas as pd

from data.normalizers import (
    SOURCE_COLUMN_MAP,
    _coerce_and_finalize_bars,
    _normalize_calendar_frame,
)

__all__ = ["normalize"]

_SUPPORTED_DATASETS = frozenset({"stock", "etf", "index", "option", "calendar"})


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 AmazingData 原始 DataFrame（``stock``、``etf``、``index``、``option`` 或 ``calendar``）转换为统一格式。"""
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError(f"unsupported dataset: {dataset!r}")
    if dataset == "calendar":
        return _normalize_calendar_frame(raw_df, ("trade_date", "cal_date"))

    # 重命名前合并两列复权因子，避免列名冲突；以 NAV_ADJ_FACTOR 为准
    df = raw_df.copy()
    if "adjfactor" in df.columns and "NAV_ADJ_FACTOR" in df.columns:
        nav_adj = pd.to_numeric(df["NAV_ADJ_FACTOR"], errors="coerce")
        base_adj = pd.to_numeric(df["adjfactor"], errors="coerce")
        df["adjfactor"] = nav_adj.fillna(base_adj)
        df = df.drop(columns=["NAV_ADJ_FACTOR"])
    elif "NAV_ADJ_FACTOR" in df.columns:
        df = df.rename(columns={"NAV_ADJ_FACTOR": "adjfactor"})

    df = df.rename(columns=SOURCE_COLUMN_MAP["amazingdata"]).copy()

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        if dataset == "index" and "ticker" in df.columns:
            sh_mask = df["ticker"].astype(str).str.endswith(".SH")
            if sh_mask.any():
                df.loc[sh_mask, "volume"] = df.loc[sh_mask, "volume"] * 100

    return _coerce_and_finalize_bars(df)
