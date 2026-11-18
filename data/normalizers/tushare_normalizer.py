"""Tushare 标准化器（仅格式转换与单位换算）。

边界处理：
- 停牌：``suspend_d`` 标记停牌（``'S'`` -> True，``' '`` -> False），缺失默认 False。
- 缺失值：原样保留 OHLC 中的 NaN，不做填充。
- 复权因子：保留 ``adj_factor`` 原值（缺失默认 ``1.0``），不因停牌标志覆写。

单位换算（Tushare 原始 -> 统一存储单位）：
- ``vol``（手）-> ``volume``（股）：×100。
- ``amount``（千元）-> ``amount``（元）：×1000。
"""

from __future__ import annotations

import pandas as pd

from data.normalizers import SOURCE_COLUMN_MAP, _coerce_and_finalize_bars

__all__ = ["normalize"]

# 同时存在 suspend_d 与 suspend 时，以 suspend_d 为主并先合并避免重命名冲突
_SUSPEND_COALESCE_PRIMARY = "suspend_d"


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 Tushare 原始 DataFrame（``stock``）转换为统一 ``bars_daily`` 格式并完成单位换算。"""
    if dataset != "stock":
        raise ValueError(f"unsupported dataset: {dataset!r}")

    df = raw_df.copy()
    if _SUSPEND_COALESCE_PRIMARY in df.columns and "suspend" in df.columns:
        # 先保留 NaN 让 fallback 兜底填充，再统一将剩余缺失填为 False
        sd = df["suspend_d"]
        if sd.dtype == object or sd.dtype.name == "string":
            coerced = sd.map({"S": True, " ": False}).astype("boolean")
        else:
            coerced = sd.astype("boolean")
        fallback = df["suspend"]
        if fallback.dtype == object or fallback.dtype.name == "string":
            fallback = fallback.map({"S": True, " ": False}).astype("boolean")
        else:
            fallback = fallback.astype("boolean")
        df["suspend_d"] = coerced.fillna(fallback).fillna(False).astype("boolean")
        df = df.drop(columns=["suspend"])

    df = df.rename(columns=SOURCE_COLUMN_MAP["tushare"])

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # 单位换算：vol（手->股 ×100）、amount（千元->元 ×1000）
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce") * 100
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce") * 1000.0

    # 停牌标记：suspend_d 协议为 'S' 停牌 / ' ' 正常交易
    if "suspend" in df.columns and (
        df["suspend"].dtype == object or df["suspend"].dtype.name == "string"
    ):
        df["suspend"] = df["suspend"].map({"S": True, " ": False})

    return _coerce_and_finalize_bars(df)
