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

from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

# Tushare pro.daily() / adj_factor() / suspend_d() -> 统一列名
_COLUMN_MAP: dict[str, str] = {
    "ts_code": "ticker",
    "trade_date": "trade_date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "vol": "volume",
    "amount": "amount",
    "adj_factor": "adjfactor",
    "suspend_d": "suspend",
}

# 同时存在 suspend_d 与 suspend 时，以 suspend_d 为主并先合并避免重命名冲突
_SUSPEND_COALESCE_PRIMARY = "suspend_d"

_NUMERIC_COLS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjfactor",
    "turnover",
)


def normalize(asset_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 Tushare 原始 DataFrame（``stock``）转换为统一 ``bars_daily`` 格式并完成单位换算。"""
    if asset_type != "stock":
        raise ValueError(f"unsupported asset_type: {asset_type!r}")

    df = raw_df.copy()
    if _SUSPEND_COALESCE_PRIMARY in df.columns and "suspend" in df.columns:
        # 先保留 NaN 让 fallback 兜底填充，再统一将剩余缺失填为 False
        sd = df["suspend_d"]
        if sd.dtype == object or sd.dtype.name == "string":
            coerced = sd.map({"S": True, " ": False})
        else:
            coerced = sd.astype("boolean")
        fallback = df["suspend"]
        if fallback.dtype == object or fallback.dtype.name == "string":
            fallback = fallback.map({"S": True, " ": False})
        df["suspend_d"] = coerced.fillna(fallback).fillna(False).astype("boolean")
        df = df.drop(columns=["suspend"])
    df = df.rename(columns=_COLUMN_MAP)

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 单位换算：vol（手->股 ×100，可空 Int64 保留 NaN）、amount（千元->元 ×1000）
    if "volume" in df.columns:
        df["volume"] = (df["volume"] * 100).round().astype("Int64")
    if "amount" in df.columns:
        df["amount"] = df["amount"] * 1000.0

    # 停牌标记：suspend_d 协议为 'S' 停牌 / ' ' 正常交易
    if "suspend" not in df.columns:
        df["suspend"] = False
    elif df["suspend"].dtype == object or df["suspend"].dtype.name == "string":
        df["suspend"] = (
            df["suspend"].map({"S": True, " ": False}).fillna(False).astype(bool)
        )
    else:
        df["suspend"] = df["suspend"].fillna(False).astype(bool)

    # 保持源端 adjfactor 原值，不因停牌标志覆写为 1.0
    if "adjfactor" not in df.columns:
        df["adjfactor"] = 1.0

    df["is_limit_up"] = False
    df["is_limit_down"] = False
    df["quality_flag"] = 0

    return df.reindex(columns=UNIFIED_COLUMNS)
