"""AmazingData 标准化器（仅格式转换）。

边界处理：
- 停牌：保留源数据 ``suspend`` 标志；严禁因 ``suspend==True`` 而覆写源端提供的 ``adjfactor``。
- 缺失值：原样保留 OHLC 中的 NaN，不做填充。
- 复权因子：优先采用 ``NAV_ADJ_FACTOR`` 并原样保留；若无复权字段则默认 ``1.0``。
"""

from __future__ import annotations

import pandas as pd

from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

# AmazingData kline / fund_nav -> 统一列名
_COLUMN_MAP: dict[str, str] = {
    "code": "ticker",
    "kline_time": "trade_date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "amount": "amount",
    "adjfactor": "adjfactor",
    "NAV_ADJ_FACTOR": "adjfactor",
    "suspend": "suspend",
}

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
    """将 AmazingData 原始 DataFrame（``stock`` 或 ``option``）转换为统一 ``bars_daily`` 格式。"""
    if asset_type not in ("stock", "option"):
        raise ValueError(f"unsupported asset_type: {asset_type!r}")

    # 重命名前合并两列复权因子，避免列名冲突；以 NAV_ADJ_FACTOR 为准
    df = raw_df.copy()
    if "adjfactor" in df.columns and "NAV_ADJ_FACTOR" in df.columns:
        df["adjfactor"] = df["NAV_ADJ_FACTOR"].fillna(df["adjfactor"])
        df = df.drop(columns=["NAV_ADJ_FACTOR"])
    elif "NAV_ADJ_FACTOR" in df.columns:
        df = df.rename(columns={"NAV_ADJ_FACTOR": "adjfactor"})

    df = df.rename(columns=_COLUMN_MAP).copy()

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "suspend" not in df.columns:
        df["suspend"] = False
    else:
        df["suspend"] = df["suspend"].fillna(False).astype(bool)

    # 保持源端 adjfactor 原值，不因停牌标志覆写为 1.0
    if "adjfactor" not in df.columns:
        df["adjfactor"] = 1.0

    df["is_limit_up"] = False
    df["is_limit_down"] = False
    df["quality_flag"] = 0

    return df.reindex(columns=UNIFIED_COLUMNS)
