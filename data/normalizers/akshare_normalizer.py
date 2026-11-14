"""AKShare 标准化器（仅格式转换）。

边界处理：
- 停牌：AKShare 不返回停牌日记录，因此现有行均设为 ``suspend=False``。
- 缺失值：原样保留 OHLC 中的 NaN，不做填充。
- 复权因子：适配器固定拉取 ``adjust=""`` 未复权价格且无复权因子列，统一设为 ``adjfactor=1.0``。
"""

from __future__ import annotations

import pandas as pd

from core.asset_rules import PREFIX_EXCHANGE
from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

# AKShare stock_zh_a_hist -> 统一列名
_COLUMN_MAP: dict[str, str] = {
    "日期": "trade_date",
    "股票代码": "ticker",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
    "换手率": "turnover",
}

_NUMERIC_COLS = ("open", "high", "low", "close", "volume", "amount", "turnover")


def _qualify_ticker(code: str) -> str:
    """为 6 位裸股票代码补全交易所后缀（``{6位}.{SH|SZ|BJ}``）。"""
    if "." in code:
        return code
    prefix = code[:2]
    exchange = PREFIX_EXCHANGE.get(prefix)
    if exchange is None:
        raise ValueError(f"cannot qualify ticker {code!r}: unknown prefix {prefix!r}")
    return f"{code}.{exchange}"


def normalize(asset_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 AKShare 原始中文列名 DataFrame 转换为统一 ``bars_daily`` 格式。"""
    if asset_type != "stock":
        raise ValueError(f"unsupported asset_type: {asset_type!r}")

    df = raw_df.rename(columns=_COLUMN_MAP).copy()

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).map(_qualify_ticker)

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 使用可空 Int64 保留 NaN，避免直接转 int64 报错
    if "volume" in df.columns:
        df["volume"] = (df["volume"]).round().astype("Int64")

    # AKShare 省略停牌行，且原始契约为不复权、无涨跌停标识
    df["suspend"] = False
    df["adjfactor"] = 1.0
    df["is_limit_up"] = False
    df["is_limit_down"] = False
    df["quality_flag"] = 0

    return df.reindex(columns=UNIFIED_COLUMNS)
