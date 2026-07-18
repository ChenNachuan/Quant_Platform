# -*- coding: utf-8 -*-
"""
期货 L2 Tick 因子共享计算函数

从 indicator_py/functions.py 迁移，增加了 DataLake 列名兼容（bid_px_1 → bid_px1）。

输入约定:
    bid_px_1..bid_px_5, ask_px_1..ask_px_5
    bid_vol_1..bid_vol_5, ask_vol_1..ask_vol_5
    last_price, volume, turnover
"""

from __future__ import annotations

import numpy as np
import pandas as pd

LEVELS = tuple(range(1, 6))


def _empty(df: pd.DataFrame, value: float = np.nan) -> pd.Series:
    return pd.Series(value, index=df.index, dtype="float64")


def first_existing(
    df: pd.DataFrame, names: list[str], default: float = np.nan
) -> pd.Series:
    for name in names:
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce")
    return _empty(df, default)


def quote_col(df: pd.DataFrame, side: str, kind: str, level: int) -> pd.Series:
    """获取某档报价，支持多种列名格式。"""
    aliases = {
        "px": ["px", "price", "p"],
        "yld": ["yld", "yield"],
        "vol": ["vol", "size", "sz", "volume"],
    }[kind]
    names: list[str] = []
    for alias in aliases:
        # 原有格式: bid_px1, bid1_px, bid_1_px
        # DataLake 格式: bid_px_1 (下划线分隔数字)
        names.extend(
            [
                f"{side}_{alias}{level}",  # bid_px1
                f"{side}{level}_{alias}",  # bid1_px
                f"{side}_{alias}_{level}",  # bid_px_1 (DataLake)
                f"{side}{level}{alias}",  # bid1px
            ]
        )
    return first_existing(df, names)


def trade_col(df: pd.DataFrame, kind: str, default: float = np.nan) -> pd.Series:
    aliases = {
        "px": ["trade_px", "last_price", "last_px", "price", "close", "$close"],
        "yld": ["trade_yld", "last_yld", "yld"],
        "vol": ["trade_vol", "volume", "vol", "trade_size", "$volume"],
        "count": ["trade_count", "trade_n", "n_trade", "num_trades"],
    }[kind]
    return first_existing(df, aliases, default=default)


def side_frame(df: pd.DataFrame, side: str, kind: str) -> pd.DataFrame:
    data = {level: quote_col(df, side, kind, level) for level in LEVELS}
    return pd.DataFrame(data, index=df.index)


def valid_quote(df: pd.DataFrame, side: str, kind: str) -> pd.DataFrame:
    values = side_frame(df, side, kind)
    vols = side_frame(df, side, "vol")
    return values.where((vols > 0) & values.notna())


def sum_side_size(df: pd.DataFrame, side: str) -> pd.Series:
    vols = side_frame(df, side, "vol").where(lambda x: x > 0, 0.0)
    return vols.sum(axis=1)


def weighted_sum_quote_size(
    df: pd.DataFrame, decay_w: float = 0.5
) -> tuple[pd.Series, pd.Series]:
    weights = pd.Series([decay_w**i for i in range(len(LEVELS))], index=LEVELS)
    bid = (
        side_frame(df, "bid", "vol")
        .where(lambda x: x > 0, 0.0)
        .mul(weights, axis=1)
        .sum(axis=1)
    )
    ask = (
        side_frame(df, "ask", "vol")
        .where(lambda x: x > 0, 0.0)
        .mul(weights, axis=1)
        .sum(axis=1)
    )
    return bid, ask


def mid_px(df: pd.DataFrame) -> pd.Series:
    """撮合中间价（从买卖一档价计算）。"""
    bid = valid_quote(df, "bid", "px").bfill(axis=1).iloc[:, 0]
    ask = valid_quote(df, "ask", "px").bfill(axis=1).iloc[:, 0]
    both = (bid + ask) / 2.0
    return both.where(bid.notna() & ask.notna(), bid.combine_first(ask))


def mid_yld(df: pd.DataFrame) -> pd.Series:
    """中间收益率。"""
    bid = valid_quote(df, "bid", "yld").bfill(axis=1).iloc[:, 0]
    ask = valid_quote(df, "ask", "yld").bfill(axis=1).iloc[:, 0]
    both = (bid + ask) / 2.0
    return both.where(bid.notna() & ask.notna(), bid.combine_first(ask))


def wmid_yld(df: pd.DataFrame) -> pd.Series:
    """成交量加权的中间收益率。"""
    bid_y = quote_col(df, "bid", "yld", 1)
    ask_y = quote_col(df, "ask", "yld", 1)
    bid_v = quote_col(df, "bid", "vol", 1)
    ask_v = quote_col(df, "ask", "vol", 1)
    denom = bid_v + ask_v
    wmid = (ask_y * bid_v + bid_y * ask_v) / denom.replace(0, np.nan)
    return wmid.combine_first(mid_yld(df))


def ema(values: pd.Series, period: int) -> pd.Series:
    return (
        pd.to_numeric(values, errors="coerce")
        .ewm(span=period, adjust=False, min_periods=1)
        .mean()
    )


def shift_logr(values: pd.Series) -> pd.Series:
    x = pd.to_numeric(values, errors="coerce")
    prev = x.shift(1)
    out = np.log(x / prev)
    return out.where((x > 1e-6) & (prev > 1e-6), 0.0).replace([np.inf, -np.inf], 0.0)


def rolling_vwap(price: pd.Series, volume: pd.Series, period: int) -> pd.Series:
    px = pd.to_numeric(price, errors="coerce")
    vol = pd.to_numeric(volume, errors="coerce").fillna(0.0).clip(lower=0.0)
    denom = vol.rolling(period, min_periods=1).sum().replace(0, np.nan)
    return (px * vol).rolling(period, min_periods=1).sum() / denom


def bounded_log_ratio(
    num: pd.Series, den: pd.Series, lower: float = -5.0, upper: float = 5.0
) -> pd.Series:
    out = np.log(num / den)
    out = out.where((num > 1e-6) & (den > 1e-6), 0.0)
    return out.replace([np.inf, -np.inf], 0.0).clip(lower, upper)


def safe_div(num: pd.Series, den: pd.Series, fill: float = 0.0) -> pd.Series:
    out = num / den.replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan).fillna(fill)


def has_l2_columns(df: pd.DataFrame) -> bool:
    """检查 DataFrame 是否包含 L2 五档数据所需的列。"""
    for side in ("bid", "ask"):
        for lvl in range(1, 3):  # 至少需要前2档
            if f"{side}_px_{lvl}" not in df.columns and f"{side}_px_{lvl}" not in [
                c.replace("_", "") for c in df.columns
            ]:
                return False
    return True


def add_yields_from_price(df: pd.DataFrame) -> pd.DataFrame:
    """如果数据集有价格但没有收益率，从价格计算收益率。

    收益率定义为价格本身的近似（用于价差计算），参考 indicator_py 的惯例。
    """
    # 检查是否已有 yield 列
    has_bid_yld = any("bid_yld" in c or "bid_yield" in c for c in df.columns)
    has_ask_yld = any("ask_yld" in c or "ask_yield" in c for c in df.columns)
    if has_bid_yld and has_ask_yld:
        return df

    # 从价格列生成 yield 列（对于期货，yield 直接等于价格）
    df = df.copy()
    for side in ("bid", "ask"):
        for lvl in LEVELS:
            px_col = quote_col(df, side, "px", lvl)
            yld_name = f"{side}_yld_{lvl}"
            if yld_name not in df.columns:
                df[yld_name] = px_col
    return df
