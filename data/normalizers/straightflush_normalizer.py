"""同花顺（扶摇 / StraightFlush）标准化器（仅格式转换）。

映射规则：
* ``thscode`` -> ``ticker``（裸代码按前缀补全交易所后缀）。
* 指数过滤：``dataset == "index"`` 时直接抛弃同花顺自编指数（``*.TI``），仅保留交易所/指数公司权威指数。
* 指数后缀对齐：对 ``000xxx.CSI``（上交所同步发布的中证指数，如 ``000510.CSI``、``000860.CSI``）
  在不与既有 ``.SH`` 代码冲突的前提下统一映射为 ``.SH``，对齐 QDC 与 AmazingData。
* ``date_ms``（``Asia/Shanghai`` 毫秒时间戳）-> ``trade_date``（``datetime.date``）。
* ``volume`` 原生单位即为股，转为可空 ``Int64``。
* 字段辨析：扶摇 ``PriceBarItem.turnover`` 实际为成交额（元），应映射为 ``amount``；
  快照字段 ``turnover_ratio_pct`` 才是换手率，映射为 ``turnover``。
* 复权因子：适配器已显式设置 ``adjust=\"none\"``，缺失时默认 ``1.0``，不因停牌覆写。
"""

from __future__ import annotations

import re

import pandas as pd

from data.normalizers import (
    SOURCE_COLUMN_MAP,
    _coerce_and_finalize_bars,
    _ms_to_shanghai_date,
    _qualify_prefix_ticker,
)

__all__ = ["normalize"]

_SUPPORTED_DATASETS = frozenset({"stock", "etf", "index", "calendar"})

_SSE_CSI_INDEX_RE = re.compile(r"^(000\d{3})\.CSI$")


def _normalize_calendar(raw_df: pd.DataFrame) -> pd.DataFrame:
    """将同花顺交易日历原始表转为 ``cn_calendar`` 规范结构（单列 ``trade_date``）。"""
    if raw_df.empty:
        return pd.DataFrame({"trade_date": pd.Series(dtype="object")})
    df = raw_df.copy()
    if "trade_date" in df.columns:
        dates = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    elif "date" in df.columns:
        dates = pd.to_datetime(
            df["date"].astype(str), format="mixed", errors="coerce"
        ).dt.date
    elif "date_ms" in df.columns:
        dates = _ms_to_shanghai_date(df["date_ms"])
    else:
        raise ValueError("calendar raw_df missing 'trade_date', 'date', or 'date_ms'")
    out = pd.DataFrame({"trade_date": dates}).dropna(subset=["trade_date"])
    return (
        out.drop_duplicates(subset=["trade_date"])
        .sort_values("trade_date")
        .reset_index(drop=True)
    )


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将扶摇原始列名与 ``date_ms`` 转换为统一 ``bars_daily`` 格式。"""
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError(f"unsupported dataset: {dataset!r}")
    if dataset == "calendar":
        return _normalize_calendar(raw_df)

    df = raw_df.copy()

    if "date_ms" in df.columns:
        df["trade_date"] = _ms_to_shanghai_date(df["date_ms"])
        df = df.drop(columns=["date_ms"])
    elif "date" in df.columns and "trade_date" not in df.columns:
        df = df.rename(columns={"date": "trade_date"})

    # 若输入同时含 amount 与扶摇 turnover，合并后移除 turnover 以免重命名冲突
    if "amount" in df.columns and "turnover" in df.columns:
        df["amount"] = df["amount"].fillna(df["turnover"])
        df = df.drop(columns=["turnover"])

    df = df.rename(columns=SOURCE_COLUMN_MAP["straightflush"])

    if "ticker" in df.columns and len(df) > 0:
        df["ticker"] = df["ticker"].astype(str).map(_qualify_prefix_ticker)
        if dataset == "index":
            # 抛弃同花顺自编 .TI 指数
            df = df.loc[~df["ticker"].str.endswith(".TI")].copy()
            existing_tickers = set(df["ticker"])
            df["ticker"] = df["ticker"].map(
                lambda t: (
                    f"{m.group(1)}.SH"
                    if (m := _SSE_CSI_INDEX_RE.match(t))
                    and f"{m.group(1)}.SH" not in existing_tickers
                    else t
                )
            )

    if "trade_date" in df.columns and len(df) > 0:
        non_null = df["trade_date"].dropna()
        if len(non_null) > 0 and isinstance(non_null.iloc[0], str):
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    return _coerce_and_finalize_bars(df)
