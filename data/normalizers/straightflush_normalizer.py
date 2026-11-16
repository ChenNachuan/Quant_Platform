"""同花顺（扶摇 / StraightFlush）标准化器（仅格式转换）。

映射规则：
* ``thscode`` -> ``ticker``（裸代码按前缀补全交易所后缀）。
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

from core.asset_rules import PREFIX_EXCHANGE
from data.normalizers import UNIFIED_COLUMNS

__all__ = ["normalize"]

_SUPPORTED_DATASETS = frozenset({"stock", "etf", "index", "calendar"})

_COLUMN_MAP: dict[str, str] = {
    "thscode": "ticker",
    "open_price": "open",
    "high_price": "high",
    "low_price": "low",
    "close_price": "close",
    "volume": "volume",
    "turnover": "amount",  # 扶摇 turnover 字段实为成交额（元），非换手率
    "turnover_ratio_pct": "turnover",
    "adjfactor": "adjfactor",
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

_FLOAT_COLS = frozenset(
    {"open", "high", "low", "close", "amount", "adjfactor", "turnover"}
)

_SSE_CSI_INDEX_RE = re.compile(r"^(000\d{3})\.CSI$")


def _qualify_ticker(code: str) -> str:
    val = str(code).strip()
    if "." in val:
        return val
    prefix = val[:2]
    exchange = PREFIX_EXCHANGE.get(prefix)
    if exchange is None:
        raise ValueError(f"cannot qualify ticker {val!r}: unknown prefix {prefix!r}")
    return f"{val}.{exchange}"


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
        dates = (
            pd.to_datetime(
                pd.to_numeric(df["date_ms"], errors="coerce"), unit="ms", utc=True
            )
            .dt.tz_convert("Asia/Shanghai")
            .dt.date
        )
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
        df["trade_date"] = (
            pd.to_datetime(
                pd.to_numeric(df["date_ms"], errors="coerce"), unit="ms", utc=True
            )
            .dt.tz_convert("Asia/Shanghai")
            .dt.date
        )
        df = df.drop(columns=["date_ms"])
    elif "date" in df.columns and "trade_date" not in df.columns:
        df = df.rename(columns={"date": "trade_date"})

    # 若输入同时含 amount 与扶摇 turnover，合并后移除 turnover 以免重命名冲突
    if "amount" in df.columns and "turnover" in df.columns:
        df["amount"] = df["amount"].fillna(df["turnover"])
        df = df.drop(columns=["turnover"])

    df = df.rename(columns=_COLUMN_MAP)

    if "ticker" in df.columns and len(df) > 0:
        df["ticker"] = df["ticker"].astype(str).map(_qualify_ticker)
        if dataset == "index":
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

    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if col in _FLOAT_COLS:
                df[col] = df[col].astype("float64")

    if "volume" in df.columns:
        df["volume"] = df["volume"].round().astype("Int64")

    if "suspend" not in df.columns:
        df["suspend"] = False
    else:
        df["suspend"] = df["suspend"].fillna(False).astype(bool)

    if "adjfactor" not in df.columns:
        df["adjfactor"] = 1.0

    df["is_limit_up"] = False
    df["is_limit_down"] = False
    df["quality_flag"] = 0

    return df.reindex(columns=UNIFIED_COLUMNS)
