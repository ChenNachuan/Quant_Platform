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
* 基础档案（``stock_basic``）：从 ``thscode`` / ``exchange`` / ``list_date`` / ``end_date`` 构造规范 9 列。
* 公司行动（``corporate_actions``）：将 ``ex_date_ms`` 转为北京日期，
  按 ``dividend`` / ``bonus`` / ``rights`` 拆分为强类型事件行（忽略非正负值异常），并合并同日同类型事件。
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

from core.asset_rules import validate_equity_ticker, validate_stock_ticker
from data.normalizers import (
    SOURCE_COLUMN_MAP,
    _coerce_and_finalize_bars,
    _finalize_corporate_actions,
    _infer_stock_market,
    _ms_to_shanghai_date,
    _qualify_prefix_ticker,
    _safe_date_series,
)

__all__ = ["normalize"]

_SUPPORTED_DATASETS = frozenset(
    {"stock", "stock_basic", "corporate_actions", "etf", "index", "calendar"}
)

_SSE_CSI_INDEX_RE = re.compile(r"^(000\d{3})\.CSI$")

_EXCHANGE_MAP: dict[str, str] = {
    "SH": "SH",
    "SZ": "SZ",
    "BJ": "BJ",
    "SSE": "SH",
    "SZSE": "SZ",
    "BSE": "BJ",
    "SHG": "SH",
    "SHE": "SZ",
}

_STOCK_BASIC_COLUMNS: list[str] = [
    "ticker",
    "name",
    "exchange",
    "market",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
    "old_code",
]


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


def _normalize_stock_basic(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=_STOCK_BASIC_COLUMNS)

    df = raw_df.copy()
    if "thscode" in df.columns:
        df["ticker"] = df["thscode"].astype(str).map(_qualify_prefix_ticker)
    elif "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).map(_qualify_prefix_ticker)
    else:
        raise ValueError("straightflush stock_basic raw_df missing 'thscode'/'ticker'")

    df = df[df["ticker"].map(validate_stock_ticker)].copy()
    if df.empty:
        return pd.DataFrame(columns=_STOCK_BASIC_COLUMNS)

    fallback_ex = df["ticker"].str.split(".").str[-1]
    if "exchange" in df.columns:
        mapped_ex = (
            df["exchange"].astype(str).str.strip().str.upper().map(_EXCHANGE_MAP)
        )
        df["exchange"] = mapped_ex.fillna(fallback_ex)
    else:
        df["exchange"] = fallback_ex

    df["market"] = df["ticker"].map(_infer_stock_market)
    df["name"] = (
        df.get("name", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    )
    df["list_date"] = _safe_date_series(df["list_date"])
    df = df[df["list_date"].notna()].copy()

    if "end_date" in df.columns:
        df["delist_date"] = _safe_date_series(df["end_date"])
    elif "delist_date" in df.columns:
        df["delist_date"] = _safe_date_series(df["delist_date"])
    else:
        df["delist_date"] = None

    df["list_status"] = np.where(df["delist_date"].notna(), "D", "L")
    df["is_hs"] = None
    df["old_code"] = None

    return (
        df.reindex(columns=_STOCK_BASIC_COLUMNS)
        .drop_duplicates(subset=["ticker"])
        .sort_values("ticker")
        .reset_index(drop=True)
    )


def _normalize_corporate_actions(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    df = raw_df.copy()
    if "thscode" in df.columns:
        df["ticker"] = df["thscode"].astype(str).map(_qualify_prefix_ticker)
    elif "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).map(_qualify_prefix_ticker)
    else:
        raise ValueError("straightflush corporate_actions missing 'thscode'/'ticker'")

    df = df[df["ticker"].map(validate_equity_ticker)].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    if "ex_date_ms" in df.columns:
        df["ex_date"] = _ms_to_shanghai_date(df["ex_date_ms"])
    elif "ex_date" in df.columns:
        df["ex_date"] = _safe_date_series(df["ex_date"])
    else:
        raise ValueError(
            "straightflush corporate_actions missing 'ex_date_ms'/'ex_date'"
        )

    df = df[df["ex_date"].notna()].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    div_ps = pd.to_numeric(df.get("dividend_per_share"), errors="coerce").fillna(0.0)
    bon_ps = pd.to_numeric(df.get("per_share_bonus"), errors="coerce").fillna(0.0)
    allot_ratio = pd.to_numeric(df.get("allotment_ratio"), errors="coerce").fillna(0.0)
    allot_price = pd.to_numeric(df.get("allotment_price"), errors="coerce").fillna(0.0)

    parts: list[pd.DataFrame] = []

    div_mask = div_ps > 0
    if div_mask.any():
        parts.append(
            pd.DataFrame(
                {
                    "ticker": df.loc[div_mask, "ticker"].values,
                    "ex_date": df.loc[div_mask, "ex_date"].values,
                    "event_type": "dividend",
                    "announce_date": None,
                    "record_date": None,
                    "detail": [f"10派{v * 10:.6g}元" for v in div_ps.loc[div_mask]],
                    "cash_dividend": div_ps.loc[div_mask].values,
                    "stock_dividend": np.nan,
                    "rights_price": np.nan,
                    "rights_ratio": np.nan,
                }
            )
        )

    # 忽略已知个别负值异常（per_share_bonus < 0）
    bon_mask = bon_ps > 0
    if bon_mask.any():
        parts.append(
            pd.DataFrame(
                {
                    "ticker": df.loc[bon_mask, "ticker"].values,
                    "ex_date": df.loc[bon_mask, "ex_date"].values,
                    "event_type": "bonus",
                    "announce_date": None,
                    "record_date": None,
                    "detail": [f"10送转{v * 10:.6g}股" for v in bon_ps.loc[bon_mask]],
                    "cash_dividend": np.nan,
                    "stock_dividend": bon_ps.loc[bon_mask].values,
                    "rights_price": np.nan,
                    "rights_ratio": np.nan,
                }
            )
        )

    rights_mask = allot_ratio > 0
    if rights_mask.any():
        r_prices = allot_price.loc[rights_mask].where(
            allot_price.loc[rights_mask] > 0, np.nan
        )
        parts.append(
            pd.DataFrame(
                {
                    "ticker": df.loc[rights_mask, "ticker"].values,
                    "ex_date": df.loc[rights_mask, "ex_date"].values,
                    "event_type": "rights",
                    "announce_date": None,
                    "record_date": None,
                    "detail": [
                        f"10配{r * 10:.6g}股"
                        + (f"(配股价{p:.4g}元)" if pd.notna(p) else "")
                        for r, p in zip(allot_ratio.loc[rights_mask], r_prices)
                    ],
                    "cash_dividend": np.nan,
                    "stock_dividend": np.nan,
                    "rights_price": r_prices.values,
                    "rights_ratio": allot_ratio.loc[rights_mask].values,
                }
            )
        )

    if not parts:
        return _finalize_corporate_actions(pd.DataFrame())
    return _finalize_corporate_actions(pd.concat(parts, ignore_index=True))


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将扶摇原始列名与 ``date_ms`` 转换为平台规范表格式。"""
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError(f"unsupported dataset: {dataset!r}")
    if dataset == "calendar":
        return _normalize_calendar(raw_df)
    if dataset == "stock_basic":
        return _normalize_stock_basic(raw_df)
    if dataset == "corporate_actions":
        return _normalize_corporate_actions(raw_df)

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
