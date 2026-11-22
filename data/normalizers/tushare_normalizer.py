"""Tushare 标准化器（仅格式转换与单位换算）。

边界处理：
* 日线行情（``stock`` / ``etf``）：``vol``（手->股 ×100）、``amount``（千元->元 ×1000），停牌 ``suspend_d`` 映射为布尔值；``etf`` 过滤场内 ETF 代码段。
* 基础档案（``stock_basic``）：``SSE/SZSE/BSE -> SH/SZ/BJ``，剔除 ``Txxxxxx`` 与 B 股，缺失 ``market`` 按前缀补全。
* 日频估值（``fundamental``）：``total_mv``/``circ_mv``（万元->元 ×10000）、``total_share``/``float_share``（万股->股 ×10000），``pe -> pe_lyr``。
* 风控状态（``daily_status``）：非正参考价/涨跌停价置 ``NaN``，按收盘价与最高/最低价计算封板/触及标志。
* 公司行动（``corporate_actions``）：``div_proc == '实施'``，按 ``dividend`` / ``bonus`` 拆分并合并同日同类型记录。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from core.asset_rules import (
    validate_equity_ticker,
    validate_etf_ticker,
    validate_stock_ticker,
)
from data.normalizers import (
    SOURCE_COLUMN_MAP,
    _coerce_and_finalize_bars,
    _finalize_corporate_actions,
    _infer_stock_market,
    _safe_date_series,
)

__all__ = ["normalize"]

_SUPPORTED_DATASETS = frozenset(
    {
        "stock",
        "etf",
        "stock_basic",
        "fundamental",
        "daily_status",
        "corporate_actions",
    }
)

# 同时存在 suspend_d 与 suspend 时，以 suspend_d 为主并先合并避免重命名冲突
_SUSPEND_COALESCE_PRIMARY = "suspend_d"

_EXCHANGE_TO_CODE: dict[str, str] = {
    "SSE": "SH",
    "SZSE": "SZ",
    "BSE": "BJ",
    "SH": "SH",
    "SZ": "SZ",
    "BJ": "BJ",
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

_FUNDAMENTAL_COLUMNS: list[str] = [
    "ticker",
    "trade_date",
    "total_market_cap",
    "float_market_cap",
    "pe_ttm",
    "pe_lyr",
    "pb",
    "ps_ttm",
    "total_shares",
    "float_shares",
    "quality_flag",
]

_DAILY_STATUS_COLUMNS: list[str] = [
    "ticker",
    "trade_date",
    "pre_close",
    "up_limit",
    "down_limit",
    "limit_pct",
    "is_st",
    "is_suspend",
    "is_limit_up",
    "is_limit_down",
    "touched_limit_up",
    "touched_limit_down",
    "is_xr",
    "quality_flag",
]


def _normalize_stock_basic(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=_STOCK_BASIC_COLUMNS)

    df = raw_df.copy()
    if "ts_code" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"ts_code": "ticker"})

    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].map(validate_stock_ticker)].copy()
    if df.empty:
        return pd.DataFrame(columns=_STOCK_BASIC_COLUMNS)

    if "exchange" in df.columns:
        mapped_ex = df["exchange"].astype(str).str.strip().map(_EXCHANGE_TO_CODE)
        fallback_ex = df["ticker"].str.split(".").str[-1]
        df["exchange"] = mapped_ex.fillna(fallback_ex)
    else:
        df["exchange"] = df["ticker"].str.split(".").str[-1]

    inferred_mkt = df["ticker"].map(_infer_stock_market)
    if "market" in df.columns:
        raw_mkt = df["market"].where(df["market"].notna(), "").astype(str).str.strip()
        df["market"] = raw_mkt.where(raw_mkt != "", inferred_mkt)
    else:
        df["market"] = inferred_mkt

    if "list_status" in df.columns:
        df["list_status"] = (
            df["list_status"].fillna("L").astype(str).str.strip().replace("", "L")
        )
    else:
        df["list_status"] = "L"

    df["list_date"] = _safe_date_series(df["list_date"])
    df = df[df["list_date"].notna()].copy()

    if "delist_date" in df.columns:
        df["delist_date"] = _safe_date_series(df["delist_date"])
    elif "end_date" in df.columns:
        df["delist_date"] = _safe_date_series(df["end_date"])
    else:
        df["delist_date"] = None

    if "is_hs" in df.columns:
        df["is_hs"] = df["is_hs"].where(df["is_hs"].notna(), None)
        df["is_hs"] = df["is_hs"].map(
            lambda v: str(v).strip() if v is not None and str(v).strip() else None
        )
    else:
        df["is_hs"] = None

    if "old_code" not in df.columns:
        df["old_code"] = None

    df["name"] = df["name"].fillna("").astype(str).str.strip()

    return (
        df.reindex(columns=_STOCK_BASIC_COLUMNS)
        .drop_duplicates(subset=["ticker"])
        .sort_values("ticker")
        .reset_index(drop=True)
    )


def _normalize_fundamental(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        out = pd.DataFrame(columns=_FUNDAMENTAL_COLUMNS)
        out["total_shares"] = pd.Series(dtype="int64")
        out["float_shares"] = pd.Series(dtype="int64")
        out["quality_flag"] = pd.Series(dtype="int64")
        return out

    df = raw_df.copy()
    if "ts_code" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"ts_code": "ticker"})
    if "pe" in df.columns and "pe_lyr" not in df.columns:
        df = df.rename(columns={"pe": "pe_lyr"})

    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].map(validate_equity_ticker)].copy()
    df["trade_date"] = _safe_date_series(df["trade_date"])

    # Tushare total_mv/circ_mv 为万元（*10000 -> 元）；total_share/float_share 为万股（*10000 -> 股）
    df["total_market_cap"] = (
        pd.to_numeric(df.get("total_mv"), errors="coerce").astype("float64") * 10000.0
    )
    df["float_market_cap"] = (
        pd.to_numeric(df.get("circ_mv"), errors="coerce").astype("float64") * 10000.0
    )
    total_shares_f = (
        pd.to_numeric(df.get("total_share"), errors="coerce").astype("float64")
        * 10000.0
    )
    float_shares_f = (
        pd.to_numeric(df.get("float_share"), errors="coerce").astype("float64")
        * 10000.0
    )

    for col in ("pe_ttm", "pe_lyr", "pb", "ps_ttm"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        else:
            df[col] = np.nan

    valid_mask = (
        df["trade_date"].notna()
        & (df["total_market_cap"] > 0)
        & (df["float_market_cap"] > 0)
        & (total_shares_f > 0)
        & (float_shares_f > 0)
    )
    df = df.loc[valid_mask].copy()
    df["total_shares"] = total_shares_f.loc[valid_mask].round().astype("int64")
    df["float_shares"] = float_shares_f.loc[valid_mask].round().astype("int64")
    df["quality_flag"] = 0

    return (
        df.reindex(columns=_FUNDAMENTAL_COLUMNS)
        .drop_duplicates(subset=["ticker", "trade_date"])
        .sort_values(["ticker", "trade_date"])
        .reset_index(drop=True)
    )


def _normalize_daily_status(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        out = pd.DataFrame(columns=_DAILY_STATUS_COLUMNS)
        for bc in (
            "is_st",
            "is_suspend",
            "is_limit_up",
            "is_limit_down",
            "touched_limit_up",
            "touched_limit_down",
            "is_xr",
        ):
            out[bc] = pd.Series(dtype="bool")
        out["quality_flag"] = pd.Series(dtype="int64")
        return out

    df = raw_df.copy()
    if "ts_code" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"ts_code": "ticker"})
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].map(validate_stock_ticker)].copy()
    df["trade_date"] = _safe_date_series(df["trade_date"])
    df = df[df["trade_date"].notna()].copy()

    for price_col in ("pre_close", "up_limit", "down_limit"):
        if price_col in df.columns:
            ser = pd.to_numeric(df[price_col], errors="coerce").astype("float64")
            df[price_col] = ser.where(ser > 0, np.nan)
        else:
            df[price_col] = np.nan

    valid_ratio = (df["pre_close"] > 0) & df["up_limit"].notna()
    limit_pct = pd.Series(np.nan, index=df.index, dtype="float64")
    limit_pct.loc[valid_ratio] = (
        (df.loc[valid_ratio, "up_limit"] / df.loc[valid_ratio, "pre_close"] - 1.0)
        .round(4)
        .clip(lower=0.0)
    )
    df["limit_pct"] = limit_pct

    df["is_st"] = (
        df["is_st"].fillna(False).astype(bool) if "is_st" in df.columns else False
    )
    df["is_suspend"] = (
        df["is_suspend"].fillna(False).astype(bool)
        if "is_suspend" in df.columns
        else False
    )
    df["is_xr"] = (
        df["is_xr"].fillna(False).astype(bool) if "is_xr" in df.columns else False
    )

    close = pd.to_numeric(df.get("close"), errors="coerce")
    high = pd.to_numeric(df.get("high"), errors="coerce")
    low = pd.to_numeric(df.get("low"), errors="coerce")
    df["is_limit_up"] = (
        (close.notna() & df["up_limit"].notna() & (close >= df["up_limit"] - 1e-6))
        if "close" in df.columns
        else False
    )
    df["is_limit_down"] = (
        (close.notna() & df["down_limit"].notna() & (close <= df["down_limit"] + 1e-6))
        if "close" in df.columns
        else False
    )
    df["touched_limit_up"] = (
        (high.notna() & df["up_limit"].notna() & (high >= df["up_limit"] - 1e-6))
        if "high" in df.columns
        else False
    )
    df["touched_limit_down"] = (
        (low.notna() & df["down_limit"].notna() & (low <= df["down_limit"] + 1e-6))
        if "low" in df.columns
        else False
    )
    df["quality_flag"] = 0

    return (
        df.reindex(columns=_DAILY_STATUS_COLUMNS)
        .drop_duplicates(subset=["ticker", "trade_date"])
        .sort_values(["ticker", "trade_date"])
        .reset_index(drop=True)
    )


def _normalize_corporate_actions(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    df = raw_df.copy()
    if "div_proc" in df.columns:
        df = df[df["div_proc"].astype(str) == "实施"].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    if "ts_code" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"ts_code": "ticker"})
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].map(validate_equity_ticker)].copy()
    df["ex_date"] = _safe_date_series(df["ex_date"])
    df = df[df["ex_date"].notna()].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    ann = (
        _safe_date_series(df["ann_date"])
        if "ann_date" in df.columns
        else pd.Series(None, index=df.index, dtype="object")
    )
    rec = (
        _safe_date_series(df["record_date"])
        if "record_date" in df.columns
        else pd.Series(None, index=df.index, dtype="object")
    )

    cash = pd.to_numeric(df.get("cash_div_tax"), errors="coerce").fillna(0.0)
    bo = pd.to_numeric(df.get("stk_bo_rate"), errors="coerce").fillna(0.0)
    co = pd.to_numeric(df.get("stk_co_rate"), errors="coerce").fillna(0.0)
    stock_div = bo + co

    parts: list[pd.DataFrame] = []
    cash_mask = cash > 0
    if cash_mask.any():
        parts.append(
            pd.DataFrame(
                {
                    "ticker": df.loc[cash_mask, "ticker"].values,
                    "ex_date": df.loc[cash_mask, "ex_date"].values,
                    "event_type": "dividend",
                    "announce_date": ann.loc[cash_mask].values,
                    "record_date": rec.loc[cash_mask].values,
                    "detail": [f"每股派{v:.6g}元" for v in cash.loc[cash_mask]],
                    "cash_dividend": cash.loc[cash_mask].values,
                    "stock_dividend": np.nan,
                    "rights_price": np.nan,
                    "rights_ratio": np.nan,
                }
            )
        )

    stock_mask = stock_div > 0
    if stock_mask.any():
        parts.append(
            pd.DataFrame(
                {
                    "ticker": df.loc[stock_mask, "ticker"].values,
                    "ex_date": df.loc[stock_mask, "ex_date"].values,
                    "event_type": "bonus",
                    "announce_date": ann.loc[stock_mask].values,
                    "record_date": rec.loc[stock_mask].values,
                    "detail": [f"每股送转{v:.6g}股" for v in stock_div.loc[stock_mask]],
                    "cash_dividend": np.nan,
                    "stock_dividend": stock_div.loc[stock_mask].values,
                    "rights_price": np.nan,
                    "rights_ratio": np.nan,
                }
            )
        )

    if not parts:
        return _finalize_corporate_actions(pd.DataFrame())
    return _finalize_corporate_actions(pd.concat(parts, ignore_index=True))


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 Tushare 原始 DataFrame 转换为平台规范表格式并完成单位换算。"""
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError(f"unsupported dataset: {dataset!r}")

    if dataset == "stock_basic":
        return _normalize_stock_basic(raw_df)
    if dataset == "fundamental":
        return _normalize_fundamental(raw_df)
    if dataset == "daily_status":
        return _normalize_daily_status(raw_df)
    if dataset == "corporate_actions":
        return _normalize_corporate_actions(raw_df)

    df = raw_df.copy()
    if _SUSPEND_COALESCE_PRIMARY in df.columns and "suspend" in df.columns:
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

    if dataset == "etf" and "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip()
        df = df[df["ticker"].map(validate_etf_ticker)].copy()

    if "trade_date" in df.columns:
        df["trade_date"] = _safe_date_series(df["trade_date"])

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
