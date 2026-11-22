"""QDCData 标准化器（仅格式转换与类型规范）。

映射规则：
* 代码后缀：``.XSHG -> .SH``、``.XSHE -> .SZ``、``.XBSE -> .BJ``，裸代码按前缀补全。
  其中 ``cn_dividend``（``corporate_actions``）与 ``daily_status`` 的 ``92xxxx.XSHE`` 北交所错缀修复为 ``.BJ``。
* 时间转换：``ts``（Unix 毫秒）按 ``Asia/Shanghai`` 时区转无时区 ``datetime`` / ``trade_date``。
* 复权因子：按 ``hfq_factor -> adj_factor -> adjfactor`` 优先级保留原值，缺失默认 ``1.0``，不因停牌覆写。
* 基本面（``fundamental``）：``pe_latest_yr`` 优先映射为 ``pe_lyr``（回退 ``pe_static``），
  ``pb_excl_other_equity_mrq`` 优先映射为 ``pb``（回退 ``pb``）；剔除 B 股与停牌/无股本空行。
* 公司行动（``corporate_actions``）：``cn_dividend`` 仅保留 ``实施分配``，原始每 10 股数值除以 10 转每股，
  并按 ``dividend`` / ``bonus``（送股 + 转增）拆分为独立强类型事件行。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from core.asset_rules import validate_equity_ticker, validate_stock_ticker
from data.normalizers import (
    SOURCE_COLUMN_MAP,
    _coerce_and_finalize_bars,
    _coerce_and_finalize_minute_bars,
    _finalize_corporate_actions,
    _ms_to_shanghai_date,
    _ms_to_shanghai_datetime,
    _qualify_prefix_ticker,
    _safe_date_series,
)

__all__ = ["normalize"]

_SUPPORTED_DATASETS = frozenset(
    {
        "stock",
        "stock_minute",
        "fundamental",
        "daily_status",
        "corporate_actions",
        "etf",
        "index",
        "convertible",
    }
)

_EXCHANGE_SUFFIX_MAP: dict[str, str] = {
    ".XSHG": ".SH",
    ".XSHE": ".SZ",
    ".XBSE": ".BJ",
}

_BSE_PREFIXES = ("43", "83", "87", "92")

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


def _normalize_qdc_ticker(value: str, *, fix_bse_xshe: bool = False) -> str:
    val = str(value).strip()
    if fix_bse_xshe and val.endswith(".XSHE") and val[:2] in _BSE_PREFIXES:
        return val[: -len(".XSHE")] + ".BJ"
    for src_suffix, dst_suffix in _EXCHANGE_SUFFIX_MAP.items():
        if val.endswith(src_suffix):
            return val[: -len(src_suffix)] + dst_suffix
    return _qualify_prefix_ticker(val)


def _normalize_stock_minute(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return _coerce_and_finalize_minute_bars(pd.DataFrame())

    df = raw_df.copy()
    if "ts" in df.columns:
        df["datetime"] = _ms_to_shanghai_datetime(df["ts"])
        df["trade_date"] = df["datetime"].dt.date
    elif "datetime" in df.columns:
        dt_series = pd.to_datetime(df["datetime"], errors="coerce")
        if getattr(dt_series.dtype, "tz", None) is not None:
            dt_series = dt_series.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
        df["datetime"] = dt_series
        if "trade_date" not in df.columns:
            df["trade_date"] = df["datetime"].dt.date

    for alt in ("symbol", "ts_code", "code"):
        if alt in df.columns:
            df = df.rename(columns={alt: "ticker"})
            break

    raw_syms = df["ticker"].astype(str)
    sym_map = {s: _normalize_qdc_ticker(s) for s in raw_syms.unique()}
    valid_map = {v: validate_equity_ticker(v) for v in set(sym_map.values())}
    df["ticker"] = raw_syms.map(sym_map)
    df = df[df["ticker"].map(valid_map).fillna(False)].copy()
    return _coerce_and_finalize_minute_bars(df)


def _normalize_fundamental(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        out = pd.DataFrame(columns=_FUNDAMENTAL_COLUMNS)
        out["total_shares"] = pd.Series(dtype="int64")
        out["float_shares"] = pd.Series(dtype="int64")
        out["quality_flag"] = pd.Series(dtype="int64")
        return out

    df = raw_df.copy()
    for alt in ("symbol", "ts_code", "code"):
        if alt in df.columns:
            df = df.rename(columns={alt: "ticker"})
            break

    if "trade_date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "trade_date"})

    df["ticker"] = df["ticker"].astype(str).map(_normalize_qdc_ticker)
    df = df[df["ticker"].map(validate_equity_ticker)].copy()
    df["trade_date"] = _safe_date_series(df["trade_date"])

    # pe_lyr 优先取 pe_latest_yr（与 Tushare pe 口径一致），缺失时回退 pe_static
    if "pe_latest_yr" in df.columns:
        pe_lyr = pd.to_numeric(df["pe_latest_yr"], errors="coerce")
        if "pe_static" in df.columns:
            pe_lyr = pe_lyr.fillna(pd.to_numeric(df["pe_static"], errors="coerce"))
        df["pe_lyr"] = pe_lyr
    elif "pe_static" in df.columns:
        df["pe_lyr"] = pd.to_numeric(df["pe_static"], errors="coerce")
    elif "pe_lyr" not in df.columns:
        df["pe_lyr"] = np.nan

    # pb 优先取 pb_excl_other_equity_mrq（与 Tushare pb 口径一致），缺失回退 pb
    if "pb_excl_other_equity_mrq" in df.columns:
        pb_val = pd.to_numeric(df["pb_excl_other_equity_mrq"], errors="coerce")
        if "pb" in df.columns:
            pb_val = pb_val.fillna(pd.to_numeric(df["pb"], errors="coerce"))
        df["pb"] = pb_val

    df = df.rename(
        columns={
            "market_cap": "total_market_cap",
            "circulating_market_cap": "float_market_cap",
            "circulating_shares": "float_shares",
        }
    )

    for col in (
        "total_market_cap",
        "float_market_cap",
        "pe_ttm",
        "pe_lyr",
        "pb",
        "ps_ttm",
        "total_shares",
        "float_shares",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        else:
            df[col] = np.nan

    # 剔除停牌或尚未挂牌导致的缺失/非正市值与股本行，确保符合 FUNDAMENTAL_SCHEMA 非空与 >0 契约
    valid_mask = (
        df["trade_date"].notna()
        & (df["total_market_cap"] > 0)
        & (df["float_market_cap"] > 0)
        & (df["total_shares"] > 0)
        & (df["float_shares"] > 0)
    )
    df = df.loc[valid_mask].copy()
    df["total_shares"] = df["total_shares"].round().astype("int64")
    df["float_shares"] = df["float_shares"].round().astype("int64")
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
    for alt in ("symbol", "ts_code", "code"):
        if alt in df.columns:
            df = df.rename(columns={alt: "ticker"})
            break

    if "trade_date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "trade_date"})

    df["ticker"] = (
        df["ticker"]
        .astype(str)
        .map(lambda v: _normalize_qdc_ticker(v, fix_bse_xshe=True))
    )
    df = df[df["ticker"].map(validate_stock_ticker)].copy()
    df["trade_date"] = _safe_date_series(df["trade_date"])
    df = df[df["trade_date"].notna()].copy()

    for price_col in ("pre_close", "up_limit", "down_limit"):
        if price_col in df.columns:
            ser = pd.to_numeric(df[price_col], errors="coerce").astype("float64")
            df[price_col] = ser.where(ser > 0, np.nan)
        else:
            df[price_col] = np.nan

    if "limit_pct" in df.columns:
        df["limit_pct"] = pd.to_numeric(df["limit_pct"], errors="coerce").astype(
            "float64"
        )
    else:
        valid_ratio = (df["pre_close"] > 0) & df["up_limit"].notna()
        limit_pct = pd.Series(np.nan, index=df.index, dtype="float64")
        limit_pct.loc[valid_ratio] = (
            (df.loc[valid_ratio, "up_limit"] / df.loc[valid_ratio, "pre_close"] - 1.0)
            .round(4)
            .clip(lower=0.0)
        )
        if "limit_status" in df.columns:
            unlim = df["limit_status"].astype(str) == "unverified_special"
            limit_pct = limit_pct.where(~unlim, np.nan)
        df["limit_pct"] = limit_pct

    def _bool_col(candidates: tuple[str, ...]) -> pd.Series:
        for c in candidates:
            if c in df.columns:
                return df[c].fillna(False).astype(bool)
        return pd.Series(False, index=df.index, dtype=bool)

    df["is_st"] = _bool_col(("is_st",))
    if "is_suspend" in df.columns:
        df["is_suspend"] = df["is_suspend"].fillna(False).astype(bool)
    elif "trading_state" in df.columns:
        df["is_suspend"] = df["trading_state"].fillna("traded").astype(str) != "traded"
    else:
        df["is_suspend"] = False

    df["is_limit_up"] = _bool_col(("is_limit_up", "close_limit_up"))
    df["is_limit_down"] = _bool_col(("is_limit_down", "close_limit_down"))
    df["touched_limit_up"] = _bool_col(("touched_limit_up", "touched_up"))
    df["touched_limit_down"] = _bool_col(("touched_limit_down", "touched_down"))
    df["is_xr"] = _bool_col(("is_xr",))
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
    if "progress" in df.columns:
        df = df[df["progress"].astype(str) == "实施分配"].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    for alt in ("symbol", "ts_code", "code"):
        if alt in df.columns:
            df = df.rename(columns={alt: "ticker"})
            break

    df["ticker"] = (
        df["ticker"]
        .astype(str)
        .map(lambda v: _normalize_qdc_ticker(v, fix_bse_xshe=True))
    )
    df = df[df["ticker"].map(validate_equity_ticker)].copy()
    df["ex_date"] = _safe_date_series(df["ex_date"])
    df = df[df["ex_date"].notna()].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    ann = (
        _safe_date_series(df["disclosure_date"])
        if "disclosure_date" in df.columns
        else pd.Series(None, index=df.index, dtype="object")
    )
    if "plan_date" in df.columns:
        ann = ann.where(ann.notna(), _safe_date_series(df["plan_date"]))
    rec = (
        _safe_date_series(df["record_date"])
        if "record_date" in df.columns
        else pd.Series(None, index=df.index, dtype="object")
    )

    cash_10 = pd.to_numeric(df.get("cash"), errors="coerce").fillna(0.0)
    bonus_10 = pd.to_numeric(df.get("bonus_share"), errors="coerce").fillna(0.0)
    conv_10 = pd.to_numeric(df.get("convert_share"), errors="coerce").fillna(0.0)
    stock_10 = bonus_10 + conv_10

    parts: list[pd.DataFrame] = []

    cash_mask = cash_10 > 0
    if cash_mask.any():
        div_df = pd.DataFrame(
            {
                "ticker": df.loc[cash_mask, "ticker"].values,
                "ex_date": df.loc[cash_mask, "ex_date"].values,
                "event_type": "dividend",
                "announce_date": ann.loc[cash_mask].values,
                "record_date": rec.loc[cash_mask].values,
                "detail": [f"10派{v:.6g}元" for v in cash_10.loc[cash_mask]],
                "cash_dividend": (cash_10.loc[cash_mask] / 10.0).values,
                "stock_dividend": np.nan,
                "rights_price": np.nan,
                "rights_ratio": np.nan,
            }
        )
        parts.append(div_df)

    stock_mask = stock_10 > 0
    if stock_mask.any():
        details = []
        for b, c in zip(bonus_10.loc[stock_mask], conv_10.loc[stock_mask]):
            segs = []
            if b > 0:
                segs.append(f"送{b:.6g}")
            if c > 0:
                segs.append(f"转{c:.6g}")
            details.append(f"10{''.join(segs)}股" if segs else "10送转0股")
        bon_df = pd.DataFrame(
            {
                "ticker": df.loc[stock_mask, "ticker"].values,
                "ex_date": df.loc[stock_mask, "ex_date"].values,
                "event_type": "bonus",
                "announce_date": ann.loc[stock_mask].values,
                "record_date": rec.loc[stock_mask].values,
                "detail": details,
                "cash_dividend": np.nan,
                "stock_dividend": (stock_10.loc[stock_mask] / 10.0).values,
                "rights_price": np.nan,
                "rights_ratio": np.nan,
            }
        )
        parts.append(bon_df)

    if not parts:
        return _finalize_corporate_actions(pd.DataFrame())
    return _finalize_corporate_actions(pd.concat(parts, ignore_index=True))


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 QDC 原始数据转换为平台规范表格式。"""
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError(f"unsupported dataset: {dataset!r}")

    if dataset == "stock_minute":
        return _normalize_stock_minute(raw_df)
    if dataset == "fundamental":
        return _normalize_fundamental(raw_df)
    if dataset == "daily_status":
        return _normalize_daily_status(raw_df)
    if dataset == "corporate_actions":
        return _normalize_corporate_actions(raw_df)

    df = raw_df.copy()

    # 重命名前合并多个候选日期列，避免列名重复
    if "ts" in df.columns:
        df["trade_date"] = _ms_to_shanghai_date(df["ts"])
        drop_extra = [c for c in ("ts", "trade_day", "d") if c in df.columns]
        if drop_extra:
            df = df.drop(columns=drop_extra)

    # 代码列优先级：symbol -> ts_code -> code
    for alt in ("symbol", "ts_code", "code"):
        if alt in df.columns:
            if alt != "ticker":
                df = df.rename(columns={alt: "ticker"})
            break

    # 复权因子优先级：hfq_factor -> adj_factor -> adjfactor
    if "hfq_factor" in df.columns:
        if "adj_factor" in df.columns:
            df["hfq_factor"] = df["hfq_factor"].fillna(df["adj_factor"])
            df = df.drop(columns=["adj_factor"])
        if "adjfactor" in df.columns:
            df["hfq_factor"] = df["hfq_factor"].fillna(df["adjfactor"])
            df = df.drop(columns=["adjfactor"])
        df = df.rename(columns={"hfq_factor": "adjfactor"})
    elif "adj_factor" in df.columns:
        if "adjfactor" in df.columns:
            df["adj_factor"] = df["adj_factor"].fillna(df["adjfactor"])
            df = df.drop(columns=["adjfactor"])
        df = df.rename(columns={"adj_factor": "adjfactor"})

    col_map = SOURCE_COLUMN_MAP["qdc"]
    df = df.rename(
        columns={
            k: v for k, v in col_map.items() if k in df.columns and v not in df.columns
        }
    )

    if "ticker" in df.columns and len(df) > 0:
        df["ticker"] = df["ticker"].astype(str).map(_normalize_qdc_ticker)

    if "trade_date" in df.columns and len(df) > 0:
        non_null = df["trade_date"].dropna()
        if not pd.api.types.is_object_dtype(df["trade_date"]) or isinstance(
            non_null.iloc[0] if len(non_null) else None,
            str,
        ):
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    return _coerce_and_finalize_bars(df)
