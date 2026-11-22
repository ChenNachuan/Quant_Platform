"""AmazingData 标准化器（仅格式转换与单位换算）。

边界处理：
- 停牌：保留源数据 ``suspend`` 标志；严禁因 ``suspend==True`` 而覆写源端提供的 ``adjfactor``。
- 缺失值：原样保留 OHLC 中的 NaN，不做填充。
- 复权因子：优先采用 ``NAV_ADJ_FACTOR`` 并原样保留；若无复权字段则默认 ``1.0``。
- 指数成交量：AmazingData ``EXTRA_INDEX_A`` 日线对上交所（``.SH``）指数按手回报，
  需 ``* 100`` 转为股；深交所（``.SZ``）/北交所（``.BJ``）按股回报保持原值，最终统一转为可空 ``Int64``。
- 基础档案（``stock_basic``）：规范 ``MARKET_CODE``/``LISTPLATE_NAME``/``IS_LISTED``/``OLD_CODE``（北交所平移旧码）。
- 风控状态（``daily_status``）：``PRECLOSE/HIGH_LIMITED/LOW_LIMITED`` 转浮点（非正置 NaN），
  ``IS_ST_SEC/IS_SUSP_SEC/IS_XR_SEC``（``'1'/'0'``）转布尔，并结合收盘/高低价计算封板与触及标志。
- 公司行动（``corporate_actions``）：``DVD_PER_SHARE_*`` 与 ``RATIO`` 原生即每股口径，拆分为 ``dividend`` / ``bonus`` / ``rights``。
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
    _infer_stock_market,
    _normalize_calendar_frame,
    _qualify_prefix_ticker,
    _safe_date_series,
)

__all__ = ["normalize"]

_SUPPORTED_DATASETS = frozenset(
    {
        "stock",
        "stock_minute",
        "stock_basic",
        "daily_status",
        "corporate_actions",
        "etf",
        "index",
        "option",
        "calendar",
    }
)

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

_LISTPLATE_TO_MARKET: dict[str, str] = {
    "主板": "主板",
    "创业板": "创业板",
    "科创板": "科创板",
    "北证": "北交所",
    "北交所": "北交所",
    "中小企业板": "主板",
    "中小板": "主板",
}


def _clean_old_code(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none", "<na>"):
        return None
    if "." in text:
        return text
    if text.isdigit():
        return _qualify_prefix_ticker(text)
    return text


def _flag_to_bool(series: pd.Series) -> pd.Series:
    def _conv(val: object) -> bool:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return False
        if isinstance(val, (bool, np.bool_)):
            return bool(val)
        s = str(val).strip().lower()
        return s in ("1", "true", "yes", "y", "1.0")

    return series.map(_conv).astype(bool)


def _normalize_stock_minute(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return _coerce_and_finalize_minute_bars(pd.DataFrame())

    df = raw_df.copy()
    if "code" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"code": "ticker"})
    elif "MARKET_CODE" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"MARKET_CODE": "ticker"})

    if "kline_time" in df.columns and "datetime" not in df.columns:
        df = df.rename(columns={"kline_time": "datetime"})
    elif "trade_time" in df.columns and "datetime" not in df.columns:
        df = df.rename(columns={"trade_time": "datetime"})

    df["ticker"] = df["ticker"].astype(str).map(_qualify_prefix_ticker)
    df = df[df["ticker"].map(validate_equity_ticker)].copy()
    return _coerce_and_finalize_minute_bars(df)


def _normalize_stock_basic(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=_STOCK_BASIC_COLUMNS)

    df = raw_df.copy()
    for alt in ("MARKET_CODE", "code", "ts_code"):
        if alt in df.columns and "ticker" not in df.columns:
            df = df.rename(columns={alt: "ticker"})
            break

    df["ticker"] = df["ticker"].astype(str).map(_qualify_prefix_ticker)
    df = df[df["ticker"].map(validate_stock_ticker)].copy()
    if df.empty:
        return pd.DataFrame(columns=_STOCK_BASIC_COLUMNS)

    if "SECURITY_NAME" in df.columns and "name" not in df.columns:
        df = df.rename(columns={"SECURITY_NAME": "name"})
    df["name"] = (
        df.get("name", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    )
    df["exchange"] = df["ticker"].str.split(".").str[-1]

    inferred_mkt = df["ticker"].map(_infer_stock_market)
    if "LISTPLATE_NAME" in df.columns:
        mapped_mkt = (
            df["LISTPLATE_NAME"].astype(str).str.strip().map(_LISTPLATE_TO_MARKET)
        )
        df["market"] = mapped_mkt.fillna(inferred_mkt)
    elif "market" in df.columns:
        raw_mkt = df["market"].fillna("").astype(str).str.strip()
        df["market"] = raw_mkt.where(raw_mkt != "", inferred_mkt)
    else:
        df["market"] = inferred_mkt

    list_col = "LISTDATE" if "LISTDATE" in df.columns else "list_date"
    delist_col = "DELISTDATE" if "DELISTDATE" in df.columns else "delist_date"
    df["list_date"] = _safe_date_series(df[list_col])
    df = df[df["list_date"].notna()].copy()
    df["delist_date"] = (
        _safe_date_series(df[delist_col]) if delist_col in df.columns else None
    )

    if "IS_LISTED" in df.columns:
        listed = pd.to_numeric(df["IS_LISTED"], errors="coerce")
        df["list_status"] = np.where(
            listed == 0,
            "D",
            np.where(df["delist_date"].notna(), "D", "L"),
        )
    elif "list_status" in df.columns:
        df["list_status"] = df["list_status"].fillna("L").astype(str).str.strip()
    else:
        df["list_status"] = np.where(df["delist_date"].notna(), "D", "L")

    df["is_hs"] = None
    if "OLD_CODE" in df.columns:
        df["old_code"] = df["OLD_CODE"].map(_clean_old_code)
    elif "old_code" in df.columns:
        df["old_code"] = df["old_code"].map(_clean_old_code)
    else:
        df["old_code"] = None

    return (
        df.reindex(columns=_STOCK_BASIC_COLUMNS)
        .drop_duplicates(subset=["ticker"])
        .sort_values("ticker")
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
    for alt in ("MARKET_CODE", "code", "ts_code"):
        if alt in df.columns and "ticker" not in df.columns:
            df = df.rename(columns={alt: "ticker"})
            break

    if "TRADE_DATE" in df.columns and "trade_date" not in df.columns:
        df = df.rename(columns={"TRADE_DATE": "trade_date"})

    df["ticker"] = df["ticker"].astype(str).map(_qualify_prefix_ticker)
    df = df[df["ticker"].map(validate_stock_ticker)].copy()
    df["trade_date"] = _safe_date_series(df["trade_date"])
    df = df[df["trade_date"].notna()].copy()

    col_alias = {
        "PRECLOSE": "pre_close",
        "HIGH_LIMITED": "up_limit",
        "LOW_LIMITED": "down_limit",
    }
    for src_col, dst_col in col_alias.items():
        if src_col in df.columns and dst_col not in df.columns:
            df[dst_col] = df[src_col]

    for price_col in ("pre_close", "up_limit", "down_limit"):
        if price_col in df.columns:
            ser = pd.to_numeric(df[price_col], errors="coerce").astype("float64")
            df[price_col] = ser.where(ser > 0, np.nan)
        else:
            df[price_col] = np.nan

    if "PRICE_HIGH_LMT_RATE" in df.columns:
        pct = pd.to_numeric(df["PRICE_HIGH_LMT_RATE"], errors="coerce").astype(
            "float64"
        )
        df["limit_pct"] = pct.where(pct >= 0, np.nan)
    elif "limit_pct" in df.columns:
        pct = pd.to_numeric(df["limit_pct"], errors="coerce").astype("float64")
        df["limit_pct"] = pct.where(pct >= 0, np.nan)
    else:
        valid_ratio = (df["pre_close"] > 0) & df["up_limit"].notna()
        limit_pct = pd.Series(np.nan, index=df.index, dtype="float64")
        limit_pct.loc[valid_ratio] = (
            (df.loc[valid_ratio, "up_limit"] / df.loc[valid_ratio, "pre_close"] - 1.0)
            .round(4)
            .clip(lower=0.0)
        )
        df["limit_pct"] = limit_pct

    df["is_st"] = (
        _flag_to_bool(df["IS_ST_SEC"])
        if "IS_ST_SEC" in df.columns
        else (
            _flag_to_bool(df["is_st"])
            if "is_st" in df.columns
            else pd.Series(False, index=df.index, dtype=bool)
        )
    )
    df["is_suspend"] = (
        _flag_to_bool(df["IS_SUSP_SEC"])
        if "IS_SUSP_SEC" in df.columns
        else (
            _flag_to_bool(df["is_suspend"])
            if "is_suspend" in df.columns
            else pd.Series(False, index=df.index, dtype=bool)
        )
    )
    df["is_xr"] = (
        _flag_to_bool(df["IS_XR_SEC"])
        if "IS_XR_SEC" in df.columns
        else (
            _flag_to_bool(df["is_xr"])
            if "is_xr" in df.columns
            else pd.Series(False, index=df.index, dtype=bool)
        )
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
    for alt in ("MARKET_CODE", "code", "ts_code"):
        if alt in df.columns and "ticker" not in df.columns:
            df = df.rename(columns={alt: "ticker"})
            break

    df["ticker"] = df["ticker"].astype(str).map(_qualify_prefix_ticker)
    df = df[df["ticker"].map(validate_equity_ticker)].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    # 兼容直接传入 get_dividend / get_right_issue 原始列或 unified raw 列
    if "_action_kind" not in df.columns:
        if "RATIO" in df.columns or "EX_DIVIDEND_DATE" in df.columns:
            df["_action_kind"] = "rights"
        else:
            df["_action_kind"] = "dividend"

    if "DIV_PROGRESS" in df.columns:
        is_div = df["_action_kind"] == "dividend"
        keep_div = (~is_div) | (df["DIV_PROGRESS"].astype(str).str.strip() == "3")
        df = df.loc[keep_div].copy()
    if "PROGRESS" in df.columns:
        is_ri = df["_action_kind"] == "rights"
        keep_ri = (~is_ri) | (df["PROGRESS"].astype(str).str.strip() == "3")
        df = df.loc[keep_ri].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    ex_col = next(
        (
            c
            for c in ("EX_DATE", "DATE_EX", "EX_DIVIDEND_DATE", "ex_date")
            if c in df.columns
        ),
        None,
    )
    if ex_col is None:
        raise ValueError("amazingdata corporate_actions missing ex_date column")
    df["ex_date"] = _safe_date_series(df[ex_col])
    if ex_col != "EX_DIVIDEND_DATE" and "EX_DIVIDEND_DATE" in df.columns:
        df["ex_date"] = df["ex_date"].where(
            df["ex_date"].notna(), _safe_date_series(df["EX_DIVIDEND_DATE"])
        )
    df = df[df["ex_date"].notna()].copy()
    if df.empty:
        return _finalize_corporate_actions(pd.DataFrame())

    ann = pd.Series(None, index=df.index, dtype="object")
    for c in ("ANN_DATE", "DATE_DVD_ANN", "announce_date"):
        if c in df.columns:
            ann = ann.where(ann.notna(), _safe_date_series(df[c]))
    rec = pd.Series(None, index=df.index, dtype="object")
    for c in ("RECORD_DATE", "DATE_EQY_RECORD", "SHAREB_REG_DATE", "record_date"):
        if c in df.columns:
            rec = rec.where(rec.notna(), _safe_date_series(df[c]))

    remark = pd.Series("", index=df.index, dtype="object")
    for c in ("REMARK", "RIGHTSISSUE_DESC", "detail"):
        if c in df.columns:
            s = df[c].fillna("").astype(str).str.strip()
            remark = remark.where(remark != "", s)

    cash_col = (
        "CASH_DIVIDEND"
        if "CASH_DIVIDEND" in df.columns
        else "DVD_PER_SHARE_PRE_TAX_CASH"
    )
    cash_ps = pd.to_numeric(df.get(cash_col), errors="coerce").fillna(0.0)

    stk_col = (
        "STOCK_DIVIDEND" if "STOCK_DIVIDEND" in df.columns else "DVD_PER_SHARE_STK"
    )
    bonus_col = "BONUS_RATE" if "BONUS_RATE" in df.columns else "DIV_BONUSRATE"
    conv_col = (
        "CONVERSED_RATE" if "CONVERSED_RATE" in df.columns else "DIV_CONVERSEDRATE"
    )
    bonus_ps = pd.to_numeric(df.get(bonus_col), errors="coerce").fillna(0.0)
    conv_ps = pd.to_numeric(df.get(conv_col), errors="coerce").fillna(0.0)
    stk_raw = pd.to_numeric(df.get(stk_col), errors="coerce").fillna(0.0)
    stock_ps = (bonus_ps + conv_ps).where((bonus_ps + conv_ps) > 0, stk_raw)

    r_price_col = "RIGHTS_PRICE" if "RIGHTS_PRICE" in df.columns else "PRICE"
    r_ratio_col = "RIGHTS_RATIO" if "RIGHTS_RATIO" in df.columns else "RATIO"
    r_price = pd.to_numeric(df.get(r_price_col), errors="coerce").fillna(0.0)
    r_ratio = pd.to_numeric(df.get(r_ratio_col), errors="coerce").fillna(0.0)

    is_div_row = df["_action_kind"] == "dividend"
    is_ri_row = df["_action_kind"] == "rights"
    parts: list[pd.DataFrame] = []

    cash_mask = is_div_row & (cash_ps > 0)
    if cash_mask.any():
        details = [f"10派{v * 10:.6g}元" for v in cash_ps.loc[cash_mask]]
        parts.append(
            pd.DataFrame(
                {
                    "ticker": df.loc[cash_mask, "ticker"].values,
                    "ex_date": df.loc[cash_mask, "ex_date"].values,
                    "event_type": "dividend",
                    "announce_date": ann.loc[cash_mask].values,
                    "record_date": rec.loc[cash_mask].values,
                    "detail": details,
                    "cash_dividend": cash_ps.loc[cash_mask].values,
                    "stock_dividend": np.nan,
                    "rights_price": np.nan,
                    "rights_ratio": np.nan,
                }
            )
        )

    stock_mask = is_div_row & (stock_ps > 0)
    if stock_mask.any():
        details = [f"10送转{v * 10:.6g}股" for v in stock_ps.loc[stock_mask]]
        parts.append(
            pd.DataFrame(
                {
                    "ticker": df.loc[stock_mask, "ticker"].values,
                    "ex_date": df.loc[stock_mask, "ex_date"].values,
                    "event_type": "bonus",
                    "announce_date": ann.loc[stock_mask].values,
                    "record_date": rec.loc[stock_mask].values,
                    "detail": details,
                    "cash_dividend": np.nan,
                    "stock_dividend": stock_ps.loc[stock_mask].values,
                    "rights_price": np.nan,
                    "rights_ratio": np.nan,
                }
            )
        )

    rights_mask = is_ri_row & (r_ratio > 0)
    if rights_mask.any():
        r_prices = r_price.loc[rights_mask].where(r_price.loc[rights_mask] > 0, np.nan)
        rem_ri = remark.loc[rights_mask]
        details = [
            (
                rem
                if rem
                else f"10配{ratio * 10:.6g}股"
                + (f"(配股价{price:.4g}元)" if pd.notna(price) else "")
            )
            for rem, ratio, price in zip(rem_ri, r_ratio.loc[rights_mask], r_prices)
        ]
        parts.append(
            pd.DataFrame(
                {
                    "ticker": df.loc[rights_mask, "ticker"].values,
                    "ex_date": df.loc[rights_mask, "ex_date"].values,
                    "event_type": "rights",
                    "announce_date": ann.loc[rights_mask].values,
                    "record_date": rec.loc[rights_mask].values,
                    "detail": details,
                    "cash_dividend": np.nan,
                    "stock_dividend": np.nan,
                    "rights_price": r_prices.values,
                    "rights_ratio": r_ratio.loc[rights_mask].values,
                }
            )
        )

    if not parts:
        return _finalize_corporate_actions(pd.DataFrame())
    return _finalize_corporate_actions(pd.concat(parts, ignore_index=True))


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 AmazingData 原始 DataFrame 转换为平台统一格式。"""
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError(f"unsupported dataset: {dataset!r}")
    if dataset == "calendar":
        return _normalize_calendar_frame(raw_df, ("trade_date", "cal_date"))
    if dataset == "stock_minute":
        return _normalize_stock_minute(raw_df)
    if dataset == "stock_basic":
        return _normalize_stock_basic(raw_df)
    if dataset == "daily_status":
        return _normalize_daily_status(raw_df)
    if dataset == "corporate_actions":
        return _normalize_corporate_actions(raw_df)

    # 重命名前合并两列复权因子，避免列名冲突；以 NAV_ADJ_FACTOR 为准
    df = raw_df.copy()
    if "adjfactor" in df.columns and "NAV_ADJ_FACTOR" in df.columns:
        nav_adj = pd.to_numeric(df["NAV_ADJ_FACTOR"], errors="coerce")
        base_adj = pd.to_numeric(df["adjfactor"], errors="coerce")
        df["adjfactor"] = nav_adj.fillna(base_adj)
        df = df.drop(columns=["NAV_ADJ_FACTOR"])
    elif "NAV_ADJ_FACTOR" in df.columns:
        df = df.rename(columns={"NAV_ADJ_FACTOR": "adjfactor"})

    df = df.rename(columns=SOURCE_COLUMN_MAP["amazingdata"]).copy()

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        if dataset == "index" and "ticker" in df.columns:
            sh_mask = df["ticker"].astype(str).str.endswith(".SH")
            if sh_mask.any():
                df.loc[sh_mask, "volume"] = df.loc[sh_mask, "volume"] * 100

    return _coerce_and_finalize_bars(df)
