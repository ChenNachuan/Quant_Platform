"""各数据源标准化器（仅做字段映射、单位换算与类型规范，不做 DQC 清洗）。

统一在模块入口维护 ``SOURCE_COLUMN_MAP`` 与 ``normalize(source, raw_df)``，
同时通过 ``get_normalizer(source)`` 暴露各源 ``normalize(dataset, raw_df) -> df``。
"""

from __future__ import annotations

import importlib
from typing import Callable, Iterable

import pandas as pd

from core.asset_rules import PREFIX_EXCHANGE

__all__ = [
    "SOURCE_COLUMN_MAP",
    "UNIFIED_COLUMNS",
    "_MINUTE_COLUMNS",
    "_CORPORATE_ACTIONS_COLUMNS",
    "get_normalizer",
    "normalize",
]

# 统一输出列（含由 DQC 阶段填充的 quality_flag 占位列）
UNIFIED_COLUMNS: list[str] = [
    "ticker",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjfactor",
    "turnover",
    "suspend",
    "is_limit_up",
    "is_limit_down",
    "quality_flag",
]

_MINUTE_COLUMNS: list[str] = [
    "ticker",
    "trade_date",
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "quality_flag",
]

_CORPORATE_ACTIONS_COLUMNS: list[str] = [
    "ticker",
    "ex_date",
    "event_type",
    "announce_date",
    "record_date",
    "detail",
    "cash_dividend",
    "stock_dividend",
    "rights_price",
    "rights_ratio",
]

_BAR_DATASETS: frozenset[str] = frozenset(
    {"stock", "stock_minute", "etf", "index", "convertible", "futures", "option"}
)

#: 归一化后行情表必需的核心字段（缺任一字段即显式报错，其余可填充默认值）。
_REQUIRED_BAR_COLS: frozenset[str] = frozenset(
    {"ticker", "trade_date", "open", "high", "low", "close", "volume", "amount"}
)

_NUMERIC_COLS: tuple[str, ...] = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjfactor",
    "turnover",
)

_FLOAT_COLS: frozenset[str] = frozenset(
    {"open", "high", "low", "close", "amount", "adjfactor", "turnover"}
)

_MARKET_BY_PREFIX: dict[str, str] = {
    "60": "主板",
    "00": "主板",
    "30": "创业板",
    "68": "科创板",
    "43": "北交所",
    "83": "北交所",
    "87": "北交所",
    "92": "北交所",
}

#: 各数据源原始列名 -> 统一标准列名映射表（集中收口维护）。
SOURCE_COLUMN_MAP: dict[str, dict[str, str]] = {
    "qdc": {
        "symbol": "ticker",
        "ts_code": "ticker",
        "code": "ticker",
        "ts": "trade_date",
        "trade_day": "trade_date",
        "d": "trade_date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "amount": "amount",
        "hfq_factor": "adjfactor",
        "adj_factor": "adjfactor",
        "adjfactor": "adjfactor",
        "turnover": "turnover",
        "suspend": "suspend",
    },
    "amazingdata": {
        "code": "ticker",
        "kline_time": "trade_date",
        "trade_time": "trade_date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "amount": "amount",
        "adjfactor": "adjfactor",
        "NAV_ADJ_FACTOR": "adjfactor",
        "suspend": "suspend",
    },
    "straightflush": {
        "thscode": "ticker",
        "date_ms": "trade_date",
        "date": "trade_date",
        "open_price": "open",
        "high_price": "high",
        "low_price": "low",
        "close_price": "close",
        "volume": "volume",
        "turnover": "amount",  # 扶摇 turnover 字段实为成交额（元），非换手率
        "turnover_ratio_pct": "turnover",
        "adjfactor": "adjfactor",
        "suspend": "suspend",
    },
    "tushare": {
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
    },
    "akshare": {
        "日期": "trade_date",
        "股票代码": "ticker",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
        "换手率": "turnover",
    },
}

_NORMALIZER_MODULES: dict[str, str] = {
    "akshare": "data.normalizers.akshare_normalizer",
    "tushare": "data.normalizers.tushare_normalizer",
    "amazingdata": "data.normalizers.amazingdata_normalizer",
    "qdc": "data.normalizers.qdc_normalizer",
    "straightflush": "data.normalizers.straightflush_normalizer",
}


def _qualify_prefix_ticker(code: str) -> str:
    """若标的代码无交易所后缀，则按前两位前缀补全 ``.{SH|SZ|BJ}``。"""
    val = str(code).strip()
    if "." in val:
        return val
    prefix = val[:2]
    exchange = PREFIX_EXCHANGE.get(prefix)
    if exchange is None:
        raise ValueError(f"cannot qualify ticker {val!r}: unknown prefix {prefix!r}")
    return f"{val}.{exchange}"


def _infer_stock_market(ticker: str) -> str:
    """按标的代码前两位推断市场板块（主板/创业板/科创板/北交所）。"""
    return _MARKET_BY_PREFIX.get(str(ticker).strip()[:2], "")


def _safe_date_series(series: pd.Series) -> pd.Series:
    """将日期字符串 / 日期对象安全转为 ``datetime.date``（缺失记为 ``None``）。"""
    if series.empty:
        return pd.Series(dtype="object")
    cleaned = series.where(series.notna(), None)
    text = cleaned.map(
        lambda v: (
            None
            if v is None or (isinstance(v, float) and pd.isna(v))
            else (v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else str(v).strip())
        )
    )
    parsed = pd.to_datetime(text, format="mixed", errors="coerce").dt.date
    return parsed.where(parsed.notna(), None)


def _ms_to_shanghai_datetime(series: pd.Series) -> pd.Series:
    """将 Unix 毫秒时间戳按 ``Asia/Shanghai`` 时区转为无时区 ``datetime64[ns]``。"""
    return (
        pd.to_datetime(pd.to_numeric(series, errors="coerce"), unit="ms", utc=True)
        .dt.tz_convert("Asia/Shanghai")
        .dt.tz_localize(None)
    )


def _ms_to_shanghai_date(series: pd.Series) -> pd.Series:
    """将 Unix 毫秒时间戳按 ``Asia/Shanghai`` 时区转为 ``datetime.date``。"""
    return _ms_to_shanghai_datetime(series).dt.date


def _normalize_calendar_frame(
    raw_df: pd.DataFrame, col_candidates: Iterable[str]
) -> pd.DataFrame:
    """通用交易日历整形：提取首个匹配的日期列并输出升序去重 ``trade_date``。"""
    if raw_df.empty:
        return pd.DataFrame({"trade_date": pd.Series(dtype="object")})
    col = next((c for c in col_candidates if c in raw_df.columns), None)
    if col is None:
        raise ValueError(f"calendar raw_df missing any of {tuple(col_candidates)!r}")
    cleaned = raw_df[col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    dates = pd.to_datetime(cleaned, format="mixed", errors="coerce").dt.date
    return (
        pd.DataFrame({"trade_date": dates})
        .dropna(subset=["trade_date"])
        .drop_duplicates(subset=["trade_date"])
        .sort_values("trade_date")
        .reset_index(drop=True)
    )


def _coerce_and_finalize_bars(df: pd.DataFrame) -> pd.DataFrame:
    """统一执行数值转型、``volume -> Int64``、默认标志位填充并按 ``UNIFIED_COLUMNS`` 对齐。"""
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


def _coerce_and_finalize_minute_bars(df: pd.DataFrame) -> pd.DataFrame:
    """统一执行分钟线数值转型、``datetime`` 标准化与列排序。"""
    if df.empty:
        out = pd.DataFrame(columns=_MINUTE_COLUMNS)
        out["quality_flag"] = pd.Series(dtype="int64")
        return out

    out = df.copy()
    for col in ("open", "high", "low", "close", "amount"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")

    if "volume" in out.columns:
        vol = pd.to_numeric(out["volume"], errors="coerce").round()
        out["volume"] = (
            vol.astype("int64") if vol.notna().all() else vol.astype("Int64")
        )

    if "datetime" in out.columns:
        dt_series = pd.to_datetime(out["datetime"], errors="coerce")
        if getattr(dt_series.dtype, "tz", None) is not None:
            dt_series = dt_series.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
        out["datetime"] = dt_series
        if "trade_date" not in out.columns or out["trade_date"].isna().any():
            out["trade_date"] = out["datetime"].dt.date

    if "trade_date" in out.columns:
        out["trade_date"] = _safe_date_series(out["trade_date"])

    out["quality_flag"] = 0
    out = out.reindex(columns=_MINUTE_COLUMNS)
    if not out.empty:
        out = (
            out.drop_duplicates(subset=["ticker", "trade_date", "datetime"])
            .sort_values(["ticker", "trade_date", "datetime"])
            .reset_index(drop=True)
        )
    return out


def _finalize_corporate_actions(rows_df: pd.DataFrame) -> pd.DataFrame:
    """标准化并按 ``[ticker, ex_date, event_type]`` 合并同日同类型公司行动事件。"""
    if rows_df.empty:
        return pd.DataFrame(columns=_CORPORATE_ACTIONS_COLUMNS)

    work = rows_df.copy()
    work["ex_date"] = _safe_date_series(work["ex_date"])
    work = work[work["ex_date"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=_CORPORATE_ACTIONS_COLUMNS)

    for col in ("announce_date", "record_date"):
        if col in work.columns:
            work[col] = _safe_date_series(work[col])
        else:
            work[col] = None

    for col in ("cash_dividend", "stock_dividend", "rights_price", "rights_ratio"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce").astype("float64")
        else:
            work[col] = float("nan")

    if "detail" not in work.columns:
        work["detail"] = ""
    else:
        work["detail"] = work["detail"].fillna("").astype(str)

    if work.duplicated(subset=["ticker", "ex_date", "event_type"]).any():

        def _first_non_null(s: pd.Series):
            non_null = s.dropna()
            return non_null.iloc[0] if len(non_null) else None

        def _join_details(s: pd.Series) -> str:
            parts: list[str] = []
            for item in s:
                txt = str(item).strip()
                if txt and txt not in parts:
                    parts.append(txt)
            return "; ".join(parts)

        work = work.groupby(
            ["ticker", "ex_date", "event_type"], as_index=False, sort=False
        ).agg(
            {
                "announce_date": _first_non_null,
                "record_date": _first_non_null,
                "detail": _join_details,
                "cash_dividend": lambda s: s.sum(min_count=1),
                "stock_dividend": lambda s: s.sum(min_count=1),
                "rights_price": _first_non_null,
                "rights_ratio": lambda s: s.sum(min_count=1),
            }
        )

    for col in ("cash_dividend", "stock_dividend", "rights_price", "rights_ratio"):
        work[col] = pd.to_numeric(work[col], errors="coerce").astype("float64")

    return (
        work.reindex(columns=_CORPORATE_ACTIONS_COLUMNS)
        .sort_values(["ticker", "ex_date", "event_type"])
        .reset_index(drop=True)
    )


def get_normalizer(source: str) -> Callable[[str, pd.DataFrame], pd.DataFrame]:
    """返回指定 *source* 对应的 ``normalize`` 函数；未知数据源抛出 ``ValueError``。"""
    module_path = _NORMALIZER_MODULES.get(source)
    if module_path is None:
        raise ValueError(f"no normalizer for source: {source!r}")
    module = importlib.import_module(module_path)
    return module.normalize


def normalize(
    source: str,
    raw_df: pd.DataFrame,
    dataset: str = "stock",
) -> pd.DataFrame:
    """统一标准化入口：映射列名、转换类型与单位并输出规范格式，不做 DQC 校验。"""
    col_map = SOURCE_COLUMN_MAP.get(source)
    if col_map is None:
        raise ValueError(f"no normalizer for source: {source!r}")

    if dataset in _BAR_DATASETS and not raw_df.empty:
        mapped_cols = set(raw_df.rename(columns=col_map).columns)
        missing = _REQUIRED_BAR_COLS - mapped_cols
        if missing:
            raise ValueError(f"[{source}] 归一化后缺失必需列: {sorted(missing)}")

    return get_normalizer(source)(dataset, raw_df)
