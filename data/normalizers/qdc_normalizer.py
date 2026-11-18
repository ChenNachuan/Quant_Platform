"""QDCData 标准化器（仅格式转换）。

映射规则：
* 代码后缀：``.XSHG -> .SH``、``.XSHE -> .SZ``、``.XBSE -> .BJ``，裸代码按前缀补全。
* 时间转换：``ts``（Unix 毫秒）按 ``Asia/Shanghai`` 时区转 ``date``（不使用朴素 UTC）。
* 复权因子：按 ``hfq_factor -> adj_factor -> adjfactor`` 优先级保留原值，缺失默认 ``1.0``，不因停牌覆写。
* 成交量：``cn_daily_full`` / ``cn_etf_1d`` 原生单位即为股，转为可空 ``Int64``。
"""

from __future__ import annotations

import pandas as pd

from data.normalizers import (
    SOURCE_COLUMN_MAP,
    _coerce_and_finalize_bars,
    _ms_to_shanghai_date,
    _qualify_prefix_ticker,
)

__all__ = ["normalize"]

_SUPPORTED_DATASETS = frozenset({"stock", "etf", "index", "convertible"})

_EXCHANGE_SUFFIX_MAP: dict[str, str] = {
    ".XSHG": ".SH",
    ".XSHE": ".SZ",
    ".XBSE": ".BJ",
}


def _normalize_qdc_ticker(value: str) -> str:
    val = str(value).strip()
    for src_suffix, dst_suffix in _EXCHANGE_SUFFIX_MAP.items():
        if val.endswith(src_suffix):
            return val[: -len(src_suffix)] + dst_suffix
    return _qualify_prefix_ticker(val)


def normalize(dataset: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """将 QDC 原始列名、交易所后缀与毫秒时间戳转换为统一 ``bars_daily`` 格式。"""
    if dataset not in _SUPPORTED_DATASETS:
        raise ValueError(f"unsupported dataset: {dataset!r}")

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
