"""临时引导拉取脚本共享工具（QUANT-35）。

直接从数据源拉取并按 ``data/storage_schema.py`` 对齐后原子写入 parquet（无 DQC/隔离），
``quality_flag`` 恒为 0；数据根目录默认使用 ``core.config.DATA_ROOT``，测试可通过 ``--data-root`` 覆盖。
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from core.config import DATA_ROOT

# 直接以脚本方式运行时将仓库根目录加入 sys.path，确保可导入 data 包
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from data.storage_ops import (  # noqa: E402
    is_monthly_partition,
    upsert_parquet_atomic,
    write_parquet_atomic,
)
from data.storage_schema import PARTITION_PATHS, SCHEMA  # noqa: E402

#: TUSHARE_TOKEN 读取路径（仅读取，严禁打印或提交）
_TOKEN_FILES = (Path("config/.env"), Path("config/secrets"))

#: 数据源缺失时以常量填充的可选列；非此映射内的缺失非空列将拒绝写入
_OPTIONAL_DEFAULTS: dict[str, object] = {
    "quality_flag": 0,
    "suspend": False,
    "is_limit_up": False,
    "is_limit_down": False,
    "turnover": float("nan"),
}

#: tushare ts_code 格式：{6位数字}.{SH|SZ|BJ}，与仓库 ticker 一致
_TS_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)\Z")


# tushare 初始化
def init_tushare():
    """从环境变量或 .env / config/secrets 读取 token 并返回 tushare pro 客户端。"""
    token = _read_token()
    if not token:
        raise RuntimeError(
            "TUSHARE_TOKEN not found in config/.env or config/secrets — "
            "copy config/.env.example to config/.env and fill it in"
        )
    import tushare as ts

    ts.set_token(token)
    return ts.pro_api()


def _read_token() -> str | None:
    env = os.environ.get("TUSHARE_TOKEN")
    if env:
        return env.strip()
    for path in _TOKEN_FILES:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() == "TUSHARE_TOKEN":
                return value.strip().strip("'\"")
    return None


# ticker 格式转换
def ticker_convert(ts_code: str) -> str:
    """校验并将 tushare ts_code 转换为仓库 ticker（当前二者格式相同，作恒等校验）。"""
    if not isinstance(ts_code, str) or _TS_CODE_RE.match(ts_code) is None:
        raise ValueError(f"not a tushare ts_code: {ts_code!r}")
    return ts_code


# Schema 对齐
def align_schema(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """按 SCHEMA[table_name] 裁剪、补缺并强转列类型，缺失必填非空列时拒绝写入。"""
    schema = SCHEMA[table_name]
    out = pd.DataFrame(index=df.index)

    for name, col in schema.columns.items():
        if name in df.columns:
            out[name] = df[name]
        elif name in _OPTIONAL_DEFAULTS:
            out[name] = _OPTIONAL_DEFAULTS[name]
        elif col.nullable or not col.required:
            out[name] = pd.NA
        else:
            raise ValueError(
                f"{table_name}: required column {name!r} missing from source frame "
                f"(have {sorted(df.columns)}); refusing to fabricate it"
            )

    for name in schema.columns:
        out[name] = _coerce_column(out[name], schema.columns[name], name, table_name)
    return out.reset_index(drop=True)


def _coerce_column(series: pd.Series, col, name: str, table_name: str) -> pd.Series:
    # pandera DataType 转字符串表示（如 int64/float64/bool/str/date）
    kind = str(col.dtype)
    if kind == "bool":
        return series.astype("boolean").fillna(False).astype(bool)
    if kind.startswith("int"):
        return _to_int64(series, name, table_name)
    if kind.startswith("float"):
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if kind == "str":
        return series.astype("string").astype(object)
    # 日期/时间列交由 schema 校验阶段严格解析，此处保持原值
    return series


def _to_int64(series: pd.Series, name: str, table_name: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    non_null = numeric.dropna()
    if not non_null.empty and not (non_null == non_null.round()).all():
        raise ValueError(
            f"{table_name}.{name}: non-integral float in integer column "
            f"(e.g. {non_null.iloc[0]!r}); refusing to truncate"
        )
    return (
        numeric.astype("Int64").astype("int64") if not numeric.isna().any() else numeric
    )


# AmazingData 暂存数据 -> Schema 格式转换（volume 为股、amount 为元，FLOAT_SHARE 为万份）


def frame_from_amazing_calendar(cal: pd.DataFrame) -> pd.DataFrame:
    """将暂存 calendar.parquet 转为单列交易日历表。"""
    if "cal_date" not in cal.columns:
        raise ValueError(
            f"amazing calendar staging missing cal_date: {list(cal.columns)}"
        )
    return pd.DataFrame({"trade_date": _ymd_int_to_date(cal["cal_date"])})


def frame_from_amazing_kline(
    kline: pd.DataFrame,
    nav: pd.DataFrame,
    share: pd.DataFrame,
) -> pd.DataFrame:
    """合并暂存 kline、fund_nav 与 fund_share 为 bars_daily 标准列格式。

    - ``adjfactor`` 取自 ``NAV_ADJ_FACTOR`` 原始累计复权因子（不归一化）
    - ``turnover = amount / (FLOAT_SHARE * 1e4 * close)``，缺少份额记录时置为 NaN
    - 数据源无停牌及涨跌停价信息，``suspend`` 与涨跌停标志统一置为 False
    """
    for name, frame in (("kline", kline), ("fund_nav", nav), ("fund_share", share)):
        if frame is None or frame.empty:
            raise ValueError(f"amazing staging {name} is empty")

    bars = pd.DataFrame(
        {
            "ticker": kline["code"].map(ticker_convert),
            "trade_date": pd.to_datetime(kline["kline_time"]).dt.date,
            "open": pd.to_numeric(kline["open"], errors="coerce").astype("float64"),
            "high": pd.to_numeric(kline["high"], errors="coerce").astype("float64"),
            "low": pd.to_numeric(kline["low"], errors="coerce").astype("float64"),
            "close": pd.to_numeric(kline["close"], errors="coerce").astype("float64"),
            "volume": pd.to_numeric(kline["volume"], errors="coerce"),
            "amount": pd.to_numeric(kline["amount"], errors="coerce").astype("float64"),
        }
    )

    factors = _amazing_series(nav, "MARKET_CODE", "PRICE_DATE", "NAV_ADJ_FACTOR")
    bars["adjfactor"] = _lookup(bars, factors, "adjfactor")
    if bars["adjfactor"].isna().any():
        missing = bars.loc[bars["adjfactor"].isna(), "trade_date"].head(5).tolist()
        raise ValueError(
            f"NAV_ADJ_FACTOR missing for {int(bars['adjfactor'].isna().sum())} bar "
            f"day(s), e.g. {missing}; adjfactor is non-nullable and is not fabricated"
        )

    float_shares = _amazing_series(share, "MARKET_CODE", "CHANGE_DATE", "FLOAT_SHARE")
    cap = _lookup(bars, float_shares, "float_share") * 1e4 * bars["close"]
    bars["turnover"] = bars["amount"] / cap

    bars["suspend"] = False
    bars["is_limit_up"] = False
    bars["is_limit_down"] = False
    bars["quality_flag"] = 0
    return bars


def _amazing_series(
    frame: pd.DataFrame, code_col: str, date_col: str, value_col: str
) -> pd.Series:
    """按 (ticker, trade_date) 建立暂存指标索引，存在重复键则报错。"""
    if not {code_col, date_col, value_col}.issubset(frame.columns):
        raise ValueError(
            f"amazing staging missing {[code_col, date_col, value_col]}: "
            f"{list(frame.columns)}"
        )
    keyed = pd.DataFrame(
        {
            "ticker": frame[code_col].map(ticker_convert),
            "trade_date": _ymd_int_to_date(frame[date_col]),
            "value": pd.to_numeric(frame[value_col], errors="coerce"),
        }
    )
    dupes = keyed.duplicated(subset=["ticker", "trade_date"]).sum()
    if dupes:
        raise ValueError(
            f"amazing staging has {dupes} duplicate (ticker, trade_date) rows"
        )
    return keyed.set_index(["ticker", "trade_date"])["value"]


def _lookup(bars: pd.DataFrame, keyed: pd.Series, label: str) -> pd.Series:
    index = pd.MultiIndex.from_arrays([bars["ticker"], bars["trade_date"]])
    values = keyed.reindex(index)
    values.index = bars.index
    return values.astype("float64").rename(label)


def _ymd_int_to_date(series: pd.Series) -> pd.Series:
    """将 YYYYMMDD 整型/字符串转为 datetime.date。"""
    text = pd.to_numeric(series, errors="coerce")
    if text.isna().any():
        raise ValueError(
            f"non-numeric YYYYMMDD in amazing staging: {series.head(3).tolist()}"
        )
    return pd.to_datetime(text.astype("int64").astype(str), format="%Y%m%d").dt.date


# 分区写入工具
def table_path(
    table_name: str,
    data_root: str | os.PathLike[str] | None = None,
    **fmt: object,
) -> Path:
    """按 PARTITION_PATHS[table_name] 模板拼接目标文件路径。"""
    root = Path(data_root) if data_root is not None else DATA_ROOT
    template = PARTITION_PATHS[table_name]
    return root / template.format(**fmt)


def write_daily(
    df: pd.DataFrame,
    table_name: str,
    data_root: str | os.PathLike[str] | None = None,
) -> list[Path]:
    """对齐 schema 后按日或按月分区原子写入 parquet。"""
    aligned = align_schema(df, table_name)
    written: list[Path] = []
    if aligned.empty:
        return written
    if "trade_date" not in aligned.columns:
        raise ValueError(f"{table_name}: write_daily needs a trade_date column")
    if is_monthly_partition(table_name):
        months = pd.to_datetime(aligned["trade_date"]).dt.strftime("%Y-%m")
        for month, group in aligned.groupby(months, sort=True):
            path = table_path(table_name, data_root, month=month)
            upsert_parquet_atomic(group.reset_index(drop=True), path, table=table_name)
            written.append(path)
        return written
    for day, group in aligned.groupby("trade_date", sort=True):
        path = table_path(table_name, data_root, date=_iso(day))
        write_parquet_atomic(group.reset_index(drop=True), path, table=table_name)
        written.append(path)
    return written


def write_reference(
    df: pd.DataFrame,
    table_name: str,
    data_root: str | os.PathLike[str] | None = None,
    **fmt: object,
) -> Path:
    """对齐 schema 后将参考表整表原子写入单一文件。"""
    aligned = align_schema(df, table_name)
    path = table_path(table_name, data_root, **fmt)
    write_parquet_atomic(aligned, path, table=table_name)
    return path


def _iso(day: object) -> str:
    if hasattr(day, "isoformat"):
        text = day.isoformat()
        return text[:10]
    return str(day)[:10]


def yyyymmdd(value: str) -> str:
    """支持 YYYYMMDD 或 YYYY-MM-DD，统一转为 YYYYMMDD 字符串。"""
    text = str(value).replace("-", "").strip()
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"not a date (YYYYMMDD or YYYY-MM-DD): {value!r}")
    return text
