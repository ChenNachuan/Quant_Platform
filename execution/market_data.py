"""执行域纯函数行情读取接口。

契约规则：
1. ``bars`` 为长表（date / ticker / OHLCV），纯函数无 I/O、无模块状态、无缓存。
2. ``asof`` 严格按交易日等值匹配，绝不向历史最近日回退；停牌日返回 ``None``。
3. 缺失、NaN 或非正价格返回 ``None``（批量查询时忽略对应键），绝不伪造价格。
4. 返回未复权的原始可交易价格，``field`` 仅允许 ``"close"`` 或 ``"open"``。
5. ``get_daily_volume`` 返回当日成交股数（shares）；同 ``(ticker, date)`` 重复行抛出 ``ValueError``。
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Sequence

import pandas as pd

PRICE_FIELDS = ("close", "open")


def get_latest_price(
    bars: pd.DataFrame,
    ticker: str,
    asof: date | str,
    field: str = "close",
) -> float | None:
    """获取 ``ticker`` 在 ``asof`` 当日的原始 ``field`` 价格；停牌或无效价格返回 ``None``。"""
    _check_field(field)
    _require_columns(bars, ("date", "ticker", field))
    rows = _day_rows(bars, ticker, asof)
    if rows.empty:
        return None
    return _positive_float(rows.iloc[0][field])


def get_prices(
    bars: pd.DataFrame,
    tickers: Sequence[str],
    asof: date | str,
    field: str = "close",
) -> dict[str, float]:
    """批量获取 ``asof`` 当日有效价格映射，缺失或停牌标的自动省略（拒绝裸字符串）。"""
    if isinstance(tickers, (str, bytes)):
        raise TypeError(
            f"tickers must be a sequence of tickers, not a bare string — "
            f"got {tickers!r}"
        )
    out: dict[str, float] = {}
    for ticker in tickers:
        price = get_latest_price(bars, ticker, asof, field=field)
        if price is not None:
            out[ticker] = price
    return out


def get_daily_volume(
    bars: pd.DataFrame,
    ticker: str,
    asof: date | str,
) -> float | None:
    """获取 ``ticker`` 在 ``asof`` 当日的成交股数；缺失或无效返回 ``None``，零成交返回 ``0.0``。"""
    _require_columns(bars, ("date", "ticker", "volume"))
    rows = _day_rows(bars, ticker, asof)
    if rows.empty:
        return None
    raw = rows.iloc[0]["volume"]
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return value


# --- 内部工具函数 ---


def _check_field(field: str) -> None:
    if field not in PRICE_FIELDS:
        raise ValueError(f"field must be one of {PRICE_FIELDS}, got {field!r}")


def _require_columns(bars: pd.DataFrame, columns: Sequence[str]) -> None:
    if not isinstance(bars, pd.DataFrame):
        raise TypeError(f"bars must be a pandas DataFrame, got {type(bars).__name__}")
    missing = [c for c in columns if c not in bars.columns]
    if missing:
        raise ValueError(f"bars must carry columns {list(columns)}, missing {missing}")


def _as_date(asof: date | str) -> date:
    if isinstance(asof, datetime):
        return asof.date()
    if isinstance(asof, date):
        return asof
    if isinstance(asof, str):
        try:
            return pd.Timestamp(asof).date()
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"asof must be a date or date string, got {asof!r}"
            ) from exc
    raise ValueError(f"asof must be a date or date string, got {asof!r}")


def _day_rows(bars: pd.DataFrame, ticker: str, asof: date | str) -> pd.DataFrame:
    """精确匹配 ``(ticker, asof)`` 当日行情行，存在重复行时抛出 ``ValueError``。"""
    day = _as_date(asof)
    subset = bars.loc[bars["ticker"] == ticker]
    if subset.empty:
        return subset
    days = pd.to_datetime(subset["date"]).dt.date
    rows = subset.loc[days == day]
    if len(rows) > 1:
        raise ValueError(
            f"duplicate (ticker, date) rows for ({ticker!r}, {day.isoformat()}) "
            f"— a frame with two rows for one day has no single price"
        )
    return rows


def _positive_float(value: object) -> float | None:
    """仅返回严格为正且有限的浮点数（NaN 安全）。"""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not (number > 0) or not math.isfinite(number):
        return None
    return number
