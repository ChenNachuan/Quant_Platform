"""TL 日内策略使用的通用趋势与动量指标。"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_adx(
    bars: pd.DataFrame,
    period: int = 14,
) -> pd.DataFrame:
    """Compute the report's rolling-ATR, exponentially smoothed DMI/ADX."""
    high = bars["high"].astype(float)
    low = bars["low"].astype(float)
    close = bars["close"].astype(float)

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    true_range = pd.concat(
        [
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    # The report explicitly uses a 14-minute rolling average for ATR and an
    # exponentially weighted average for directional movement and DX.  This
    # differs from Wilder's 1 / period smoothing used by many TA libraries.
    atr = true_range.rolling(period, min_periods=period).mean()
    plus_smoothed = plus_dm.ewm(span=period, adjust=False, min_periods=period).mean()
    minus_smoothed = minus_dm.ewm(span=period, adjust=False, min_periods=period).mean()
    plus_di = 100.0 * plus_smoothed / atr.replace(0.0, np.nan)
    minus_di = 100.0 * minus_smoothed / atr.replace(0.0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    adx = dx.ewm(span=period, adjust=False, min_periods=period).mean()
    return pd.DataFrame(
        {
            "plus_di": plus_di,
            "minus_di": minus_di,
            "atr": atr,
            "dx": dx,
            "adx": adx,
        },
        index=bars.index,
    )


def compute_macd_turning_points(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    rising_minutes: int = 3,
) -> pd.DataFrame:
    """Compute DIF/DEA/BAR and the two momentum turning points in the report."""
    close = close.astype(float)
    dif = (
        close.ewm(span=fast, adjust=False, min_periods=fast).mean()
        - close.ewm(span=slow, adjust=False, min_periods=slow).mean()
    )
    dea = dif.ewm(span=signal, adjust=False, min_periods=signal).mean()
    bar = 2.0 * (dif - dea)
    sign = np.sign(bar).replace(0.0, np.nan).ffill().fillna(0.0)
    turning_1 = sign.ne(sign.shift()) & sign.ne(0.0) & sign.shift().ne(0.0)

    magnitude = bar.abs()
    rising = pd.Series(True, index=close.index)
    for lag in range(rising_minutes, 1, -1):
        rising &= magnitude.shift(lag) < magnitude.shift(lag - 1)
    turning_2 = rising & magnitude.lt(magnitude.shift(1)) & sign.ne(0.0)
    return pd.DataFrame(
        {
            "dif": dif,
            "dea": dea,
            "bar": bar,
            "bar_sign": sign,
            "turning_point_1": turning_1.fillna(False),
            "turning_point_2": turning_2.fillna(False),
        },
        index=close.index,
    )


def compute_daily_efficiency_ratio(bars: pd.DataFrame) -> pd.Series:
    """Kaufman efficiency ratio computed from each day's minute close path."""
    dates = pd.Index(pd.to_datetime(bars.index).date, name="date")

    def _one_day(close: pd.Series) -> float:
        close = close.dropna().astype(float)
        if len(close) < 2:
            return np.nan
        path = close.diff().abs().sum()
        return float(abs(close.iloc[-1] - close.iloc[0]) / path) if path > 0 else 0.0

    return bars["close"].groupby(dates).apply(_one_day).rename("daily_er")


__all__ = [
    "compute_adx",
    "compute_daily_efficiency_ratio",
    "compute_macd_turning_points",
]
