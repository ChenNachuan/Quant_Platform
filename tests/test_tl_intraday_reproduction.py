import numpy as np
import pandas as pd

from engine.backtest.intraday_futures import run_intraday_target_backtest
from factor_library.futures.tl_intraday import (
    compute_adx,
    compute_daily_efficiency_ratio,
    compute_macd_turning_points,
)
from infra.futures_continuous import build_volume_dominant_series


def _bars(index, close):
    close = np.asarray(close, dtype=float)
    return pd.DataFrame(
        {
            "open": close,
            "high": close + 0.1,
            "low": close - 0.1,
            "close": close,
            "vwap": close,
            "volume": 100.0,
        },
        index=index,
    )


def test_indicators_are_aligned_and_finite_after_warmup():
    index = pd.date_range("2025-01-02 09:30", periods=120, freq="min")
    bars = _bars(
        index, 100 + np.linspace(0, 2, len(index)) + np.sin(np.arange(len(index)) / 4)
    )
    adx = compute_adx(bars)
    macd = compute_macd_turning_points(bars["close"])
    assert adx.index.equals(index)
    assert macd.index.equals(index)
    assert adx["adx"].dropna().between(0, 100).all()
    assert macd["turning_point_2"].dtype == bool


def test_daily_er_distinguishes_trend_from_round_trip():
    day_one = pd.date_range("2025-01-02 09:30", periods=5, freq="min")
    day_two = pd.date_range("2025-01-03 09:30", periods=5, freq="min")
    bars = _bars(day_one.append(day_two), [1, 2, 3, 4, 5, 1, 2, 1, 2, 1])
    er = compute_daily_efficiency_ratio(bars)
    assert er.iloc[0] == 1.0
    assert er.iloc[1] == 0.0


def test_momentum_peak_follows_three_rising_bars_and_one_decline():
    index = pd.date_range("2025-01-02 09:30", periods=200, freq="min")
    close = pd.Series(
        100 + np.sin(np.arange(len(index)) / 8) + np.arange(len(index)) / 500,
        index=index,
    )
    indicator = compute_macd_turning_points(
        close, fast=2, slow=3, signal=2, rising_minutes=3
    )
    assert indicator["turning_point_2"].any()


def test_dominant_roll_is_ratio_adjusted_without_jump():
    index = pd.to_datetime(
        [
            "2025-01-02 09:30",
            "2025-01-02 15:14",
            "2025-01-03 09:30",
            "2025-01-03 15:14",
        ]
    )
    rows = []
    for code, closes, volumes in [
        ("TL2503.CFE", [100, 101, 102, 103], [200, 200, 20, 20]),
        ("TL2506.CFE", [110, 111, 112, 113], [20, 20, 200, 200]),
    ]:
        for timestamp, close, volume in zip(index, closes, volumes):
            rows.append(
                {
                    "code": code,
                    "timestamp": timestamp,
                    "date": timestamp.date(),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "vwap": close,
                    "volume": volume,
                }
            )
    continuous = build_volume_dominant_series(pd.DataFrame(rows))
    first_day_close = continuous.loc["2025-01-02", "close"].iloc[-1]
    second_day_open = continuous.loc["2025-01-03", "open"].iloc[0]
    expected = 112 * (first_day_close / 111)
    assert np.isclose(second_day_open, expected)
    assert continuous.loc["2025-01-03", "dominant_code"].eq("TL2506.CFE").all()


def test_delayed_execution_charges_one_way_cost():
    index = pd.date_range("2025-01-02 09:30", periods=12, freq="min")
    bars = _bars(index, np.full(len(index), 100.0))
    target = pd.Series(0.0, index=index)
    target.iloc[0] = 1.0
    target.iloc[1:7] = 1.0
    target.iloc[7:] = 0.0
    result = run_intraday_target_backtest(
        bars, target, execution_bars=2, one_way_cost=0.001
    )
    assert len(result.executions) == 2
    assert result.daily_equity.iloc[-1] < 1.0


def test_execution_windows_do_not_overlap():
    index = pd.date_range("2025-01-02 09:30", periods=20, freq="min")
    bars = _bars(index, np.full(len(index), 100.0))
    target = pd.Series(np.where(np.arange(len(index)) % 2, 1.0, -1.0), index=index)
    result = run_intraday_target_backtest(
        bars, target, execution_bars=3, one_way_cost=0.0
    )
    execution_times = pd.to_datetime(result.executions["execution_time"])
    gaps = execution_times.diff().dropna().dt.total_seconds() / 60
    assert gaps.ge(4).all()
