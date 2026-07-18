"""Signal assembly and end-to-end backtest for the TL report reproduction."""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd

from engine.backtest.intraday_futures import (
    IntradayBacktestResult,
    performance_metrics,
    run_intraday_target_backtest,
)
from factor_library.futures.tl_intraday import (
    compute_adx,
    compute_daily_efficiency_ratio,
    compute_macd_turning_points,
)
from strategies.futures.tl_intraday.config import TLIntradayConfig


def _session_dates(index: pd.Index) -> pd.Index:
    return pd.Index(pd.to_datetime(index).date, name="date")


def _previous_day_close_quantiles(
    bars: pd.DataFrame,
    low_quantile: float,
    high_quantile: float,
) -> tuple[pd.Series, pd.Series]:
    dates = _session_dates(bars.index)
    daily = (
        bars["close"]
        .groupby(dates)
        .agg(
            low=lambda values: values.quantile(low_quantile),
            high=lambda values: values.quantile(high_quantile),
        )
    )
    previous = daily.shift(1)
    date_series = pd.Series(dates, index=bars.index)
    low = date_series.map(previous["low"]).astype(float)
    high = date_series.map(previous["high"]).astype(float)
    return low, high


def build_adx_target(bars: pd.DataFrame, config: TLIntradayConfig) -> pd.Series:
    indicator = compute_adx(
        bars, period=config.minute_window(config.adx_period_minutes)
    )
    previous_low, previous_high = _previous_day_close_quantiles(
        bars,
        config.previous_day_low_quantile,
        config.previous_day_high_quantile,
    )
    entry_cross = indicator["adx"].shift(1).lt(config.adx_entry_threshold) & indicator[
        "adx"
    ].ge(config.adx_entry_threshold)
    exit_cross = indicator["adx"].shift(1).gt(config.adx_exit_threshold) & indicator[
        "adx"
    ].le(config.adx_exit_threshold)
    extreme = bars["close"].gt(previous_high) | bars["close"].lt(previous_low)
    force_flat = pd.to_datetime(bars.index).strftime("%H:%M") >= config.force_flat_time

    position = 0.0
    values: list[float] = []
    for row in range(len(bars)):
        if (
            force_flat[row]
            or exit_cross.iloc[row]
            or (position != 0 and extreme.iloc[row])
        ):
            position = 0.0
        elif position == 0.0 and entry_cross.iloc[row]:
            position = (
                1.0
                if indicator["plus_di"].iloc[row] > indicator["minus_di"].iloc[row]
                else -1.0
            )
        values.append(position)
    return pd.Series(values, index=bars.index, name="adx_target")


def build_momentum_target(
    bars: pd.DataFrame,
    config: TLIntradayConfig,
) -> pd.Series:
    indicator = compute_macd_turning_points(
        bars["close"],
        fast=config.minute_window(config.macd_fast_minutes),
        slow=config.minute_window(config.macd_slow_minutes),
        signal=config.minute_window(config.macd_signal_minutes),
        rising_minutes=config.minute_window(config.momentum_rising_minutes),
    )
    force_flat = pd.to_datetime(bars.index).strftime("%H:%M") >= config.force_flat_time
    position = 0.0
    regime_sign = 0.0
    peak_count = 0
    values: list[float] = []

    for row in range(len(bars)):
        sign = float(indicator["bar_sign"].iloc[row])
        if sign != 0.0 and sign != regime_sign:
            regime_sign = sign
            peak_count = 0
        if force_flat[row] or indicator["turning_point_1"].iloc[row]:
            position = 0.0
        elif indicator["turning_point_2"].iloc[row]:
            peak_count += 1
            if peak_count == 1:
                position = sign
            elif np.sign(position) == sign and position != 0.0:
                reduced = max(0.0, abs(position) - config.momentum_reduction)
                position = float(np.sign(position) * reduced)
        values.append(position)
    return pd.Series(values, index=bars.index, name="momentum_target")


def _block_target_dates(
    target: pd.Series,
    blocked_dates: set,
) -> pd.Series:
    dates = _session_dates(target.index)
    blocked = dates.isin(blocked_dates)
    result = target.copy()
    result.loc[blocked] = 0.0
    return result


def adx_stop_dates(
    base_result: IntradayBacktestResult,
    config: TLIntradayConfig,
) -> tuple[set, float]:
    returns = base_result.daily_returns
    in_sample = returns.loc[: config.in_sample_end]
    threshold = float(in_sample.quantile(config.adx_stop_quantile))
    trading_dates = list(returns.index.date)
    blocked: set = set()
    for trigger in returns[returns < threshold].index.date:
        location = trading_dates.index(trigger)
        blocked.update(
            trading_dates[location + 1 : location + 1 + config.adx_stop_days]
        )
    return blocked, threshold


def momentum_stop_dates(
    bars: pd.DataFrame,
    config: TLIntradayConfig,
) -> tuple[set, float, pd.DataFrame]:
    daily_er = compute_daily_efficiency_ratio(bars)
    er_mean = daily_er.rolling(
        config.er_mean_days, min_periods=config.er_mean_days
    ).mean()
    in_sample = er_mean.loc[: pd.Timestamp(config.in_sample_end).date()]
    threshold = float(in_sample.quantile(config.er_stop_quantile))
    diagnostic = pd.concat([daily_er, er_mean.rename("er_mean")], axis=1)
    previous_er_mean = er_mean.shift(1)
    blocked = set(previous_er_mean[previous_er_mean < threshold].index)
    return blocked, threshold, diagnostic


def run_report_strategies(
    bars: pd.DataFrame,
    config: TLIntradayConfig,
) -> dict[str, object]:
    adx_target = build_adx_target(bars, config)
    momentum_target = build_momentum_target(bars, config)
    common_kwargs = {
        "execution_bars": config.execution_bars,
        "one_way_cost": config.one_way_cost,
    }
    adx_base = run_intraday_target_backtest(bars, adx_target, **common_kwargs)
    momentum_base = run_intraday_target_backtest(bars, momentum_target, **common_kwargs)

    blocked_adx, adx_threshold = adx_stop_dates(adx_base, config)
    blocked_momentum, er_threshold, er_diagnostic = momentum_stop_dates(bars, config)
    adx_stopped_target = _block_target_dates(adx_target, blocked_adx)
    momentum_stopped_target = _block_target_dates(momentum_target, blocked_momentum)
    adx_stopped = run_intraday_target_backtest(
        bars, adx_stopped_target, **common_kwargs
    )
    momentum_stopped = run_intraday_target_backtest(
        bars, momentum_stopped_target, **common_kwargs
    )

    combined_returns = (
        pd.concat([adx_stopped.daily_returns, momentum_stopped.daily_returns], axis=1)
        .fillna(0.0)
        .mean(axis=1)
    )
    combined_equity = (1.0 + combined_returns).cumprod().rename("composite")
    composite_metrics = performance_metrics(combined_returns)

    return {
        "adx_target": adx_target,
        "momentum_target": momentum_target,
        "adx_base": adx_base,
        "momentum_base": momentum_base,
        "adx_stopped": adx_stopped,
        "momentum_stopped": momentum_stopped,
        "composite_daily_returns": combined_returns.rename("return"),
        "composite_equity": combined_equity,
        "composite_metrics": composite_metrics,
        "adx_stop_threshold": adx_threshold,
        "er_stop_threshold": er_threshold,
        "er_diagnostic": er_diagnostic,
        "blocked_adx_dates": sorted(blocked_adx),
        "blocked_momentum_dates": sorted(blocked_momentum),
    }


def with_source_frequency(
    config: TLIntradayConfig,
    source_bar_minutes: int,
) -> TLIntradayConfig:
    return replace(config, source_bar_minutes=int(source_bar_minutes))


__all__ = [
    "adx_stop_dates",
    "build_adx_target",
    "build_momentum_target",
    "momentum_stop_dates",
    "run_report_strategies",
    "with_source_frequency",
]
