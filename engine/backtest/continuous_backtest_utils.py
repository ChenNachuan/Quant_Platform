from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from engine.backtest.futures_backtest import StrategyRuleConfig, run_backtest


def period_end(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return (
        ts + pd.Timedelta(hours=23, minutes=59, seconds=59)
        if ts == ts.normalize()
        else ts
    )


def concat_datetime_series(
    series_list: Iterable[pd.Series], name: str = "signal"
) -> pd.Series:
    parts = []
    for ser in series_list:
        if ser is None or len(ser) == 0:
            continue
        s = pd.to_numeric(ser, errors="coerce").copy()
        s.index = pd.to_datetime(s.index, errors="coerce")
        s = s.loc[s.index.notna()].sort_index()
        if not s.empty:
            parts.append(s)
    if not parts:
        return pd.Series(dtype=float, name=name)
    out = pd.concat(parts).sort_index().groupby(level=0).last()
    return out.rename(name)


def continuous_price_series(
    frame: pd.DataFrame,
    *,
    start_time,
    end_time,
    price_col_candidates: list[str],
    datetime_col: str = "datetime",
) -> pd.Series:
    price_col = next((c for c in price_col_candidates if c in frame.columns), None)
    if price_col is None:
        raise KeyError(f"未找到价格列，候选={price_col_candidates}")
    if datetime_col in frame.columns:
        dt = pd.to_datetime(frame[datetime_col], errors="coerce")
    else:
        dt = pd.to_datetime(frame.index, errors="coerce")
    ser = pd.Series(pd.to_numeric(frame[price_col], errors="coerce").values, index=dt)
    ser = ser.loc[ser.index.notna()].sort_index().groupby(level=0).last()
    ser = ser.replace([np.inf, -np.inf], np.nan).dropna()
    ser = ser[
        (ser.index >= pd.Timestamp(start_time)) & (ser.index <= period_end(end_time))
    ]
    if ser.empty:
        raise ValueError(f"连续回测价格序列为空: {start_time} -> {end_time}")
    return ser


def run_continuous_signal_backtest(
    *,
    signal_parts: Iterable[pd.Series],
    price_series: pd.Series,
    test_start,
    end_time,
    account: float,
    strategy_rule: StrategyRuleConfig,
):
    signal = concat_datetime_series(signal_parts, name="signal")
    if signal.empty:
        raise ValueError("连续回测信号为空")
    return run_backtest(
        pred=signal,
        test_start=str(pd.Timestamp(test_start).date()),
        end_time=str(pd.Timestamp(end_time).date()),
        account=float(account),
        strategy_rule=strategy_rule,
        price_series=price_series,
    )


def account_metrics(
    account_curve: pd.Series, initial_account: float, periods_per_year: int = 240 * 252
) -> dict:
    curve = (
        pd.to_numeric(account_curve, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    if curve.empty:
        return {
            "samples": 0,
            "initial_account": float(initial_account),
            "final_account": np.nan,
            "absolute_pnl": np.nan,
            "total_return": np.nan,
            "sharpe_ratio": np.nan,
            "annualized_sharpe": np.nan,
            "max_drawdown": np.nan,
        }
    ret = curve.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    sharpe = ret.mean() / ret.std() if len(ret) > 1 and ret.std() > 0 else np.nan
    drawdown = curve / curve.cummax() - 1.0
    return {
        "samples": int(len(curve)),
        "initial_account": float(initial_account),
        "final_account": float(curve.iloc[-1]),
        "absolute_pnl": float(curve.iloc[-1] - initial_account),
        "total_return": float(curve.iloc[-1] / initial_account - 1.0),
        "sharpe_ratio": float(sharpe) if np.isfinite(sharpe) else np.nan,
        "annualized_sharpe": float(sharpe * np.sqrt(periods_per_year))
        if np.isfinite(sharpe)
        else np.nan,
        "max_drawdown": float(drawdown.min()) if len(drawdown) else np.nan,
    }
