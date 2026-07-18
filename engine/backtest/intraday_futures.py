"""Small event-driven backtester for delayed intraday futures target signals."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IntradayBacktestResult:
    equity: pd.Series
    daily_equity: pd.Series
    daily_returns: pd.Series
    executions: pd.DataFrame
    metrics: dict[str, float]


def _schedule_executions(
    bars: pd.DataFrame,
    target: pd.Series,
    execution_bars: int,
) -> dict[int, list[tuple[float, float, pd.Timestamp]]]:
    target = target.reindex(bars.index).ffill().fillna(0.0).clip(-1.0, 1.0)
    dates = pd.Index(pd.to_datetime(bars.index).date)
    schedule: dict[int, list[tuple[float, float, pd.Timestamp]]] = {}
    signal_position = 0
    scheduled_target = 0.0
    while signal_position < len(bars):
        new_target = float(target.iloc[signal_position])
        if np.isclose(new_target, scheduled_target):
            signal_position += 1
            continue
        candidates = np.arange(
            signal_position + 1, signal_position + 1 + execution_bars
        )
        if len(candidates) != execution_bars or candidates[-1] >= len(bars):
            break
        if not np.all(dates[candidates] == dates[signal_position]):
            signal_position += 1
            continue
        price = float(bars["vwap"].iloc[candidates].mean())
        execute_position = int(candidates[-1])
        schedule.setdefault(execute_position, []).append(
            (new_target, price, bars.index[signal_position])
        )
        scheduled_target = new_target
        signal_position = execute_position + 1
    return schedule


def performance_metrics(
    daily_returns: pd.Series,
    executions: pd.DataFrame | None = None,
    annual_days: int = 242,
) -> dict[str, float]:
    returns = daily_returns.dropna().astype(float)
    if returns.empty:
        return {
            "annual_return": np.nan,
            "annual_volatility": np.nan,
            "sharpe": np.nan,
            "max_drawdown": np.nan,
            "calmar": np.nan,
            "win_rate": np.nan,
            "profit_loss_ratio": np.nan,
        }
    equity = (1.0 + returns).cumprod()
    years = max(len(returns) / annual_days, 1.0 / annual_days)
    annual_return = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    volatility = float(returns.std(ddof=1) * np.sqrt(annual_days))
    sharpe = (
        float(returns.mean() / returns.std(ddof=1) * np.sqrt(annual_days))
        if returns.std(ddof=1) > 0
        else np.nan
    )
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    calmar = annual_return / abs(max_drawdown) if max_drawdown < 0 else np.nan
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    win_rate = float((returns > 0).mean())
    profit_loss_ratio = (
        float(wins.mean() / abs(losses.mean()))
        if not wins.empty and not losses.empty
        else np.nan
    )
    return {
        "annual_return": annual_return,
        "annual_volatility": volatility,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "calmar": float(calmar),
        "win_rate": win_rate,
        "profit_loss_ratio": profit_loss_ratio,
        "turnover": float(executions["turnover"].sum())
        if executions is not None and not executions.empty
        else 0.0,
        "execution_count": int(len(executions)) if executions is not None else 0,
    }


def run_intraday_target_backtest(
    bars: pd.DataFrame,
    target: pd.Series,
    execution_bars: int = 5,
    one_way_cost: float = 0.000005,
    initial_equity: float = 1.0,
) -> IntradayBacktestResult:
    """Execute target changes at the mean VWAP of the following bars."""
    if bars.empty:
        raise ValueError("bars cannot be empty")
    bars = bars.sort_index()
    if not {"close", "vwap"}.issubset(bars.columns):
        raise ValueError("bars must contain close and vwap")
    schedule = _schedule_executions(bars, target, int(execution_bars))

    equity = float(initial_equity)
    units = 0.0
    exposure = 0.0
    last_price = float(bars["close"].iloc[0])
    equity_values: list[float] = []
    execution_rows: list[dict] = []

    for position, (timestamp, row) in enumerate(bars.iterrows()):
        close = float(row["close"])
        events = schedule.get(position, [])
        if not events:
            equity += units * (close - last_price)
        else:
            mark_price = last_price
            for new_target, execution_price, signal_time in events:
                equity += units * (execution_price - mark_price)
                old_units = units
                provisional_units = new_target * equity / execution_price
                turnover = abs(provisional_units - old_units) * execution_price
                cost = turnover * one_way_cost
                equity -= cost
                units = new_target * equity / execution_price
                execution_rows.append(
                    {
                        "signal_time": signal_time,
                        "execution_time": timestamp,
                        "execution_price": execution_price,
                        "old_target": exposure,
                        "new_target": new_target,
                        "turnover": turnover,
                        "cost": cost,
                        "equity_after_cost": equity,
                    }
                )
                exposure = new_target
                mark_price = execution_price
            equity += units * (close - mark_price)
        last_price = close
        equity_values.append(equity)

    equity_series = pd.Series(equity_values, index=bars.index, name="equity")
    daily_equity = equity_series.groupby(
        pd.to_datetime(equity_series.index).date
    ).last()
    daily_equity.index = pd.to_datetime(daily_equity.index)
    daily_returns = daily_equity.pct_change().fillna(
        daily_equity.iloc[0] / initial_equity - 1.0
    )
    executions = pd.DataFrame(execution_rows)
    metrics = performance_metrics(daily_returns, executions)
    return IntradayBacktestResult(
        equity=equity_series,
        daily_equity=daily_equity.rename("equity"),
        daily_returns=daily_returns.rename("return"),
        executions=executions,
        metrics=metrics,
    )


__all__ = [
    "IntradayBacktestResult",
    "performance_metrics",
    "run_intraday_target_backtest",
]
