from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass
class DiagnosticConfig:
    quantile_bins: int = 3
    min_group_samples: int = 30
    periods_per_year: int = 240 * 252
    correlation_method: str = "spearman"
    threshold_quantile: float = 0.80
    state_probability_quantile: float = 0.70
    hurst_windows: tuple[int, ...] = (60, 120, 240)


def _datetime_index(obj: pd.Series | pd.DataFrame) -> pd.Series | pd.DataFrame:
    out = obj.copy()
    if isinstance(out.index, pd.MultiIndex):
        names = list(out.index.names)
        level = "datetime" if "datetime" in names else names[0]
        out.index = pd.to_datetime(out.index.get_level_values(level), errors="coerce")
    else:
        out.index = pd.to_datetime(out.index, errors="coerce")
    out = out.loc[~out.index.isna()]
    return out.sort_index()


def _series(obj, name: str) -> pd.Series:
    if obj is None:
        return pd.Series(dtype=float, name=name)
    if isinstance(obj, pd.DataFrame):
        if obj.empty:
            return pd.Series(dtype=float, name=name)
        obj = obj.iloc[:, 0]
    return pd.to_numeric(_datetime_index(pd.Series(obj)), errors="coerce").rename(name)


def _frame(obj) -> pd.DataFrame:
    if obj is None:
        return pd.DataFrame()
    return _datetime_index(pd.DataFrame(obj))


def _quantile_bucket(values: pd.Series, bins: int, prefix: str) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce")
    valid = values.dropna()
    out = pd.Series(pd.NA, index=values.index, dtype="object")
    if valid.empty or valid.nunique() < 2:
        return out
    q = max(2, min(int(bins), int(valid.nunique())))
    labels = [f"{prefix}_q{i + 1}" for i in range(q)]
    try:
        out.loc[valid.index] = pd.qcut(
            valid.rank(method="first"), q=q, labels=labels
        ).astype(str)
    except ValueError:
        pass
    return out


def _period_end(value) -> pd.Timestamp:
    end = pd.Timestamp(value)
    return (
        end + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
        if end == end.normalize()
        else end
    )


def _fit_quantile_bucket(
    values: pd.Series,
    bins: int,
    prefix: str,
    fit_mask: pd.Series | None = None,
) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan)
    fit_values = (
        values.loc[fit_mask.reindex(values.index).fillna(False)]
        if fit_mask is not None
        else values
    )
    valid = fit_values.dropna()
    out = pd.Series(pd.NA, index=values.index, dtype="object")
    if valid.empty or valid.nunique() < 2:
        return out
    qs = np.linspace(0.0, 1.0, int(bins) + 1)[1:-1]
    boundaries = sorted(set(float(x) for x in valid.quantile(qs).dropna()))
    if not boundaries:
        return out
    cut_bins = [-np.inf, *boundaries, np.inf]
    labels = [f"{prefix}_q{i + 1}" for i in range(len(cut_bins) - 1)]
    out = pd.cut(values, bins=cut_bins, labels=labels).astype("object")
    return out


def _hurst_exponent(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = len(arr)
    if n < 30 or np.nanstd(arr) <= 0:
        return np.nan
    max_lag = min(20, n // 2)
    lags = np.array(
        [lag for lag in (2, 3, 4, 6, 8, 12, 16, 20) if lag <= max_lag], dtype=int
    )
    if len(lags) < 3:
        return np.nan
    tau = []
    valid_lags = []
    for lag in lags:
        diff = arr[lag:] - arr[:-lag]
        std = np.nanstd(diff)
        if np.isfinite(std) and std > 0:
            tau.append(std)
            valid_lags.append(lag)
    if len(tau) < 3:
        return np.nan
    slope = np.polyfit(np.log(valid_lags), np.log(tau), 1)[0]
    return float(np.clip(slope, 0.0, 1.0))


def _rolling_hurst(log_price: pd.Series, window: int) -> pd.Series:
    window = int(window)
    min_periods = max(30, window // 2)
    return log_price.rolling(window, min_periods=min_periods).apply(
        _hurst_exponent, raw=True
    )


def prepare_market_regimes(
    market_frame: pd.DataFrame,
    *,
    datetime_col: str = "datetime",
    price_col: str | None = None,
    volume_col: str | None = None,
    intraday_vol_window: int = 30,
    daily_vol_window: int = 20,
    daily_volume_window: int = 20,
    volume_history_days: int = 20,
    quantile_bins: int = 3,
    hurst_windows: Iterable[int] = (60, 120, 240),
    valid_start: str | pd.Timestamp | None = None,
    valid_end: str | pd.Timestamp | None = None,
    causal_volume: bool = True,
    fit_regime_on_valid: bool = True,
) -> pd.DataFrame:
    frame = market_frame.copy()
    if datetime_col in frame.columns:
        frame.index = pd.to_datetime(frame[datetime_col], errors="coerce")
    frame = _datetime_index(frame)

    def choose(candidates: Iterable[str]) -> str | None:
        return next((col for col in candidates if col in frame.columns), None)

    price_col = price_col or choose(("IF_vwap", "vwap", "IF_close", "close"))
    volume_col = volume_col or choose(
        ("IF_volume", "volume", "IF_total_turnover", "total_turnover")
    )
    if price_col is None:
        raise ValueError("market_frame 中找不到价格列；请传入 price_col")
    if volume_col is None:
        raise ValueError("market_frame 中找不到 volume/turnover 列；请传入 volume_col")

    price = pd.to_numeric(frame[price_col], errors="coerce")
    volume = pd.to_numeric(frame[volume_col], errors="coerce")
    log_price = np.log(price.replace(0.0, np.nan))
    trade_date = pd.Series(frame.index.normalize(), index=frame.index)
    minute_ret = price.groupby(trade_date).pct_change()
    intraday_vol = (
        minute_ret.groupby(trade_date)
        .rolling(
            int(intraday_vol_window), min_periods=max(5, int(intraday_vol_window) // 3)
        )
        .std()
        .reset_index(level=0, drop=True)
    )

    daily_close = price.groupby(trade_date).last()
    daily_ret = daily_close.pct_change()
    daily_vol = daily_ret.rolling(
        int(daily_vol_window), min_periods=max(5, int(daily_vol_window) // 3)
    ).std()
    daily_volume = volume.groupby(trade_date).sum()
    daily_volume_ratio = (
        daily_volume
        / daily_volume.shift(1)
        .rolling(
            int(daily_volume_window), min_periods=max(5, int(daily_volume_window) // 3)
        )
        .median()
    )

    minute_key = pd.Series(frame.index.strftime("%H:%M"), index=frame.index)
    if causal_volume:
        tmp = pd.DataFrame(
            {"volume": volume, "minute_key": minute_key}, index=frame.index
        )
        expected_volume = tmp.groupby("minute_key", group_keys=False)["volume"].apply(
            lambda s: (
                s.shift(1)
                .rolling(
                    int(volume_history_days),
                    min_periods=max(5, int(volume_history_days) // 3),
                )
                .median()
            )
        )
        intraday_volume_ratio = volume / expected_volume.replace(0.0, np.nan)
    else:
        intraday_median = (
            volume.groupby(trade_date).transform("median").replace(0.0, np.nan)
        )
        intraday_volume_ratio = volume / intraday_median

    out = pd.DataFrame(index=frame.index)
    out["price"] = price
    out["volume"] = volume
    out["intraday_volatility"] = intraday_vol
    out["daily_volatility"] = trade_date.map(daily_vol)
    out["daily_volume_ratio"] = trade_date.map(daily_volume_ratio)
    out["causal_volume_ratio"] = intraday_volume_ratio
    out["intraday_volume_ratio"] = out["causal_volume_ratio"]
    out["hour"] = out.index.hour
    out["time_bucket"] = pd.cut(
        out.index.hour * 60 + out.index.minute,
        bins=[0, 600, 690, 810, 900, 1440],
        labels=["open", "morning", "midday", "afternoon", "close"],
        include_lowest=True,
    ).astype("object")
    fit_mask = None
    if fit_regime_on_valid and valid_start is not None and valid_end is not None:
        fit_mask = pd.Series(
            (out.index >= pd.Timestamp(valid_start))
            & (out.index <= _period_end(valid_end)),
            index=out.index,
        )
    for col in (
        "intraday_volatility",
        "daily_volatility",
        "causal_volume_ratio",
        "daily_volume_ratio",
    ):
        out[f"{col}_regime"] = _fit_quantile_bucket(
            out[col], quantile_bins, col, fit_mask=fit_mask
        )
    out["intraday_volume_ratio_regime"] = out["causal_volume_ratio_regime"].str.replace(
        "causal_volume_ratio", "intraday_volume_ratio", regex=False
    )
    clean_hurst_windows = []
    for window in hurst_windows:
        try:
            window = int(window)
        except (TypeError, ValueError):
            continue
        if window <= 0 or window in clean_hurst_windows:
            continue
        clean_hurst_windows.append(window)
        col = f"hurst_{window}"
        out[col] = _rolling_hurst(log_price, window)
        out[f"{col}_regime"] = _fit_quantile_bucket(
            out[col], quantile_bins, col, fit_mask=fit_mask
        )
    out["vol_volume_regime"] = (
        out["intraday_volatility_regime"].fillna("unknown")
        + "|"
        + out["causal_volume_ratio_regime"].fillna("unknown")
    )
    out["causal_vol_volume_regime"] = (
        out["intraday_volatility_regime"].fillna("unknown")
        + "|"
        + out["causal_volume_ratio_regime"].fillna("unknown")
    )
    if "hurst_120_regime" in out.columns:
        out["vol_volume_hurst_regime"] = (
            out["causal_vol_volume_regime"].fillna("unknown").astype(str)
            + "|"
            + out["hurst_120_regime"].fillna("unknown").astype(str)
        )
        out["causal_vol_volume_hurst_regime"] = out["vol_volume_hurst_regime"]
    return out


def build_regression_diagnostic_frame(
    prediction,
    label,
    report_df: pd.DataFrame | None = None,
    market_regimes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    pred = _series(prediction, "prediction")
    y = _series(label, "label")
    out = pd.concat([pred, y], axis=1)
    out["predicted_direction"] = np.sign(out["prediction"])
    out["true_direction"] = np.sign(out["label"])
    out["direction_correct"] = out["predicted_direction"] == out["true_direction"]
    out["squared_error"] = (out["prediction"] - out["label"]) ** 2
    out["abs_prediction"] = out["prediction"].abs()
    return _attach_report_and_regimes(out, report_df, market_regimes)


def build_state_diagnostic_frame(
    state_prob: pd.DataFrame | None,
    state_prediction,
    state_label,
    report_df: pd.DataFrame | None = None,
    market_regimes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    prob = _frame(state_prob)
    pred = _series(state_prediction, "state_prediction")
    y = _series(state_label, "state_label")
    out = pd.concat([prob, pred, y], axis=1)
    if {"prob_up", "prob_down"}.issubset(out.columns):
        out["direction_score"] = pd.to_numeric(
            out["prob_up"], errors="coerce"
        ) - pd.to_numeric(out["prob_down"], errors="coerce")
        out["confidence"] = out[
            [c for c in ("prob_down", "prob_flat", "prob_up") if c in out]
        ].max(axis=1)
        inferred = out[
            [c for c in ("prob_down", "prob_flat", "prob_up") if c in out]
        ].idxmax(axis=1)
        inferred = inferred.map({"prob_down": 0.0, "prob_flat": 1.0, "prob_up": 2.0})
        out["state_prediction"] = out["state_prediction"].fillna(inferred)
    else:
        out["direction_score"] = out["state_prediction"].map({0: -1.0, 1: 0.0, 2: 1.0})
        out["confidence"] = np.nan
    out["state_correct"] = out["state_prediction"] == out["state_label"]
    out["true_trade"] = out["state_label"].isin([0.0, 2.0])
    out["pred_trade"] = out["state_prediction"].isin([0.0, 2.0])
    out["correct_trade"] = out["state_correct"] & out["pred_trade"]
    return _attach_report_and_regimes(out, report_df, market_regimes)


def _attach_report_and_regimes(
    frame: pd.DataFrame,
    report_df: pd.DataFrame | None,
    market_regimes: pd.DataFrame | None,
) -> pd.DataFrame:
    out = _datetime_index(frame)
    if report_df is not None and not report_df.empty:
        report = _datetime_index(report_df)
        cols = [
            c
            for c in ("account", "position_lots", "score", "price")
            if c in report.columns
        ]
        out = out.join(report[cols], how="left")
        if "account" in out:
            out["strategy_pnl"] = pd.to_numeric(out["account"], errors="coerce").diff()
            out["strategy_return"] = pd.to_numeric(
                out["account"], errors="coerce"
            ).pct_change()
        if "position_lots" in out:
            out["is_trade"] = (
                pd.to_numeric(out["position_lots"], errors="coerce")
                .diff()
                .abs()
                .fillna(0)
                > 0
            )
            out = _attach_completed_trade_metrics(out)
    if market_regimes is not None and not market_regimes.empty:
        out = out.join(_datetime_index(market_regimes), how="left", rsuffix="_market")
    out["date"] = out.index.normalize()
    out["month"] = out.index.to_period("M").astype(str)
    out["hour"] = out.index.hour
    return out


def _attach_completed_trade_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    """在平仓或反手分钟记录完整交易收益及其开仓时间。"""
    out = frame.copy()
    out["completed_trade_return"] = np.nan
    out["completed_trade_pnl"] = np.nan
    out["completed_trade_win"] = np.nan
    out["completed_trade_entry_time"] = pd.NaT

    if "account" not in out.columns or "position_lots" not in out.columns:
        return out

    account = pd.to_numeric(out["account"], errors="coerce")
    position = pd.to_numeric(out["position_lots"], errors="coerce").fillna(0.0)
    active_direction = 0.0
    entry_account = np.nan
    entry_time = pd.NaT

    for i, ts in enumerate(out.index):
        current_position = float(position.iloc[i])
        current_direction = float(np.sign(current_position))
        current_account = (
            float(account.iloc[i]) if pd.notna(account.iloc[i]) else np.nan
        )
        previous_account = (
            float(account.iloc[i - 1])
            if i > 0 and pd.notna(account.iloc[i - 1])
            else current_account
        )

        if active_direction == 0.0 and current_direction != 0.0:
            active_direction = current_direction
            entry_account = previous_account
            entry_time = ts
            continue

        closes_trade = active_direction != 0.0 and current_direction != active_direction
        if (
            closes_trade
            and np.isfinite(entry_account)
            and np.isfinite(current_account)
            and entry_account != 0
        ):
            trade_pnl = current_account - entry_account
            trade_return = current_account / entry_account - 1.0
            out.loc[ts, "completed_trade_pnl"] = trade_pnl
            out.loc[ts, "completed_trade_return"] = trade_return
            out.loc[ts, "completed_trade_win"] = float(trade_pnl > 0)
            out.loc[ts, "completed_trade_entry_time"] = entry_time

        if current_direction == 0.0:
            active_direction = 0.0
            entry_account = np.nan
            entry_time = pd.NaT
        elif current_direction != active_direction:
            active_direction = current_direction
            entry_account = current_account
            entry_time = ts

    return out


def _safe_corr(x: pd.Series, y: pd.Series, method: str) -> float:
    joined = pd.concat(
        [pd.to_numeric(x, errors="coerce"), pd.to_numeric(y, errors="coerce")], axis=1
    ).dropna()
    if (
        len(joined) < 3
        or joined.iloc[:, 0].nunique() < 2
        or joined.iloc[:, 1].nunique() < 2
    ):
        return np.nan
    return float(joined.iloc[:, 0].corr(joined.iloc[:, 1], method=method))


def _completed_trade_summary(
    returns: pd.Series,
    wins: pd.Series,
    prefix: str,
    pnls: pd.Series | None = None,
) -> dict[str, float | int]:
    completed_returns = (
        pd.to_numeric(returns, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    completed_wins = pd.to_numeric(wins, errors="coerce").dropna()
    out = {
        f"{prefix}_completed_trade_count": int(len(completed_returns)),
        f"{prefix}_avg_return_per_trade": float(completed_returns.mean())
        if not completed_returns.empty
        else np.nan,
        f"{prefix}_win_rate": float(completed_wins.mean())
        if not completed_wins.empty
        else np.nan,
    }
    if pnls is not None:
        completed_pnls = (
            pd.to_numeric(pnls, errors="coerce")
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )
        wins_pnl = completed_pnls.loc[completed_pnls > 0]
        losses_pnl = completed_pnls.loc[completed_pnls < 0]
        avg_profit = float(wins_pnl.mean()) if not wins_pnl.empty else np.nan
        avg_loss = float(losses_pnl.mean()) if not losses_pnl.empty else np.nan
        out.update(
            {
                f"{prefix}_avg_pnl_per_trade": float(completed_pnls.mean())
                if not completed_pnls.empty
                else np.nan,
                f"{prefix}_avg_profit": avg_profit,
                f"{prefix}_avg_loss": avg_loss,
                f"{prefix}_payoff_ratio": float(avg_profit / abs(avg_loss))
                if np.isfinite(avg_profit) and np.isfinite(avg_loss) and avg_loss != 0
                else np.nan,
            }
        )
    return out


def summarize_diagnostics(
    diagnostic_frame: pd.DataFrame,
    *,
    model_type: str,
    group_cols: Iterable[str],
    config: DiagnosticConfig | None = None,
) -> pd.DataFrame:
    config = config or DiagnosticConfig()
    rows = []
    for group_col in group_cols:
        if group_col not in diagnostic_frame.columns:
            continue
        entry_trade_metrics = {}
        decision_entry_trade_metrics = {}
        decision_exit_trade_metrics = {}
        if {
            "completed_trade_return",
            "completed_trade_win",
            "completed_trade_entry_time",
        }.issubset(diagnostic_frame.columns):
            completed_cols = [
                "completed_trade_return",
                "completed_trade_win",
                "completed_trade_entry_time",
            ]
            if "completed_trade_pnl" in diagnostic_frame.columns:
                completed_cols.append("completed_trade_pnl")
            completed = diagnostic_frame.loc[
                pd.to_numeric(
                    diagnostic_frame["completed_trade_return"], errors="coerce"
                ).notna(),
                completed_cols,
            ].copy()
            completed["completed_trade_entry_time"] = pd.to_datetime(
                completed["completed_trade_entry_time"], errors="coerce"
            )
            group_values_by_time = diagnostic_frame[group_col].groupby(level=0).last()
            completed["entry_group_value"] = group_values_by_time.reindex(
                completed["completed_trade_entry_time"]
            ).to_numpy()
            # 交易在 execution bar 发生，decision Regime 使用 execution bar 前一根可用 bar。
            decision_group_values_by_execution_time = group_values_by_time.shift(1)
            completed["decision_entry_group_value"] = (
                decision_group_values_by_execution_time.reindex(
                    completed["completed_trade_entry_time"]
                ).to_numpy()
            )
            completed["decision_exit_group_value"] = (
                decision_group_values_by_execution_time.reindex(
                    completed.index
                ).to_numpy()
            )
            for entry_group_value, entry_group in completed.groupby(
                "entry_group_value", dropna=False, observed=True
            ):
                entry_trade_metrics[str(entry_group_value)] = _completed_trade_summary(
                    entry_group["completed_trade_return"],
                    entry_group["completed_trade_win"],
                    "entry",
                    entry_group.get("completed_trade_pnl"),
                )
            for decision_entry_group_value, decision_entry_group in completed.groupby(
                "decision_entry_group_value", dropna=False, observed=True
            ):
                decision_entry_trade_metrics[str(decision_entry_group_value)] = (
                    _completed_trade_summary(
                        decision_entry_group["completed_trade_return"],
                        decision_entry_group["completed_trade_win"],
                        "decision_entry",
                        decision_entry_group.get("completed_trade_pnl"),
                    )
                )
            for decision_exit_group_value, decision_exit_group in completed.groupby(
                "decision_exit_group_value", dropna=False, observed=True
            ):
                decision_exit_trade_metrics[str(decision_exit_group_value)] = (
                    _completed_trade_summary(
                        decision_exit_group["completed_trade_return"],
                        decision_exit_group["completed_trade_win"],
                        "decision_exit",
                        decision_exit_group.get("completed_trade_pnl"),
                    )
                )
        for group_value, group in diagnostic_frame.groupby(
            group_col, dropna=False, observed=True
        ):
            if len(group) < int(config.min_group_samples):
                continue
            row = {
                "group_col": group_col,
                "group_value": str(group_value),
                "samples": int(len(group)),
            }
            if str(model_type).lower() == "regression":
                row.update(
                    {
                        "ic": _safe_corr(
                            group["prediction"], group["label"], "pearson"
                        ),
                        "rank_ic": _safe_corr(
                            group["prediction"],
                            group["label"],
                            config.correlation_method,
                        ),
                        "direction_accuracy": float(group["direction_correct"].mean()),
                        "mse": float(
                            pd.to_numeric(
                                group["squared_error"], errors="coerce"
                            ).mean()
                        ),
                        "mean_abs_prediction": float(
                            pd.to_numeric(
                                group["abs_prediction"], errors="coerce"
                            ).mean()
                        ),
                    }
                )
            else:
                pred_trade = group["pred_trade"].fillna(False).astype(bool)
                trade_precision = (
                    float(group.loc[pred_trade, "correct_trade"].mean())
                    if pred_trade.any()
                    else np.nan
                )
                row.update(
                    {
                        "trade_precision": trade_precision,
                        "pred_trade_ratio": float(pred_trade.mean()),
                        "mean_confidence": float(
                            pd.to_numeric(group["confidence"], errors="coerce").mean()
                        ),
                    }
                )
            if "strategy_return" in group:
                ret = (
                    pd.to_numeric(group["strategy_return"], errors="coerce")
                    .replace([np.inf, -np.inf], np.nan)
                    .dropna()
                )
                std = ret.std(ddof=1)
                sharpe = (
                    ret.mean() / std
                    if len(ret) > 1 and np.isfinite(std) and std > 0
                    else np.nan
                )
                row.update(
                    {
                        "strategy_pnl": float(
                            pd.to_numeric(
                                group.get("strategy_pnl"), errors="coerce"
                            ).sum()
                        ),
                        "strategy_sharpe": float(sharpe)
                        if np.isfinite(sharpe)
                        else np.nan,
                        "annualized_sharpe": float(
                            sharpe * np.sqrt(config.periods_per_year)
                        )
                        if np.isfinite(sharpe)
                        else np.nan,
                        "trade_count": int(
                            group.get(
                                "is_trade", pd.Series(False, index=group.index)
                            ).sum()
                        ),
                    }
                )
                trade_count = int(row["trade_count"])
                row["pnl_per_trade"] = (
                    row["strategy_pnl"] / trade_count if trade_count > 0 else np.nan
                )
                if "completed_trade_return" in group:
                    row.update(
                        _completed_trade_summary(
                            group["completed_trade_return"],
                            group["completed_trade_win"],
                            "exit",
                            group.get("completed_trade_pnl"),
                        )
                    )
                    row.update(
                        entry_trade_metrics.get(
                            str(group_value),
                            {
                                "entry_completed_trade_count": 0,
                                "entry_avg_return_per_trade": np.nan,
                                "entry_win_rate": np.nan,
                                "entry_avg_pnl_per_trade": np.nan,
                                "entry_avg_profit": np.nan,
                                "entry_avg_loss": np.nan,
                                "entry_payoff_ratio": np.nan,
                            },
                        )
                    )
                    row.update(
                        decision_entry_trade_metrics.get(
                            str(group_value),
                            {
                                "decision_entry_completed_trade_count": 0,
                                "decision_entry_avg_return_per_trade": np.nan,
                                "decision_entry_win_rate": np.nan,
                                "decision_entry_avg_pnl_per_trade": np.nan,
                                "decision_entry_avg_profit": np.nan,
                                "decision_entry_avg_loss": np.nan,
                                "decision_entry_payoff_ratio": np.nan,
                            },
                        )
                    )
                    row.update(
                        decision_exit_trade_metrics.get(
                            str(group_value),
                            {
                                "decision_exit_completed_trade_count": 0,
                                "decision_exit_avg_return_per_trade": np.nan,
                                "decision_exit_win_rate": np.nan,
                                "decision_exit_avg_pnl_per_trade": np.nan,
                                "decision_exit_avg_profit": np.nan,
                                "decision_exit_avg_loss": np.nan,
                                "decision_exit_payoff_ratio": np.nan,
                            },
                        )
                    )
            rows.append(row)
    return pd.DataFrame(rows)


def recommend_dynamic_thresholds(
    diagnostic_frame: pd.DataFrame,
    *,
    model_type: str,
    regime_cols: Iterable[str],
    config: DiagnosticConfig | None = None,
) -> pd.DataFrame:
    config = config or DiagnosticConfig()
    rows = []
    for regime_col in regime_cols:
        if regime_col not in diagnostic_frame.columns:
            continue
        for regime, group in diagnostic_frame.groupby(
            regime_col, dropna=False, observed=True
        ):
            if len(group) < int(config.min_group_samples):
                continue
            pnl = (
                float(pd.to_numeric(group.get("strategy_pnl"), errors="coerce").sum())
                if "strategy_pnl" in group
                else np.nan
            )
            if str(model_type).lower() == "regression":
                signal = pd.to_numeric(group["prediction"], errors="coerce").dropna()
                positive = signal[signal > 0]
                negative = signal[signal < 0]
                rows.append(
                    {
                        "regime_col": regime_col,
                        "regime": str(regime),
                        "samples": int(len(group)),
                        "long_open_threshold": float(
                            positive.quantile(config.threshold_quantile)
                        )
                        if not positive.empty
                        else np.nan,
                        "short_open_threshold": float(
                            negative.quantile(1 - config.threshold_quantile)
                        )
                        if not negative.empty
                        else np.nan,
                        "long_close_threshold": 0.0,
                        "short_close_threshold": 0.0,
                        "prediction_metric": _safe_corr(
                            group["prediction"],
                            group["label"],
                            config.correlation_method,
                        ),
                        "strategy_pnl": pnl,
                    }
                )
            else:
                up = pd.to_numeric(group.get("prob_up"), errors="coerce").dropna()
                down = pd.to_numeric(group.get("prob_down"), errors="coerce").dropna()
                pred_trade = group["pred_trade"].fillna(False).astype(bool)
                trade_precision = (
                    float(group.loc[pred_trade, "correct_trade"].mean())
                    if pred_trade.any()
                    else np.nan
                )
                rows.append(
                    {
                        "regime_col": regime_col,
                        "regime": str(regime),
                        "samples": int(len(group)),
                        "prob_up_threshold": float(
                            up.quantile(config.state_probability_quantile)
                        )
                        if not up.empty
                        else np.nan,
                        "prob_down_threshold": float(
                            down.quantile(config.state_probability_quantile)
                        )
                        if not down.empty
                        else np.nan,
                        "min_prob_margin": float(
                            pd.to_numeric(group["direction_score"], errors="coerce")
                            .abs()
                            .quantile(config.state_probability_quantile)
                        ),
                        "trade_precision": trade_precision,
                        "pred_trade_ratio": float(pred_trade.mean()),
                        "strategy_pnl": pnl,
                    }
                )
    return pd.DataFrame(rows)
