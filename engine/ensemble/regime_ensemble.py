from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from engine.backtest.futures_backtest import StrategyRuleConfig


@dataclass
class EnsembleRegimeThresholdConfig:
    weak_open_multiplier: float = 1.5
    weak_close_multiplier: float = 1.0


@dataclass
class EnsembleStrictRegimeFilterConfig:
    allowed_vol_regime: str = "intraday_volatility_q3"
    allowed_volume_regime: str = "causal_volume_ratio_q3"
    regime_threshold_multipliers: dict[str, float] | None = None
    blocked_regime_patterns: tuple[str, ...] = ()
    exit_enhance_regime_patterns: tuple[str, ...] = (
        "intraday_volatility_q1|causal_volume_ratio_q1|*",
        "intraday_volatility_q1|causal_volume_ratio_q2|*",
        "*|causal_volume_ratio_q1|hurst_120_q3",
        "*|*|unknown",
    )
    hurst_threshold_multipliers: dict[str, float] | None = None
    unknown_hurst_multiplier: float = 1.6
    trailing_activation_pct: float = 0.0010
    trailing_stop_pct: float = 0.0005
    no_profit_exit_bars: int = 20
    no_profit_exit_min_return: float = 0.0
    adverse_score_exit: bool = True


def _datetime_series(obj, name: str) -> pd.Series:
    if isinstance(obj, pd.DataFrame):
        obj = obj.iloc[:, 0]
    out = pd.to_numeric(pd.Series(obj), errors="coerce").rename(name)
    if isinstance(out.index, pd.MultiIndex):
        names = list(out.index.names)
        level = "datetime" if "datetime" in names else names[0]
        out.index = pd.to_datetime(out.index.get_level_values(level), errors="coerce")
    else:
        out.index = pd.to_datetime(out.index, errors="coerce")
    return out.loc[out.index.notna()].sort_index().groupby(level=0).last()


def _datetime_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "datetime" in out.columns:
        out.index = pd.to_datetime(out["datetime"], errors="coerce")
    elif isinstance(out.index, pd.MultiIndex):
        names = list(out.index.names)
        level = "datetime" if "datetime" in names else names[0]
        out.index = pd.to_datetime(out.index.get_level_values(level), errors="coerce")
    else:
        out.index = pd.to_datetime(out.index, errors="coerce")
    return out.loc[out.index.notna()].sort_index().groupby(level=0).last()


def summarize_expert_regime_ic(
    signals: pd.DataFrame,
    label,
    regimes: pd.DataFrame,
    *,
    regime_cols: list[str],
    correlation_method: str = "spearman",
    min_samples: int = 30,
    min_daily_samples: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Summarize each expert and ensemble raw signal IC/ICIR by causal regime."""
    signal_frame = _datetime_frame(signals)
    label_ser = _datetime_series(label, "label")
    regime_frame = _datetime_frame(regimes)
    joined = signal_frame.join(label_ser, how="inner").join(
        regime_frame[[c for c in regime_cols if c in regime_frame.columns]], how="left"
    )
    rows = []
    daily_rows = []
    for regime_col in regime_cols:
        if regime_col not in joined.columns:
            continue
        for regime, group in joined.groupby(regime_col, dropna=False, observed=True):
            for signal_name in signal_frame.columns:
                pair = (
                    group[[signal_name, "label"]]
                    .replace([np.inf, -np.inf], np.nan)
                    .dropna()
                )
                if (
                    len(pair) < int(min_samples)
                    or pair[signal_name].nunique() < 2
                    or pair["label"].nunique() < 2
                ):
                    continue
                daily_ic = (
                    pair.groupby(pair.index.normalize())
                    .apply(
                        lambda x: (
                            x[signal_name].corr(x["label"], method=correlation_method)
                            if len(x) >= int(min_daily_samples)
                            and x[signal_name].nunique() > 1
                            and x["label"].nunique() > 1
                            else np.nan
                        )
                    )
                    .dropna()
                )
                daily_std = float(daily_ic.std(ddof=1)) if len(daily_ic) > 1 else np.nan
                daily_mean = float(daily_ic.mean()) if len(daily_ic) else np.nan
                daily_icir = (
                    daily_mean / daily_std * np.sqrt(len(daily_ic))
                    if np.isfinite(daily_std) and daily_std > 0
                    else np.nan
                )
                rows.append(
                    {
                        "regime_col": regime_col,
                        "regime": str(regime),
                        "signal": str(signal_name),
                        "samples": int(len(pair)),
                        "ic": float(
                            pair[signal_name].corr(pair["label"], method="pearson")
                        ),
                        "rank_ic": float(
                            pair[signal_name].corr(
                                pair["label"], method=correlation_method
                            )
                        ),
                        "daily_ic_mean": daily_mean,
                        "daily_ic_std": daily_std,
                        "daily_icir": float(daily_icir)
                        if np.isfinite(daily_icir)
                        else np.nan,
                        "n_days": int(len(daily_ic)),
                    }
                )
                for dt, value in daily_ic.items():
                    daily_rows.append(
                        {
                            "regime_col": regime_col,
                            "regime": str(regime),
                            "signal": str(signal_name),
                            "date": dt,
                            "daily_ic": float(value),
                        }
                    )
    summary = pd.DataFrame(rows)
    if not summary.empty:
        summary["rank_in_regime"] = summary.groupby(["regime_col", "regime"])[
            "daily_icir"
        ].rank(ascending=False, method="min")
    return summary, pd.DataFrame(daily_rows)


def run_dynamic_threshold_ensemble_backtest(
    *,
    raw_signal,
    market_regimes: pd.DataFrame,
    test_start: str,
    test_end: str,
    account: float,
    strategy_rule: StrategyRuleConfig,
    config: EnsembleRegimeThresholdConfig,
) -> tuple[pd.DataFrame, dict]:
    """Keep ensemble weights fixed and raise only new-entry thresholds in weak regimes."""
    signal = _datetime_series(raw_signal, "raw_signal")
    market = _datetime_frame(market_regimes).copy()
    end = pd.Timestamp(test_end)
    if end == end.normalize():
        end = end + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    market = market.loc[
        (market.index >= pd.Timestamp(test_start)) & (market.index <= end)
    ]
    market = market.join(signal, how="left")
    market["decision_signal"] = market["raw_signal"].shift(1)
    market["decision_weak_environment"] = market["weak_environment"].shift(1)

    multiplier = float(strategy_rule.contract_multiplier)
    lot = float(strategy_rule.fixed_trade_amount)
    force_dates = set()
    if strategy_rule.force_flatten_dates is not None:
        try:
            force_dates = {
                pd.Timestamp(x).normalize()
                for x in strategy_rule.force_flatten_dates
                if pd.notna(x)
            }
        except TypeError:
            force_dates = set()

    def is_trade_time(ts: pd.Timestamp) -> bool:
        if (
            ts.time() < strategy_rule.first_trade_time
            or ts.time() > strategy_rule.last_trade_time
        ):
            return False
        anchor = pd.Timestamp.combine(ts.date(), strategy_rule.first_trade_time)
        return (
            int((ts - anchor).total_seconds() // 60)
            % int(strategy_rule.trade_interval_minutes)
            == 0
        )

    def is_force_flatten(ts: pd.Timestamp) -> bool:
        if ts.time() < strategy_rule.force_flatten_exec_time:
            return False
        return bool(strategy_rule.force_flatten_daily) or ts.normalize() in force_dates

    def base_target(score: float, position: float) -> float:
        if score > float(strategy_rule.long_open_threshold):
            return lot
        if score < float(strategy_rule.short_open_threshold):
            return -lot
        if (
            float(strategy_rule.long_close_threshold)
            <= score
            <= float(strategy_rule.short_close_threshold)
        ):
            return 0.0
        return position

    def strict_entry(score: float, weak: bool) -> float:
        multiplier_ = float(config.weak_open_multiplier) if weak else 1.0
        long_threshold = float(strategy_rule.long_open_threshold) * multiplier_
        short_threshold = float(strategy_rule.short_open_threshold) * multiplier_
        if score > long_threshold:
            return lot
        if score < short_threshold:
            return -lot
        return 0.0

    def fee(current: float, target: float, price: float) -> float:
        if current * target >= 0:
            open_lots = max(abs(target) - abs(current), 0.0)
            close_lots = max(abs(current) - abs(target), 0.0)
        else:
            close_lots, open_lots = abs(current), abs(target)
        return float(
            (
                open_lots * float(strategy_rule.open_cost)
                + close_lots * float(strategy_rule.close_cost)
            )
            * multiplier
            * abs(price)
        )

    cash = float(account)
    position = 0.0
    trade_count = 0
    weak_open_filtered_count = 0
    records = []
    for ts, row in market.iterrows():
        price = float(row["price"])
        target = position
        reason = ""
        weak = (
            bool(row.get("decision_weak_environment", False))
            if pd.notna(row.get("decision_weak_environment"))
            else False
        )
        if is_force_flatten(ts) and abs(position) > 1e-12:
            target, reason = 0.0, "force_flatten"
        elif is_trade_time(ts) and pd.notna(row.get("decision_signal")):
            score = float(row["decision_signal"])
            normal_target = base_target(score, position)
            allowed_entry = strict_entry(score, weak)
            if abs(position) <= 1e-12:
                target = allowed_entry
                if weak and normal_target != 0 and allowed_entry == 0:
                    weak_open_filtered_count += 1
            elif normal_target == 0:
                target, reason = 0.0, "signal_close"
            elif np.sign(normal_target) != np.sign(position):
                # Closing is always allowed; opposite re-entry must pass the stricter entry threshold.
                target = allowed_entry if allowed_entry == normal_target else 0.0
                reason = "reverse" if target != 0 else "reverse_close_only"

        fee_cash = (
            fee(position, target, price) if abs(target - position) > 1e-12 else 0.0
        )
        if abs(target - position) > 1e-12:
            cash -= (target - position) * multiplier * price + fee_cash
            trade_count += 1
        position = target
        account_value = cash + position * multiplier * price
        records.append(
            {
                "datetime": ts,
                "return_with_cost": account_value / float(account),
                "account": account_value,
                "cash": cash,
                "position_lots": position,
                "position_value": position * multiplier * price,
                "price": price,
                "score": row.get("decision_signal"),
                "decision_weak_environment": weak,
                "exit_reason": reason,
            }
        )
    report = pd.DataFrame(records).set_index("datetime")
    return report, {
        "trade_count": int(trade_count),
        "weak_open_filtered_count": int(weak_open_filtered_count),
        "final_account": float(report["account"].iloc[-1]),
    }


def run_high_vol_volume_hurst_filter_backtest(
    *,
    raw_signal,
    market_regimes: pd.DataFrame,
    test_start: str,
    test_end: str,
    account: float,
    strategy_rule: StrategyRuleConfig,
    config: EnsembleStrictRegimeFilterConfig,
) -> tuple[pd.DataFrame, dict]:
    """Open only in high-vol/high-volume regimes, adjust entry thresholds by Hurst, and strengthen exits."""
    signal = _datetime_series(raw_signal, "raw_signal")
    market = _datetime_frame(market_regimes).copy()
    end = pd.Timestamp(test_end)
    if end == end.normalize():
        end = end + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    market = market.loc[
        (market.index >= pd.Timestamp(test_start)) & (market.index <= end)
    ]
    market = market.join(signal, how="left")
    market["decision_signal"] = market["raw_signal"].shift(1)
    for col in (
        "intraday_volatility_regime",
        "causal_volume_ratio_regime",
        "hurst_120_regime",
    ):
        if col not in market.columns:
            market[col] = np.nan
        market[f"decision_{col}"] = market[col].shift(1)

    multiplier = float(strategy_rule.contract_multiplier)
    lot = float(strategy_rule.fixed_trade_amount)
    force_dates = set()
    if strategy_rule.force_flatten_dates is not None:
        try:
            force_dates = {
                pd.Timestamp(x).normalize()
                for x in strategy_rule.force_flatten_dates
                if pd.notna(x)
            }
        except TypeError:
            force_dates = set()

    hurst_multipliers = {
        "hurst_120_q1": 1.4,
        "hurst_120_q2": 1.2,
        "hurst_120_q3": 1.0,
    }
    if config.hurst_threshold_multipliers:
        hurst_multipliers.update(
            {str(k): float(v) for k, v in config.hurst_threshold_multipliers.items()}
        )

    def is_trade_time(ts: pd.Timestamp) -> bool:
        if (
            ts.time() < strategy_rule.first_trade_time
            or ts.time() > strategy_rule.last_trade_time
        ):
            return False
        anchor = pd.Timestamp.combine(ts.date(), strategy_rule.first_trade_time)
        return (
            int((ts - anchor).total_seconds() // 60)
            % int(strategy_rule.trade_interval_minutes)
            == 0
        )

    def is_force_flatten(ts: pd.Timestamp) -> bool:
        if ts.time() < strategy_rule.force_flatten_exec_time:
            return False
        return bool(strategy_rule.force_flatten_daily) or ts.normalize() in force_dates

    def decision_regime_key(row) -> str:
        vol = str(row.get("decision_intraday_volatility_regime"))
        volume = str(row.get("decision_causal_volume_ratio_regime"))
        hurst = str(row.get("decision_hurst_120_regime"))
        if hurst in {"nan", "None", "<NA>"}:
            hurst = "unknown"
        return f"{vol}|{volume}|{hurst}"

    def match_pattern(key: str, pattern: str) -> bool:
        key_parts = key.split("|")
        pattern_parts = str(pattern).split("|")
        if len(key_parts) != len(pattern_parts):
            return False
        return all(p == "*" or p == k for k, p in zip(key_parts, pattern_parts))

    def high_vol_volume(row) -> bool:
        key = decision_regime_key(row)
        return key.split("|")[0] == str(config.allowed_vol_regime) and key.split("|")[
            1
        ] == str(config.allowed_volume_regime)

    def entry_threshold_multiplier(row) -> float | None:
        key = decision_regime_key(row)
        for pattern in config.blocked_regime_patterns:
            if match_pattern(key, pattern):
                return None
        if config.regime_threshold_multipliers:
            for pattern, multiplier_ in config.regime_threshold_multipliers.items():
                if match_pattern(key, pattern):
                    return float(multiplier_)
            return None
        if high_vol_volume(row):
            return hurst_multiplier(row)
        return None

    def hurst_multiplier(row) -> float:
        regime = str(row.get("decision_hurst_120_regime"))
        return float(hurst_multipliers.get(regime, config.unknown_hurst_multiplier))

    def base_target(score: float, position: float) -> float:
        if score > float(strategy_rule.long_open_threshold):
            return lot
        if score < float(strategy_rule.short_open_threshold):
            return -lot
        if (
            float(strategy_rule.long_close_threshold)
            <= score
            <= float(strategy_rule.short_close_threshold)
        ):
            return 0.0
        return position

    def strict_entry(score: float, row) -> float:
        threshold_multiplier = entry_threshold_multiplier(row)
        if threshold_multiplier is None:
            return 0.0
        long_threshold = float(strategy_rule.long_open_threshold) * threshold_multiplier
        short_threshold = (
            float(strategy_rule.short_open_threshold) * threshold_multiplier
        )
        if score > long_threshold:
            return lot
        if score < short_threshold:
            return -lot
        return 0.0

    def enhanced_exit(
        position: float,
        price: float,
        entry_price: float,
        favorable_price: float,
        score: float,
        row,
        bars: int,
    ):
        if abs(position) <= 1e-12 or not np.isfinite(entry_price) or entry_price <= 0:
            return False, ""
        key = decision_regime_key(row)
        weak_exit_env = any(
            match_pattern(key, pattern)
            for pattern in config.exit_enhance_regime_patterns
        )
        if not weak_exit_env:
            return False, ""

        direction = 1.0 if position > 0 else -1.0
        current_return = direction * (price / entry_price - 1.0)
        if config.adverse_score_exit and np.isfinite(score) and score * direction < 0:
            return True, "adverse_score_exit"

        if bars >= int(config.no_profit_exit_bars) and current_return <= float(
            config.no_profit_exit_min_return
        ):
            return True, "no_profit_exit"

        if np.isfinite(favorable_price) and favorable_price > 0:
            if position > 0:
                best_return = favorable_price / entry_price - 1.0
                giveback = price / favorable_price - 1.0
            else:
                best_return = entry_price / favorable_price - 1.0
                giveback = favorable_price / price - 1.0
            if best_return >= float(
                config.trailing_activation_pct
            ) and giveback <= -float(config.trailing_stop_pct):
                return True, "enhanced_trailing_stop"
        return False, ""

    def fee(current: float, target: float, price: float) -> float:
        if current * target >= 0:
            open_lots = max(abs(target) - abs(current), 0.0)
            close_lots = max(abs(current) - abs(target), 0.0)
        else:
            close_lots, open_lots = abs(current), abs(target)
        return float(
            (
                open_lots * float(strategy_rule.open_cost)
                + close_lots * float(strategy_rule.close_cost)
            )
            * multiplier
            * abs(price)
        )

    cash = float(account)
    position = 0.0
    entry_price = np.nan
    favorable_price = np.nan
    holding_bars = 0
    trade_count = 0
    open_filtered_count = 0
    hurst_threshold_filtered_count = 0
    enhanced_exit_count = 0
    records = []

    for ts, row in market.iterrows():
        price = float(row["price"])
        target = position
        reason = ""
        score = (
            float(row["decision_signal"])
            if pd.notna(row.get("decision_signal"))
            else np.nan
        )
        if abs(position) > 1e-12:
            holding_bars += 1
            if position > 0:
                favorable_price = (
                    price
                    if not np.isfinite(favorable_price)
                    else max(favorable_price, price)
                )
            else:
                favorable_price = (
                    price
                    if not np.isfinite(favorable_price)
                    else min(favorable_price, price)
                )

        if is_force_flatten(ts) and abs(position) > 1e-12:
            target, reason = 0.0, "force_flatten"
        elif is_trade_time(ts) and np.isfinite(score):
            normal_target = base_target(score, position)
            allowed_entry = strict_entry(score, row)
            if abs(position) <= 1e-12:
                target = allowed_entry
                if normal_target != 0 and allowed_entry == 0:
                    open_filtered_count += 1
                    if entry_threshold_multiplier(row) is not None:
                        hurst_threshold_filtered_count += 1
            elif normal_target == 0:
                target, reason = 0.0, "signal_close"
            elif np.sign(normal_target) != np.sign(position):
                target = allowed_entry if allowed_entry == normal_target else 0.0
                reason = "reverse" if target != 0 else "reverse_close_only"
                if target == 0 and allowed_entry == 0:
                    open_filtered_count += 1
            else:
                should_exit, exit_reason = enhanced_exit(
                    position,
                    price,
                    entry_price,
                    favorable_price,
                    score,
                    row,
                    holding_bars,
                )
                if should_exit:
                    target, reason = 0.0, exit_reason
                    enhanced_exit_count += 1

        fee_cash = (
            fee(position, target, price) if abs(target - position) > 1e-12 else 0.0
        )
        if abs(target - position) > 1e-12:
            cash -= (target - position) * multiplier * price + fee_cash
            trade_count += 1
            if abs(target) > 1e-12 and abs(position) <= 1e-12:
                entry_price = price
                favorable_price = price
                holding_bars = 0
            elif abs(target) > 1e-12 and np.sign(target) != np.sign(position):
                entry_price = price
                favorable_price = price
                holding_bars = 0
            elif abs(target) <= 1e-12:
                entry_price = np.nan
                favorable_price = np.nan
                holding_bars = 0
        position = target
        account_value = cash + position * multiplier * price
        records.append(
            {
                "datetime": ts,
                "return_with_cost": account_value / float(account),
                "account": account_value,
                "cash": cash,
                "position_lots": position,
                "position_value": position * multiplier * price,
                "price": price,
                "score": score,
                "decision_high_vol_volume": high_vol_volume(row),
                "decision_entry_allowed": entry_threshold_multiplier(row) is not None,
                "decision_regime_key": decision_regime_key(row),
                "decision_hurst_120_regime": row.get("decision_hurst_120_regime"),
                "exit_reason": reason,
            }
        )
    report = pd.DataFrame(records).set_index("datetime")
    return report, {
        "trade_count": int(trade_count),
        "open_filtered_count": int(open_filtered_count),
        "hurst_threshold_filtered_count": int(hurst_threshold_filtered_count),
        "enhanced_exit_count": int(enhanced_exit_count),
        "final_account": float(report["account"].iloc[-1]),
    }
