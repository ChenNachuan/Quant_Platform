from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from engine.backtest.futures_backtest import StrategyRuleConfig


@dataclass
class RegimeFilterConfig:
    vol_window: int = 30
    volume_history_days: int = 20
    quantile_bins: int = 3
    base_prob_threshold: float = 0.35
    base_min_margin: float = 0.01
    weak_prob_threshold: float = 0.43
    weak_min_margin: float = 0.05
    weak_trailing_stop_pct: float = 0.0006
    weak_trailing_activation_pct: float = 0.0012
    weak_rule: str = (
        "or"  # or / bad_combo / low_vol_and_low_volume / mid_vol_and_low_volume
    )
    weak_exit_min_direction_margin: float = 0.03
    weak_no_profit_exit_bars: int = 20
    weak_reg_open_threshold_multiplier: float = 1.5
    allow_open_rule: str = "all"  # all / high_vol_and_high_volume / not_low_vol_and_low_volume / not_predicted_weak_vol / predicted_high_vol
    allow_open_vol_regime: str = "intraday_volatility_q3"
    allow_open_volume_regime: str = "causal_volume_ratio_q3"
    future_vol_prob_col: str = "prob_vol_high"
    future_vol_state_col: str = "pred_vol_state_name"
    future_vol_open_prob_threshold: float = 0.55
    future_vol_weak_prob_threshold: float = 0.45
    future_vol_high_state: str = "high"
    future_vol_low_state: str = "low"
    use_future_vol_as_weak: bool = False
    future_vol_weak_mode: str = "weak"  # weak / mid


def _datetime_frame(
    frame: pd.DataFrame, datetime_col: str = "datetime"
) -> pd.DataFrame:
    out = frame.copy()
    if datetime_col in out.columns:
        out.index = pd.to_datetime(out[datetime_col], errors="coerce")
    else:
        out.index = pd.to_datetime(out.index, errors="coerce")
    return out.loc[out.index.notna()].sort_index()


def _period_end(value) -> pd.Timestamp:
    end = pd.Timestamp(value)
    return (
        end + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
        if end == end.normalize()
        else end
    )


def prepare_causal_regimes(
    market_frame: pd.DataFrame,
    *,
    price_col: str,
    volume_col: str,
    valid_start: str,
    valid_end: str,
    config: RegimeFilterConfig,
) -> tuple[pd.DataFrame, dict[str, list[float]]]:
    """Build causal intraday metrics and fit regime boundaries on valid only."""
    frame = _datetime_frame(market_frame)
    price = pd.to_numeric(frame[price_col], errors="coerce")
    volume = pd.to_numeric(frame[volume_col], errors="coerce")
    trade_date = pd.Series(frame.index.normalize(), index=frame.index)

    minute_ret = price.groupby(trade_date).pct_change()
    intraday_vol = (
        minute_ret.groupby(trade_date)
        .rolling(
            int(config.vol_window), min_periods=max(5, int(config.vol_window) // 3)
        )
        .std()
        .reset_index(level=0, drop=True)
    )

    minute_key = pd.Series(frame.index.strftime("%H:%M"), index=frame.index)
    tmp = pd.DataFrame({"volume": volume, "minute_key": minute_key}, index=frame.index)
    expected_volume = tmp.groupby("minute_key", group_keys=False)["volume"].apply(
        lambda s: (
            s.shift(1)
            .rolling(
                int(config.volume_history_days),
                min_periods=max(5, int(config.volume_history_days) // 3),
            )
            .median()
        )
    )
    volume_ratio = volume / expected_volume.replace(0.0, np.nan)

    out = pd.DataFrame(
        {
            "price": price,
            "volume": volume,
            "intraday_volatility": intraday_vol,
            "causal_volume_ratio": volume_ratio,
        },
        index=frame.index,
    )

    valid_mask = (out.index >= pd.Timestamp(valid_start)) & (
        out.index <= _period_end(valid_end)
    )
    boundaries: dict[str, list[float]] = {}
    for col in ("intraday_volatility", "causal_volume_ratio"):
        valid_values = (
            out.loc[valid_mask, col].replace([np.inf, -np.inf], np.nan).dropna()
        )
        qs = np.linspace(0.0, 1.0, int(config.quantile_bins) + 1)[1:-1]
        boundaries[col] = sorted(
            set(float(x) for x in valid_values.quantile(qs).dropna())
        )
        bins = [-np.inf, *boundaries[col], np.inf]
        labels = [f"{col}_q{i + 1}" for i in range(len(bins) - 1)]
        out[f"{col}_regime"] = pd.cut(out[col], bins=bins, labels=labels).astype(
            "object"
        )

    vol_q1 = out["intraday_volatility_regime"].eq("intraday_volatility_q1")
    vol_q2 = out["intraday_volatility_regime"].eq("intraday_volatility_q2")
    volume_q1 = out["causal_volume_ratio_regime"].eq("causal_volume_ratio_q1")
    volume_q2 = out["causal_volume_ratio_regime"].eq("causal_volume_ratio_q2")
    weak_rule = str(getattr(config, "weak_rule", "or")).lower()
    if weak_rule == "bad_combo":
        out["weak_environment"] = (vol_q1 & (volume_q1 | volume_q2)) | (
            vol_q2 & volume_q1
        )
    elif weak_rule == "low_vol_and_low_volume":
        out["weak_environment"] = vol_q1 & volume_q1
    elif weak_rule == "mid_vol_and_low_volume":
        out["weak_environment"] = vol_q2 & volume_q1
    else:
        out["weak_environment"] = vol_q1 | volume_q1
    out["causal_vol_volume_regime"] = (
        out["intraday_volatility_regime"].fillna("unknown").astype(str)
        + "|"
        + out["causal_volume_ratio_regime"].fillna("unknown").astype(str)
    )
    return out, boundaries


def _probability_direction(
    p_down: float, p_up: float, threshold: float, margin: float
) -> float:
    long_ok = p_up >= threshold and p_up - p_down >= margin
    short_ok = p_down >= threshold and p_down - p_up >= margin
    if long_ok and not short_ok:
        return 1.0
    if short_ok and not long_ok:
        return -1.0
    if long_ok and short_ok:
        return 1.0 if p_up >= p_down else -1.0
    return 0.0


def _normalize_force_dates(values) -> set[pd.Timestamp]:
    if values is None:
        return set()
    if isinstance(values, (str, pd.Timestamp)):
        values = [values]
    try:
        return {pd.Timestamp(x).normalize() for x in values if pd.notna(x)}
    except TypeError:
        return set()


def run_regime_filtered_state_backtest(
    *,
    state_prob: pd.DataFrame,
    market_regimes: pd.DataFrame,
    test_start: str,
    test_end: str,
    account: float,
    strategy_rule: StrategyRuleConfig,
    config: RegimeFilterConfig,
) -> tuple[pd.DataFrame, dict]:
    """Run dynamic-entry-threshold and weak-regime trailing-stop state backtest."""
    prob = state_prob.copy()
    if isinstance(prob.index, pd.MultiIndex):
        names = list(prob.index.names)
        dt_level = "datetime" if "datetime" in names else names[0]
        if "instrument" in names:
            try:
                prob = prob.xs(strategy_rule.instrument, level="instrument")
            except KeyError:
                prob.index = prob.index.get_level_values(dt_level)
        else:
            prob.index = prob.index.get_level_values(dt_level)
    prob.index = pd.to_datetime(prob.index, errors="coerce")
    prob = prob.loc[prob.index.notna()].sort_index().groupby(level=0).last()

    market = _datetime_frame(market_regimes)
    end = _period_end(test_end)
    market = market.loc[
        (market.index >= pd.Timestamp(test_start)) & (market.index <= end)
    ].copy()
    market = market.join(prob[["prob_down", "prob_up"]], how="left")
    for col in (
        "prob_down",
        "prob_up",
        "weak_environment",
        "intraday_volatility_regime",
        "causal_volume_ratio_regime",
        "causal_vol_volume_regime",
        str(getattr(config, "future_vol_prob_col", "prob_vol_high")),
        str(getattr(config, "future_vol_state_col", "pred_vol_state_name")),
    ):
        if col in market.columns:
            market[f"decision_{col}"] = market[col].shift(1)

    multiplier = float(strategy_rule.contract_multiplier)
    lot = float(strategy_rule.fixed_trade_amount)
    force_dates = _normalize_force_dates(strategy_rule.force_flatten_dates)

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

    def force_flatten(ts: pd.Timestamp) -> bool:
        if ts.time() < strategy_rule.force_flatten_exec_time:
            return False
        return bool(strategy_rule.force_flatten_daily) or ts.normalize() in force_dates

    def open_allowed(row) -> bool:
        rule = str(getattr(config, "allow_open_rule", "all")).lower()
        if rule in {"all", "none", "", "false"}:
            return True
        if rule == "high_vol_and_high_volume":
            return str(row.get("decision_intraday_volatility_regime")) == str(
                config.allow_open_vol_regime
            ) and str(row.get("decision_causal_volume_ratio_regime")) == str(
                config.allow_open_volume_regime
            )
        if rule == "not_low_vol_and_low_volume":
            return not (
                str(row.get("decision_intraday_volatility_regime"))
                == "intraday_volatility_q1"
                and str(row.get("decision_causal_volume_ratio_regime"))
                == "causal_volume_ratio_q1"
            )
        if rule == "predicted_high_vol":
            prob_col = (
                f"decision_{getattr(config, 'future_vol_prob_col', 'prob_vol_high')}"
            )
            state_col = f"decision_{getattr(config, 'future_vol_state_col', 'pred_vol_state_name')}"
            prob = row.get(prob_col)
            state = row.get(state_col)
            prob_ok = pd.notna(prob) and float(prob) >= float(
                config.future_vol_open_prob_threshold
            )
            state_ok = pd.isna(state) or str(state) == str(config.future_vol_high_state)
            return bool(prob_ok and state_ok)
        if rule == "not_predicted_weak_vol":
            prob_col = (
                f"decision_{getattr(config, 'future_vol_prob_col', 'prob_vol_high')}"
            )
            state_col = f"decision_{getattr(config, 'future_vol_state_col', 'pred_vol_state_name')}"
            prob = row.get(prob_col)
            state = row.get(state_col)
            weak = (
                pd.notna(prob)
                and float(prob) < float(config.future_vol_weak_prob_threshold)
            ) or (pd.notna(state) and str(state) == str(config.future_vol_low_state))
            return not bool(weak)
        raise ValueError(
            "allow_open_rule 仅支持 all / high_vol_and_high_volume / not_low_vol_and_low_volume / not_predicted_weak_vol / predicted_high_vol"
        )

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
    trailing_exit_count = 0
    weak_prob_decay_exit_count = 0
    weak_no_profit_exit_count = 0
    weak_open_filtered_count = 0
    records = []

    for ts, row in market.iterrows():
        price = float(row["price"])
        target = position
        exit_reason = ""
        base_direction = 0.0
        strict_direction = 0.0
        decision_weak = (
            bool(row.get("decision_weak_environment", False))
            if pd.notna(row.get("decision_weak_environment"))
            else False
        )
        if bool(getattr(config, "use_future_vol_as_weak", False)):
            prob_col = (
                f"decision_{getattr(config, 'future_vol_prob_col', 'prob_vol_high')}"
            )
            state_col = f"decision_{getattr(config, 'future_vol_state_col', 'pred_vol_state_name')}"
            prob = row.get(prob_col)
            state = row.get(state_col)
            mode = str(getattr(config, "future_vol_weak_mode", "weak")).lower()
            if mode == "mid":
                strong = (
                    pd.notna(prob)
                    and float(prob) >= float(config.future_vol_open_prob_threshold)
                ) and (
                    pd.isna(state) or str(state) == str(config.future_vol_high_state)
                )
                weak = (
                    pd.notna(prob)
                    and float(prob) < float(config.future_vol_weak_prob_threshold)
                ) or (
                    pd.notna(state) and str(state) == str(config.future_vol_low_state)
                )
                pred_weak = (not strong) and (not weak)
            else:
                pred_weak = (
                    pd.notna(prob)
                    and float(prob) < float(config.future_vol_weak_prob_threshold)
                ) or (
                    pd.notna(state) and str(state) == str(config.future_vol_low_state)
                )
            decision_weak = bool(decision_weak or pred_weak)
        decision_open_allowed = open_allowed(row)
        if abs(position) > 1e-12:
            holding_bars += 1
        else:
            holding_bars = 0

        if position > 0:
            favorable_price = (
                price
                if not np.isfinite(favorable_price)
                else max(favorable_price, price)
            )
        elif position < 0:
            favorable_price = (
                price
                if not np.isfinite(favorable_price)
                else min(favorable_price, price)
            )

        weak_trailing_exit = False
        if (
            abs(position) > 1e-12
            and decision_weak
            and np.isfinite(favorable_price)
            and np.isfinite(entry_price)
        ):
            trail = float(config.weak_trailing_stop_pct)
            activation = float(getattr(config, "weak_trailing_activation_pct", 0.0))
            activated = (
                position > 0 and favorable_price >= entry_price * (1.0 + activation)
            ) or (position < 0 and favorable_price <= entry_price * (1.0 - activation))
            weak_trailing_exit = (
                activated and position > 0 and price <= favorable_price * (1.0 - trail)
            ) or (
                activated and position < 0 and price >= favorable_price * (1.0 + trail)
            )

        weak_prob_decay_exit = False
        weak_no_profit_exit = False
        if (
            abs(position) > 1e-12
            and decision_weak
            and pd.notna(row.get("decision_prob_down"))
            and pd.notna(row.get("decision_prob_up"))
        ):
            p_down, p_up = (
                float(row["decision_prob_down"]),
                float(row["decision_prob_up"]),
            )
            direction_margin = (p_up - p_down) if position > 0 else (p_down - p_up)
            weak_prob_decay_exit = direction_margin < float(
                config.weak_exit_min_direction_margin
            )
        if (
            abs(position) > 1e-12
            and decision_weak
            and np.isfinite(entry_price)
            and holding_bars >= int(config.weak_no_profit_exit_bars)
        ):
            unrealized_return = (price / entry_price - 1.0) * np.sign(position)
            weak_no_profit_exit = unrealized_return <= 0.0

        if force_flatten(ts) and abs(position) > 1e-12:
            target, exit_reason = 0.0, "force_flatten"
        elif weak_trailing_exit:
            target, exit_reason = 0.0, "weak_trailing_stop"
            trailing_exit_count += 1
        elif weak_prob_decay_exit:
            target, exit_reason = 0.0, "weak_prob_decay_exit"
            weak_prob_decay_exit_count += 1
        elif weak_no_profit_exit:
            target, exit_reason = 0.0, "weak_no_profit_timeout"
            weak_no_profit_exit_count += 1
        elif (
            is_trade_time(ts)
            and pd.notna(row.get("decision_prob_down"))
            and pd.notna(row.get("decision_prob_up"))
        ):
            p_down, p_up = (
                float(row["decision_prob_down"]),
                float(row["decision_prob_up"]),
            )
            base_direction = _probability_direction(
                p_down,
                p_up,
                float(config.base_prob_threshold),
                float(config.base_min_margin),
            )
            threshold = (
                float(config.weak_prob_threshold)
                if decision_weak
                else float(config.base_prob_threshold)
            )
            margin = (
                float(config.weak_min_margin)
                if decision_weak
                else float(config.base_min_margin)
            )
            strict_direction = _probability_direction(p_down, p_up, threshold, margin)

            if abs(position) <= 1e-12:
                target = strict_direction * lot if decision_open_allowed else 0.0
                if base_direction != 0 and (
                    (decision_weak and strict_direction == 0)
                    or not decision_open_allowed
                ):
                    weak_open_filtered_count += 1
            elif base_direction == 0:
                target, exit_reason = 0.0, "base_flat"
            elif np.sign(base_direction) != np.sign(position):
                # Always allow closing. Re-open the opposite side only if it passes the dynamic entry threshold.
                target = (
                    strict_direction * lot
                    if decision_open_allowed and strict_direction == base_direction
                    else 0.0
                )
                exit_reason = "reverse" if target != 0 else "reverse_close_only"
                if base_direction != 0 and target == 0 and not decision_open_allowed:
                    weak_open_filtered_count += 1

        fee_cash = (
            fee(position, target, price) if abs(target - position) > 1e-12 else 0.0
        )
        if abs(target - position) > 1e-12:
            cash -= (target - position) * multiplier * price
            cash -= fee_cash
            trade_count += 1
            if abs(target) <= 1e-12:
                entry_price = favorable_price = np.nan
                holding_bars = 0
            elif abs(position) <= 1e-12 or np.sign(target) != np.sign(position):
                entry_price = favorable_price = price
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
                "decision_prob_down": row.get("decision_prob_down"),
                "decision_prob_up": row.get("decision_prob_up"),
                "base_direction": base_direction,
                "strict_direction": strict_direction,
                "decision_weak_environment": decision_weak,
                "decision_open_allowed": decision_open_allowed,
                "decision_intraday_volatility_regime": row.get(
                    "decision_intraday_volatility_regime"
                ),
                "decision_causal_volume_ratio_regime": row.get(
                    "decision_causal_volume_ratio_regime"
                ),
                "decision_causal_vol_volume_regime": row.get(
                    "decision_causal_vol_volume_regime"
                ),
                "decision_future_vol_high_prob": row.get(
                    f"decision_{getattr(config, 'future_vol_prob_col', 'prob_vol_high')}"
                ),
                "decision_future_vol_state": row.get(
                    f"decision_{getattr(config, 'future_vol_state_col', 'pred_vol_state_name')}"
                ),
                "exit_reason": exit_reason,
            }
        )

    report = pd.DataFrame(records).set_index("datetime")
    metadata = {
        "trade_count": int(trade_count),
        "weak_open_filtered_count": int(weak_open_filtered_count),
        "weak_trailing_exit_count": int(trailing_exit_count),
        "weak_prob_decay_exit_count": int(weak_prob_decay_exit_count),
        "weak_no_profit_exit_count": int(weak_no_profit_exit_count),
        "final_account": float(report["account"].iloc[-1]),
    }
    return report, metadata


def build_regime_filtered_state_target_lots(
    *,
    state_prob: pd.DataFrame,
    market_regimes: pd.DataFrame,
    test_start: str,
    test_end: str,
    strategy_rule: StrategyRuleConfig,
    config: RegimeFilterConfig,
) -> tuple[pd.Series, pd.DataFrame, dict]:
    """Build execution-time target lots for regime-filtered state strategy.

    This mirrors run_regime_filtered_state_backtest's decision logic, but does
    not update cash/account. Feed the returned target_lots into run_backtest
    with target_lots=... for a single continuous yearly backtest.
    """
    prob = state_prob.copy()
    if isinstance(prob.index, pd.MultiIndex):
        names = list(prob.index.names)
        dt_level = "datetime" if "datetime" in names else names[0]
        if "instrument" in names:
            try:
                prob = prob.xs(strategy_rule.instrument, level="instrument")
            except KeyError:
                prob.index = prob.index.get_level_values(dt_level)
        else:
            prob.index = prob.index.get_level_values(dt_level)
    prob.index = pd.to_datetime(prob.index, errors="coerce")
    prob = prob.loc[prob.index.notna()].sort_index().groupby(level=0).last()

    market = _datetime_frame(market_regimes)
    end = _period_end(test_end)
    market = market.loc[
        (market.index >= pd.Timestamp(test_start)) & (market.index <= end)
    ].copy()
    market = market.join(prob[["prob_down", "prob_up"]], how="left")
    for col in (
        "prob_down",
        "prob_up",
        "weak_environment",
        "intraday_volatility_regime",
        "causal_volume_ratio_regime",
        "causal_vol_volume_regime",
        str(getattr(config, "future_vol_prob_col", "prob_vol_high")),
        str(getattr(config, "future_vol_state_col", "pred_vol_state_name")),
    ):
        if col in market.columns:
            market[f"decision_{col}"] = market[col].shift(1)

    lot = float(strategy_rule.fixed_trade_amount)
    force_dates = _normalize_force_dates(strategy_rule.force_flatten_dates)

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

    def force_flatten(ts: pd.Timestamp) -> bool:
        if ts.time() < strategy_rule.force_flatten_exec_time:
            return False
        return bool(strategy_rule.force_flatten_daily) or ts.normalize() in force_dates

    def open_allowed(row) -> bool:
        rule = str(getattr(config, "allow_open_rule", "all")).lower()
        if rule in {"all", "none", "", "false"}:
            return True
        if rule == "high_vol_and_high_volume":
            return str(row.get("decision_intraday_volatility_regime")) == str(
                config.allow_open_vol_regime
            ) and str(row.get("decision_causal_volume_ratio_regime")) == str(
                config.allow_open_volume_regime
            )
        if rule == "not_low_vol_and_low_volume":
            return not (
                str(row.get("decision_intraday_volatility_regime"))
                == "intraday_volatility_q1"
                and str(row.get("decision_causal_volume_ratio_regime"))
                == "causal_volume_ratio_q1"
            )
        if rule == "predicted_high_vol":
            prob_col = (
                f"decision_{getattr(config, 'future_vol_prob_col', 'prob_vol_high')}"
            )
            state_col = f"decision_{getattr(config, 'future_vol_state_col', 'pred_vol_state_name')}"
            prob = row.get(prob_col)
            state = row.get(state_col)
            prob_ok = pd.notna(prob) and float(prob) >= float(
                config.future_vol_open_prob_threshold
            )
            state_ok = pd.isna(state) or str(state) == str(config.future_vol_high_state)
            return bool(prob_ok and state_ok)
        if rule == "not_predicted_weak_vol":
            prob_col = (
                f"decision_{getattr(config, 'future_vol_prob_col', 'prob_vol_high')}"
            )
            state_col = f"decision_{getattr(config, 'future_vol_state_col', 'pred_vol_state_name')}"
            prob = row.get(prob_col)
            state = row.get(state_col)
            weak = (
                pd.notna(prob)
                and float(prob) < float(config.future_vol_weak_prob_threshold)
            ) or (pd.notna(state) and str(state) == str(config.future_vol_low_state))
            return not bool(weak)
        raise ValueError(
            "allow_open_rule 仅支持 all / high_vol_and_high_volume / not_low_vol_and_low_volume / not_predicted_weak_vol / predicted_high_vol"
        )

    position = 0.0
    entry_price = np.nan
    favorable_price = np.nan
    holding_bars = 0
    trailing_exit_count = 0
    weak_prob_decay_exit_count = 0
    weak_no_profit_exit_count = 0
    weak_open_filtered_count = 0
    rows = []

    for ts, row in market.iterrows():
        price = float(row["price"])
        target = position
        exit_reason = ""
        base_direction = 0.0
        strict_direction = 0.0
        decision_weak = (
            bool(row.get("decision_weak_environment", False))
            if pd.notna(row.get("decision_weak_environment"))
            else False
        )
        if bool(getattr(config, "use_future_vol_as_weak", False)):
            prob_col = (
                f"decision_{getattr(config, 'future_vol_prob_col', 'prob_vol_high')}"
            )
            state_col = f"decision_{getattr(config, 'future_vol_state_col', 'pred_vol_state_name')}"
            prob = row.get(prob_col)
            state = row.get(state_col)
            mode = str(getattr(config, "future_vol_weak_mode", "weak")).lower()
            if mode == "mid":
                strong = (
                    pd.notna(prob)
                    and float(prob) >= float(config.future_vol_open_prob_threshold)
                ) and (
                    pd.isna(state) or str(state) == str(config.future_vol_high_state)
                )
                weak = (
                    pd.notna(prob)
                    and float(prob) < float(config.future_vol_weak_prob_threshold)
                ) or (
                    pd.notna(state) and str(state) == str(config.future_vol_low_state)
                )
                pred_weak = (not strong) and (not weak)
            else:
                pred_weak = (
                    pd.notna(prob)
                    and float(prob) < float(config.future_vol_weak_prob_threshold)
                ) or (
                    pd.notna(state) and str(state) == str(config.future_vol_low_state)
                )
            decision_weak = bool(decision_weak or pred_weak)
        decision_open_allowed = open_allowed(row)
        holding_bars = holding_bars + 1 if abs(position) > 1e-12 else 0

        if position > 0:
            favorable_price = (
                price
                if not np.isfinite(favorable_price)
                else max(favorable_price, price)
            )
        elif position < 0:
            favorable_price = (
                price
                if not np.isfinite(favorable_price)
                else min(favorable_price, price)
            )

        weak_trailing_exit = False
        if (
            abs(position) > 1e-12
            and decision_weak
            and np.isfinite(favorable_price)
            and np.isfinite(entry_price)
        ):
            trail = float(config.weak_trailing_stop_pct)
            activation = float(getattr(config, "weak_trailing_activation_pct", 0.0))
            activated = (
                position > 0 and favorable_price >= entry_price * (1.0 + activation)
            ) or (position < 0 and favorable_price <= entry_price * (1.0 - activation))
            weak_trailing_exit = (
                activated and position > 0 and price <= favorable_price * (1.0 - trail)
            ) or (
                activated and position < 0 and price >= favorable_price * (1.0 + trail)
            )

        weak_prob_decay_exit = False
        weak_no_profit_exit = False
        if (
            abs(position) > 1e-12
            and decision_weak
            and pd.notna(row.get("decision_prob_down"))
            and pd.notna(row.get("decision_prob_up"))
        ):
            p_down, p_up = (
                float(row["decision_prob_down"]),
                float(row["decision_prob_up"]),
            )
            direction_margin = (p_up - p_down) if position > 0 else (p_down - p_up)
            weak_prob_decay_exit = direction_margin < float(
                config.weak_exit_min_direction_margin
            )
        if (
            abs(position) > 1e-12
            and decision_weak
            and np.isfinite(entry_price)
            and holding_bars >= int(config.weak_no_profit_exit_bars)
        ):
            unrealized_return = (price / entry_price - 1.0) * np.sign(position)
            weak_no_profit_exit = unrealized_return <= 0.0

        if force_flatten(ts) and abs(position) > 1e-12:
            target, exit_reason = 0.0, "force_flatten"
        elif weak_trailing_exit:
            target, exit_reason = 0.0, "weak_trailing_stop"
            trailing_exit_count += 1
        elif weak_prob_decay_exit:
            target, exit_reason = 0.0, "weak_prob_decay_exit"
            weak_prob_decay_exit_count += 1
        elif weak_no_profit_exit:
            target, exit_reason = 0.0, "weak_no_profit_timeout"
            weak_no_profit_exit_count += 1
        elif (
            is_trade_time(ts)
            and pd.notna(row.get("decision_prob_down"))
            and pd.notna(row.get("decision_prob_up"))
        ):
            p_down, p_up = (
                float(row["decision_prob_down"]),
                float(row["decision_prob_up"]),
            )
            base_direction = _probability_direction(
                p_down,
                p_up,
                float(config.base_prob_threshold),
                float(config.base_min_margin),
            )
            threshold = (
                float(config.weak_prob_threshold)
                if decision_weak
                else float(config.base_prob_threshold)
            )
            margin = (
                float(config.weak_min_margin)
                if decision_weak
                else float(config.base_min_margin)
            )
            strict_direction = _probability_direction(p_down, p_up, threshold, margin)

            if abs(position) <= 1e-12:
                target = strict_direction * lot if decision_open_allowed else 0.0
                if base_direction != 0 and (
                    (decision_weak and strict_direction == 0)
                    or not decision_open_allowed
                ):
                    weak_open_filtered_count += 1
            elif base_direction == 0:
                target, exit_reason = 0.0, "base_flat"
            elif np.sign(base_direction) != np.sign(position):
                target = (
                    strict_direction * lot
                    if decision_open_allowed and strict_direction == base_direction
                    else 0.0
                )
                exit_reason = "reverse" if target != 0 else "reverse_close_only"
                if base_direction != 0 and target == 0 and not decision_open_allowed:
                    weak_open_filtered_count += 1

        if abs(target - position) > 1e-12:
            if abs(target) <= 1e-12:
                entry_price = favorable_price = np.nan
                holding_bars = 0
            elif abs(position) <= 1e-12 or np.sign(target) != np.sign(position):
                entry_price = favorable_price = price
                holding_bars = 0
        position = target
        rows.append(
            {
                "datetime": ts,
                "target_lots": float(target),
                "price": price,
                "base_direction": base_direction,
                "strict_direction": strict_direction,
                "decision_weak_environment": decision_weak,
                "decision_open_allowed": decision_open_allowed,
                "exit_reason": exit_reason,
            }
        )

    target_frame = pd.DataFrame(rows).set_index("datetime") if rows else pd.DataFrame()
    target_lots = (
        target_frame["target_lots"].rename("target_lots")
        if not target_frame.empty
        else pd.Series(dtype=float, name="target_lots")
    )
    metadata = {
        "weak_open_filtered_count": int(weak_open_filtered_count),
        "weak_trailing_exit_count": int(trailing_exit_count),
        "weak_prob_decay_exit_count": int(weak_prob_decay_exit_count),
        "weak_no_profit_exit_count": int(weak_no_profit_exit_count),
    }
    return target_lots, target_frame, metadata


def run_regime_filtered_regression_backtest(
    *,
    prediction: pd.Series | pd.DataFrame,
    market_regimes: pd.DataFrame,
    test_start: str,
    test_end: str,
    account: float,
    strategy_rule: StrategyRuleConfig,
    config: RegimeFilterConfig,
) -> tuple[pd.DataFrame, dict]:
    """Run a regression-signal backtest with causal weak-regime entry thresholds."""
    if isinstance(prediction, pd.DataFrame):
        if prediction.empty:
            raise ValueError("prediction 为空")
        pred = prediction.iloc[:, 0].copy()
    else:
        pred = prediction.copy()
    if isinstance(pred.index, pd.MultiIndex):
        names = list(pred.index.names)
        dt_level = "datetime" if "datetime" in names else names[0]
        if "instrument" in names:
            try:
                pred = pred.xs(strategy_rule.instrument, level="instrument")
            except KeyError:
                pred.index = pred.index.get_level_values(dt_level)
        else:
            pred.index = pred.index.get_level_values(dt_level)
    pred = pd.to_numeric(pred, errors="coerce")
    pred.index = pd.to_datetime(pred.index, errors="coerce")
    pred = pred.loc[pred.index.notna()].sort_index().groupby(level=0).last()

    market = _datetime_frame(market_regimes)
    end = _period_end(test_end)
    market = market.loc[
        (market.index >= pd.Timestamp(test_start)) & (market.index <= end)
    ].copy()
    market = market.join(pred.rename("prediction"), how="left")
    for col in (
        "prediction",
        "weak_environment",
        "intraday_volatility_regime",
        "causal_volume_ratio_regime",
        "causal_vol_volume_regime",
    ):
        market[f"decision_{col}"] = market[col].shift(1)

    multiplier = float(strategy_rule.contract_multiplier)
    lot = float(strategy_rule.fixed_trade_amount)
    force_dates = _normalize_force_dates(strategy_rule.force_flatten_dates)
    open_multiplier = max(
        float(getattr(config, "weak_reg_open_threshold_multiplier", 1.0)), 1.0
    )

    base_long_open = float(strategy_rule.long_open_threshold)
    base_short_open = float(strategy_rule.short_open_threshold)
    weak_long_open = abs(base_long_open) * open_multiplier
    weak_short_open = -abs(base_short_open) * open_multiplier
    long_close = float(strategy_rule.long_close_threshold)
    short_close = float(strategy_rule.short_close_threshold)

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

    def force_flatten(ts: pd.Timestamp) -> bool:
        if ts.time() < strategy_rule.force_flatten_exec_time:
            return False
        return bool(strategy_rule.force_flatten_daily) or ts.normalize() in force_dates

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

    def direction(score: float, long_open: float, short_open: float) -> float:
        if pd.isna(score):
            return 0.0
        if score > long_open:
            return 1.0
        if score < short_open:
            return -1.0
        return 0.0

    cash = float(account)
    position = 0.0
    trade_count = 0
    weak_open_filtered_count = 0
    records = []

    for ts, row in market.iterrows():
        price = float(row["price"])
        target = position
        exit_reason = ""
        base_direction = 0.0
        strict_direction = 0.0
        decision_weak = (
            bool(row.get("decision_weak_environment", False))
            if pd.notna(row.get("decision_weak_environment"))
            else False
        )

        if force_flatten(ts) and abs(position) > 1e-12:
            target, exit_reason = 0.0, "force_flatten"
        elif is_trade_time(ts) and pd.notna(row.get("decision_prediction")):
            score = float(row["decision_prediction"])
            dyn_long_open = weak_long_open if decision_weak else base_long_open
            dyn_short_open = weak_short_open if decision_weak else base_short_open
            base_direction = direction(score, base_long_open, base_short_open)
            strict_direction = direction(score, dyn_long_open, dyn_short_open)

            if abs(position) <= 1e-12:
                target = strict_direction * lot
                if decision_weak and base_direction != 0 and strict_direction == 0:
                    weak_open_filtered_count += 1
            elif position > 0:
                if score < base_short_open:
                    target = -lot if strict_direction < 0 else 0.0
                    exit_reason = "reverse" if target < 0 else "reverse_close_only"
                elif score <= long_close:
                    target, exit_reason = 0.0, "base_flat"
            elif position < 0:
                if score > base_long_open:
                    target = lot if strict_direction > 0 else 0.0
                    exit_reason = "reverse" if target > 0 else "reverse_close_only"
                elif score >= short_close:
                    target, exit_reason = 0.0, "base_flat"

        fee_cash = (
            fee(position, target, price) if abs(target - position) > 1e-12 else 0.0
        )
        if abs(target - position) > 1e-12:
            cash -= (target - position) * multiplier * price
            cash -= fee_cash
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
                "score": row.get("decision_prediction"),
                "base_direction": base_direction,
                "strict_direction": strict_direction,
                "decision_weak_environment": decision_weak,
                "decision_intraday_volatility_regime": row.get(
                    "decision_intraday_volatility_regime"
                ),
                "decision_causal_volume_ratio_regime": row.get(
                    "decision_causal_volume_ratio_regime"
                ),
                "decision_causal_vol_volume_regime": row.get(
                    "decision_causal_vol_volume_regime"
                ),
                "exit_reason": exit_reason,
            }
        )

    report = pd.DataFrame(records).set_index("datetime")
    metadata = {
        "trade_count": int(trade_count),
        "weak_open_filtered_count": int(weak_open_filtered_count),
        "weak_reg_open_threshold_multiplier": float(open_multiplier),
        "final_account": float(report["account"].iloc[-1]),
    }
    return report, metadata
