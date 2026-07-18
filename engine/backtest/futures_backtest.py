from __future__ import annotations

import warnings
from dataclasses import dataclass, replace
from datetime import time

import numpy as np
import pandas as pd

from qlib.backtest.decision import Order, TradeDecisionWO
from qlib.backtest.exchange import Exchange
from qlib.backtest.position import Position
from qlib.contrib.evaluate import risk_analysis
from qlib.contrib.strategy.signal_strategy import BaseSignalStrategy
from qlib.data import D


@dataclass
class StrategyRuleConfig:
    """交易执行规则配置。"""

    instrument: str = "IF"
    # 合约乘数（IF 常用 300）
    contract_multiplier: float = 300.0
    # 策略模式：long_only（单多）、short_only（单空）、long_short（多空）
    strategy_mode: str = "long_only"
    # 固定交易手数（每次开仓目标手数）
    fixed_trade_amount: float = 1000.0
    # 是否使用信号值作为手数（True: 信号值=手数，False: 使用fixed_trade_amount）
    use_signal_as_lots: bool = False
    long_open_threshold: float = 0.0002
    long_close_threshold: float = -0.0002
    short_open_threshold: float = -0.0002
    short_close_threshold: float = 0.0002
    first_trade_time: time = time(9, 36)
    last_trade_time: time = time(14, 56)
    trade_interval_minutes: int = 5
    force_flatten_exec_time: time = time(15, 0)
    # True: 每个交易日到 force_flatten_exec_time 强平；False: 只在 force_flatten_dates 指定日期强平。
    force_flatten_daily: bool = True
    force_flatten_dates: object | None = None
    # 交易费用
    open_cost: float = 0.0002  # 开仓费用
    close_cost: float = 0.0002  # 平仓费用


def _normalize_force_flatten_dates(dates) -> set[pd.Timestamp]:
    if dates is None:
        return set()
    values = dates
    if isinstance(values, (pd.Series, pd.Index)):
        values = values.to_list()
    elif isinstance(values, (str, pd.Timestamp)):
        values = [values]
    return {pd.Timestamp(x).normalize() for x in values if pd.notna(x)}


def shift_signal_for_execution(signal: pd.Series, lag_bars: int = 1) -> pd.Series:
    """把 t bar 收盘后生成的信号延后到 t+lag_bars bar 执行。"""
    lag_bars = int(lag_bars)
    if lag_bars <= 0:
        return signal.rename("state_signal")
    s = pd.to_numeric(signal, errors="coerce").sort_index()
    if isinstance(s.index, pd.MultiIndex):
        idx_names = list(s.index.names)
        if "instrument" in idx_names:
            return (
                s.groupby(level="instrument", group_keys=False)
                .shift(lag_bars)
                .rename("state_signal")
            )
    return s.shift(lag_bars).rename("state_signal")


def _safe_risk_analysis(return_series: pd.Series):
    """Run qlib risk_analysis without leaking divide-by-zero warnings/inf metrics."""
    ser = (
        pd.to_numeric(return_series, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    if ser.empty:
        return pd.Series(dtype=float)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="divide by zero encountered in scalar divide",
            category=RuntimeWarning,
        )
        warnings.filterwarnings(
            "ignore", message="invalid value encountered", category=RuntimeWarning
        )
        out = risk_analysis(ser)
    if isinstance(out, pd.DataFrame):
        return out.replace([np.inf, -np.inf], np.nan)
    if isinstance(out, pd.Series):
        return out.replace([np.inf, -np.inf], np.nan)
    if isinstance(out, dict):
        return {
            k: (
                v.replace([np.inf, -np.inf], np.nan)
                if isinstance(v, (pd.Series, pd.DataFrame))
                else v
            )
            for k, v in out.items()
        }
    return out


class IFIntradaySignalStrategy(BaseSignalStrategy):
    """
    单资产日内策略：
    - 信号执行网格：09:36, 09:41, ..., 14:56
    - 成交价格：VWAP
    - 15:00 强制平仓
    """

    def __init__(self, *, rule_config: StrategyRuleConfig, **kwargs):
        super().__init__(**kwargs)
        self.rule = rule_config

    def _is_signal_trade_time(self, ts: pd.Timestamp) -> bool:
        t = ts.time()
        if t < self.rule.first_trade_time or t > self.rule.last_trade_time:
            return False
        anchor = pd.Timestamp.combine(ts.date(), self.rule.first_trade_time)
        delta_minutes = int((ts - anchor).total_seconds() // 60)
        return delta_minutes % self.rule.trade_interval_minutes == 0

    def _is_force_flatten_time(self, ts: pd.Timestamp) -> bool:
        if ts.time() < self.rule.force_flatten_exec_time:
            return False
        if bool(self.rule.force_flatten_daily):
            return True
        force_dates = _normalize_force_flatten_dates(self.rule.force_flatten_dates)
        return pd.Timestamp(ts).normalize() in force_dates

    def _get_signal_value(
        self, pred_start_time: pd.Timestamp, pred_end_time: pd.Timestamp
    ) -> float | None:
        pred_score = self.signal.get_signal(
            start_time=pred_start_time, end_time=pred_end_time
        )
        if pred_score is None:
            return None

        if isinstance(pred_score, pd.DataFrame):
            if pred_score.shape[1] == 0:
                return None
            pred_score = pred_score.iloc[:, 0]

        if isinstance(pred_score, pd.Series):
            if self.rule.instrument in pred_score.index:
                return float(pred_score.loc[self.rule.instrument])
            if len(pred_score) > 0:
                return float(pred_score.iloc[0])
            return None

        return float(pred_score)

    def _create_order_units(
        self,
        direction: int,
        amount_units: float,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> Order:
        return Order(
            stock_id=self.rule.instrument,
            amount=amount_units,
            start_time=start_time,
            end_time=end_time,
            direction=direction,
        )

    def _create_order_lots(
        self,
        direction: int,
        amount_lots: float,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> Order:
        """按“手”下单，内部自动换算为底层数量单位。"""
        multiplier = float(self.rule.contract_multiplier)
        if multiplier <= 0:
            raise ValueError(f"contract_multiplier 必须大于 0，当前值为 {multiplier}")
        return self._create_order_units(
            direction=direction,
            amount_units=amount_lots * multiplier,
            start_time=start_time,
            end_time=end_time,
        )

    def _target_amount_by_signal(self, score: float, held_amount: float) -> float:
        """根据策略模式和信号，给出目标持仓手数。

        支持两种模式：
        1. 固定手数模式：使用 fixed_trade_amount
        2. 信号值模式：当 use_signal_as_lots=True 时，直接使用信号值作为目标手数
        """
        mode = (self.rule.strategy_mode or "long_only").lower()
        lot = float(self.rule.fixed_trade_amount)
        if lot <= 0:
            return 0.0

        # 信号值模式：直接使用信号值作为目标手数
        if getattr(self.rule, "use_signal_as_lots", False):
            if score == 0:
                return 0.0
            return float(score)

        # 固定手数模式
        if mode == "long_only":
            if score > self.rule.long_open_threshold:
                return lot
            if score < self.rule.long_close_threshold:
                return 0.0
            return held_amount

        if mode == "short_only":
            if score < self.rule.short_open_threshold:
                return -lot
            if score > self.rule.short_close_threshold:
                return 0.0
            return held_amount

        if mode == "long_short":
            if score > self.rule.long_open_threshold:
                return lot
            if score < self.rule.short_open_threshold:
                return -lot
            # 中性区间平仓，避免在弱信号下持仓
            if (
                self.rule.long_close_threshold
                <= score
                <= self.rule.short_close_threshold
            ):
                return 0.0
            return held_amount

        raise ValueError("strategy_mode 仅支持: long_only, short_only, long_short")

    def _gen_orders_to_target(
        self, trade_time: pd.Timestamp, held_amount: float, target_amount: float
    ) -> list[Order]:
        """把当前持仓调整到目标手数（amount=手），支持跨方向反手。"""
        orders: list[Order] = []
        eps = 1e-12
        if abs(target_amount - held_amount) <= eps:
            return orders

        # 反手：多 -> 空，先平多再开空
        if held_amount > eps and target_amount < -eps:
            close_long = self._create_order_lots(
                Order.SELL, held_amount, trade_time, trade_time
            )
            open_short = self._create_order_lots(
                Order.SELL, abs(target_amount), trade_time, trade_time
            )
            if self.trade_exchange.check_order(close_long):
                orders.append(close_long)
            if self.trade_exchange.check_order(open_short):
                orders.append(open_short)
            return orders

        # 反手：空 -> 多，先平空再开多
        if held_amount < -eps and target_amount > eps:
            close_short = self._create_order_lots(
                Order.BUY, abs(held_amount), trade_time, trade_time
            )
            open_long = self._create_order_lots(
                Order.BUY, target_amount, trade_time, trade_time
            )
            if self.trade_exchange.check_order(close_short):
                orders.append(close_short)
            if self.trade_exchange.check_order(open_long):
                orders.append(open_long)
            return orders

        delta = target_amount - held_amount
        if delta > eps:
            od = self._create_order_lots(Order.BUY, delta, trade_time, trade_time)
            if self.trade_exchange.check_order(od):
                orders.append(od)
        elif delta < -eps:
            od = self._create_order_lots(Order.SELL, abs(delta), trade_time, trade_time)
            if self.trade_exchange.check_order(od):
                orders.append(od)
        return orders

    def generate_trade_decision(self, execute_result=None):
        trade_step = self.trade_calendar.get_trade_step()
        trade_start_time, _trade_end_time = self.trade_calendar.get_step_time(
            trade_step
        )
        pred_start_time, pred_end_time = self.trade_calendar.get_step_time(
            trade_step, shift=1
        )

        held_amount_units = (
            self.trade_position.get_stock_amount(self.rule.instrument)
            if self.trade_position.check_stock(self.rule.instrument)
            else 0.0
        )
        multiplier = float(self.rule.contract_multiplier)
        if multiplier <= 0:
            raise ValueError(f"contract_multiplier 必须大于 0，当前值为 {multiplier}")
        held_amount = held_amount_units / multiplier
        order_list = []

        # 指定强平日到 force_flatten_exec_time 及之后强制平仓（无论多空）
        if self._is_force_flatten_time(trade_start_time) and abs(held_amount_units) > 0:
            close_direction = Order.SELL if held_amount_units > 0 else Order.BUY
            close_order = self._create_order_units(
                direction=close_direction,
                amount_units=abs(held_amount_units),
                start_time=trade_start_time,
                end_time=trade_start_time,
            )
            if self.trade_exchange.check_order(close_order):
                order_list.append(close_order)
            return TradeDecisionWO(order_list, self)

        # 非信号网格时刻不交易
        if not self._is_signal_trade_time(trade_start_time):
            return TradeDecisionWO([], self)

        score = self._get_signal_value(pred_start_time, pred_end_time)
        if score is None:
            return TradeDecisionWO([], self)

        target_amount = self._target_amount_by_signal(
            score=score, held_amount=held_amount
        )
        order_list.extend(
            self._gen_orders_to_target(
                trade_time=trade_start_time,
                held_amount=held_amount,
                target_amount=target_amount,
            )
        )

        return TradeDecisionWO(order_list, self)


class IFFuturesPosition(Position):
    """允许负持仓的 IF 期货式 Position。

    qlib 默认 Position 偏股票逻辑：不能从 0 直接 SELL，也不能把持仓卖成负数。
    IF 日内多空策略需要支持直接开空和反手，因此这里只改订单更新规则：
    BUY 增加持仓、扣现金；SELL 减少持仓、加现金。持仓可以为负。
    """

    def _buy_stock(
        self, stock_id: str, trade_val: float, cost: float, trade_price: float
    ) -> None:
        trade_amount = trade_val / trade_price
        if stock_id not in self.position:
            self._init_stock(stock_id=stock_id, amount=trade_amount, price=trade_price)
        else:
            self.position[stock_id]["amount"] += trade_amount
            self.position[stock_id]["price"] = trade_price
            if np.isclose(self.position[stock_id]["amount"], 0.0):
                self._del_stock(stock_id)
        self.position["cash"] -= trade_val + cost

    def _sell_stock(
        self, stock_id: str, trade_val: float, cost: float, trade_price: float
    ) -> None:
        trade_amount = trade_val / trade_price
        if stock_id not in self.position:
            self._init_stock(stock_id=stock_id, amount=-trade_amount, price=trade_price)
        else:
            self.position[stock_id]["amount"] -= trade_amount
            self.position[stock_id]["price"] = trade_price
            if np.isclose(self.position[stock_id]["amount"], 0.0):
                self._del_stock(stock_id)

        new_cash = trade_val - cost
        if self._settle_type == self.ST_CASH:
            self.position["cash_delay"] += new_cash
        elif self._settle_type == self.ST_NO:
            self.position["cash"] += new_cash
        else:
            raise NotImplementedError("This type of input is not supported")

    def get_stock_price(self, code: str) -> float:
        # qlib Account.update_order 会在 Position.update_order 前查询上一价格来记录订单收益。
        # 直接开空时此前没有该合约持仓，返回 0 作为“新开仓无历史持仓收益”的统计基准。
        if code not in self.position:
            return 0.0
        return super().get_stock_price(code)

    def update_stock_price(self, stock_id: str, price: float) -> None:
        if stock_id in self.position:
            super().update_stock_price(stock_id, price)


class IFFuturesExchange(Exchange):
    """期货式 qlib Exchange：允许直接开空，并按 IF 开/平仓费率计费。"""

    def get_quote_from_qlib(self) -> None:
        if len(self.codes) == 0:
            self.codes = D.instruments()
        self.quote_df = D.features(
            self.codes,
            self.all_fields,
            self.start_time,
            self.end_time,
            freq=self.freq,
            disk_cache=0,
        )
        self.quote_df.columns = self.all_fields

        for attr in ("buy_price", "sell_price"):
            pstr = getattr(self, attr)
            if self.quote_df[pstr].isna().any():
                self.logger.warning("{} field data contains nan.".format(pstr))

        factor_missing = (
            self.quote_df["$factor"].isna() & ~self.quote_df["$close"].isna()
        )
        if factor_missing.any():
            self.quote_df.loc[factor_missing, "$factor"] = 1.0
            self.logger.warning("IF futures exchange filled missing $factor with 1.0.")
        self.trade_w_adj_price = False
        self._update_limit(self.limit_threshold)

        if self.extra_quote is not None:
            if "$close" not in self.extra_quote:
                raise ValueError("$close is necessray in extra_quote")
            for attr in "buy_price", "sell_price":
                pstr = getattr(self, attr)
                if pstr not in self.extra_quote.columns:
                    self.extra_quote[pstr] = self.extra_quote["$close"]
                    self.logger.warning(
                        f"No {pstr} set for extra_quote. Use $close as {pstr}."
                    )
            if "$factor" not in self.extra_quote.columns:
                self.extra_quote["$factor"] = 1.0
                self.logger.warning(
                    "No $factor set for extra_quote. Use 1.0 as $factor."
                )
            if "limit_sell" not in self.extra_quote.columns:
                self.extra_quote["limit_sell"] = False
                self.logger.warning(
                    "No limit_sell set for extra_quote. All stock will be able to be sold."
                )
            if "limit_buy" not in self.extra_quote.columns:
                self.extra_quote["limit_buy"] = False
                self.logger.warning(
                    "No limit_buy set for extra_quote. All stock will be able to be bought."
                )
            assert set(self.extra_quote.columns) == set(self.quote_df.columns) - {
                "$change"
            }
            self.quote_df = pd.concat(
                [self.quote_df, self.extra_quote], sort=False, axis=0
            )

    @staticmethod
    def _split_open_close_units(
        current_amount: float, target_amount: float
    ) -> tuple[float, float]:
        if np.isclose(current_amount, target_amount):
            return 0.0, 0.0
        if current_amount * target_amount >= 0:
            open_units = max(abs(target_amount) - abs(current_amount), 0.0)
            close_units = max(abs(current_amount) - abs(target_amount), 0.0)
        else:
            close_units = abs(current_amount)
            open_units = abs(target_amount)
        return float(open_units), float(close_units)

    def _calc_trade_info_by_order(
        self, order: Order, position, dealt_order_amount: dict
    ):
        trade_price = float(
            self.get_deal_price(
                order.stock_id,
                order.start_time,
                order.end_time,
                direction=order.direction,
            )
        )
        total_trade_val = (
            float(self.get_volume(order.stock_id, order.start_time, order.end_time))
            * trade_price
        )
        order.factor = self.get_factor(order.stock_id, order.start_time, order.end_time)
        order.deal_amount = order.amount
        self._clip_amount_by_volume(order, dealt_order_amount)

        trade_val = order.deal_amount * trade_price
        if not total_trade_val or np.isnan(total_trade_val):
            adj_cost_ratio = self.impact_cost
        else:
            adj_cost_ratio = self.impact_cost * (trade_val / total_trade_val) ** 2

        if position is not None:
            current_amount = (
                position.get_stock_amount(order.stock_id)
                if position.check_stock(order.stock_id)
                else 0.0
            )
        else:
            current_amount = 0.0

        if order.direction == Order.BUY:
            order.deal_amount = self.round_amount_by_trade_unit(
                order.deal_amount, order.factor
            )
        elif order.direction == Order.SELL:
            # qlib 原生这里会 min(current_amount, order.deal_amount)，导致不能从 0 开空。
            # 期货口径不裁剪 SELL 数量，允许 target amount 为负。
            order.deal_amount = self.round_amount_by_trade_unit(
                order.deal_amount, order.factor
            )
        else:
            raise NotImplementedError(
                "order direction {} error".format(order.direction)
            )

        target_amount = current_amount + (
            order.deal_amount if order.direction == Order.BUY else -order.deal_amount
        )
        open_units, close_units = self._split_open_close_units(
            current_amount, target_amount
        )

        trade_val = float(order.deal_amount * trade_price)
        trade_cost = (
            open_units * float(self.open_cost)
            + close_units * float(self.close_cost)
            + abs(float(order.deal_amount)) * float(adj_cost_ratio)
        ) * abs(trade_price)
        if trade_cost > 0:
            trade_cost = max(trade_cost, float(self.min_cost))
        if trade_val <= 1e-5:
            trade_cost = 0.0
        return trade_price, trade_val, float(trade_cost)


def run_backtest(
    pred,
    test_start: str,
    end_time: str,
    account: float,
    strategy_rule: StrategyRuleConfig,
    price_series: pd.Series | None = None,
    target_lots: pd.Series | pd.DataFrame | None = None,
):
    """
    分钟级期货风格回测（支持多头/空头）：
    - 持仓单位：手（lots）
    - 成交价格：VWAP
    - 盈亏口径：逐分钟盯市（position * multiplier * Δprice）
    - target_lots 可选：传入已按执行时间对齐的目标手数序列时，直接按目标手数调仓；
      此时 pred 只用于占位/兼容，阈值开平仓逻辑不再生效。
    """

    def _extract_price_series() -> pd.Series:
        if price_series is not None:
            ser = pd.to_numeric(price_series, errors="coerce")
            ser.index = pd.to_datetime(ser.index)
            ser = ser.sort_index().groupby(level=0).last()
            ser = ser.replace([np.inf, -np.inf], np.nan).dropna()
            if ser.empty:
                raise ValueError("传入的 price_series 为空")
            return ser

        raw = D.features(
            instruments=[strategy_rule.instrument],
            fields=["$vwap"],
            start_time=test_start,
            end_time=end_time,
            freq="1min",
        )
        if raw is None or raw.empty:
            raise ValueError("未读取到回测行情，请检查 qlib 数据与时间区间")
        df = raw.reset_index()
        dt_col = "datetime" if "datetime" in df.columns else df.columns[0]
        inst_col = "instrument" if "instrument" in df.columns else None
        price_col = "$vwap" if "$vwap" in df.columns else df.columns[-1]
        if inst_col is not None:
            df = df[df[inst_col] == strategy_rule.instrument]
        ser = pd.Series(
            pd.to_numeric(df[price_col], errors="coerce").values,
            index=pd.to_datetime(df[dt_col]),
        )
        ser = ser.sort_index().groupby(level=0).last()
        ser = ser.replace([np.inf, -np.inf], np.nan).dropna()
        ser = ser[ser > 0]
        if ser.empty:
            raise ValueError("回测价格序列为空（可能全为 NaN/0）")
        return ser

    def _extract_pred_series() -> pd.Series:
        if isinstance(pred, pd.DataFrame):
            if pred.shape[1] == 0:
                raise ValueError("预测 DataFrame 没有可用列")
            score = pred.iloc[:, 0]
        elif isinstance(pred, pd.Series):
            score = pred
        else:
            raise ValueError("pred 仅支持 Series 或 DataFrame")

        if isinstance(score.index, pd.MultiIndex):
            s_df = score.rename("score").reset_index()
            dt_col = "datetime" if "datetime" in s_df.columns else s_df.columns[0]
            inst_col = "instrument" if "instrument" in s_df.columns else None
            if inst_col is not None:
                s_df = s_df[s_df[inst_col] == strategy_rule.instrument]
            s = pd.Series(
                pd.to_numeric(s_df["score"], errors="coerce").values,
                index=pd.to_datetime(s_df[dt_col]),
            )
        else:
            s = pd.Series(
                pd.to_numeric(score, errors="coerce").values,
                index=pd.to_datetime(score.index),
            )
        s = s.sort_index().groupby(level=0).last().dropna()
        return s

    def _extract_target_lots_series() -> pd.Series | None:
        if target_lots is None:
            return None
        if isinstance(target_lots, pd.DataFrame):
            if target_lots.shape[1] == 0:
                raise ValueError("target_lots DataFrame 没有可用列")
            lots = target_lots.iloc[:, 0]
        elif isinstance(target_lots, pd.Series):
            lots = target_lots
        else:
            raise ValueError("target_lots 仅支持 Series 或 DataFrame")

        if isinstance(lots.index, pd.MultiIndex):
            lots_df = lots.rename("target_lots").reset_index()
            dt_col = "datetime" if "datetime" in lots_df.columns else lots_df.columns[0]
            inst_col = "instrument" if "instrument" in lots_df.columns else None
            if inst_col is not None:
                lots_df = lots_df[lots_df[inst_col] == strategy_rule.instrument]
            s = pd.Series(
                pd.to_numeric(lots_df["target_lots"], errors="coerce").values,
                index=pd.to_datetime(lots_df[dt_col]),
            )
        else:
            s = pd.Series(
                pd.to_numeric(lots, errors="coerce").values,
                index=pd.to_datetime(lots.index),
            )
        return s.sort_index().groupby(level=0).last().dropna()

    def _is_signal_trade_time(ts: pd.Timestamp) -> bool:
        t = ts.time()
        if t < strategy_rule.first_trade_time or t > strategy_rule.last_trade_time:
            return False
        anchor = pd.Timestamp.combine(ts.date(), strategy_rule.first_trade_time)
        delta_minutes = int((ts - anchor).total_seconds() // 60)
        return delta_minutes % strategy_rule.trade_interval_minutes == 0

    force_flatten_dates = _normalize_force_flatten_dates(
        strategy_rule.force_flatten_dates
    )

    def _is_force_flatten_time(ts: pd.Timestamp) -> bool:
        if ts.time() < strategy_rule.force_flatten_exec_time:
            return False
        if bool(strategy_rule.force_flatten_daily):
            return True
        return pd.Timestamp(ts).normalize() in force_flatten_dates

    def _target_lots(score: float, current_lots: float) -> float:
        mode = (strategy_rule.strategy_mode or "long_only").lower()
        lot = float(strategy_rule.fixed_trade_amount)
        if lot <= 0:
            return 0.0

        # 信号值模式：直接使用信号值作为目标手数
        if getattr(strategy_rule, "use_signal_as_lots", False):
            if score == 0:
                return 0.0
            return float(score)

        # 固定手数模式
        if mode == "long_only":
            if score > strategy_rule.long_open_threshold:
                return lot
            if score < strategy_rule.long_close_threshold:
                return 0.0
            return current_lots
        if mode == "short_only":
            if score < strategy_rule.short_open_threshold:
                return -lot
            if score > strategy_rule.short_close_threshold:
                return 0.0
            return current_lots
        if mode == "long_short":
            if score > strategy_rule.long_open_threshold:
                return lot
            if score < strategy_rule.short_open_threshold:
                return -lot
            if (
                strategy_rule.long_close_threshold
                <= score
                <= strategy_rule.short_close_threshold
            ):
                return 0.0
            return current_lots
        raise ValueError("strategy_mode 仅支持: long_only, short_only, long_short")

    def _calc_fee_cash(current_lots: float, target_lots: float, price: float) -> float:
        # 费用口径：平仓按 close_cost，开仓按 open_cost；反手拆分为“先平后开”
        open_cost = float(strategy_rule.open_cost)
        close_cost = float(strategy_rule.close_cost)
        min_cost = 0.0
        multiplier = float(strategy_rule.contract_multiplier)

        if abs(target_lots - current_lots) <= 1e-12:
            return 0.0
        if current_lots * target_lots >= 0:
            open_lots = max(abs(target_lots) - abs(current_lots), 0.0)
            close_lots = max(abs(current_lots) - abs(target_lots), 0.0)
        else:
            close_lots = abs(current_lots)
            open_lots = abs(target_lots)

        fee = (
            (open_lots * open_cost + close_lots * close_cost) * multiplier * abs(price)
        )
        if fee > 0:
            fee = max(fee, min_cost)
        return float(fee)

    multiplier = float(strategy_rule.contract_multiplier)
    if multiplier <= 0:
        raise ValueError(f"contract_multiplier 必须大于 0，当前值为 {multiplier}")

    price_ser = _extract_price_series()
    target_lots_ser = _extract_target_lots_series()
    pred_ser = (
        pd.Series(dtype=float)
        if target_lots_ser is not None
        else _extract_pred_series()
    )

    cash = float(account)
    pos_lots = 0.0
    trade_count = 0
    records = []

    for i, (ts, price) in enumerate(price_ser.items()):
        price = float(price)
        # 计算持仓价值（随价格变动而变化，空头为负值）
        # 计算账户总价值（现金 + 持仓价值）

        score = np.nan
        target = pos_lots
        force_flatten = False

        # 指定强平日到 force_flatten_exec_time 及之后强制平仓
        if _is_force_flatten_time(ts) and abs(pos_lots) > 1e-12:
            target = 0.0
            force_flatten = True
        elif target_lots_ser is not None:
            if ts in target_lots_ser.index:
                target = float(target_lots_ser.loc[ts])
                score = target
        elif _is_signal_trade_time(ts):
            if ts in pred_ser.index:
                score = float(pred_ser.loc[ts])
                target = _target_lots(score=score, current_lots=pos_lots)

        # 计算交易费用
        fee_cash = _calc_fee_cash(
            current_lots=pos_lots, target_lots=target, price=price
        )

        # 处理交易（开仓/平仓）对现金的影响
        if abs(target - pos_lots) > 1e-12:
            # trade_count 口径：反手拆成“平仓 + 开仓”两笔；同向加减仓或单纯开/平仓算一笔。
            trade_count += 2 if pos_lots * target < 0 else 1
            # 计算交易金额（手数变化 * 乘数 * 价格）
            trade_amount = (target - pos_lots) * multiplier * price
            # 现金 = 现金 - 交易金额
            cash -= trade_amount
            # 扣除交易费用
            cash -= fee_cash

        # 更新持仓
        pos_lots = target

        # 计算最新的持仓价值和账户总价值
        latest_position_value = pos_lots * multiplier * price
        latest_account_value = cash + latest_position_value

        # 收益率（含费用）= 总收益率变化 - 不含费用收益率
        return_with_cost = latest_account_value / account

        records.append(
            {
                "datetime": ts,
                # "return_no_cost": return_no_cost,
                "return_with_cost": return_with_cost,
                "account": latest_account_value,
                "cash": cash,
                "position_lots": pos_lots,
                "position_value": latest_position_value,
                "price": price,
                "score": score,
                "force_flatten": force_flatten,
            }
        )

    report_df = pd.DataFrame(records).set_index("datetime")

    analysis = {
        # "return_no_cost": risk_analysis(report_df["return_no_cost"]),
        "return_with_cost": _safe_risk_analysis(report_df["return_with_cost"]),
    }

    indicator_dict = {
        "1min": {
            "trade_count": int(trade_count),
            "final_account": float(report_df["account"].iloc[-1]),
            "final_position_lots": float(report_df["position_lots"].iloc[-1]),
        }
    }
    positions = report_df[["position_lots", "account"]].copy()

    return report_df, analysis, indicator_dict, positions


def search_optimal_thresholds(
    pred_valid,
    valid_start: str,
    valid_end: str,
    account: float,
    base_rule: StrategyRuleConfig,
    objective_metric: str = "information_ratio",
    objective_direction: str | None = None,
    open_threshold_grid: list[float] | None = None,
    close_threshold_grid: list[float] | None = None,
    symmetric_long_short: bool = True,
    top_k: int = 10,
):
    """
    在验证集上自动搜索开/平仓阈值。

    Returns:
        best_rule: 最优阈值对应的 StrategyRuleConfig
        results_df: 全部候选结果（按目标指标排序）
        best_result: 最优行（dict）
    """

    def _to_series_score(pred_obj) -> pd.Series:
        if isinstance(pred_obj, pd.DataFrame):
            if pred_obj.shape[1] == 0:
                raise ValueError("预测 DataFrame 没有可用列")
            score = pred_obj.iloc[:, 0]
        elif isinstance(pred_obj, pd.Series):
            score = pred_obj
        else:
            raise ValueError("pred_valid 仅支持 Series 或 DataFrame")

        if isinstance(score.index, pd.MultiIndex):
            s_df = score.rename("score").reset_index()
            dt_col = "datetime" if "datetime" in s_df.columns else s_df.columns[0]
            inst_col = "instrument" if "instrument" in s_df.columns else None
            if inst_col is not None:
                s_df = s_df[s_df[inst_col] == base_rule.instrument]
            s = pd.Series(
                pd.to_numeric(s_df["score"], errors="coerce").values,
                index=pd.to_datetime(s_df[dt_col]),
            )
        else:
            s = pd.Series(
                pd.to_numeric(score, errors="coerce").values,
                index=pd.to_datetime(score.index),
            )
        return s.sort_index().dropna()

    def _safe_float(v) -> float:
        try:
            return float(v)
        except Exception:
            return float("nan")

    def _extract_metric(x, key: str) -> float:
        if x is None:
            return float("nan")
        if isinstance(x, dict):
            if key in x:
                return _safe_float(x[key])
            for vv in x.values():
                out = _extract_metric(vv, key)
                if np.isfinite(out):
                    return out
            return float("nan")
        if isinstance(x, pd.Series):
            if key in x.index:
                return _safe_float(x.loc[key])
            return float("nan")
        if isinstance(x, pd.DataFrame):
            if key in x.index:
                row = x.loc[key]
                if isinstance(row, pd.Series):
                    if "risk" in row.index:
                        return _safe_float(row["risk"])
                    return _safe_float(row.iloc[0]) if len(row) > 0 else float("nan")
                return _safe_float(row)
            if key in x.columns:
                col = x[key]
                return _safe_float(col.iloc[0]) if len(col) > 0 else float("nan")
            return float("nan")
        return float("nan")

    def _infer_direction(metric_name: str) -> str:
        metric_l = (metric_name or "").lower()
        if metric_l in {"max_drawdown", "volatility", "std"}:
            return "min"
        return "max"

    score_ser = _to_series_score(pred_valid)
    abs_score = score_ser.abs()
    if abs_score.empty:
        raise ValueError("pred_valid 为空，无法进行阈值搜索")

    if open_threshold_grid is None:
        q_open = [0.60, 0.70, 0.80, 0.90, 0.95]
        open_threshold_grid = sorted(
            {
                _safe_float(abs_score.quantile(q))
                for q in q_open
                if np.isfinite(abs_score.quantile(q))
            }
        )
    if close_threshold_grid is None:
        q_close = [0.40, 0.50, 0.60, 0.70, 0.80]
        close_threshold_grid = sorted(
            {
                _safe_float(abs_score.quantile(q))
                for q in q_close
                if np.isfinite(abs_score.quantile(q))
            }
        )

    if not open_threshold_grid or not close_threshold_grid:
        raise ValueError("阈值网格为空，请检查 pred_valid 或手动传入网格")

    direction = (objective_direction or _infer_direction(objective_metric)).lower()
    if direction not in {"max", "min"}:
        raise ValueError("objective_direction 仅支持 'max' 或 'min'")

    mode = (base_rule.strategy_mode or "long_only").lower()
    candidates: list[tuple[float, float, StrategyRuleConfig]] = []
    for open_thr in sorted(set(open_threshold_grid)):
        if not np.isfinite(open_thr) or open_thr <= 0:
            continue
        for close_thr in sorted(set(close_threshold_grid)):
            if not np.isfinite(close_thr) or close_thr <= 0:
                continue
            if close_thr > open_thr:
                continue

            if mode == "long_only":
                rule_i = replace(
                    base_rule,
                    long_open_threshold=float(open_thr),
                    long_close_threshold=float(-close_thr),
                )
            elif mode == "short_only":
                rule_i = replace(
                    base_rule,
                    short_open_threshold=float(-open_thr),
                    short_close_threshold=float(close_thr),
                )
            elif mode == "long_short":
                if symmetric_long_short:
                    rule_i = replace(
                        base_rule,
                        long_open_threshold=float(open_thr),
                        short_open_threshold=float(-open_thr),
                        long_close_threshold=float(-close_thr),
                        short_close_threshold=float(close_thr),
                    )
                else:
                    rule_i = replace(
                        base_rule,
                        long_open_threshold=float(open_thr),
                        short_open_threshold=float(-open_thr),
                        long_close_threshold=float(-close_thr),
                        short_close_threshold=float(close_thr),
                    )
            else:
                raise ValueError(
                    "strategy_mode 仅支持: long_only, short_only, long_short"
                )
            candidates.append((float(open_thr), float(close_thr), rule_i))

    if not candidates:
        raise ValueError("没有可用候选阈值组合（请检查网格与约束）")

    rows: list[dict] = []
    for open_thr, close_thr, rule_i in candidates:
        report_df, analysis, indicator_dict, _positions = run_backtest(
            pred=pred_valid,
            test_start=valid_start,
            end_time=valid_end,
            account=account,
            strategy_rule=rule_i,
        )
        metric_val = _extract_metric(analysis.get("return_with_cost"), objective_metric)
        trade_count = int(indicator_dict.get("1min", {}).get("trade_count", 0))
        final_account = float(
            indicator_dict.get("1min", {}).get("final_account", np.nan)
        )
        rows.append(
            {
                "strategy_mode": mode,
                "open_threshold_abs": open_thr,
                "close_threshold_abs": close_thr,
                "long_open_threshold": float(rule_i.long_open_threshold),
                "long_close_threshold": float(rule_i.long_close_threshold),
                "short_open_threshold": float(rule_i.short_open_threshold),
                "short_close_threshold": float(rule_i.short_close_threshold),
                "objective_metric": objective_metric,
                "objective_value": metric_val,
                "trade_count": trade_count,
                "final_account": final_account,
                "n_bars": int(len(report_df)),
            }
        )

    results_df = pd.DataFrame(rows)
    results_df["rank_score"] = (
        results_df["objective_value"]
        if direction == "max"
        else -results_df["objective_value"]
    )
    results_df = results_df.sort_values(by="rank_score", ascending=False).reset_index(
        drop=True
    )

    if top_k is not None and top_k > 0:
        results_df = results_df.copy()
        results_df["is_top_k"] = False
        top_n = min(int(top_k), len(results_df))
        if top_n > 0:
            results_df.loc[: top_n - 1, "is_top_k"] = True

    best = results_df.iloc[0].to_dict()
    best_rule = replace(
        base_rule,
        long_open_threshold=float(best["long_open_threshold"]),
        long_close_threshold=float(best["long_close_threshold"]),
        short_open_threshold=float(best["short_open_threshold"]),
        short_close_threshold=float(best["short_close_threshold"]),
    )
    return best_rule, results_df, best
