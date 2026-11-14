"""截面回测主链（按调仓日遍历时间模型）。

策略通过 ``BaseCrossStrategy.on_rebalance`` 仅输出目标权重，经由
``execution.order_generation.generate_orders`` 生成订单并交由 ``broker.submit_orders``
完成风控、撮合与记账，确保回测与实盘共用同一执行路径。
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date, timedelta

import pandas as pd

# 统一复用 execution.market_data 行情接口，避免在此处重复实现
from execution.market_data import (  # noqa: F401
    get_daily_volume,
    get_latest_price,
    get_prices,
)

# 统一复用 execution.order_generation 生成订单
from execution.order_generation import generate_orders

logger = logging.getLogger(__name__)

__all__ = [
    "BaseCrossStrategy",
    "CrossSectionalBacktest",
]

# 支持的调仓频率（周频/月频默认取各自首个交易日）
REBALANCE_FREQS = ("daily", "weekly", "monthly")


class BaseCrossStrategy(ABC):
    """截面策略抽象基类，仅负责在调仓日输出目标权重 ``{ticker: weight}``。"""

    @abstractmethod
    def on_rebalance(
        self,
        date: date,
        bars: pd.DataFrame,
        positions: dict,
        account,
    ) -> dict[str, float]:
        """返回指定调仓日的目标权重字典，输入含行情长表 ``bars``、当前持仓与账户视图。"""
        ...

    @property
    def universe(self) -> list[str]:
        """可交易标的池，默认空列表，由子类重写。"""
        return []

    @property
    def rebalance_freq(self) -> str:
        """调仓频率（``"daily"`` | ``"weekly"`` | ``"monthly"``），默认 ``"daily"``。"""
        return "daily"


class CrossSectionalBacktest:
    """截面回测引擎，串联 ``on_rebalance -> generate_orders -> broker.submit_orders`` 主链。"""

    def __init__(self, broker):
        self._broker = broker

    def run(
        self,
        strategy: BaseCrossStrategy,
        bars: pd.DataFrame,
        dates: list[date],
    ) -> list[dict]:
        """按 ``strategy.rebalance_freq`` 在 *dates* 上运行回测，返回每日 ``{"date", "weights", "orders", "fills"}`` 记录。"""
        freq = strategy.rebalance_freq
        rebalance_days = _select_rebalance_dates(dates, freq)

        results: list[dict] = []
        for day in rebalance_days:
            # 下单前按当日收盘价市值重估持仓，确保净值实时准确
            _mark_to_market(self._broker, bars, day)

            account = _BrokerAccount(self._broker)
            positions = _extract_positions(self._broker)

            # 策略仅返回目标权重
            weights = strategy.on_rebalance(day, bars, positions, account)

            # 委托 T3 生成订单
            target_df = _weights_frame(weights)
            orders = generate_orders(target_df, account, account.nav, bars, day)

            # 提交前将撮合时钟推进至下一交易日（T+1）
            _advance_fill_clock(self._broker, day, dates)

            # 经由 broker 统一完成撮合与账本更新
            fills = self._broker.submit_orders(orders)

            results.append(
                {
                    "date": day,
                    "weights": weights,
                    "orders": orders,
                    "fills": fills,
                }
            )

        return results


# ---------------------------------------------------------------------------
# 内部辅助组件
# ---------------------------------------------------------------------------


class _BrokerAccount:
    """Broker 的鸭子类型账户适配器，提供 ``cash``、``holdings``、``nav`` 只读属性。"""

    def __init__(self, broker):
        self._broker = broker

    @property
    def cash(self) -> float:
        return self._broker.get_cash()

    @property
    def holdings(self) -> dict[str, int]:
        return self._broker.get_holdings()

    @property
    def nav(self) -> float:
        return self._broker.get_nav()


def _extract_positions(broker) -> dict[str, int]:
    """通过 ``broker.get_holdings()`` 提取当前持仓，保持实盘与回测视图一致。"""
    return broker.get_holdings()


def _advance_fill_clock(broker, day: date, dates: list[date]) -> None:
    """将 broker 的 ``decision_date`` 设为 *day*、``fill_date`` 推进至 *dates* 中下一交易日（末日回退 +1 天）。"""
    fill_date: date = day + timedelta(days=1)
    candidates = [d for d in dates if d > day]
    if candidates:
        fill_date = min(candidates)
    if hasattr(broker, "fill_date"):
        broker.fill_date = fill_date
    if hasattr(broker, "decision_date"):
        broker.decision_date = day


def _mark_to_market(broker, bars: pd.DataFrame, day: date) -> None:
    """按当日收盘价更新账本持仓市值；无对应行情或无账本的 broker 跳过。"""
    account = getattr(broker, "account", None)
    if account is None:
        return
    ledger = getattr(account, "ledger", None)
    if ledger is None:
        return
    for ticker, pos in list(ledger.positions.items()):
        price = get_latest_price(bars, ticker, day, field="close")
        if price is None:
            continue
        new_mv = price * pos.quantity
        if abs(new_mv - pos.market_value) > 1e-9:
            ledger.update_position(ticker, pos.quantity, pos.avg_cost, new_mv)


def _weights_frame(weights: dict[str, float]) -> pd.DataFrame:
    """将目标权重字典转为 ``[ticker, weight]`` DataFrame。"""
    if not weights:
        return pd.DataFrame(columns=["ticker", "weight"])
    return pd.DataFrame(
        [{"ticker": t, "weight": w} for t, w in weights.items()],
        columns=["ticker", "weight"],
    )


def _select_rebalance_dates(dates: list[date], freq: str) -> list[date]:
    """按频率从 *dates* 中筛选调仓日（``weekly`` 取每周首日，``monthly`` 取每月首日）。"""
    if freq not in REBALANCE_FREQS:
        raise ValueError(
            f"rebalance_freq must be one of {REBALANCE_FREQS}, got {freq!r}"
        )
    if freq == "daily":
        return list(dates)
    seen: set = set()
    out: list[date] = []
    for d in dates:
        key = d.isocalendar()[:2] if freq == "weekly" else (d.year, d.month)
        if key not in seen:
            seen.add(key)
            out.append(d)
    return out
