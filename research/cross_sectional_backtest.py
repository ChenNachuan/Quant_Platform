"""Cross-sectional backtest main chain (for-loop time model).

Design authority: ``docs/design/research/cross_sectional_backtest.md`` (§26).
Seams settled in QUANT-28 ``kb_snapshot`` (1)(4)(5) — implement exactly.

``BaseCrossStrategy.on_rebalance`` produces target weights ONLY — no
matching/accounting (those live in ``broker.submit_orders``, red line 4).
Orders come from T3's :func:`execution.order_generation.generate_orders`
(import, never re-derive).  Data access is :mod:`execution.market_data`
(gap #10) — import the contract, never re-derive.

Suspended (gap #1/#2 ruling A): ``cs_portfolio_sim``, ``simulate_single_trade``
and ``rebalance()`` stay out of this module — execution semantics belong to
the execution domain (``broker.submit_orders`` is the only fill/accounting
path).  The PR description records the suspension and its reason.

``rebalance_freq`` V1 gates ``"daily"``; ``"weekly"`` / ``"monthly"`` use a
documented default (first trading day of each ISO week / calendar month).
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date

import pandas as pd

# Data-entry contract (AC-5): import T3's market_data, never re-derive.
# noqa: F401 — these are the contract surface this module commits to; the
# actual calls happen inside generate_orders.  Importing them here is the
# same-source assertion: this module never defines its own price lookup.
from execution.market_data import (  # noqa: F401
    get_daily_volume,
    get_latest_price,
    get_prices,
)

# Order path (kb_snapshot 5): T3's generate_orders, never re-derive.
from execution.order_generation import generate_orders

logger = logging.getLogger(__name__)

__all__ = [
    "BaseCrossStrategy",
    "CrossSectionalBacktest",
]

# V1 rebalance frequency set.  weekly / monthly day-selection rules stay
# open (knowledge_audit 6); the defaults below are documented, not gated.
REBALANCE_FREQS = ("daily", "weekly", "monthly")


class BaseCrossStrategy(ABC):
    """Cross-sectional strategy interface.

    :meth:`on_rebalance` is called once per rebalance day and returns target
    weights ``{ticker: weight}`` — **only** weights.  No order generation,
    no matching, no fee or ledger logic here; the backtest engine routes the
    weights through ``generate_orders`` -> ``broker.submit_orders`` (red
    line 4: backtest and live share that one path).
    """

    @abstractmethod
    def on_rebalance(
        self,
        date: date,
        bars: pd.DataFrame,
        positions: dict,
        account,
    ) -> dict[str, float]:
        """Return target weights for the given rebalance day.

        Parameters
        ----------
        date:
            Rebalance date.
        bars:
            ``load_bars`` long table (date / ticker / OHLCV).
        positions:
            Current positions — ``dict[str, Position]`` from the ledger when
            available, else ``{}``.
        account:
            Duck-typed on ``.cash``, ``.holdings``, ``.nav`` (same surface
            ``generate_orders`` expects).
        """
        ...

    @property
    def universe(self) -> list[str]:
        """Tradable ticker universe.  Empty default — strategies override."""
        return []

    @property
    def rebalance_freq(self) -> str:
        """``"daily"`` | ``"weekly"`` | ``"monthly"``.  Default ``"daily"``."""
        return "daily"


class CrossSectionalBacktest:
    """Cross-sectional backtest engine: for-loop main chain over dates.

    Per rebalance day the chain is::

        on_rebalance  ->  generate_orders  ->  broker.submit_orders

    — strategy produces weights, T3 turns them into orders, broker handles
    risk + fill + ledger.  The same ``submit_orders`` call path is what live
    uses (red line 4).
    """

    def __init__(self, broker):
        self._broker = broker

    def run(
        self,
        strategy: BaseCrossStrategy,
        bars: pd.DataFrame,
        dates: list[date],
    ) -> list[dict]:
        """Run the backtest over *dates*, rebalancing per *strategy.rebalance_freq*.

        Returns one record per rebalance day:
        ``{"date", "weights", "orders", "fills"}``.
        """
        freq = strategy.rebalance_freq
        rebalance_days = _select_rebalance_dates(dates, freq)

        results: list[dict] = []
        for day in rebalance_days:
            account = _BrokerAccount(self._broker)
            positions = _extract_positions(self._broker)

            # Strategy: target weights ONLY (AC-4 responsibility boundary).
            weights = strategy.on_rebalance(day, bars, positions, account)

            # Orders from T3 (import, never re-derive) (AC-5).
            target_df = _weights_frame(weights)
            orders = generate_orders(target_df, account, account.nav, bars, day)

            # Fill / accounting through broker — the only path (red line 4).
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
# internals
# ---------------------------------------------------------------------------


class _BrokerAccount:
    """Duck-typed account surface over a :class:`Broker` for strategy + orders.

    Provides ``cash`` / ``holdings`` / ``nav`` from the Broker ABC — the same
    shape ``generate_orders`` and ``on_rebalance`` expect.  No fill or ledger
    logic lives here.
    """

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


def _extract_positions(broker) -> dict:
    """Positions dict from the broker when available (SimBroker), else ``{}``.

    The Broker ABC does not expose positions; SimBroker's account does via
    ``ledger.positions``.  A generic Broker gets ``{}``.
    """
    account = getattr(broker, "account", None)
    if account is None:
        return {}
    ledger = getattr(account, "ledger", None)
    if ledger is None:
        return {}
    return dict(ledger.positions)


def _weights_frame(weights: dict[str, float]) -> pd.DataFrame:
    """Target weights dict -> DataFrame with the T3 contract columns."""
    if not weights:
        return pd.DataFrame(columns=["ticker", "weight"])
    return pd.DataFrame(
        [{"ticker": t, "weight": w} for t, w in weights.items()],
        columns=["ticker", "weight"],
    )


def _select_rebalance_dates(dates: list[date], freq: str) -> list[date]:
    """Pick rebalance days from *dates* by *freq*.

    ``"daily"`` keeps every date.  ``"weekly"`` keeps the first date in each
    ISO week; ``"monthly"`` the first in each calendar month.  (V1 documented
    default — the day-selection rule for weekly/monthly is an open design
    item, knowledge_audit 6.)
    """
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
