"""Broker: order submission with risk control, matching, and settlement.

Design authority: ``docs/design/execution/broker.md`` (§22, §66.4, §66.5).
Seams settled in QUANT-28 ``kb_snapshot`` (1)-(4) — implement exactly.

``submit_orders`` internal three-step order (AC-1): (a) risk
``pre_trade_risk_check`` + ``log_risk_audit``, (b) fill price lookup via
``execution.market_data`` at ``fill_basis``, (c) ledger ``settle_fill``.

Red line 4: backtest and live share one ``submit_orders`` path; the only
difference is the ``Broker`` implementation selected by ``create_broker``.
"""

from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from datetime import date, datetime

from core.types import Direction, Order, OrderStatus, PriceType, Trade
from execution.fee_model import FeeModel
from execution.ledger import SimAccount as LedgerSimAccount
from execution.ledger import transition
from execution.market_data import get_latest_price
from execution.risk_control import log_risk_audit, pre_trade_risk_check

logger = logging.getLogger(__name__)

# Fill-basis switch (gap #5 ruling B).  "t1_open" gates; "decision_close" is
# comparison-only — written to the baseline, never gates acceptance.
FILL_BASIS_T1_OPEN = "t1_open"
FILL_BASIS_DECISION_CLOSE = "decision_close"
VALID_FILL_BASES = (FILL_BASIS_T1_OPEN, FILL_BASIS_DECISION_CLOSE)

_order_seq = itertools.count(1)


class Broker(ABC):
    """Broker abstraction.  Capped at 4 methods (§22)."""

    @abstractmethod
    def submit_orders(self, orders: list[dict]) -> list[dict]: ...

    @abstractmethod
    def get_holdings(self) -> dict[str, int]: ...

    @abstractmethod
    def get_cash(self) -> float: ...

    @abstractmethod
    def get_nav(self) -> float: ...


class SimAccountAdapter:
    """Adapter over ``ledger.SimAccount`` exposing the risk_control surface.

    ``risk_control.pre_trade_risk_check`` needs ``cash: float`` and
    ``holdings: dict[str, int]``; the book of record is ``ledger.SimAccount``
    with ``positions: dict[str, Position]``.  ``holdings`` is a derived view
    over ledger positions — never a parallel book.  Settlement goes through
    ``ledger.SimAccount.settle_fill``.  cost_basis / trade_log from broker.md's
    sketch map to ``Position.avg_cost`` / ``Journal`` — not reimplemented here.
    """

    def __init__(self, initial_cash: float, account_id: str = "sim"):
        self._ledger = LedgerSimAccount(cash=initial_cash, account_id=account_id)

    @property
    def cash(self) -> float:
        return self._ledger.cash

    @property
    def holdings(self) -> dict[str, int]:
        return {t: p.quantity for t, p in self._ledger.positions.items()}

    @property
    def nav(self) -> float:
        return self._ledger.nav

    @property
    def ledger(self) -> LedgerSimAccount:
        return self._ledger


class SimBroker(Broker):
    """Simulated broker: risk + matching + settlement in ``submit_orders``.

    Parameters in *config*:
    - ``initial_cash`` — starting cash.
    - ``bars`` — ``load_bars`` long table (date / ticker / OHLCV).
    - ``fill_date`` — date fills execute at (broker clock for ``t1_open``).
    - ``fill_basis`` — ``"t1_open"`` (default, gates) | ``"decision_close"``.
    - ``risk`` — config dict for ``pre_trade_risk_check``.
    - ``account_id``, ``portfolio_id`` — optional identifiers.
    """

    def __init__(self, config: dict):
        self._config = config
        self._account = SimAccountAdapter(
            initial_cash=config["initial_cash"],
            account_id=config.get("account_id", "sim"),
        )
        self._fee_model = FeeModel()
        self._bars = config["bars"]
        self._fill_basis = config.get("fill_basis", FILL_BASIS_T1_OPEN)
        if self._fill_basis not in VALID_FILL_BASES:
            raise ValueError(
                f"fill_basis must be one of {VALID_FILL_BASES}, "
                f"got {self._fill_basis!r}"
            )
        self._fill_date: date | str | None = config.get("fill_date")
        self._risk_config: dict = config.get("risk", {})

    @property
    def fill_date(self) -> date | str | None:
        return self._fill_date

    @fill_date.setter
    def fill_date(self, value: date | str | None) -> None:
        self._fill_date = value

    @property
    def account(self) -> SimAccountAdapter:
        return self._account

    def submit_orders(self, orders: list[dict]) -> list[dict]:
        """Submit a batch: risk -> fill -> ledger (AC-1 three-step order).

        Returns one result dict per input: ``{**order, status: FILLED|REJECTED}``
        with ``fill_price`` on fills.  No price (suspended) -> REJECTED, never
        silently skipped.  Risk-blocked -> REJECTED.  Partial fills not
        simulated (all-or-none).
        """
        # (a) risk
        check = pre_trade_risk_check(
            orders, self._account, self.get_nav(), self._risk_config
        )
        log_risk_audit(check)

        results: list[dict] = []
        for o in orders:
            if check.is_blocked(o["ticker"]) or o.get("skip"):
                results.append({**o, "status": "REJECTED"})
                continue

            # (b) fill price lookup
            price = self._lookup_fill_price(o)
            if price is None:
                results.append({**o, "status": "REJECTED"})
                continue

            # (c) ledger settle
            try:
                self._settle(o, price)
            except (ValueError, TypeError) as exc:
                logger.warning("settlement failed for %s: %s", o.get("ticker"), exc)
                results.append({**o, "status": "REJECTED"})
                continue

            results.append({**o, "status": "FILLED", "fill_price": price})

        return results

    def get_holdings(self) -> dict[str, int]:
        return self._account.holdings

    def get_cash(self) -> float:
        return self._account.cash

    def get_nav(self) -> float:
        return self._account.nav

    # ── internals ────────────────────────────────────────────────────────────

    def _lookup_fill_price(self, order: dict) -> float | None:
        """Fill price at *fill_basis*.  Raw, never pre-skewed (kb_snapshot 2)."""
        if self._fill_basis == FILL_BASIS_DECISION_CLOSE:
            return order["limit_price"]
        # t1_open: T+1 open via market_data — raw, FeeModel bills slippage.
        if self._fill_date is None:
            raise ValueError(
                "fill_date is required for fill_basis='t1_open' — set it via "
                "config or the fill_date property"
            )
        return get_latest_price(
            self._bars, order["ticker"], self._fill_date, field="open"
        )

    def _settle(self, order_dict: dict, fill_price: float) -> None:
        """Step 3: settle through ledger (cash-flow + position bookkeeping)."""
        fees = self._fee_model.compute(
            order_dict["side"],
            fill_price,
            order_dict["shares"],
            order_dict["ticker"],
        )
        direction = Direction.BUY if order_dict["side"] == "BUY" else Direction.SELL
        now = datetime.utcnow()
        portfolio_id = self._config.get("portfolio_id", "sim")
        seq = next(_order_seq)

        order = Order(
            order_id=f"sim-{seq}",
            portfolio_id=portfolio_id,
            datetime=now,
            ticker=order_dict["ticker"],
            direction=direction,
            quantity=order_dict["shares"],
            price_type=PriceType.LIMIT,
            limit_price=order_dict["limit_price"],
        )
        transition(order, OrderStatus.SUBMITTED)

        trade = Trade(
            trade_id=f"fill-{seq}",
            order_id=order.order_id,
            portfolio_id=portfolio_id,
            datetime=now,
            ticker=order_dict["ticker"],
            direction=direction,
            filled_qty=order_dict["shares"],
            fill_price=fill_price,
            commission=fees,
        )

        self._account.ledger.settle_fill(order, trade)
        transition(order, OrderStatus.FILLED)


def create_broker(mode: str, config: dict) -> Broker:
    """Factory: selects the Broker implementation only (red line 4).

    V1 ``"sim"`` -> ``SimBroker``.  ``"qmt"`` is the V2 slot.
    """
    if mode == "sim":
        return SimBroker(config)
    if mode == "qmt":
        raise NotImplementedError(
            "QmtBroker is the V2 slot — not implemented in V1"
        )
    raise ValueError(f"unknown broker mode {mode!r}, expected 'sim' or 'qmt'")
