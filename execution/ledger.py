"""Ledger: order state machine, cash-flow settlement, and event journal.

Design authority: ``docs/design/execution/ledger.md`` (§23, §29.8).
Field-level dataclasses live in :mod:`core.types`; this module provides the
behavioural layer — transitions, settlement, and the append-only journal.

Red lines honoured:
- No event-driven kernel; for-loop is the time model.
- Backtest and live share one code path.
"""

from __future__ import annotations

import logging
from datetime import datetime, date
from pathlib import Path

from core.types import (
    Account,
    Direction,
    EventType,
    JournalEntry,
    Order,
    OrderStatus,
    Position,
    Trade,
)

logger = logging.getLogger(__name__)

# ── Order state machine (6 states) ────────────────────────────────────────────

LEGAL_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.CREATED: frozenset({OrderStatus.SUBMITTED}),
    OrderStatus.SUBMITTED: frozenset({
        OrderStatus.PARTIAL,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
    }),
    OrderStatus.PARTIAL: frozenset({OrderStatus.FILLED, OrderStatus.CANCELLED}),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
}

TERMINAL_STATES: frozenset[OrderStatus] = frozenset({
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.REJECTED,
})


class IllegalTransition(ValueError):
    """Raised when an order attempts an illegal status transition."""


def transition(order: Order, new_status: OrderStatus, *, reason: str = "") -> None:
    """Apply a status transition to *order* in-place.

    Raises :class:`IllegalTransition` on an illegal edge or a terminal exit.
    """
    allowed = LEGAL_TRANSITIONS.get(order.status, frozenset())
    if new_status not in allowed:
        raise IllegalTransition(
            f"{order.order_id}: {order.status.value} -> {new_status.value} "
            f"is not a legal transition (allowed: "
            f"{sorted(s.value for s in allowed) or 'none'})"
        )
    order.status = new_status
    order.updated_at = datetime.utcnow()
    if reason:
        order.reject_reason = reason


# ── Cash-flow settlement ─────────────────────────────────────────────────────


def freeze_cash(order: Order, account: Account, price: float | None = None) -> None:
    """Reserve cash for a BUY order before submission.

    *price* defaults to ``ref_price``; for LIMIT orders it should be the limit.
    """
    if order.direction != Direction.BUY:
        return
    ref = price if price is not None else (order.limit_price or order.ref_price)
    amount = ref * order.quantity
    if amount > account.cash:
        raise ValueError(
            f"{order.order_id}: insufficient cash to freeze {amount:.2f} "
            f"(available {account.cash:.2f})"
        )
    account.cash -= amount
    account.frozen_cash += amount
    order.frozen_cash += amount


def settle_fill(order: Order, account: Account, trade: Trade) -> None:
    """Settle one fill: BUY spends from frozen first, shortfall from cash.

    Frozen-cash invariant: each fill converts frozen to spent exactly once.
    Remaining frozen is refunded by :func:`release_frozen` at terminal state.

    Commission may exceed the frozen base amount (price * qty).  The excess is
    taken from available cash so frozen never goes negative.
    """
    total_cost = trade.fill_price * trade.filled_qty + trade.commission

    if order.direction == Direction.BUY:
        # Spend from frozen first; any shortfall (e.g. commission) from cash.
        from_frozen = min(total_cost, order.frozen_cash)
        from_cash = total_cost - from_frozen

        order.frozen_cash -= from_frozen
        account.frozen_cash -= from_frozen

        if from_cash > 1e-9:
            if account.cash < from_cash - 1e-6:
                raise ValueError(
                    f"{order.order_id}: insufficient cash for settlement "
                    f"(need {from_cash:.2f}, have {account.cash:.2f})"
                )
            account.cash -= from_cash

        # Guards: neither pool may go negative.
        if order.frozen_cash < -1e-6:
            raise ValueError(
                f"{order.order_id}: settle overdraws frozen "
                f"(remaining {order.frozen_cash:.2f})"
            )
        if account.frozen_cash < -1e-6:
            raise ValueError(
                f"account frozen overdrawn after settling {order.order_id} "
                f"(remaining {account.frozen_cash:.2f})"
            )
    else:  # SELL
        account.cash += trade.fill_price * trade.filled_qty - trade.commission

    # Track cumulative commission.
    account.total_commission += trade.commission
    # Update order fill progress.
    order.filled_qty += trade.filled_qty


def release_frozen(order: Order, account: Account) -> None:
    """Return remaining frozen cash at terminal state. Never overdraws.

    Must only be called when *order* is in a terminal state
    (FILLED / CANCELLED / REJECTED).
    """
    if order.status not in TERMINAL_STATES:
        raise ValueError(
            f"{order.order_id}: release_frozen requires terminal state, "
            f"got {order.status.value}"
        )
    refund = order.frozen_cash
    if refund < 0:
        raise ValueError(
            f"{order.order_id}: negative frozen balance ({refund:.2f})"
        )
    account.cash += refund
    account.frozen_cash -= refund
    order.frozen_cash = 0.0


# ── Journal (append-only daily Parquet) ──────────────────────────────────────

#: Event types that are persisted to Parquet (BAR excluded — market data has
#: its own table; ACCOUNT_UPDATE is day-end snapshot only).
PERSIST_TYPES: frozenset[EventType] = frozenset({
    EventType.SESSION_START,
    EventType.SESSION_END,
    EventType.DAY_START,
    EventType.DAY_END,
    EventType.SIGNAL,
    EventType.PORTFOLIO,
    EventType.ORDER_CREATED,
    EventType.ORDER_REJECTED,
    EventType.ORDER_FILLED,
    EventType.ORDER_CANCELLED,
    EventType.TRADE,
    EventType.POSITION_UPDATE,
})


class Journal:
    """Append-only event journal backed by daily Parquet files.

    Usage::

        j = Journal("./journal")
        j.log(entry)
        j.flush()          # force write buffer to disk

    Files are named ``{YYYY-MM-DD}.parquet`` under *root_dir*.
    """

    FLUSH_SIZE = 256

    def __init__(self, root_dir: str | Path, flush_size: int | None = None):
        self._root = Path(root_dir)
        self._root.mkdir(parents=True, exist_ok=True)
        self._buffer: list[dict] = []
        self._flush_size = flush_size or self.FLUSH_SIZE

    def log(self, entry: JournalEntry) -> None:
        """Buffer *entry* if its type is persisted; flush when buffer is full."""
        if entry.event_type not in PERSIST_TYPES:
            return
        self._buffer.append(self._entry_to_row(entry))
        if len(self._buffer) >= self._flush_size:
            self.flush()

    def flush(self) -> None:
        """Write buffered rows to their daily Parquet files (append semantics)."""
        if not self._buffer:
            return
        import pandas as pd

        # Group by date so each day's file is self-contained.
        by_date: dict[str, list[dict]] = {}
        for row in self._buffer:
            d = row["_date"]
            by_date.setdefault(d, []).append(row)

        for day_str, rows in by_date.items():
            for r in rows:
                r.pop("_date", None)
            df_new = pd.DataFrame(rows)
            path = self._root / f"{day_str}.parquet"
            if path.exists():
                df_old = pd.read_parquet(path)
                df_all = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_all = df_new
            df_all.to_parquet(path, index=False)

        self._buffer.clear()

    def read_day(self, day: str | date):
        """Read one day's journal as a DataFrame (empty if no file)."""
        import pandas as pd

        day_str = str(day)
        path = self._root / f"{day_str}.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    @property
    def buffered_count(self) -> int:
        return len(self._buffer)

    @staticmethod
    def _entry_to_row(entry: JournalEntry) -> dict:
        ts = entry.timestamp
        return {
            "timestamp": ts.isoformat(),
            "event_type": entry.event_type.value,
            "portfolio_id": entry.portfolio_id,
            "payload": _safe_json(entry.payload),
            "_date": ts.strftime("%Y-%m-%d"),
        }


def _safe_json(obj) -> str:
    """Best-effort JSON serialisation for Parquet payload column."""
    import json

    def _default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        if hasattr(o, "value"):  # Enum
            return o.value
        if hasattr(o, "__dict__"):
            return {k: v for k, v in o.__dict__.items() if not k.startswith("_")}
        return str(o)

    return json.dumps(obj, default=_default, ensure_ascii=False)


# ── Ledger: in-memory account + position bookkeeping ─────────────────────────


class SimAccount:
    """Convenience wrapper: account + positions with NAV identity.

    ``cash + Σ market_value == nav`` holds after every mutation.
    """

    def __init__(self, cash: float, account_id: str = "sim"):
        self._account = Account(
            account_id=account_id,
            total_asset=cash,
            cash=cash,
        )
        self._positions: dict[str, Position] = {}  # ticker -> Position

    @property
    def cash(self) -> float:
        return self._account.cash

    @property
    def frozen_cash(self) -> float:
        return self._account.frozen_cash

    @property
    def market_value(self) -> float:
        return sum(p.market_value for p in self._positions.values())

    @property
    def nav(self) -> float:
        """Net asset value = available cash + Σ market_value.

        Frozen cash is committed (not counted); it returns to cash or
        converts to position on settlement.
        """
        return self._account.cash + self.market_value

    @property
    def account(self) -> Account:
        return self._account

    @property
    def positions(self) -> dict[str, Position]:
        return dict(self._positions)

    def mark_to_market(self, market_value: float) -> None:
        """Set a synthetic single-position market value (for identity tests)."""
        if not self._positions:
            self._positions["__synthetic__"] = Position(
                portfolio_id="sim",
                ticker="__synthetic__",
                quantity=0,
                avg_cost=0.0,
            )
        self._positions["__synthetic__"].market_value = market_value

    def update_position(
        self, ticker: str, quantity: int, avg_cost: float,
        market_value: float = 0.0,
    ) -> None:
        """Create or update a position entry."""
        self._positions[ticker] = Position(
            portfolio_id=self._account.account_id,
            ticker=ticker,
            quantity=quantity,
            avg_cost=avg_cost,
            market_value=market_value,
        )

    def settle_fill(self, order: Order, trade: Trade) -> None:
        """Settle a fill against this account."""
        settle_fill(order, self._account, trade)

    def release_frozen(self, order: Order) -> None:
        """Release remaining frozen cash for a terminal order."""
        release_frozen(order, self._account)

    def check_identity(self, atol: float = 1e-6) -> bool:
        """Return True if ``cash + Σ market_value == nav``."""
        computed = self._account.cash + self.market_value
        return abs(computed - self.nav) < atol
