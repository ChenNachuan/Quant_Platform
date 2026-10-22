"""Tests for execution.ledger — state machine, cash-flow invariants, journal.

Acceptance criteria (QUANT-25):
1. Six-state FSM: legal transitions work, illegal ones raise.
2. After each fill: cash + Σ market_value == nav (hypothesis).
3. Frozen cash: terminal states refund remainder, never overdraw.
4. Journal: append-only daily Parquet.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from hypothesis import given, settings, strategies as st

from core.types import (
    Account,
    Direction,
    EventType,
    JournalEntry,
    Order,
    OrderStatus,
    PriceType,
    Trade,
)
from execution.ledger import (
    LEGAL_TRANSITIONS,
    TERMINAL_STATES,
    IllegalTransition,
    Journal,
    SimAccount,
    freeze_cash,
    release_frozen,
    settle_fill,
    transition,
)


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_order(
    direction: Direction = Direction.BUY,
    quantity: int = 100,
    ref_price: float = 10.0,
    status: OrderStatus = OrderStatus.CREATED,
) -> Order:
    now = datetime(2024, 9, 28, 9, 30, 0)
    return Order(
        order_id="ORD-001",
        portfolio_id="PF-001",
        datetime=now,
        ticker="600519.SH",
        direction=direction,
        quantity=quantity,
        price_type=PriceType.MARKET,
        ref_price=ref_price,
        status=status,
        created_at=now,
        updated_at=now,
    )


def _make_account(cash: float = 100_000.0) -> Account:
    return Account(
        account_id="ACC-001",
        total_asset=cash,
        cash=cash,
    )


def _make_trade(
    order_id: str = "ORD-001",
    direction: Direction = Direction.BUY,
    filled_qty: int = 100,
    fill_price: float = 10.0,
    commission: float = 5.0,
) -> Trade:
    return Trade(
        trade_id="TRD-001",
        order_id=order_id,
        portfolio_id="PF-001",
        datetime=datetime(2024, 9, 28, 9, 35, 0),
        ticker="600519.SH",
        direction=direction,
        filled_qty=filled_qty,
        fill_price=fill_price,
        commission=commission,
    )


# ── 1. Six-state FSM ─────────────────────────────────────────────────────────


class TestStateMachine:
    def test_six_states(self):
        assert len(LEGAL_TRANSITIONS) == 6
        assert set(LEGAL_TRANSITIONS.keys()) == set(OrderStatus)

    def test_terminal_states_have_no_exits(self):
        for ts in TERMINAL_STATES:
            assert LEGAL_TRANSITIONS[ts] == frozenset()

    def test_legal_path_created_to_filled(self):
        o = _make_order()
        transition(o, OrderStatus.SUBMITTED)
        assert o.status == OrderStatus.SUBMITTED
        transition(o, OrderStatus.PARTIAL)
        assert o.status == OrderStatus.PARTIAL
        transition(o, OrderStatus.FILLED)
        assert o.status == OrderStatus.FILLED

    def test_legal_path_created_to_cancelled(self):
        o = _make_order()
        transition(o, OrderStatus.SUBMITTED)
        transition(o, OrderStatus.CANCELLED)
        assert o.status == OrderStatus.CANCELLED

    def test_legal_path_created_to_rejected(self):
        o = _make_order()
        transition(o, OrderStatus.SUBMITTED)
        transition(o, OrderStatus.REJECTED, reason="insufficient margin")
        assert o.status == OrderStatus.REJECTED
        assert o.reject_reason == "insufficient margin"

    def test_partial_to_cancelled(self):
        o = _make_order(status=OrderStatus.PARTIAL)
        transition(o, OrderStatus.CANCELLED)
        assert o.status == OrderStatus.CANCELLED

    def test_illegal_created_to_filled(self):
        o = _make_order()
        with pytest.raises(IllegalTransition, match="CREATED -> FILLED"):
            transition(o, OrderStatus.FILLED)

    def test_illegal_filled_to_anything(self):
        o = _make_order(status=OrderStatus.FILLED)
        for target in OrderStatus:
            if target == OrderStatus.FILLED:
                continue
            with pytest.raises(IllegalTransition):
                transition(o, target)

    def test_illegal_cancelled_to_submitted(self):
        o = _make_order(status=OrderStatus.CANCELLED)
        with pytest.raises(IllegalTransition):
            transition(o, OrderStatus.SUBMITTED)

    def test_illegal_created_to_partial(self):
        o = _make_order()
        with pytest.raises(IllegalTransition):
            transition(o, OrderStatus.PARTIAL)

    def test_illegal_rejected_to_filled(self):
        o = _make_order(status=OrderStatus.REJECTED)
        with pytest.raises(IllegalTransition):
            transition(o, OrderStatus.FILLED)

    @pytest.mark.parametrize(
        "start,target",
        [
            (OrderStatus.CREATED, OrderStatus.FILLED),
            (OrderStatus.CREATED, OrderStatus.PARTIAL),
            (OrderStatus.CREATED, OrderStatus.CANCELLED),
            (OrderStatus.CREATED, OrderStatus.REJECTED),
            (OrderStatus.SUBMITTED, OrderStatus.CREATED),
            (OrderStatus.PARTIAL, OrderStatus.SUBMITTED),
            (OrderStatus.PARTIAL, OrderStatus.CREATED),
            (OrderStatus.PARTIAL, OrderStatus.REJECTED),
            (OrderStatus.FILLED, OrderStatus.SUBMITTED),
            (OrderStatus.CANCELLED, OrderStatus.FILLED),
            (OrderStatus.REJECTED, OrderStatus.SUBMITTED),
        ],
    )
    def test_illegal_transitions_raise(self, start, target):
        o = _make_order(status=start)
        with pytest.raises(IllegalTransition):
            transition(o, target)


# ── 2. NAV identity after each fill (hypothesis) ─────────────────────────────


class TestNavIdentity:
    @given(
        initial_cash=st.floats(min_value=10_000, max_value=1e6),
        fill_price=st.floats(min_value=1, max_value=100),
        fill_qty=st.integers(min_value=1, max_value=50),
        commission=st.floats(min_value=0, max_value=50),
        market_value=st.floats(min_value=0, max_value=1e6),
    )
    @settings(max_examples=100, deadline=None)
    def test_cash_plus_market_value_equals_nav(
        self, initial_cash, fill_price, fill_qty, commission, market_value
    ):
        """After every fill: cash + frozen + Σ market_value == independently tracked nav."""
        # Ensure freeze amount is affordable.
        freeze_amount = fill_price * fill_qty
        if freeze_amount > initial_cash:
            fill_qty = max(1, int(initial_cash / fill_price) - 1)
            freeze_amount = fill_price * fill_qty

        acct = SimAccount(cash=initial_cash)
        order = _make_order(quantity=fill_qty, ref_price=fill_price)
        trade = _make_trade(
            filled_qty=fill_qty, fill_price=fill_price, commission=commission
        )

        # Freeze then settle via SimAccount (manages positions too)
        freeze_cash(order, acct.account, price=fill_price)
        acct.settle_fill(order, trade)
        acct.mark_to_market(market_value)

        # Identity: cash + frozen + Σ market_value == independently tracked nav
        assert acct.cash + acct.frozen_cash + acct.market_value == pytest.approx(
            acct.nav, abs=1e-4
        )

    @given(
        initial_cash=st.floats(min_value=10_000, max_value=1e6),
        sell_price=st.floats(min_value=1, max_value=500),
        sell_qty=st.integers(min_value=1, max_value=1000),
        commission=st.floats(min_value=0, max_value=50),
    )
    @settings(max_examples=80, deadline=None)
    def test_sell_fill_identity(self, initial_cash, sell_price, sell_qty, commission):
        """SELL converts position to cash; nav identity holds (minus commission)."""
        acct = SimAccount(cash=initial_cash)
        position_value = sell_price * sell_qty

        # Set up a position to sell (ticker must match trade's ticker).
        acct.update_position(
            "600519.SH", sell_qty, sell_price, market_value=position_value
        )

        order = _make_order(
            direction=Direction.SELL, quantity=sell_qty, ref_price=sell_price
        )
        trade = _make_trade(
            direction=Direction.SELL,
            filled_qty=sell_qty,
            fill_price=sell_price,
            commission=commission,
        )

        acct.settle_fill(order, trade)

        expected_cash = initial_cash + position_value - commission
        assert acct.cash == pytest.approx(expected_cash, abs=1e-4)
        # Position fully sold — should be gone.
        assert "600519.SH" not in acct.positions
        total = acct.cash + acct.frozen_cash + acct.market_value
        assert total == pytest.approx(acct.nav, abs=1e-4)

    @given(
        initial_cash=st.floats(min_value=10_000, max_value=1e6),
        mark_price=st.floats(min_value=1, max_value=200),
        fill_price=st.floats(min_value=1, max_value=200),
        sell_qty=st.integers(min_value=1, max_value=500),
        commission=st.floats(min_value=0, max_value=50),
    )
    @settings(max_examples=100, deadline=None)
    def test_sell_divergent_price_full_close(
        self, initial_cash, mark_price, fill_price, sell_qty, commission
    ):
        """Full-close SELL at fill price ≠ mark price — nav identity must hold."""
        acct = SimAccount(cash=initial_cash)
        position_value = mark_price * sell_qty
        acct.update_position(
            "600519.SH", sell_qty, mark_price, market_value=position_value
        )

        order = _make_order(
            direction=Direction.SELL, quantity=sell_qty, ref_price=fill_price
        )
        trade = _make_trade(
            direction=Direction.SELL,
            filled_qty=sell_qty,
            fill_price=fill_price,
            commission=commission,
        )

        acct.settle_fill(order, trade)

        # Full close — position must be gone with no residual.
        assert "600519.SH" not in acct.positions
        # Identity: cash + frozen + Σ mv == independently tracked nav.
        total = acct.cash + acct.frozen_cash + acct.market_value
        assert total == pytest.approx(acct.nav, abs=1e-4)

    @given(
        initial_cash=st.floats(min_value=10_000, max_value=1e6),
        existing_qty=st.integers(min_value=1, max_value=200),
        existing_mark=st.floats(min_value=1, max_value=200),
        fill_price=st.floats(min_value=1, max_value=200),
        fill_qty=st.integers(min_value=1, max_value=200),
        commission=st.floats(min_value=0, max_value=50),
    )
    @settings(max_examples=100, deadline=None)
    def test_buy_divergent_price_adds_to_position(
        self,
        initial_cash,
        existing_qty,
        existing_mark,
        fill_price,
        fill_qty,
        commission,
    ):
        """BUY at fill price ≠ mark price on existing position — nav identity must hold."""
        # Ensure freeze amount is affordable.
        freeze_amount = fill_price * fill_qty
        if freeze_amount > initial_cash:
            fill_qty = max(1, int(initial_cash / fill_price) - 1)
            freeze_amount = fill_price * fill_qty

        acct = SimAccount(cash=initial_cash)
        # Existing position at mark price (divergent from fill).
        existing_mv = existing_mark * existing_qty
        acct.update_position(
            "600519.SH", existing_qty, existing_mark, market_value=existing_mv
        )

        order = _make_order(quantity=fill_qty, ref_price=fill_price)
        trade = _make_trade(
            filled_qty=fill_qty, fill_price=fill_price, commission=commission
        )

        freeze_cash(order, acct.account, price=fill_price)
        acct.settle_fill(order, trade)

        # Identity: cash + frozen + Σ mv == independently tracked nav.
        total = acct.cash + acct.frozen_cash + acct.market_value
        assert total == pytest.approx(acct.nav, abs=1e-4)

    @pytest.mark.parametrize("status", list(TERMINAL_STATES))
    def test_settle_fill_on_terminal_order_raises(self, status):
        """Fills on terminal-state orders are rejected."""
        acct = _make_account()
        order = _make_order(status=status)
        trade = _make_trade()
        with pytest.raises(ValueError, match="terminal"):
            settle_fill(order, acct, trade)
        # Cash and frozen unchanged.
        assert acct.cash == pytest.approx(100_000)
        assert acct.frozen_cash == pytest.approx(0)


# ── 3. Frozen cash: refund remainder, never overdraw ─────────────────────────


class TestFrozenCash:
    def test_freeze_reduces_cash_increases_frozen(self):
        acct = _make_account(cash=100_000)
        order = _make_order(quantity=100, ref_price=10.0)
        freeze_cash(order, acct, price=10.0)
        assert acct.cash == pytest.approx(99_000)
        assert acct.frozen_cash == pytest.approx(1_000)
        assert order.frozen_cash == pytest.approx(1_000)

    def test_sell_does_not_freeze(self):
        acct = _make_account()
        order = _make_order(direction=Direction.SELL)
        freeze_cash(order, acct)
        assert acct.frozen_cash == 0
        assert order.frozen_cash == 0

    def test_settle_buy_spends_frozen_first(self):
        acct = _make_account(cash=100_000)
        order = _make_order(quantity=100, ref_price=10.0)
        freeze_cash(order, acct, price=10.0)
        cash_before = acct.cash  # 99_000

        trade = _make_trade(filled_qty=100, fill_price=10.0, commission=5.0)
        settle_fill(order, acct, trade)

        # Fill amount (1000) spent from frozen; commission (5) from cash.
        assert acct.frozen_cash == pytest.approx(0)
        assert order.frozen_cash == pytest.approx(0)
        assert acct.cash == pytest.approx(cash_before - 5.0)

    def test_release_frozen_refunds_remainder(self):
        acct = _make_account(cash=100_000)
        order = _make_order(quantity=100, ref_price=10.0)
        freeze_cash(order, acct, price=10.0)

        # Partial fill
        trade = _make_trade(filled_qty=50, fill_price=10.0, commission=5.0)
        settle_fill(order, acct, trade)

        transition(order, OrderStatus.SUBMITTED)
        transition(order, OrderStatus.PARTIAL)
        transition(order, OrderStatus.CANCELLED)

        cash_before = acct.cash
        frozen_before = acct.frozen_cash
        release_frozen(order, acct)

        # Refund = remaining frozen.
        assert acct.cash == pytest.approx(cash_before + frozen_before)
        assert acct.frozen_cash == pytest.approx(0)
        assert order.frozen_cash == pytest.approx(0)

    def test_release_frozen_requires_terminal_state(self):
        acct = _make_account()
        order = _make_order(status=OrderStatus.SUBMITTED)
        order.frozen_cash = 500
        with pytest.raises(ValueError, match="terminal"):
            release_frozen(order, acct)

    def test_settle_excess_takes_from_cash(self):
        """Fill cost exceeding frozen takes the shortfall from cash."""
        acct = _make_account(cash=100_000)
        order = _make_order(quantity=100, ref_price=10.0)
        freeze_cash(order, acct, price=10.0)  # frozen = 1000

        # Fill price higher than frozen rate: cost=2000, frozen=1000, shortfall=1000.
        trade = _make_trade(filled_qty=100, fill_price=20.0, commission=0.0)
        settle_fill(order, acct, trade)

        assert acct.frozen_cash == pytest.approx(0)
        assert acct.cash == pytest.approx(99_000 - 1_000)  # 98_000

    def test_settle_insufficient_raises_frozen_survives(self):
        """When cash cannot cover the shortfall, settle raises — frozen survives."""
        acct = _make_account(cash=500)  # barely enough to freeze
        order = _make_order(quantity=100, ref_price=5.0)
        freeze_cash(order, acct, price=5.0)  # frozen=500, cash=0

        # Cost=2000, frozen=500, shortfall=1500 > cash=0 → raise.
        trade = _make_trade(filled_qty=100, fill_price=20.0, commission=0.0)
        with pytest.raises(ValueError, match="insufficient cash"):
            settle_fill(order, acct, trade)

        # Frozen must survive the raise — not zeroed.
        assert order.frozen_cash == pytest.approx(500)
        assert acct.frozen_cash == pytest.approx(500)
        assert acct.cash == pytest.approx(0)

    @given(
        initial_cash=st.floats(min_value=50_000, max_value=1e6),
        freeze_price=st.floats(min_value=1, max_value=100),
        qty=st.integers(min_value=10, max_value=500),
        fill_ratio=st.floats(min_value=0.1, max_value=1.0),
        commission=st.floats(min_value=0, max_value=30),
    )
    @settings(max_examples=80, deadline=None)
    def test_frozen_never_overdraws(
        self, initial_cash, freeze_price, qty, fill_ratio, commission
    ):
        """After any fill and release, cash never goes below zero from frozen."""
        acct = _make_account(cash=initial_cash)
        order = _make_order(quantity=qty, ref_price=freeze_price)
        freeze_cash(order, acct, price=freeze_price)

        fill_qty = max(1, int(qty * fill_ratio))
        fill_price = freeze_price  # same price, no slippage
        trade = _make_trade(
            filled_qty=fill_qty, fill_price=fill_price, commission=commission
        )
        settle_fill(order, acct, trade)

        # Move to terminal state and release.
        transition(order, OrderStatus.SUBMITTED)
        if fill_qty >= qty:
            transition(order, OrderStatus.FILLED)
        else:
            transition(order, OrderStatus.PARTIAL)
            transition(order, OrderStatus.CANCELLED)

        release_frozen(order, acct)

        # Cash must never be negative.
        assert acct.cash >= -1e-6
        assert acct.frozen_cash >= -1e-6
        assert order.frozen_cash == pytest.approx(0)


# ── 4. Journal: append-only daily Parquet ────────────────────────────────────


class TestJournal:
    def _entry(self, ts: datetime | None = None) -> JournalEntry:
        return JournalEntry(
            timestamp=ts or datetime(2024, 9, 28, 10, 0, 0),
            event_type=EventType.ORDER_CREATED,
            portfolio_id="PF-001",
            payload={"order_id": "ORD-001", "ticker": "600519.SH"},
        )

    def test_log_buffers_until_flush(self, tmp_path):
        j = Journal(tmp_path / "journal")
        j.log(self._entry())
        assert j.buffered_count == 1
        assert not (tmp_path / "journal" / "2024-09-28.parquet").exists()
        j.flush()
        assert j.buffered_count == 0
        assert (tmp_path / "journal" / "2024-09-28.parquet").exists()

    def test_append_only_semantics(self, tmp_path):
        """Writing more events appends, does not overwrite."""
        j = Journal(tmp_path / "journal")
        e1 = self._entry(datetime(2024, 9, 28, 10, 0, 0))
        e2 = self._entry(datetime(2024, 9, 28, 11, 0, 0))
        j.log(e1)
        j.flush()
        j.log(e2)
        j.flush()

        df = j.read_day("2024-09-28")
        assert len(df) == 2

    def test_daily_partition(self, tmp_path):
        """Events on different days land in separate Parquet files."""
        j = Journal(tmp_path / "journal")
        j.log(self._entry(datetime(2024, 9, 28, 10, 0, 0)))
        j.log(self._entry(datetime(2024, 9, 29, 10, 0, 0)))
        j.flush()

        assert (tmp_path / "journal" / "2024-09-28.parquet").exists()
        assert (tmp_path / "journal" / "2024-09-29.parquet").exists()
        assert len(j.read_day("2024-09-28")) == 1
        assert len(j.read_day("2024-09-29")) == 1

    def test_non_persist_types_skipped(self, tmp_path):
        """BAR and ACCOUNT_UPDATE are not persisted."""
        j = Journal(tmp_path / "journal")
        bar_entry = JournalEntry(
            timestamp=datetime(2024, 9, 28, 10, 0, 0),
            event_type=EventType.BAR,
            portfolio_id="PF-001",
            payload={},
        )
        acct_entry = JournalEntry(
            timestamp=datetime(2024, 9, 28, 15, 0, 0),
            event_type=EventType.ACCOUNT_UPDATE,
            portfolio_id="PF-001",
            payload={},
        )
        j.log(bar_entry)
        j.log(acct_entry)
        assert j.buffered_count == 0

    def test_auto_flush_at_capacity(self, tmp_path):
        j = Journal(tmp_path / "journal", flush_size=3)
        for i in range(3):
            j.log(self._entry(datetime(2024, 9, 28, 10, i, 0)))
        # Auto-flushed when buffer hit capacity.
        assert j.buffered_count == 0
        assert (tmp_path / "journal" / "2024-09-28.parquet").exists()

    def test_payload_serialisation(self, tmp_path):
        j = Journal(tmp_path / "journal")
        entry = JournalEntry(
            timestamp=datetime(2024, 9, 28, 10, 0, 0),
            event_type=EventType.TRADE,
            portfolio_id="PF-001",
            payload={
                "trade_id": "TRD-001",
                "fill_price": 10.5,
                "direction": Direction.BUY,  # enum in payload
            },
        )
        j.log(entry)
        j.flush()
        df = j.read_day("2024-09-28")
        assert len(df) == 1
        payload = json.loads(df.iloc[0]["payload"])
        assert payload["direction"] == "BUY"
        assert payload["fill_price"] == 10.5

    def test_read_missing_day_returns_empty(self, tmp_path):
        j = Journal(tmp_path / "journal")
        df = j.read_day("2024-01-01")
        assert df.empty
