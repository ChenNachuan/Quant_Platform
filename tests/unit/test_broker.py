"""Tests for execution.broker — QUANT-28 iteration 1, ACs 1-3.

Acceptance criteria (QUANT-28):
1. submit_orders internal three-step order is risk -> fill -> ledger —
   ordering assertion.
2. No price (suspended day) -> REJECTED, never silently skipped.
3. Backtest and live share one submit_orders path, only the Broker
   implementation differs (red line 4) — dual-mode same-path assertion.

Seams from kb_snapshot (1)-(4): market_data import, fill_basis switch,
SimAccount holdings-over-positions adapter, 4-method Broker ABC + create_broker.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from execution.broker import (
    FILL_BASIS_DECISION_CLOSE,
    FILL_BASIS_T1_OPEN,
    Broker,
    SimAccountAdapter,
    SimBroker,
    create_broker,
)

TRADE_DATE = date(2026, 1, 5)
FILL_DATE = date(2026, 1, 6)


def make_bars(rows: list[dict]) -> pd.DataFrame:
    columns = ["date", "ticker", "open", "high", "low", "close", "volume"]
    return pd.DataFrame(rows, columns=columns)


def bar(
    ticker: str,
    day: date,
    open_: float = 10.0,
    close: float = 10.0,
    volume: int = 100_000,
) -> dict:
    return {
        "date": pd.Timestamp(day),
        "ticker": ticker,
        "open": open_,
        "high": close * 1.02,
        "low": close * 0.98,
        "close": close,
        "volume": volume,
    }


def make_config(**overrides) -> dict:
    config = {
        "initial_cash": 1_000_000.0,
        "bars": make_bars([bar("600519.SH", FILL_DATE, open_=10.5)]),
        "fill_date": FILL_DATE,
        "fill_basis": FILL_BASIS_T1_OPEN,
        "risk": {"min_trade_value": 100.0},
    }
    config.update(overrides)
    return config


def make_order(ticker: str = "600519.SH", shares: int = 100) -> dict:
    return {
        "ticker": ticker,
        "side": "BUY",
        "shares": shares,
        "limit_price": 10.0,
    }


# ── AC-1: three-step ordering (risk -> fill -> ledger) ──────────────────────


class TestThreeStepOrdering:
    def test_risk_before_fill_before_ledger(self):
        """AC-1: submit_orders calls risk, then fill lookup, then settle — in order."""
        call_order: list[str] = []

        def track_risk(*args, **kwargs):
            call_order.append("risk")
            result = MagicMock()
            result.is_blocked.return_value = False
            return result

        def track_fill(*args, **kwargs):
            call_order.append("fill")
            return 10.5

        def track_settle(*args, **kwargs):
            call_order.append("ledger")

        config = make_config()
        broker = SimBroker(config)
        order = make_order()

        with (
            patch("execution.broker.pre_trade_risk_check", side_effect=track_risk),
            patch("execution.broker.log_risk_audit"),
            patch.object(SimBroker, "_lookup_fill_price", side_effect=track_fill),
            patch.object(SimBroker, "_settle", side_effect=track_settle),
        ):
            results = broker.submit_orders([order])

        assert call_order == ["risk", "fill", "ledger"]
        assert results[0]["status"] == "FILLED"
        assert results[0]["fill_price"] == 10.5

    def test_multiple_orders_all_three_steps_per_order(self):
        """Each order goes through the full three-step path."""
        call_order: list[str] = []

        def track_risk(*args, **kwargs):
            call_order.append("risk")
            result = MagicMock()
            result.is_blocked.return_value = False
            return result

        def track_fill(*args, **kwargs):
            call_order.append("fill")
            return 10.5

        def track_settle(*args, **kwargs):
            call_order.append("ledger")

        broker = SimBroker(make_config())
        orders = [
            {"ticker": "600519.SH", "side": "BUY", "shares": 100, "limit_price": 10.0},
            {"ticker": "000001.SZ", "side": "BUY", "shares": 200, "limit_price": 20.0},
        ]

        with (
            patch("execution.broker.pre_trade_risk_check", side_effect=track_risk),
            patch("execution.broker.log_risk_audit"),
            patch.object(SimBroker, "_lookup_fill_price", side_effect=track_fill),
            patch.object(SimBroker, "_settle", side_effect=track_settle),
        ):
            results = broker.submit_orders(orders)

        # Risk is per-batch (once); fill+settle are per-order.
        assert call_order == ["risk", "fill", "ledger", "fill", "ledger"]
        assert all(r["status"] == "FILLED" for r in results)


# ── AC-2: suspended day -> REJECTED ──────────────────────────────────────────


class TestSuspendedDayRejected:
    def test_no_price_rejected_not_skipped(self):
        """AC-2: a ticker with no bar on fill_date returns REJECTED, not silence."""
        bars = make_bars([bar("600519.SH", FILL_DATE)])  # 000001.SZ has no bar
        config = make_config(bars=bars)
        broker = SimBroker(config)
        order = make_order(ticker="000001.SZ")

        results = broker.submit_orders([order])

        assert len(results) == 1
        assert results[0]["status"] == "REJECTED"
        assert "fill_price" not in results[0]

    def test_suspended_day_mixed_with_filling(self):
        """One suspended ticker REJECTED, the other FILLED — same batch."""
        bars = make_bars([bar("600519.SH", FILL_DATE, open_=10.5)])
        config = make_config(bars=bars)
        broker = SimBroker(config)
        orders = [
            make_order(ticker="600519.SH"),
            make_order(ticker="000001.SZ"),  # suspended
        ]

        results = broker.submit_orders(orders)

        assert results[0]["status"] == "FILLED"
        assert results[1]["status"] == "REJECTED"

    def test_wrong_date_is_suspended(self):
        """Strict asof equality: a bar on the wrong day is not a price."""
        bars = make_bars([bar("600519.SH", TRADE_DATE)])  # bar on T, not T+1
        config = make_config(bars=bars, fill_date=FILL_DATE)
        broker = SimBroker(config)

        results = broker.submit_orders([make_order()])

        assert results[0]["status"] == "REJECTED"


# ── AC-3: dual-mode same-path ────────────────────────────────────────────────


class TestDualModeSamePath:
    def test_create_broker_sim_returns_simbroker(self):
        broker = create_broker("sim", make_config())
        assert isinstance(broker, SimBroker)
        assert isinstance(broker, Broker)

    def test_same_call_path_across_implementations(self):
        """AC-3: submit_orders is the one path — impl is the only difference."""

        class StubBroker(Broker):
            """Minimal stand-in for a V2 QmtBroker — same call surface."""

            def submit_orders(self, orders):
                return [
                    {**o, "status": "FILLED", "fill_price": o["limit_price"]}
                    for o in orders
                ]

            def get_holdings(self):
                return {}

            def get_cash(self):
                return 0.0

            def get_nav(self):
                return 1.0

        sim = create_broker("sim", make_config())
        stub = StubBroker()
        order = make_order()

        # Identical call path — no mode branching in the caller.
        sim_results = sim.submit_orders([dict(order)])
        stub_results = stub.submit_orders([dict(order)])

        # Both return the same result shape: {**order, status, fill_price?}.
        for r in sim_results + stub_results:
            assert r["status"] in ("FILLED", "REJECTED")
            assert "ticker" in r and "shares" in r
        assert sim_results[0]["status"] == "FILLED"
        assert stub_results[0]["status"] == "FILLED"

    def test_qmt_is_v2_slot(self):
        with pytest.raises(NotImplementedError):
            create_broker("qmt", make_config())

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="unknown broker mode"):
            create_broker("fix", make_config())


# ── Fill-basis switch (kb_snapshot 2) ────────────────────────────────────────


class TestFillBasis:
    def test_t1_open_uses_open_price(self):
        bars = make_bars([bar("600519.SH", FILL_DATE, open_=10.5, close=11.0)])
        config = make_config(bars=bars, fill_basis=FILL_BASIS_T1_OPEN)
        broker = SimBroker(config)

        results = broker.submit_orders([make_order()])

        assert results[0]["status"] == "FILLED"
        assert results[0]["fill_price"] == 10.5  # open, not close

    def test_decision_close_uses_close_price(self):
        bars = make_bars([bar("600519.SH", TRADE_DATE, open_=10.5, close=11.0)])
        config = make_config(bars=bars, fill_basis=FILL_BASIS_DECISION_CLOSE)
        config["decision_date"] = TRADE_DATE
        broker = SimBroker(config)
        order = make_order()
        order["limit_price"] = 9.5

        results = broker.submit_orders([order])

        assert results[0]["status"] == "FILLED"
        assert results[0]["fill_price"] == 11.0  # close lookup, not limit_price

    def test_decision_close_suspended_rejected(self):
        """AC-2 holds for both bases: no price -> REJECTED, never a silent fill."""
        bars = make_bars([bar("600519.SH", TRADE_DATE, close=11.0)])
        config = make_config(bars=bars, fill_basis=FILL_BASIS_DECISION_CLOSE)
        config["decision_date"] = TRADE_DATE
        broker = SimBroker(config)

        results = broker.submit_orders([make_order(ticker="000001.SZ")])

        assert results[0]["status"] == "REJECTED"
        assert "fill_price" not in results[0]

    def test_decision_close_requires_decision_date(self):
        """No fill_date fallback — decision_date is mandatory for decision_close."""
        bars = make_bars([bar("600519.SH", TRADE_DATE, close=11.0)])
        config = make_config(bars=bars, fill_basis=FILL_BASIS_DECISION_CLOSE)
        broker = SimBroker(config)  # no decision_date, fill_date is set

        with pytest.raises(ValueError, match="decision_date"):
            broker.submit_orders([make_order()])

    def test_invalid_fill_basis_raises(self):
        with pytest.raises(ValueError, match="fill_basis"):
            SimBroker(make_config(fill_basis="twap"))


# ── SimAccount adapter (kb_snapshot 3) ──────────────────────────────────────


class TestSimAccountAdapter:
    def test_holdings_derived_from_positions(self):
        adapter = SimAccountAdapter(initial_cash=100_000.0)
        assert adapter.holdings == {}
        assert adapter.cash == 100_000.0

        adapter.ledger.update_position("600519.SH", 500, 10.0)
        assert adapter.holdings == {"600519.SH": 500}

    def test_no_parallel_book(self):
        """holdings is a property over ledger.positions, not a second dict."""
        adapter = SimAccountAdapter(initial_cash=100_000.0)
        adapter.ledger.update_position("600519.SH", 300, 10.0)
        # Mutating ledger positions is reflected in holdings.
        assert adapter.holdings == {"600519.SH": 300}
        assert adapter.holdings is not adapter.ledger.positions  # fresh dict

    def test_nav_tracks_ledger_nav(self):
        adapter = SimAccountAdapter(initial_cash=50_000.0)
        assert adapter.nav == 50_000.0
        assert adapter.nav == adapter.ledger.nav


# ── Integration: full fill path ──────────────────────────────────────────────


class TestFullFillPath:
    def test_fill_updates_holdings_and_cash(self):
        bars = make_bars([bar("600519.SH", FILL_DATE, open_=10.0)])
        config = make_config(bars=bars, risk={"min_trade_value": 100.0})
        broker = SimBroker(config)
        order = {
            "ticker": "600519.SH",
            "side": "BUY",
            "shares": 100,
            "limit_price": 10.0,
        }

        results = broker.submit_orders([order])

        assert results[0]["status"] == "FILLED"
        assert results[0]["fill_price"] == 10.0
        assert broker.get_holdings() == {"600519.SH": 100}
        # Cash reduced by fill_value + fees.
        assert broker.get_cash() < 1_000_000.0

    def test_risk_blocked_rejected(self):
        bars = make_bars([bar("600519.SH", FILL_DATE)])
        config = make_config(
            bars=bars,
            risk={"max_single_weight": 0.0001, "min_trade_value": 100.0},
        )
        broker = SimBroker(config)
        # Order value 1000 / NAV 1M = 0.1% > max_single_weight 0.01%.
        order = {
            "ticker": "600519.SH",
            "side": "BUY",
            "shares": 100,
            "limit_price": 10.0,
        }

        results = broker.submit_orders([order])

        assert results[0]["status"] == "REJECTED"

    def test_result_shape_passthrough(self):
        """Result carries the original order fields plus status/fill_price."""
        bars = make_bars([bar("600519.SH", FILL_DATE, open_=10.5)])
        broker = SimBroker(make_config(bars=bars))
        order = {
            "ticker": "600519.SH",
            "side": "BUY",
            "shares": 100,
            "limit_price": 10.0,
            "custom_field": "preserved",
        }

        results = broker.submit_orders([order])

        assert results[0]["custom_field"] == "preserved"
        assert results[0]["ticker"] == "600519.SH"
        assert results[0]["status"] == "FILLED"

    def test_risk_blocked_has_reject_reason(self):
        bars = make_bars([bar("600519.SH", FILL_DATE)])
        config = make_config(
            bars=bars,
            risk={"max_single_weight": 0.0001, "min_trade_value": 100.0},
        )
        broker = SimBroker(config)

        results = broker.submit_orders([make_order()])

        assert results[0]["status"] == "REJECTED"
        assert results[0]["reject_reason"] == "risk_blocked"

    def test_no_price_has_reject_reason(self):
        bars = make_bars([bar("600519.SH", FILL_DATE)])  # 000001.SZ suspended
        config = make_config(bars=bars)
        broker = SimBroker(config)

        results = broker.submit_orders([make_order(ticker="000001.SZ")])

        assert results[0]["status"] == "REJECTED"
        assert results[0]["reject_reason"] == "no_price"
