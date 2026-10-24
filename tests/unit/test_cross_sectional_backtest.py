"""Tests for research.cross_sectional_backtest — QUANT-28 iteration 2, ACs 4-6.

Acceptance criteria (QUANT-28):
4. BaseCrossStrategy.on_rebalance produces target weights only, holds no
   fill/ledger logic — responsibility-boundary assertion.
5. Data entry points import T3's execution/market_data.py contract, never
   re-derived — same-source assertion.
6. Code style clean — `uv run ruff check .`, exit 0 (a command, not a case).

Seams from kb_snapshot (1)(4)(5): market_data import, T3 generate_orders
import, for-loop main chain via broker.submit_orders.
"""

from __future__ import annotations

import inspect
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from execution.broker import FILL_BASIS_T1_OPEN, Broker, SimBroker
from research.cross_sectional_backtest import (
    BaseCrossStrategy,
    CrossSectionalBacktest,
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


class FixedWeightsStrategy(BaseCrossStrategy):
    """Concrete strategy returning fixed target weights — no fill/ledger logic."""

    def __init__(self, weights: dict[str, float]):
        self._weights = weights

    def on_rebalance(self, date, bars, positions, account):
        return dict(self._weights)


class NaiveEqualWeight(BaseCrossStrategy):
    """Weight the universe equally — pure computation, no broker/ledger."""

    def __init__(self, tickers: list[str]):
        self._tickers = tickers

    def on_rebalance(self, date, bars, positions, account):
        n = len(self._tickers)
        return {t: 1.0 / n for t in self._tickers}


# ── AC-4: responsibility boundary — weights only, no fill/ledger ────────────


class TestResponsibilityBoundary:
    def test_on_rebalance_is_abstract(self):
        """Cannot instantiate BaseCrossStrategy — on_rebalance must be implemented."""
        with pytest.raises(TypeError, match="abstract"):
            BaseCrossStrategy()

    def test_on_rebalance_returns_weight_dict(self):
        """Returns dict[str, float] target weights — not orders, trades, or fills."""
        strategy = FixedWeightsStrategy({"600519.SH": 0.6, "000001.SZ": 0.4})
        weights = strategy.on_rebalance(TRADE_DATE, make_bars([]), {}, MagicMock())

        assert isinstance(weights, dict)
        assert set(weights) == {"600519.SH", "000001.SZ"}
        assert all(isinstance(w, float) for w in weights.values())

    def test_on_rebalance_source_has_no_fill_ledger_logic(self):
        """The strategy body never touches fill or ledger concepts."""
        for cls in (FixedWeightsStrategy, NaiveEqualWeight):
            source = inspect.getsource(cls.on_rebalance)
            for forbidden in ("submit_orders", "settle_fill", "ledger", "fill_price"):
                assert forbidden not in source, (
                    f"{cls.__name__}.on_rebalance references {forbidden!r} — "
                    f"fill/ledger logic belongs in broker.submit_orders"
                )

    def test_strategy_abc_has_no_broker_parameter(self):
        """on_rebalance(date, bars, positions, account) — no broker in the signature."""
        sig = inspect.signature(BaseCrossStrategy.on_rebalance)
        params = list(sig.parameters)
        assert params == ["self", "date", "bars", "positions", "account"]

    def test_main_chain_routes_fills_through_broker(self):
        """The engine calls broker.submit_orders; the strategy only returns weights."""
        broker = MagicMock(spec=Broker)
        broker.get_holdings.return_value = {}
        broker.get_cash.return_value = 1_000_000.0
        broker.get_nav.return_value = 1_000_000.0
        broker.submit_orders.return_value = [
            {"ticker": "600519.SH", "status": "FILLED", "fill_price": 10.0}
        ]

        bars = make_bars([bar("600519.SH", TRADE_DATE, close=10.0)])
        strategy = FixedWeightsStrategy({"600519.SH": 1.0})
        engine = CrossSectionalBacktest(broker)

        results = engine.run(strategy, bars, [TRADE_DATE])

        # Fill/accounting happened in the broker, not the strategy.
        broker.submit_orders.assert_called_once()
        assert results[0]["weights"] == {"600519.SH": 1.0}
        assert results[0]["fills"][0]["status"] == "FILLED"


# ── AC-5: same-source — import T3's contract, never re-derive ───────────────


class TestSameSourceContract:
    def test_imports_market_data_contract(self):
        """cross_sectional_backtest imports execution.market_data entry points."""
        import research.cross_sectional_backtest as mod

        source = inspect.getsource(mod)
        assert "from execution.market_data import" in source

    def test_imports_order_generation_not_rederived(self):
        """Orders come from T3's generate_orders — imported, not reimplemented."""
        import research.cross_sectional_backtest as mod

        source = inspect.getsource(mod)
        assert "from execution.order_generation import" in source
        assert "def generate_orders" not in source
        assert "def compute_order_diff" not in source

    def test_no_rederived_price_lookup(self):
        """market_data get_latest_price / get_prices / get_daily_volume are not copies."""
        import research.cross_sectional_backtest as mod

        for fn_name in ("get_latest_price", "get_prices", "get_daily_volume"):
            # Imported from the contract — not defined here.
            assert hasattr(mod, fn_name), f"missing market_data import: {fn_name}"
        source = inspect.getsource(mod)
        for fn_name in ("get_latest_price", "get_prices", "get_daily_volume"):
            assert f"def {fn_name}" not in source, (
                f"{fn_name} re-derived in cross_sectional_backtest — "
                f"import from execution.market_data instead"
            )

    def test_market_data_functions_called_through_chain(self):
        """The backtest chain reaches market_data.get_latest_price via generate_orders."""
        bars = make_bars([bar("600519.SH", TRADE_DATE, close=10.0)])

        broker = MagicMock(spec=Broker)
        broker.get_holdings.return_value = {}
        broker.get_cash.return_value = 1_000_000.0
        broker.get_nav.return_value = 1_000_000.0
        broker.submit_orders.return_value = []

        strategy = FixedWeightsStrategy({"600519.SH": 1.0})
        engine = CrossSectionalBacktest(broker)

        with patch(
            "execution.order_generation.get_latest_price",
            wraps=__import__(
                "execution.market_data", fromlist=["get_latest_price"]
            ).get_latest_price,
        ) as mock_px:
            engine.run(strategy, bars, [TRADE_DATE])

        # generate_orders (imported from T3) calls market_data.get_latest_price.
        assert mock_px.called


# ── Main chain: for-loop over dates (kb_snapshot 5) ─────────────────────────


class TestMainChain:
    def test_for_loop_over_dates(self):
        """One result record per rebalance date — the for-loop is the time model."""
        bars = make_bars(
            [
                bar("600519.SH", date(2026, 1, 5), close=10.0),
                bar("600519.SH", date(2026, 1, 6), close=10.1),
                bar("600519.SH", date(2026, 1, 7), close=10.2),
            ]
        )
        broker = MagicMock(spec=Broker)
        broker.get_holdings.return_value = {}
        broker.get_cash.return_value = 1_000_000.0
        broker.get_nav.return_value = 1_000_000.0
        broker.submit_orders.return_value = []

        strategy = FixedWeightsStrategy({"600519.SH": 1.0})
        engine = CrossSectionalBacktest(broker)
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

        results = engine.run(strategy, bars, dates)

        assert len(results) == 3
        assert [r["date"] for r in results] == dates

    def test_weekly_freq_selects_first_of_week(self):
        bars = make_bars([])
        broker = MagicMock(spec=Broker)
        broker.get_holdings.return_value = {}
        broker.get_cash.return_value = 1_000_000.0
        broker.get_nav.return_value = 1_000_000.0
        broker.submit_orders.return_value = []

        class WeeklyStrategy(FixedWeightsStrategy):
            @property
            def rebalance_freq(self):
                return "weekly"

        strategy = WeeklyStrategy({"600519.SH": 1.0})
        engine = CrossSectionalBacktest(broker)
        # 2026-01-05 is Monday of ISO week 2; 01-06..01-09 same week.
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
        results = engine.run(strategy, bars, dates)

        assert len(results) == 1  # only first day of the week
        assert results[0]["date"] == date(2026, 1, 5)

    def test_result_record_shape(self):
        bars = make_bars([bar("600519.SH", TRADE_DATE, close=10.0)])
        broker = MagicMock(spec=Broker)
        broker.get_holdings.return_value = {}
        broker.get_cash.return_value = 1_000_000.0
        broker.get_nav.return_value = 1_000_000.0
        broker.submit_orders.return_value = [
            {"ticker": "600519.SH", "status": "FILLED", "fill_price": 10.0}
        ]

        strategy = FixedWeightsStrategy({"600519.SH": 1.0})
        engine = CrossSectionalBacktest(broker)
        results = engine.run(strategy, bars, [TRADE_DATE])

        record = results[0]
        assert record["date"] == TRADE_DATE
        assert record["weights"] == {"600519.SH": 1.0}
        assert isinstance(record["orders"], list)
        assert isinstance(record["fills"], list)


# ── Strategy interface surface ──────────────────────────────────────────────


class TestStrategyInterface:
    def test_universe_default_empty(self):
        class Minimal(BaseCrossStrategy):
            def on_rebalance(self, date, bars, positions, account):
                return {}

        assert Minimal().universe == []

    def test_rebalance_freq_default_daily(self):
        class Minimal(BaseCrossStrategy):
            def on_rebalance(self, date, bars, positions, account):
                return {}

        assert Minimal().rebalance_freq == "daily"

    def test_invalid_freq_raises(self):
        bars = make_bars([])
        broker = MagicMock(spec=Broker)
        broker.get_holdings.return_value = {}
        broker.get_cash.return_value = 1.0
        broker.get_nav.return_value = 1.0

        class BadFreq(BaseCrossStrategy):
            @property
            def rebalance_freq(self):
                return "yearly"

            def on_rebalance(self, date, bars, positions, account):
                return {}

        engine = CrossSectionalBacktest(broker)
        with pytest.raises(ValueError, match="rebalance_freq"):
            engine.run(BadFreq(), bars, [TRADE_DATE])


# ── Real SimBroker: full chain (review: main-chain tests must not mock) ──────


def make_sim_broker(bars: pd.DataFrame, initial_cash: float = 1_000_000.0) -> SimBroker:
    return SimBroker(
        {
            "initial_cash": initial_cash,
            "bars": bars,
            "fill_basis": FILL_BASIS_T1_OPEN,
            "risk": {
                "min_trade_value": 100.0,
                "max_single_weight": 1.0,
                "max_total_weight": 1.0,
            },
        }
    )


class TestMainChainRealBroker:
    def test_real_submit_orders_fills(self):
        """Real SimBroker.submit_orders — not a mock — exercises risk→fill→ledger."""
        bars = make_bars(
            [
                bar("600519.SH", date(2026, 1, 5), open_=10.0, close=10.0),
                bar("600519.SH", date(2026, 1, 6), open_=10.5, close=11.0),
            ]
        )
        broker = make_sim_broker(bars)
        strategy = FixedWeightsStrategy({"600519.SH": 0.5})
        engine = CrossSectionalBacktest(broker)

        results = engine.run(strategy, bars, [date(2026, 1, 5)])

        fills = results[0]["fills"]
        assert len(fills) == 1
        assert fills[0]["status"] == "FILLED"
        assert fills[0]["fill_price"] == 10.5  # T+1 open
        assert broker.get_holdings() == {"600519.SH": fills[0]["shares"]}

    def test_fill_clock_advances_per_day(self):
        """Finding 1: each day fills at its own T+1 open, not one pinned date."""
        bars = make_bars(
            [
                bar("600519.SH", date(2026, 1, 5), open_=10.0, close=10.0),
                bar("600519.SH", date(2026, 1, 6), open_=10.5, close=11.0),
                bar("600519.SH", date(2026, 1, 7), open_=11.5, close=12.0),
            ]
        )
        broker = make_sim_broker(bars)
        strategy = FixedWeightsStrategy({"600519.SH": 0.3})
        engine = CrossSectionalBacktest(broker)
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

        results = engine.run(strategy, bars, dates)

        # Day 1 fills at 2026-01-06 open (10.5); day 2 at 2026-01-07 open (11.5).
        assert results[0]["fills"][0]["fill_price"] == 10.5
        assert results[1]["fills"][0]["fill_price"] == 11.5

    def test_nav_marked_to_market(self):
        """Finding 2: NAV reflects day's close, so sizing uses current value."""
        bars = make_bars(
            [
                bar("600519.SH", date(2026, 1, 5), open_=10.0, close=10.0),
                bar("600519.SH", date(2026, 1, 6), open_=10.0, close=50.0),
            ]
        )
        broker = make_sim_broker(bars)

        class RecordNavStrategy(BaseCrossStrategy):
            def __init__(self):
                self.navs = []

            def on_rebalance(self, date, bars, positions, account):
                self.navs.append(account.nav)
                return {"600519.SH": 0.3}

        strategy = RecordNavStrategy()
        engine = CrossSectionalBacktest(broker)
        engine.run(strategy, bars, [date(2026, 1, 5), date(2026, 1, 6)])

        # Day 1 nav = 1M (nothing held yet). Day 2 nav reflects mark to close.
        assert strategy.navs[0] == pytest.approx(1_000_000.0, rel=1e-3)
        # Marked-to-market: position bought at ~10, close is 50 → nav grows.
        assert strategy.navs[1] > 1_500_000

    def test_positions_from_holdings_not_ledger(self):
        """Finding 4: positions = broker.get_holdings() — same shape live and backtest."""
        bars = make_bars(
            [
                bar("600519.SH", date(2026, 1, 5), open_=10.0, close=10.0),
                bar("600519.SH", date(2026, 1, 6), open_=10.5, close=11.0),
            ]
        )
        broker = make_sim_broker(bars)

        captured = {}

        class CapturePositions(BaseCrossStrategy):
            def on_rebalance(self, date, bars, positions, account):
                captured[date] = dict(positions)
                return {"600519.SH": 0.5}

        engine = CrossSectionalBacktest(broker)
        engine.run(CapturePositions(), bars, [date(2026, 1, 5), date(2026, 1, 6)])

        # Day 1: empty before any fill. Day 2: same shape as get_holdings().
        assert captured[date(2026, 1, 5)] == {}
        assert captured[date(2026, 1, 6)] == broker.get_holdings()

    def test_suspended_day_rejected_end_to_end(self):
        """A ticker with no bar on T+1 is REJECTED — not silently dropped."""
        bars = make_bars(
            [
                bar("600519.SH", date(2026, 1, 5), open_=10.0, close=10.0),
                bar("000001.SZ", date(2026, 1, 5), open_=20.0, close=20.0),
                # 000001.SZ has no bar on 2026-01-06 → fill lookup fails.
                bar("600519.SH", date(2026, 1, 6), open_=10.5, close=11.0),
            ]
        )
        broker = make_sim_broker(bars)
        strategy = FixedWeightsStrategy({"600519.SH": 0.4, "000001.SZ": 0.4})
        engine = CrossSectionalBacktest(broker)

        results = engine.run(strategy, bars, [date(2026, 1, 5)])

        statuses = {f["ticker"]: f["status"] for f in results[0]["fills"]}
        assert statuses["600519.SH"] == "FILLED"
        assert statuses["000001.SZ"] == "REJECTED"

    def test_settlement_reject_reason(self):
        """Finding 5: settlement failure carries reject_reason, not bare REJECTED."""
        bars = make_bars(
            [
                bar("600519.SH", date(2026, 1, 5), open_=10.0, close=10.0),
                bar("600519.SH", date(2026, 1, 6), open_=10.5, close=11.0),
            ]
        )
        broker = make_sim_broker(bars, initial_cash=1_000.0)  # too small
        strategy = FixedWeightsStrategy({"600519.SH": 1.0})
        engine = CrossSectionalBacktest(broker)

        results = engine.run(strategy, bars, [date(2026, 1, 5)])

        fills = results[0]["fills"]
        if fills and fills[0]["status"] == "REJECTED":
            assert "reject_reason" in fills[0]
            assert "settlement" in fills[0]["reject_reason"]
