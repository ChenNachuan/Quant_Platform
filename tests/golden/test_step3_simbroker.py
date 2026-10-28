"""Golden-sample step 3: SimBroker full chain vs the pandas reference (S8-T2).

Runs the S6->S7 main chain verbatim — ``CrossSectionalBacktest.run`` ->
``generate_orders`` -> ``broker.submit_orders`` — over the golden fixture and
compares NAV-derived metrics to ``golden_expected_portfolio.json`` at
< 0.01 % relative (ac-1).  The difference between backtest and live is
injected only at the Broker seam (ac-3, red line 4), and fills strike the T+1
open (ac-4, gap #5 ruling B).

Seams (kb_snapshot, plan 2026-10-25) — implement exactly:

* P1 chain imported verbatim; account seam is ``SimAccountAdapter``.
* P2 oracle weight clock: 1/5 target ONLY on the four decision dates
  (2024-12-31 formation + the three month-end rebalances), rebalance_freq
  ``"daily"``, zero orders AND fills on the other 54 days.  Never
  ``_select_rebalance_dates("monthly")``.
* P3 fill_basis ``"t1_open"`` raw at *fill_date* via ``field="open"``;
  ``"decision_close"`` is comparison-only; a third basis is a finding.
* P4 the fixture is closes-only, so bars carry ``open(D) := close(D-1)``
  (flat-overnight — a fixture modelling choice, NOT a third basis).  A
  synthetic ``open != close`` case proves the fill price is the open.
* P5 the comparison is fee-free on both sides — a test-side zero-fee
  ``FeeModel`` double on the SimBroker instance (fees are step 4).
* P6 ``initial_cash = 1e10``.
* P9 one ``submit_orders`` path; the dual-mode assert never touches QmtBroker.
* P10 this file only — ``execution/`` and ``research/`` are untouched.
"""

from __future__ import annotations

import inspect
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from execution.broker import (
    FILL_BASIS_DECISION_CLOSE,
    FILL_BASIS_T1_OPEN,
    VALID_FILL_BASES,
    Broker,
    SimBroker,
    create_broker,
)
from execution.fee_model import FeeModel
from execution.market_data import get_latest_price
from research.cross_sectional_backtest import (
    BaseCrossStrategy,
    CrossSectionalBacktest,
)
from tests.golden.portfolio_baseline import portfolio_baseline

pytestmark = pytest.mark.golden

FIXTURE_DIR = Path(__file__).parent
REL_TOL = 1e-4  # 0.01 %
TRADING_DAYS = 252
SAMPLE_DAYS = 57

UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]

# P2 — the oracle weight clock: 2024-12-31 formation close (weights decided
# for sample day 0) plus the three month-end rebalance closes.  Explicit, never
# derived from ``_select_rebalance_dates``.
DECISION_DATES = (
    date(2024, 12, 31),
    date(2025, 1, 27),
    date(2025, 2, 28),
    date(2025, 3, 31),
)

# P6
INITIAL_CASH = 1e10

RISK_CONFIG = {
    # The golden sample is fully invested (target weights sum to 1), so the
    # §66.3 default 0.95 total cap would block every formation order.  The
    # single-name cap (0.25 > 0.20 target) and the warn-only rules stay at
    # their defaults.
    "max_total_weight": 1.0,
}


# ---------------------------------------------------------------------------
# fixture frames
# ---------------------------------------------------------------------------


def load_prices() -> pd.DataFrame:
    return pd.read_parquet(FIXTURE_DIR / "golden_prices.parquet")


def long_bars(prices: pd.DataFrame) -> pd.DataFrame:
    """Wide closes -> the ``load_bars`` long table the chain consumes.

    P4 flat-overnight: the fixture is closes-only, so ``open(D) := close(D-1)``
    per ticker.  A T+1 open fill then strikes the hand-calc's decision-close
    price — a fixture modelling choice, not a third fill basis.  ``high``/``low``
    coincide with the close (no intraday range in the fixture); ``volume`` is a
    large constant so the warn-only liquidity checks stay non-binding.
    """
    rows: list[dict] = []
    dates = prices["date"].tolist()
    for i, raw_day in enumerate(dates):
        day = pd.Timestamp(raw_day)
        for ticker in UNIVERSE:
            close = float(prices.iloc[i][ticker])
            prev_close = float(prices.iloc[i - 1][ticker]) if i > 0 else close
            rows.append(
                {
                    "date": day,
                    "ticker": ticker,
                    "open": prev_close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": 1e12,
                }
            )
    # Continuation bar: the 2025-03-31 decision fills at T+1, which is past
    # the sample window.  Its open continues the flat-overnight rule.
    last = prices.iloc[-1]
    continuation = pd.Timestamp("2025-04-01")
    for ticker in UNIVERSE:
        close = float(last[ticker])
        rows.append(
            {
                "date": continuation,
                "ticker": ticker,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1e12,
            }
        )
    return pd.DataFrame(rows)


def run_dates(prices: pd.DataFrame) -> list[date]:
    """P2 — [2024-12-31] + the 57 sample days, and nothing else."""
    sample = prices[prices["phase"] == "sample"]["date"].tolist()
    return [date(2024, 12, 31)] + [pd.Timestamp(d).date() for d in sample]


# ---------------------------------------------------------------------------
# test doubles (P5, P9) — no execution/ or research/ change for this seam
# ---------------------------------------------------------------------------


class ZeroFeeModel(FeeModel):
    """P5 — fee-free slice of the same chain.  Fees belong to step 4."""

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        return 0.0


class OracleWeightClock(BaseCrossStrategy):
    """P2 — the hand-calc weight clock, expressed as target weights.

    Returns 1/5 each on a decision date and, on every other day, the weights
    that reproduce the current book exactly (so the order diff is empty and
    positions drift with prices between rebalances, as the oracle does).
    """

    def __init__(self, universe: list[str], decision_dates: tuple[date, ...]):
        self._universe = list(universe)
        self._decision_dates = set(decision_dates)

    @property
    def universe(self) -> list[str]:
        return list(self._universe)

    @property
    def rebalance_freq(self) -> str:
        return "daily"

    def on_rebalance(self, date_, bars, positions, account) -> dict[str, float]:
        if date_ in self._decision_dates:
            return {t: 1.0 / len(self._universe) for t in self._universe}
        # Hold: express the current share counts as a target weight.  The
        # one-share bias keeps ``floor_to_lot(w * nav / price)`` idempotent —
        # the float round-trip through the weight can land a few ulps under
        # the integer share count, and the lot floor would then drop a full
        # lot.  The bias is under one lot, so the floored target is still
        # exactly the held quantity.
        nav = account.nav
        out: dict[str, float] = {}
        for ticker in self._universe:
            shares = positions.get(ticker, 0)
            if shares <= 0:
                continue
            price = get_latest_price(bars, ticker, date_, field="close")
            if price is None:
                continue
            out[ticker] = (shares + 1.0) * price / nav
        return out


class NavRecordingSimBroker(SimBroker):
    """P5/P6 SimBroker recording the end-of-day NAV of each chain step."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._fee_model = ZeroFeeModel()
        self.nav_path: list[float] = []
        self.orders_log: list[list[dict]] = []
        self.fill_dates: list[date | str | None] = []

    def submit_orders(self, orders: list[dict]) -> list[dict]:
        results = super().submit_orders(orders)
        self.nav_path.append(float(self.get_nav()))
        self.orders_log.append(list(orders))
        self.fill_dates.append(self.fill_date)
        return results


class StubBroker(Broker):
    """Second Broker implementation — the V2 QmtBroker stand-in (P9)."""

    def __init__(self):
        self.submitted: list[list[dict]] = []

    def submit_orders(self, orders: list[dict]) -> list[dict]:
        self.submitted.append(list(orders))
        return [
            {**o, "status": "FILLED", "fill_price": o["limit_price"]} for o in orders
        ]

    def get_holdings(self) -> dict[str, int]:
        return {}

    def get_cash(self) -> float:
        return 0.0

    def get_nav(self) -> float:
        return 1.0


def chain_metrics(nav: np.ndarray) -> dict[str, float]:
    """NAV path -> the three step-1/2 metrics (same formulas, same clock)."""
    r_p = nav[1:] / nav[:-1] - 1.0
    peak = np.maximum.accumulate(nav)
    return {
        "total_return": float(nav[-1] / nav[0] - 1.0),
        "sharpe": float(r_p.mean() / r_p.std(ddof=1) * np.sqrt(TRADING_DAYS)),
        "mdd": float((nav / peak - 1.0).min()),
    }


# ---------------------------------------------------------------------------
# module-scoped chain run
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def prices() -> pd.DataFrame:
    return load_prices()


@pytest.fixture(scope="module")
def bars(prices) -> pd.DataFrame:
    return long_bars(prices)


@pytest.fixture(scope="module")
def dates(prices) -> list[date]:
    return run_dates(prices)


@pytest.fixture(scope="module")
def expected() -> dict:
    return json.loads((FIXTURE_DIR / "golden_expected_portfolio.json").read_text())


@pytest.fixture(scope="module")
def chain(bars, dates) -> dict:
    """One fee-free pass of the S6->S7 chain over the golden fixture."""
    config = {
        "initial_cash": INITIAL_CASH,
        "bars": bars,
        "fill_basis": FILL_BASIS_T1_OPEN,
        "risk": dict(RISK_CONFIG),
    }
    broker = NavRecordingSimBroker(config)
    strategy = OracleWeightClock(UNIVERSE, DECISION_DATES)
    engine = CrossSectionalBacktest(broker)
    results = engine.run(strategy, bars, dates)
    nav = np.array(broker.nav_path)
    return {
        "broker": broker,
        "strategy": strategy,
        "engine": engine,
        "results": results,
        "nav": nav,
        "metrics": chain_metrics(nav),
    }


# ---------------------------------------------------------------------------
# ac-1: step-3 full-chain deviation < 0.01 % vs the pandas reference
# ---------------------------------------------------------------------------


class TestStep3Metrics:
    """ac-1: the SimBroker chain reproduces the oracle metrics."""

    def test_three_metrics(self, chain, expected):
        assert set(expected) == {"total_return", "sharpe", "mdd"}
        for key in expected:
            assert key in chain["metrics"], f"missing metric: {key}"

    def test_total_return_within_1bp(self, chain, expected):
        got, exp = chain["metrics"]["total_return"], expected["total_return"]
        rel = abs(got - exp) / max(abs(exp), 1e-15)
        assert rel < REL_TOL, f"total_return rel_err={rel:.2e} (got {got}, exp {exp})"

    def test_sharpe_within_1bp(self, chain, expected):
        got, exp = chain["metrics"]["sharpe"], expected["sharpe"]
        rel = abs(got - exp) / max(abs(exp), 1e-15)
        assert rel < REL_TOL, f"sharpe rel_err={rel:.2e} (got {got}, exp {exp})"

    def test_mdd_within_1bp(self, chain, expected):
        got, exp = chain["metrics"]["mdd"], expected["mdd"]
        rel = abs(got - exp) / max(abs(exp), 1e-15)
        assert rel < REL_TOL, f"mdd rel_err={rel:.2e} (got {got}, exp {exp})"

    def test_total_return_matches_nav_path(self, chain):
        nav = chain["nav"]
        assert abs(chain["metrics"]["total_return"] - (nav[-1] / nav[0] - 1.0)) < 1e-12

    def test_nav_path_has_one_point_per_chain_day(self, chain, dates):
        # NAV_0 at close(2024-12-31) through NAV_57 at close(2025-03-31).
        assert len(chain["nav"]) == len(dates) == SAMPLE_DAYS + 1

    def test_fee_free_on_both_sides(self, chain):
        """P5 — the < 0.01 % comparison is fee-free; fees are step 4.

        The double is load-bearing: the landed FeeModel bills a non-zero
        amount on the same fill, and charging it would blow the 1e-4 budget.
        """
        assert isinstance(chain["broker"]._fee_model, ZeroFeeModel)
        fill = next(
            f
            for r in chain["results"]
            for f in r["fills"]
            if f.get("status") == "FILLED"
        )
        landed = FeeModel()
        charged = landed.compute(
            fill["side"], fill["fill_price"], fill["shares"], fill["ticker"]
        )
        assert charged > 0.0, "the landed FeeModel charges — the double must zero it"
        assert (
            chain["broker"]._fee_model.compute(
                fill["side"], fill["fill_price"], fill["shares"], fill["ticker"]
            )
            == 0.0
        )


# ---------------------------------------------------------------------------
# P2: the oracle weight clock — 1/5 on four decision dates only
# ---------------------------------------------------------------------------


class TestOracleWeightClock:
    def test_decision_dates_are_the_explicit_four(self, dates):
        """The clock is the formation close + three month-end closes."""
        assert set(DECISION_DATES) == {
            date(2024, 12, 31),
            date(2025, 1, 27),
            date(2025, 2, 28),
            date(2025, 3, 31),
        }
        assert all(d in dates for d in DECISION_DATES)
        assert len(dates) == SAMPLE_DAYS + 1

    def test_never_monthly_day_selection(self, dates):
        """P2 — the decision set is not ``_select_rebalance_dates('monthly')``.

        Monthly keeps the *first* trading day of each calendar month, which
        here is 2024-12-31 / 2025-01-02 / 2025-02-05 / 2025-03-03 — a
        different clock that would move every fill and break the comparison.
        """
        from research.cross_sectional_backtest import _select_rebalance_dates

        monthly = set(_select_rebalance_dates(dates, "monthly"))
        assert monthly != set(DECISION_DATES)
        daily = _select_rebalance_dates(dates, "daily")
        assert daily == dates

    def test_rebalance_freq_is_daily(self, chain):
        assert chain["strategy"].rebalance_freq == "daily"

    def test_equal_weight_target_on_decision_dates(self, chain):
        for result in chain["results"]:
            if result["date"] in DECISION_DATES:
                weights = result["weights"]
                assert set(weights) == set(UNIVERSE), (
                    f"{result['date']}: target {weights}"
                )
                for ticker, w in weights.items():
                    assert abs(w - 1.0 / len(UNIVERSE)) < 1e-15, (
                        f"{result['date']} {ticker}: {w} != 1/5"
                    )

    def test_zero_orders_and_fills_off_decision_dates(self, chain):
        decision = set(DECISION_DATES)
        off = [r for r in chain["results"] if r["date"] not in decision]
        assert len(off) == SAMPLE_DAYS + 1 - len(DECISION_DATES)
        for result in off:
            assert result["orders"] == [], (
                f"{result['date']}: unexpected orders {result['orders']}"
            )
            assert result["fills"] == [], (
                f"{result['date']}: unexpected fills {result['fills']}"
            )

    def test_decision_dates_do_trade(self, chain):
        """The four decision dates are the only days the clock acts on."""
        for result in chain["results"]:
            if result["date"] in DECISION_DATES:
                assert len(result["orders"]) > 0, f"{result['date']}: no orders"
                filled = [f for f in result["fills"] if f.get("status") == "FILLED"]
                assert filled, f"{result['date']}: no fills"

    def test_no_rejected_fills_in_window(self, chain):
        """Every order the clock raises in the sample window executes."""
        rejected = [
            f
            for r in chain["results"]
            for f in r["fills"]
            if f.get("status") == "REJECTED"
        ]
        assert rejected == [], f"rejected fills: {rejected}"

    def test_return_path_matches_pandas(self, chain, prices):
        """ac-1 — the chain's daily returns track the pandas reference.

        This is the step-3 comparison itself (SimBroker full chain vs pandas):
        equal metric tails can hide an offsetting path error, so the per-day
        returns are checked too.  Drift between rebalances is what makes the
        path sensitive to the weight clock.
        """
        oracle = portfolio_baseline(prices)
        nav = chain["nav"]
        r_chain = nav[1:] / nav[:-1] - 1.0
        r_oracle = np.asarray(oracle["portfolio_returns"], dtype=float)
        assert len(r_chain) == len(r_oracle) == SAMPLE_DAYS
        max_abs = float(np.max(np.abs(r_chain - r_oracle)))
        assert max_abs < 1e-6, f"daily return path diverges: max|Δ|={max_abs:.2e}"

    def test_hold_weights_track_the_drifted_book(self, chain):
        """Off decision dates the returned weights follow the held book."""
        decision = set(DECISION_DATES)
        for result in chain["results"]:
            if result["date"] in decision:
                continue
            weights = result["weights"]
            assert weights, f"{result['date']}: hold weights missing"
            assert abs(sum(weights.values()) - 1.0) < 1e-6, (
                f"{result['date']}: hold weights sum to {sum(weights.values())}"
            )


# ---------------------------------------------------------------------------
# ac-3: one submit_orders path, only the Broker impl differs (red line 4)
# ---------------------------------------------------------------------------


class TestDualModeSamePath:
    def test_engine_uses_the_landed_chain(self, chain):
        """P1 — the chain is imported verbatim, not re-derived here."""
        import research.cross_sectional_backtest as mod

        source = inspect.getsource(mod)
        assert "from execution.order_generation import" in source
        assert "from execution.market_data import" in source
        assert "def generate_orders" not in source

    def test_engine_source_has_one_submit_orders_path(self):
        """The engine calls ``submit_orders`` and never branches on a mode."""
        source = inspect.getsource(CrossSectionalBacktest.run)
        assert source.count("submit_orders") == 1
        for branch in ("if mode", "isinstance", "create_broker", "fill_basis"):
            assert branch not in source, (
                f"engine branches on {branch!r} — the difference belongs in "
                "the Broker implementation (red line 4)"
            )

    def test_two_broker_impls_share_the_same_call_path(self, bars, dates):
        """Backtest and live differ only in the Broker instance injected."""
        config = {
            "initial_cash": INITIAL_CASH,
            "bars": bars,
            "fill_basis": FILL_BASIS_T1_OPEN,
            "risk": dict(RISK_CONFIG),
        }
        sim = NavRecordingSimBroker(config)
        stub = StubBroker()

        for broker in (sim, stub):
            engine = CrossSectionalBacktest(broker)
            engine.run(OracleWeightClock(UNIVERSE, DECISION_DATES), bars, dates)

        # Same call surface: each broker was driven through submit_orders by
        # the same engine loop, once per chain day.
        assert sim.orders_log and stub.submitted
        assert len(sim.orders_log) == len(stub.submitted) == len(dates)
        sample = sim.orders_log[0] + stub.submitted[0]
        for order in sample:
            assert set(order) >= {"ticker", "side", "shares", "limit_price"}

    def test_create_broker_injects_the_implementation_only(self, bars):
        """The factory selects the Broker impl — not a code path."""
        config = {
            "initial_cash": INITIAL_CASH,
            "bars": bars,
            "fill_basis": FILL_BASIS_T1_OPEN,
            "risk": dict(RISK_CONFIG),
        }
        sim = create_broker("sim", config)
        assert isinstance(sim, SimBroker)
        assert isinstance(sim, Broker)
        with pytest.raises(NotImplementedError):
            create_broker("qmt", config)


# ---------------------------------------------------------------------------
# ac-4: fill basis is T+1 open (gap #5 ruling B), never a third basis
# ---------------------------------------------------------------------------


def _basis_bars(open_: float, close: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp(date(2026, 1, 5)),
                "ticker": "510300.SH",
                "open": open_,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1e12,
            },
            {
                "date": pd.Timestamp(date(2026, 1, 6)),
                "ticker": "510300.SH",
                "open": open_,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1e12,
            },
        ]
    )


def _basis_config(**overrides) -> dict:
    """Minimal SimBroker config for a fill-price probe.

    ``min_trade_value`` is zeroed so the tiny probe order is not rule-4
    skipped — the point of these cases is which *price* a fill strikes, not
    the order-size floor.
    """
    config = {
        "initial_cash": INITIAL_CASH,
        "bars": _basis_bars(open_=10.5, close=11.0),
        "fill_date": date(2026, 1, 6),
        "decision_date": date(2026, 1, 5),
        "risk": {"min_trade_value": 0.0},
    }
    config.update(overrides)
    return config


def _basis_order() -> dict:
    return {"ticker": "510300.SH", "side": "BUY", "shares": 100, "limit_price": 10.0}


class TestFillBasis:
    def test_t1_open_is_the_gating_default(self):
        """ac-4 — the default fill basis is ``t1_open``, not ``decision_close``."""
        assert VALID_FILL_BASES[0] == FILL_BASIS_T1_OPEN
        config = _basis_config()
        assert "fill_basis" not in config
        broker = SimBroker(config)  # no fill_basis key
        results = broker.submit_orders([_basis_order()])
        assert results[0]["status"] == "FILLED"
        assert results[0]["fill_price"] == 10.5  # the open, not the close

    def test_open_not_close_on_a_split_bar(self):
        """ac-4 — a basis mixup cannot hide behind an ``open == close`` fixture.

        P4's flat-overnight bars make open and close coincide at the fill
        date, so an outright basis mixup would look correct.  This synthetic
        bar splits them and asserts the fill is the open at the fill date.
        """
        bars = _basis_bars(open_=10.5, close=11.0)
        broker = SimBroker(_basis_config(bars=bars, fill_basis=FILL_BASIS_T1_OPEN))
        results = broker.submit_orders([_basis_order()])

        fill = results[0]
        assert fill["status"] == "FILLED"
        open_at_fill = get_latest_price(bars, "510300.SH", date(2026, 1, 6), "open")
        close_at_fill = get_latest_price(bars, "510300.SH", date(2026, 1, 6), "close")
        assert open_at_fill == 10.5 and close_at_fill == 11.0
        assert open_at_fill != close_at_fill, "the control case needs a split bar"
        assert fill["fill_price"] == open_at_fill, (
            f"fill_price {fill['fill_price']} != open {open_at_fill}"
        )
        assert fill["fill_price"] != close_at_fill

    def test_golden_bars_are_flat_overnight(self, bars):
        """P4 — the fixture's opens are modelled as the prior close.

        This is a fixture modelling choice, not a third fill basis: the
        chain's basis rule is asserted separately, on a split bar.
        """
        for ticker in UNIVERSE:
            series = bars[bars["ticker"] == ticker].reset_index(drop=True)
            opens = series["open"].to_numpy(dtype=float)
            closes = series["close"].to_numpy(dtype=float)
            # open(D) == close(D-1) for every bar after the first.
            assert np.allclose(opens[1:], closes[:-1]), (
                f"{ticker}: open(D) is not close(D-1) — P4 flat-overnight broken"
            )

    def test_decision_close_is_comparison_only(self):
        """ac-4 — ``decision_close`` prices off the decision close, for control.

        It is a comparison arm, never the gating basis: ``t1_open`` is what
        the chain runs and what the < 0.01 % gate measures.
        """
        assert FILL_BASIS_DECISION_CLOSE in VALID_FILL_BASES
        assert FILL_BASIS_DECISION_CLOSE != FILL_BASIS_T1_OPEN
        bars = _basis_bars(open_=10.5, close=11.0)
        broker = SimBroker(
            _basis_config(bars=bars, fill_basis=FILL_BASIS_DECISION_CLOSE)
        )
        results = broker.submit_orders([_basis_order()])
        assert results[0]["status"] == "FILLED"
        # close at the decision date — the comparison arm's definition.
        assert results[0]["fill_price"] == 11.0

    def test_exactly_two_bases(self):
        """ac-4 — a third basis is a finding, not an extension point."""
        assert tuple(VALID_FILL_BASES) == (
            FILL_BASIS_T1_OPEN,
            FILL_BASIS_DECISION_CLOSE,
        )

    def test_third_basis_refused(self):
        with pytest.raises(ValueError, match="fill_basis"):
            SimBroker(_basis_config(fill_basis="vwap"))

    def test_chain_fills_are_raw_lookup_prices(self, chain):
        """ac-4/P3 — the recorded fill price is the raw open at the fill date.

        Never a slippage-skewed price: FeeModel bills slippage on top of this
        raw number (QUANT-26), so a pre-skewed fill price would charge it
        twice.
        """
        broker = chain["broker"]
        bars = broker._bars
        for day_index, result in enumerate(chain["results"]):
            fill_date = broker.fill_dates[day_index]
            for fill in result["fills"]:
                if fill.get("status") != "FILLED":
                    continue
                raw = get_latest_price(bars, fill["ticker"], fill_date, "open")
                assert raw is not None
                assert abs(fill["fill_price"] - raw) < 1e-12, (
                    f"{fill['ticker']} @ {fill_date}: fill_price "
                    f"{fill['fill_price']} != raw open {raw}"
                )
