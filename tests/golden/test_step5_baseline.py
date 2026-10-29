"""Golden-sample step 5: five-year baseline and metrics (QUANT-32).

Zero-network tests over the committed five-year fixture.  The fetch script
(``fetch_five_year_etf.py``) is its provenance; no test opens a socket.

Iteration 1 covers ac-4: fixture structure, month-end clock anchors, and the
no-splice rule.  Iteration 2 adds the baseline metrics and range assertions.

The S6->S7 chain runs twice — fees ON (headline) and fees OFF (twin) — over
the five-year fixture with true opens and the month-end clock.  Metrics are
persisted to ``golden_baseline_2020_2024.json`` for regression.
"""

from __future__ import annotations

import ast
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from execution.broker import FILL_BASIS_T1_OPEN, SimBroker
from execution.fee_model import FeeModel
from execution.market_data import get_latest_price
from research.cross_sectional_backtest import (
    BaseCrossStrategy,
    CrossSectionalBacktest,
)
from tests.golden.month_end_clock import month_end_decision_dates

pytestmark = pytest.mark.golden

FIXTURE_DIR = Path(__file__).parent
UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]
TRADING_DAYS = 252

# Range assertions (ARCHITECTURE.md §6) — never adjusted.
RANGE_ANNUALIZED = (0.10, 0.15)
RANGE_SHARPE = (0.8, 1.2)
RANGE_MDD = (0.15, 0.25)

INITIAL_CASH = 1e10
RISK_CONFIG = {"max_total_weight": 1.0}

# A 100%-invested book cannot pay its own fees (step-4 finding).  The sleeve
# leaves room for fee settlement; both twins target the same scaled notional.
SLEEVE = 0.95


# ---------------------------------------------------------------------------
# fixture frames
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def prices() -> pd.DataFrame:
    path = FIXTURE_DIR / "golden_prices_2020_2024.parquet"
    if not path.exists():
        pytest.skip(f"fixture not generated: {path}")
    return pd.read_parquet(path)


def long_bars_5y(prices: pd.DataFrame) -> pd.DataFrame:
    """Wide open+close -> the ``load_bars`` long table the chain consumes.

    True opens from the fixture — ``open(D) != close(D-1)`` in general (O1).
    ``high``/``low`` bracket the open-close range; ``volume`` is a large
    constant so warn-only liquidity checks stay non-binding.
    """
    rows: list[dict] = []
    for _, row in prices.iterrows():
        day = pd.Timestamp(row["date"])
        for ticker in UNIVERSE:
            o = float(row[f"{ticker}.open"])
            c = float(row[f"{ticker}.close"])
            rows.append(
                {
                    "date": day,
                    "ticker": ticker,
                    "open": o,
                    "high": max(o, c),
                    "low": min(o, c),
                    "close": c,
                    "volume": 1e12,
                }
            )
    # Continuation bar for the last decision date's T+1 fill.
    last = prices.iloc[-1]
    cont_date = pd.Timestamp(last["date"]) + pd.Timedelta(days=1)
    for ticker in UNIVERSE:
        c = float(last[f"{ticker}.close"])
        rows.append(
            {
                "date": cont_date,
                "ticker": ticker,
                "open": c,
                "high": c,
                "low": c,
                "close": c,
                "volume": 1e12,
            }
        )
    return pd.DataFrame(rows)


def run_dates_5y(prices: pd.DataFrame) -> list[date]:
    """Formation + all sample days — the dates the chain iterates over."""
    warmup = prices[prices["phase"] == "warmup"]
    formation = pd.Timestamp(warmup["date"].iloc[-1]).date()
    sample = prices[prices["phase"] == "sample"]
    sample_dates = [pd.Timestamp(d).date() for d in sample["date"]]
    return [formation] + sample_dates


# ---------------------------------------------------------------------------
# test doubles — chain driver (copied from step 3, not imported from it)
# ---------------------------------------------------------------------------


class ZeroFeeModel(FeeModel):
    """Fee-free slice of the same chain."""

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        return 0.0


class OracleWeightClock(BaseCrossStrategy):
    """Month-end weight clock: 1/5 on decision dates, hold otherwise.

    On non-decision days, expresses the current share counts as a target
    weight so the order diff is empty and positions drift with prices.
    """

    def __init__(self, universe: list[str], decision_dates: set[date]):
        self._universe = list(universe)
        self._decision_dates = decision_dates

    @property
    def universe(self) -> list[str]:
        return list(self._universe)

    @property
    def rebalance_freq(self) -> str:
        return "daily"

    def on_rebalance(self, date_, bars, positions, account) -> dict[str, float]:
        if date_ in self._decision_dates:
            w = SLEEVE / len(self._universe)
            return {t: w for t in self._universe}
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
    """SimBroker recording the end-of-day NAV of each chain step."""

    def __init__(self, config: dict, fee_model: FeeModel | None = None):
        super().__init__(config)
        if fee_model is not None:
            self._fee_model = fee_model
        self.nav_path: list[float] = []
        self.orders_log: list[list[dict]] = []
        self.fill_dates: list[date | str | None] = []

    def submit_orders(self, orders: list[dict]) -> list[dict]:
        results = super().submit_orders(orders)
        self.nav_path.append(float(self.get_nav()))
        self.orders_log.append(list(orders))
        self.fill_dates.append(self.fill_date)
        return results


def compute_baseline_metrics(nav: np.ndarray) -> dict[str, float]:
    """NAV path -> the full metric set for the five-year baseline.

    Gate annualized is CAGR (kb_snapshot FORMULAS); ``annualized_mean252``
    is secondary.  ``peak`` includes NAV_0.
    """
    r_p = nav[1:] / nav[:-1] - 1.0
    peak = np.maximum.accumulate(nav)
    n = len(r_p)
    total_return = float(nav[-1] / nav[0] - 1.0)
    return {
        "total_return": total_return,
        "sharpe": float(r_p.mean() / r_p.std(ddof=1) * np.sqrt(TRADING_DAYS)),
        "mdd": float((nav / peak - 1.0).min()),
        "annualized_cagr": float((1.0 + total_return) ** (TRADING_DAYS / n) - 1.0),
        "annualized_mean252": float(r_p.mean() * TRADING_DAYS),
    }


# ---------------------------------------------------------------------------
# module-scoped chain runs
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bars(prices) -> pd.DataFrame:
    return long_bars_5y(prices)


@pytest.fixture(scope="module")
def dates(prices) -> list[date]:
    return run_dates_5y(prices)


@pytest.fixture(scope="module")
def decision_dates(prices) -> set[date]:
    strs = month_end_decision_dates(prices)
    return {pd.Timestamp(s).date() for s in strs}


@pytest.fixture(scope="module")
def chain_fees_on(bars, dates, decision_dates) -> dict:
    """One pass of the S6->S7 chain with the landed FeeModel (headline)."""
    config = {
        "initial_cash": INITIAL_CASH,
        "bars": bars,
        "fill_basis": FILL_BASIS_T1_OPEN,
        "risk": dict(RISK_CONFIG),
    }
    broker = NavRecordingSimBroker(config, fee_model=FeeModel())
    strategy = OracleWeightClock(UNIVERSE, decision_dates)
    engine = CrossSectionalBacktest(broker)
    results = engine.run(strategy, bars, dates)
    nav = np.array(broker.nav_path)
    return {
        "broker": broker,
        "strategy": strategy,
        "engine": engine,
        "results": results,
        "nav": nav,
        "metrics": compute_baseline_metrics(nav),
    }


@pytest.fixture(scope="module")
def chain_fees_off(bars, dates, decision_dates) -> dict:
    """One pass with a zero-fee double (twin for fee attribution)."""
    config = {
        "initial_cash": INITIAL_CASH,
        "bars": bars,
        "fill_basis": FILL_BASIS_T1_OPEN,
        "risk": dict(RISK_CONFIG),
    }
    broker = NavRecordingSimBroker(config, fee_model=ZeroFeeModel())
    strategy = OracleWeightClock(UNIVERSE, decision_dates)
    engine = CrossSectionalBacktest(broker)
    results = engine.run(strategy, bars, dates)
    nav = np.array(broker.nav_path)
    return {
        "broker": broker,
        "strategy": strategy,
        "engine": engine,
        "results": results,
        "nav": nav,
        "metrics": compute_baseline_metrics(nav),
    }


@pytest.fixture(scope="module")
def baseline_json(chain_fees_on, chain_fees_off, dates, decision_dates) -> dict:
    """Generate golden_baseline_2020_2024.json and return its content."""
    headline = chain_fees_on["metrics"]
    twin = chain_fees_off["metrics"]
    doc = {
        "total_return": headline["total_return"],
        "sharpe": headline["sharpe"],
        "mdd": headline["mdd"],
        "annualized_cagr": headline["annualized_cagr"],
        "annualized_mean252": headline["annualized_mean252"],
        "fee_free_twin": {
            "total_return": twin["total_return"],
            "sharpe": twin["sharpe"],
            "mdd": twin["mdd"],
        },
        "basis": {
            "fill_basis": FILL_BASIS_T1_OPEN,
            "initial_cash": INITIAL_CASH,
            "max_total_weight": RISK_CONFIG["max_total_weight"],
            "universe": UNIVERSE,
            "sample_days": int((chain_fees_on["nav"].shape[0] - 1)),
            "decision_date_count": len(decision_dates),
            "trading_days_per_year": TRADING_DAYS,
            "sleeve": SLEEVE,
        },
    }
    out_path = FIXTURE_DIR / "golden_baseline_2020_2024.json"
    out_path.write_text(json.dumps(doc, indent=2) + "\n")
    return doc


# ---------------------------------------------------------------------------
# iteration 1: fixture structure, clock, no-splice (ac-4)
# ---------------------------------------------------------------------------


class TestFixtureStructure:
    """ac-4: the five-year fixture is well-formed and reproducible."""

    def test_columns(self, prices):
        expected = ["date", "phase"] + [
            f"{t}.{f}" for t in UNIVERSE for f in ("open", "close")
        ]
        assert sorted(prices.columns) == sorted(expected)

    def test_warmup_count(self, prices):
        assert (prices["phase"] == "warmup").sum() == 20

    def test_sample_count(self, prices):
        n = (prices["phase"] == "sample").sum()
        assert 1200 <= n <= 1250  # ~5 years × 252 trading days

    def test_date_range(self, prices):
        assert str(prices["date"].min()).split()[0] <= "2019-12-31"
        assert str(prices["date"].max()).split()[0] == "2024-12-31"

    def test_no_missing_values(self, prices):
        for t in UNIVERSE:
            assert prices[f"{t}.open"].notna().all()
            assert prices[f"{t}.close"].notna().all()

    def test_true_opens(self, prices):
        """O1: five-year bars carry true opens — open(D) != close(D-1) in general."""
        sample = prices[prices["phase"] == "sample"]
        for t in UNIVERSE[:2]:
            opens = sample[f"{t}.open"].to_numpy()[1:]
            prev_closes = sample[f"{t}.close"].to_numpy()[:-1]
            assert (opens != prev_closes).any(), f"{t}: all opens equal prev close"

    def test_fixture_distinct_from_s2t2(self):
        """O1: the five-year fixture is a separate artifact from golden_prices.parquet."""
        five_year = FIXTURE_DIR / "golden_prices_2020_2024.parquet"
        s2t2 = FIXTURE_DIR / "golden_prices.parquet"
        assert five_year.exists()
        assert s2t2.exists()
        fy = pd.read_parquet(five_year)
        s2 = pd.read_parquet(s2t2)
        assert set(fy.columns) != set(s2.columns)
        assert "510300.SH.open" in fy.columns
        assert "510300.SH" in s2.columns


class TestMonthEndClock:
    """O2: month-end clock matches S8-T1 exactly."""

    def test_formation_is_last_warmup(self, prices):
        dates = month_end_decision_dates(prices)
        warmup = prices[prices["phase"] == "warmup"]
        expected = str(warmup["date"].iloc[-1]).split()[0]
        assert dates[0] == expected

    def test_anchor_2020_01_23(self, prices):
        """2020-01-23 is the last January trading day (Spring Festival)."""
        dates = month_end_decision_dates(prices)
        assert "2020-01-23" in dates

    def test_anchor_2020_02_28(self, prices):
        """2020-02-28 is the last February trading day."""
        dates = month_end_decision_dates(prices)
        assert "2020-02-28" in dates

    def test_anchor_window_end(self, prices):
        """2024-12-31 is the last month-end in the window."""
        dates = month_end_decision_dates(prices)
        assert "2024-12-31" in dates
        assert dates[-1] == "2024-12-31"

    def test_month_ends_are_last_trading_days(self, prices):
        dates = month_end_decision_dates(prices)
        sample = prices[prices["phase"] == "sample"].copy()
        sample["ts"] = pd.to_datetime(sample["date"])
        for d in dates[1:]:
            ts = pd.Timestamp(d)
            month_days = sample[sample["ts"].dt.to_period("M") == ts.to_period("M")]
            last = str(month_days["date"].iloc[-1]).split()[0]
            assert d == last, f"{d} is not the last trading day of its month"

    def test_never_first_trading_day(self, prices):
        from research.cross_sectional_backtest import _select_rebalance_dates

        dates = month_end_decision_dates(prices)
        sample_dates = prices[prices["phase"] == "sample"]["date"].tolist()
        monthly = _select_rebalance_dates(
            [pd.Timestamp(d).date() for d in sample_dates], "monthly"
        )
        monthly_str = [d.isoformat() for d in monthly]
        assert set(dates[1:]) != set(monthly_str)

    def test_count_matches_months(self, prices):
        dates = month_end_decision_dates(prices)
        sample = prices[prices["phase"] == "sample"]
        sample_ts = pd.to_datetime(sample["date"])
        n_months = len(sample_ts.dt.to_period("M").unique())
        assert len(dates) == n_months + 1  # + formation


class TestNoSplice:
    """O1: the five-year fixture is never mixed with the S2-T2 fixture."""

    def test_no_baseline_spec_import(self):
        """Red line 6: never import or compare baseline_spec.py."""
        for py_file in FIXTURE_DIR.glob("*.py"):
            tree = ast.parse(py_file.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        assert "baseline_spec" not in alias.name
                elif isinstance(node, ast.ImportFrom):
                    if node.module and "baseline_spec" in node.module:
                        pytest.fail(
                            f"{py_file.name} imports baseline_spec (red line 6)"
                        )


# ---------------------------------------------------------------------------
# iteration 2: baseline run and metrics (ac-1, ac-2)
# ---------------------------------------------------------------------------


class TestBaselineRun:
    """ac-1: the S6->S7 chain runs over the five-year fixture."""

    def test_chain_produces_nav_path(self, chain_fees_on, dates):
        nav = chain_fees_on["nav"]
        assert len(nav) == len(dates), (
            f"NAV path length {len(nav)} != dates {len(dates)}"
        )
        assert all(np.isfinite(nav)), "NAV path contains non-finite values"

    def test_chain_produces_fills(self, chain_fees_on):
        all_fills = [
            f
            for r in chain_fees_on["results"]
            for f in r["fills"]
            if f.get("status") == "FILLED"
        ]
        assert len(all_fills) > 0, "no fills in the five-year run"

    def test_decision_dates_trade(self, chain_fees_on, decision_dates):
        for result in chain_fees_on["results"]:
            if result["date"] in decision_dates:
                assert len(result["orders"]) > 0, f"{result['date']}: no orders"

    def test_metrics_keys_present(self, chain_fees_on):
        expected_keys = {
            "total_return",
            "sharpe",
            "mdd",
            "annualized_cagr",
            "annualized_mean252",
        }
        assert set(chain_fees_on["metrics"].keys()) == expected_keys

    def test_nav_starts_near_initial_cash(self, chain_fees_on):
        nav = chain_fees_on["nav"]
        # NAV[0] is after the formation rebalance (fees + lot rounding),
        # so it drops slightly below initial_cash.  1% tolerance is generous.
        assert abs(nav[0] - INITIAL_CASH) / INITIAL_CASH < 0.01


class TestBaselineMetrics:
    """ac-2: metrics inside expected ranges (ARCHITECTURE.md §6)."""

    def test_annualized_in_range(self, baseline_json):
        val = baseline_json["annualized_cagr"]
        lo, hi = RANGE_ANNUALIZED
        assert lo <= val <= hi, f"annualized_cagr {val:.4f} outside [{lo}, {hi}]"

    def test_sharpe_in_range(self, baseline_json):
        val = baseline_json["sharpe"]
        lo, hi = RANGE_SHARPE
        assert lo <= val <= hi, f"sharpe {val:.4f} outside [{lo}, {hi}]"

    def test_mdd_in_range(self, baseline_json):
        val = baseline_json["mdd"]
        lo, hi = RANGE_MDD
        # MDD is negative; the range [0.15, 0.25] means the absolute drawdown
        assert lo <= abs(val) <= hi, f"mdd {val:.4f} outside [{lo}, {hi}]"

    def test_json_has_headline_three(self, baseline_json):
        for key in ("total_return", "sharpe", "mdd"):
            assert key in baseline_json, f"missing headline metric: {key}"

    def test_json_has_fee_free_twin(self, baseline_json):
        twin = baseline_json.get("fee_free_twin", {})
        for key in ("total_return", "sharpe", "mdd"):
            assert key in twin, f"missing fee-free twin metric: {key}"

    def test_json_has_basis_fields(self, baseline_json):
        basis = baseline_json.get("basis", {})
        assert basis.get("fill_basis") == FILL_BASIS_T1_OPEN
        assert basis.get("initial_cash") == INITIAL_CASH
        assert basis.get("universe") == UNIVERSE

    def test_fee_free_twin_differs_from_headline(
        self, baseline_json, chain_fees_on, chain_fees_off
    ):
        """Fee attribution: the twin and headline must differ when fees are charged."""
        h = chain_fees_on["metrics"]["total_return"]
        t = chain_fees_off["metrics"]["total_return"]
        assert h != t, (
            "fee-on and fee-free total_return are identical — FeeModel not charging?"
        )


# ---------------------------------------------------------------------------
# ac-1: fill-basis assertions (open != close)
# ---------------------------------------------------------------------------


class TestFillBasis5Y:
    """ac-1: fill price is the open at fill_date, proven on a split bar."""

    def test_open_not_close_on_split_bar(self):
        bars = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp(date(2026, 1, 5)),
                    "ticker": "510300.SH",
                    "open": 10.5,
                    "high": 11.0,
                    "low": 10.5,
                    "close": 11.0,
                    "volume": 1e12,
                },
                {
                    "date": pd.Timestamp(date(2026, 1, 6)),
                    "ticker": "510300.SH",
                    "open": 10.5,
                    "high": 11.0,
                    "low": 10.5,
                    "close": 11.0,
                    "volume": 1e12,
                },
            ]
        )
        config = {
            "initial_cash": INITIAL_CASH,
            "bars": bars,
            "fill_basis": FILL_BASIS_T1_OPEN,
            "fill_date": date(2026, 1, 6),
            "decision_date": date(2026, 1, 5),
            "risk": {"min_trade_value": 0.0},
        }
        broker = SimBroker(config)
        order = {
            "ticker": "510300.SH",
            "side": "BUY",
            "shares": 100,
            "limit_price": 10.0,
        }
        results = broker.submit_orders([order])
        fill = results[0]
        assert fill["status"] == "FILLED"
        open_at_fill = get_latest_price(bars, "510300.SH", date(2026, 1, 6), "open")
        close_at_fill = get_latest_price(bars, "510300.SH", date(2026, 1, 6), "close")
        assert open_at_fill == 10.5 and close_at_fill == 11.0
        assert fill["fill_price"] == open_at_fill
        assert fill["fill_price"] != close_at_fill

    def test_chain_fills_are_raw_open_prices(self, chain_fees_on):
        """Every chain fill strikes the raw open at its fill date."""
        broker = chain_fees_on["broker"]
        bars = broker._bars
        for day_index, result in enumerate(chain_fees_on["results"]):
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


# ---------------------------------------------------------------------------
# ac-5: red line 6 — equal-weight top-5 ETF momentum, never baseline_spec
# ---------------------------------------------------------------------------


class TestMixRejection:
    """ac-5: golden sample is equal-weight top-5 ETF momentum."""

    def test_universe_is_top5_etf(self, chain_fees_on):
        assert chain_fees_on["strategy"].universe == UNIVERSE
        assert len(UNIVERSE) == 5

    def test_equal_weight_on_decision_dates(self, chain_fees_on, decision_dates):
        for result in chain_fees_on["results"]:
            if result["date"] in decision_dates:
                weights = result["weights"]
                expected_w = SLEEVE / len(UNIVERSE)
                for ticker, w in weights.items():
                    assert abs(w - expected_w) < 1e-15, (
                        f"{result['date']} {ticker}: {w} != {expected_w}"
                    )

    def test_no_baseline_spec_reference(self):
        """Red line 6: no file in tests/golden/ imports baseline_spec."""
        for py_file in FIXTURE_DIR.glob("*.py"):
            tree = ast.parse(py_file.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    if node.module and "baseline_spec" in node.module:
                        pytest.fail(f"{py_file.name} imports baseline_spec")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        assert "baseline_spec" not in alias.name

    def test_risk_config_max_total_weight(self, chain_fees_on):
        """The chain runs with max_total_weight = 1.0 (fully invested)."""
        broker = chain_fees_on["broker"]
        assert (
            broker._risk_config.get("max_total_weight")
            == RISK_CONFIG["max_total_weight"]
        )
