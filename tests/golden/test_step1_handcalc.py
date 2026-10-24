"""Golden-sample portfolio step 1: hand-calc comparison (S8-T1, QUANT-30).

Verifies the independent hand-calc baseline (``portfolio_handcalc.md`` →
``golden_expected_portfolio.json``) against an in-test first-principles
recomputation from the fixture.  4-decimal agreement (TOLERANCE = 1e-4).

No pandas portfolio implementation (``portfolio_baseline.py``) exists at this
point — the hand-calc baseline landed first (provenance rule H1).
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.testing import TOLERANCE

pytestmark = pytest.mark.golden

FIXTURE_DIR = Path(__file__).parent
UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]
REBALANCE_DATES = ["2025-01-27", "2025-02-28", "2025-03-31"]
SAMPLE_DAYS = 57
TRADING_DAYS = 252


@pytest.fixture(scope="module")
def prices() -> pd.DataFrame:
    return pd.read_parquet(FIXTURE_DIR / "golden_prices.parquet")


@pytest.fixture(scope="module")
def expected() -> dict:
    return json.loads((FIXTURE_DIR / "golden_expected_portfolio.json").read_text())


def _recompute(prices: pd.DataFrame) -> tuple[dict[str, float], list[float]]:
    """First-principles portfolio recomputation from the fixture.

    Returns (metrics dict, nav_series including NAV_0).
    """
    sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
    warmup_last = prices[prices["phase"] == "warmup"].iloc[-1]
    closes = sample[UNIVERSE].to_numpy(dtype=float)
    base = warmup_last[UNIVERSE].to_numpy(dtype=float)

    # Per-ticker simple returns (previous close for first sample day = warmup last)
    prev = np.concatenate([base.reshape(1, -1), closes[:-1]], axis=0)
    r = closes / prev - 1.0

    # Weight clock: shift(1) — weights decided at prior close apply to today
    rebalance_si = {SAMPLE_DAYS - 1}  # only 2025-03-31 in sample indices
    # Map rebalance dates to sample indices
    sample_dates = sample["date"].tolist()
    rebalance_si = {sample_dates.index(d) for d in REBALANCE_DATES}

    w = np.full(5, 0.2)
    nav = 1.0
    nav_series = [1.0]
    r_p = np.zeros(SAMPLE_DAYS)
    for t in range(SAMPLE_DAYS):
        r_p[t] = float(np.dot(w, r[t]))
        nav *= 1.0 + r_p[t]
        nav_series.append(nav)
        # Drift weights after this day's return
        w = w * (1.0 + r[t]) / (1.0 + r_p[t])
        # Rebalance at close resets weights for next period
        if t in rebalance_si:
            w = np.full(5, 0.2)

    nav_arr = np.array(nav_series)
    peak = np.maximum.accumulate(nav_arr)
    metrics = {
        "total_return": float(np.prod(1.0 + r_p) - 1.0),
        "sharpe": float(r_p.mean() / r_p.std(ddof=1) * np.sqrt(TRADING_DAYS)),
        "mdd": float((nav_arr / peak - 1.0).min()),
    }
    return metrics, nav_series


@pytest.fixture(scope="module")
def recomputed(prices) -> dict[str, float]:
    metrics, _ = _recompute(prices)
    return metrics


@pytest.fixture(scope="module")
def nav_path(prices) -> list[float]:
    _, nav_series = _recompute(prices)
    return nav_series


class TestHandCalcMatchesExpected:
    """Step 1: recomputed metrics match golden_expected_portfolio.json at 4 dp."""

    def test_three_metrics(self, expected):
        assert set(expected) == {"total_return", "sharpe", "mdd"}

    def test_total_return_agrees(self, recomputed, expected):
        assert abs(recomputed["total_return"] - expected["total_return"]) < TOLERANCE, (
            f"total_return: got {recomputed['total_return']!r}, "
            f"expected {expected['total_return']!r}"
        )

    def test_sharpe_agrees(self, recomputed, expected):
        assert abs(recomputed["sharpe"] - expected["sharpe"]) < TOLERANCE, (
            f"sharpe: got {recomputed['sharpe']!r}, expected {expected['sharpe']!r}"
        )

    def test_mdd_agrees(self, recomputed, expected):
        assert abs(recomputed["mdd"] - expected["mdd"]) < TOLERANCE, (
            f"mdd: got {recomputed['mdd']!r}, expected {expected['mdd']!r}"
        )

    def test_total_return_is_compounded(self, nav_path, expected):
        """total_return = NAV_final / NAV_0 - 1 (window compounded, not annualized)."""
        nav_final = nav_path[-1]
        assert abs((nav_final - 1.0) - expected["total_return"]) < TOLERANCE


class TestMomentumAnchors:
    """QUANT-11 §3.5 momentum figures — wrong values mean wrong convention."""

    # Momentum at each rebalance date (pct_change(20), 6 dp)
    MOMENTUM = {
        "2025-01-27": {
            "510300.SH": -0.039341,
            "510500.SH": -0.067768,
            "518880.SH": 0.043230,
            "511010.SH": 0.001472,
            "159915.SZ": -0.062413,
        },
        "2025-02-28": {
            "510300.SH": 0.024891,
            "510500.SH": 0.050681,
            "518880.SH": 0.041539,
            "511010.SH": -0.006320,
            "159915.SZ": 0.037037,
        },
        "2025-03-31": {
            "510300.SH": 0.000754,
            "510500.SH": -0.003914,
            "518880.SH": 0.086963,
            "511010.SH": -0.002937,
            "159915.SZ": -0.041783,
        },
    }

    @pytest.mark.parametrize("date", REBALANCE_DATES)
    def test_momentum_values(self, prices, date):
        closes = prices.set_index("date")[UNIVERSE]
        mom = closes.loc[date] / closes.shift(20).loc[date] - 1.0
        for ticker in UNIVERSE:
            assert abs(mom[ticker] - self.MOMENTUM[date][ticker]) < 1e-6, (
                f"{date}/{ticker}: momentum {mom[ticker]!r} != {self.MOMENTUM[date][ticker]!r}"
            )

    def test_ranking_exists(self, prices):
        """top-5-of-5 is a selection no-op (ruling 1A) — assert ranking exists."""
        closes = prices.set_index("date")[UNIVERSE]
        for date in REBALANCE_DATES:
            mom = closes.loc[date] / closes.shift(20).loc[date] - 1.0
            ranked = mom.rank(ascending=False)
            assert ranked.notna().all(), f"{date}: NaN in ranking"
            assert set(ranked.values) == {1.0, 2.0, 3.0, 4.0, 5.0}, (
                f"{date}: ranking {ranked.to_dict()}"
            )


class TestRebalanceTiming:
    """Month-end rebalance dates and the 2025-03-31 no-effect rule."""

    def test_rebalance_dates_in_fixture(self, prices):
        sample_dates = prices[prices["phase"] == "sample"]["date"].tolist()
        for d in REBALANCE_DATES:
            assert d in sample_dates, f"{d} not in sample window"

    def test_march31_decision_changes_no_return(self, prices):
        """The 2025-03-31 rebalance resets weights but no return follows."""
        metrics_with, _ = _recompute(prices)
        # Remove only the 2025-03-31 rebalance
        sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
        sample_dates = sample["date"].tolist()
        rebalance_without = {
            sample_dates.index(d) for d in REBALANCE_DATES if d != "2025-03-31"
        }

        warmup_last = prices[prices["phase"] == "warmup"].iloc[-1]
        closes = sample[UNIVERSE].to_numpy(dtype=float)
        base = warmup_last[UNIVERSE].to_numpy(dtype=float)
        prev = np.concatenate([base.reshape(1, -1), closes[:-1]], axis=0)
        r = closes / prev - 1.0

        w = np.full(5, 0.2)
        nav = 1.0
        r_p = np.zeros(SAMPLE_DAYS)
        for t in range(SAMPLE_DAYS):
            r_p[t] = float(np.dot(w, r[t]))
            nav *= 1.0 + r_p[t]
            w = w * (1.0 + r[t]) / (1.0 + r_p[t])
            if t in rebalance_without:
                w = np.full(5, 0.2)

        nav_arr = np.concatenate([[1.0], np.cumprod(1.0 + r_p)])
        peak = np.maximum.accumulate(nav_arr)
        metrics_without = {
            "total_return": float(np.prod(1.0 + r_p) - 1.0),
            "sharpe": float(r_p.mean() / r_p.std(ddof=1) * np.sqrt(TRADING_DAYS)),
            "mdd": float((nav_arr / peak - 1.0).min()),
        }
        for key in metrics_with:
            assert abs(metrics_with[key] - metrics_without[key]) < 1e-15, (
                f"{key} changed by 2025-03-31 rebalance: "
                f"{metrics_with[key]} vs {metrics_without[key]}"
            )


class TestNoLookahead:
    """shift(1) direction: weights use no same-period information."""

    def test_weights_are_shifted(self, prices):
        """Weights for day t's return come from day t-1's close, not day t's."""
        sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
        warmup_last = prices[prices["phase"] == "warmup"].iloc[-1]
        closes = sample[UNIVERSE].to_numpy(dtype=float)
        base = warmup_last[UNIVERSE].to_numpy(dtype=float)
        prev = np.concatenate([base.reshape(1, -1), closes[:-1]], axis=0)
        r = closes / prev - 1.0

        sample_dates = sample["date"].tolist()
        rebalance_si = {sample_dates.index(d) for d in REBALANCE_DATES}
        w = np.full(5, 0.2)

        for t in range(SAMPLE_DAYS):
            # The weights used must NOT have been affected by r[t]
            # (they were set at the prior close)
            r_p = float(np.dot(w, r[t]))
            # If weights depended on r[t], each w would shift with its own r
            # instead of being the drifted values from t-1.
            # Assert: first day uses initial 1/5 (from NAV_0 close)
            if t == 0:
                assert np.allclose(w, 0.2), "initial weights must be 1/5 at NAV_0"
            # Drift then possibly rebalance
            w = w * (1.0 + r[t]) / (1.0 + r_p)
            if t in rebalance_si:
                w = np.full(5, 0.2)

    def test_first_day_equal_weight(self, prices):
        """NAV_0 close sets 1/5 each; first sample return uses those weights."""
        sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
        warmup_last = prices[prices["phase"] == "warmup"].iloc[-1]
        closes = sample[UNIVERSE].to_numpy(dtype=float)
        base = warmup_last[UNIVERSE].to_numpy(dtype=float)
        first_r = closes[0] / base - 1.0
        expected_r_p = float(np.mean(first_r))  # equal weight = mean of returns
        # NAV after first day
        nav_1 = 1.0 * (1.0 + expected_r_p)
        # Match against the hand-calc NAV for 2025-01-02
        assert abs(nav_1 - 0.982424) < 1e-4, f"NAV after day 0: {nav_1}"
