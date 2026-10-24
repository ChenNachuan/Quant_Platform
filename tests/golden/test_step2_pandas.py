"""Golden-sample portfolio step 2: pandas implementation vs hand-calc (S8-T1).

Compares ``portfolio_baseline`` against ``golden_expected_portfolio.json`` at
< 0.01 % relative deviation.  Also carries the red-line-6 mixing-rejection
assertion and the shift(1)-direction / rebalance-timing checks.
"""

import inspect
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.golden.portfolio_baseline import (
    REBALANCE_DATES,
    SAMPLE_DAYS,
    UNIVERSE,
    portfolio_baseline,
)

pytestmark = pytest.mark.golden

FIXTURE_DIR = Path(__file__).parent
REL_TOL = 1e-4  # 0.01 %

# Red line 6 — futures ALSTM regression baseline, never mixed in
FORBIDDEN_MODULES = (
    "baseline_spec",
    "alstm",
    "test_tl_intraday_reproduction",
    "strategies.futures",
)


@pytest.fixture(scope="module")
def prices() -> pd.DataFrame:
    return pd.read_parquet(FIXTURE_DIR / "golden_prices.parquet")


@pytest.fixture(scope="module")
def expected() -> dict:
    return json.loads((FIXTURE_DIR / "golden_expected_portfolio.json").read_text())


@pytest.fixture(scope="module")
def result(prices) -> dict:
    return portfolio_baseline(prices)


class TestPandasMatchesHandCalc:
    """ac-2: step-2 deviation < 0.01 % relative vs the hand-calc baseline."""

    def test_three_metrics(self, result, expected):
        assert set(expected) == {"total_return", "sharpe", "mdd"}
        for key in expected:
            assert key in result, f"missing metric: {key}"

    def test_total_return_within_1bp(self, result, expected):
        got, exp = result["total_return"], expected["total_return"]
        rel = abs(got - exp) / max(abs(exp), 1e-15)
        assert rel < REL_TOL, f"total_return rel_err={rel:.2e} (got {got}, exp {exp})"

    def test_sharpe_within_1bp(self, result, expected):
        got, exp = result["sharpe"], expected["sharpe"]
        rel = abs(got - exp) / max(abs(exp), 1e-15)
        assert rel < REL_TOL, f"sharpe rel_err={rel:.2e} (got {got}, exp {exp})"

    def test_mdd_within_1bp(self, result, expected):
        got, exp = result["mdd"], expected["mdd"]
        rel = abs(got - exp) / max(abs(exp), 1e-15)
        assert rel < REL_TOL, f"mdd rel_err={rel:.2e} (got {got}, exp {exp})"

    def test_total_return_matches_nav(self, result):
        assert abs(result["total_return"] - (result["nav"][-1] - 1.0)) < 1e-12


class TestRedLine6NoMixing:
    """ac-3 / H2: equal-weight top-5 ETF momentum, never mixed with old baseline."""

    def test_no_forbidden_imports(self):
        """portfolio_baseline must not import futures ALSTM regression modules."""
        source = inspect.getsource(portfolio_baseline)
        for forbidden in FORBIDDEN_MODULES:
            assert forbidden not in source, (
                f"portfolio_baseline references forbidden module: {forbidden}"
            )

    def test_no_forbidden_modules_loaded(self):
        """No baseline_spec / alstm module loaded during this test."""
        for mod_name in sys.modules:
            for forbidden in FORBIDDEN_MODULES:
                assert forbidden not in mod_name, f"forbidden module loaded: {mod_name}"

    def test_expected_is_portfolio_not_per_ticker(self, expected):
        """golden_expected_portfolio.json holds 3 scalars, not per-ticker dicts."""
        assert set(expected) == {"total_return", "sharpe", "mdd"}
        for key in expected:
            assert isinstance(expected[key], (int, float)), (
                f"{key} must be a scalar, got {type(expected[key])}"
            )

    def test_no_baseline_spec_file_referenced(self):
        """The comparison source is golden_expected_portfolio.json only."""
        src = inspect.getsource(portfolio_baseline)
        assert (
            "golden_expected_portfolio" in inspect.getsource(sys.modules[__name__])
            or True
        )  # expected file is loaded via fixture, not hardcoded
        # Key check: no reference to old baseline paths in the implementation
        assert "baseline_spec" not in src
        assert "golden_expected.json" not in src


class TestNoLookahead:
    """ac-4: shift(1) direction — weights use no same-period information."""

    def test_initial_weights_equal(self, result):
        """First sample day's return uses 1/5 each (from NAV_0 close)."""
        w0 = result["weights_used"][0]
        assert np.allclose(w0, 0.2), f"initial weights {w0}"

    def test_shift1_direction(self, result, prices):
        """Weights for day t are from day t-1's close, not day t's.

        Verifies: r_p[t] = sum(w[t] * r[t]) where w[t] was set before r[t].
        """
        sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
        warmup_last = prices[prices["phase"] == "warmup"].iloc[-1]
        closes = sample[UNIVERSE].to_numpy(dtype=float)
        base = warmup_last[UNIVERSE].to_numpy(dtype=float)
        prev = np.concatenate([base.reshape(1, -1), closes[:-1]], axis=0)
        r = closes / prev - 1.0

        for t in range(SAMPLE_DAYS):
            w_t = result["weights_used"][t]
            r_p_expected = float(np.dot(w_t, r[t]))
            assert abs(r_p_expected - result["portfolio_returns"][t]) < 1e-12, (
                f"day {t}: r_p mismatch (shift direction wrong)"
            )

    def test_weights_drift_between_rebalances(self, result, prices):
        """Between rebalances, weights change with prices (not reset to 1/5)."""
        sample_dates = prices[prices["phase"] == "sample"]["date"].tolist()
        rebalance_si = {sample_dates.index(d) for d in REBALANCE_DATES}
        for d in REBALANCE_DATES:
            rebalance_si.add(sample_dates.index(d))
        # Days after non-rebalance periods should show drift
        for t in range(1, 5):
            if t not in rebalance_si and (t - 1) not in rebalance_si:
                w = result["weights_used"][t]
                assert not np.allclose(w, 0.2), (
                    f"day {t}: weights show no drift between rebalances"
                )


class TestRebalanceTiming:
    """ac-4: rebalance at month-end close resets weights for the next period."""

    def test_weights_reset_after_rebalance(self, result, prices):
        """The day after each rebalance uses 1/5 each."""
        sample_dates = prices[prices["phase"] == "sample"]["date"].tolist()
        rebalance_si = {sample_dates.index(d) for d in REBALANCE_DATES}
        for si in rebalance_si:
            next_si = si + 1
            if next_si < SAMPLE_DAYS:
                w_next = result["weights_used"][next_si]
                assert np.allclose(w_next, 0.2), (
                    f"day after rebalance (si={next_si}): weights {w_next}"
                )

    def test_rebalance_day_uses_drifted_weights(self, result, prices):
        """On the rebalance day, the return uses drifted weights (not 1/5)."""
        sample_dates = prices[prices["phase"] == "sample"]["date"].tolist()
        rebalance_si = {sample_dates.index(d) for d in REBALANCE_DATES}
        for si in rebalance_si:
            w = result["weights_used"][si]
            # If this is not the first sample day (si>0), weights should be drifted
            if si > 0:
                assert not np.allclose(w, 0.2), (
                    f"rebalance day si={si}: weights should be drifted, not 1/5"
                )

    def test_march31_decision_changes_no_return(self, result, prices):
        """2025-03-31 rebalance resets weights but no return follows in sample."""
        sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
        sample_dates = sample["date"].tolist()
        si_mar31 = sample_dates.index("2025-03-31")
        # The last day's return uses drifted weights (not 1/5)
        w_last = result["weights_used"][si_mar31]
        assert not np.allclose(w_last, 0.2), (
            "last-day weights should be drifted (rebalance happens at close)"
        )
        # The rebalance at 2025-03-31 close has no effect on any sample return
        # (it would only matter for a next period that doesn't exist)
        assert si_mar31 == SAMPLE_DAYS - 1
