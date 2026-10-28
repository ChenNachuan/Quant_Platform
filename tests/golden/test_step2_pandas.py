"""Golden-sample portfolio step 2: pandas implementation vs hand-calc (S8-T1).

Compares ``portfolio_baseline`` against ``golden_expected_portfolio.json`` at
< 0.01 % relative deviation.  Also carries the red-line-6 mixing-rejection
assertion and the shift(1)-direction / rebalance-timing checks.
"""

import inspect
import json
import sys
import types
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

# The implementation *module* — `inspect.getsource(portfolio_baseline)` returns
# the function body only, so module-level imports would be invisible.
IMPL_MODULE = sys.modules[portfolio_baseline.__module__]
IMPL_SOURCE = inspect.getsource(IMPL_MODULE)

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
        """portfolio_baseline's module source must not reference forbidden modules.

        Checked against the module source, not the function body — a top-level
        ``import baseline_spec`` sits in the import block, which
        ``inspect.getsource(portfolio_baseline)`` never returns.
        """
        for forbidden in FORBIDDEN_MODULES:
            assert forbidden not in IMPL_SOURCE, (
                f"portfolio_baseline module references forbidden module: {forbidden}"
            )

    def test_no_forbidden_modules_loaded(self):
        """portfolio_baseline's own import graph must not touch forbidden modules.

        Scoped to the implementation's import namespace. The process may
        legitimately hold e.g. ``test_tl_intraday_reproduction`` as part of the
        project suite; that says nothing about this implementation.
        """
        imported = {IMPL_MODULE.__name__}
        for val in vars(IMPL_MODULE).values():
            if isinstance(val, types.ModuleType):
                imported.add(val.__name__)
            elif callable(val) or isinstance(val, type):
                owner = getattr(val, "__module__", None)
                if owner and owner != "builtins":
                    imported.add(owner)
        for name in imported:
            for forbidden in FORBIDDEN_MODULES:
                assert forbidden not in name, (
                    f"portfolio_baseline imports forbidden module: {name}"
                )

    def test_expected_is_portfolio_not_per_ticker(self, expected):
        """golden_expected_portfolio.json holds 3 scalars, not per-ticker dicts."""
        assert set(expected) == {"total_return", "sharpe", "mdd"}
        for key in expected:
            assert isinstance(expected[key], (int, float)), (
                f"{key} must be a scalar, got {type(expected[key])}"
            )

    def test_no_baseline_spec_file_referenced(self):
        """The comparison source is golden_expected_portfolio.json only."""
        assert "golden_expected_portfolio" in inspect.getsource(sys.modules[__name__])
        # Key check: no reference to old baseline paths in the implementation
        assert "baseline_spec" not in IMPL_SOURCE
        assert "golden_expected.json" not in IMPL_SOURCE


class TestNoLookahead:
    """ac-4: shift(1) direction — weights use no same-period information."""

    def test_initial_weights_equal(self, result):
        """First sample day's return uses 1/5 each (from NAV_0 close)."""
        w0 = result["weights_used"][0]
        assert np.allclose(w0, 0.2), f"initial weights {w0}"

    def test_shift1_direction(self, result, prices):
        """Weights for day t derive from r[0..t-1] and the rebalance set alone.

        Re-derives the whole weight path from first principles without calling
        ``portfolio_baseline``. A path contaminated by r[t] — drift applied
        before the day's weights are recorded — disagrees at day t, unlike a
        plain ``r_p[t] == w[t] . r[t]`` check, which holds by construction.
        """
        sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
        warmup_last = prices[prices["phase"] == "warmup"].iloc[-1]
        closes = sample[UNIVERSE].to_numpy(dtype=float)
        base = warmup_last[UNIVERSE].to_numpy(dtype=float)
        prev = np.concatenate([base.reshape(1, -1), closes[:-1]], axis=0)
        r = closes / prev - 1.0

        sample_dates = sample["date"].tolist()
        rebalance_si = {sample_dates.index(d) for d in REBALANCE_DATES}

        n_assets = len(UNIVERSE)
        expected_w = np.full(n_assets, 1.0 / n_assets)  # NAV_0 close decision
        for t in range(SAMPLE_DAYS):
            got_w = result["weights_used"][t]
            assert np.allclose(got_w, expected_w), (
                f"day {t}: weights {got_w} != past-only path {expected_w}"
            )
            r_p_t = float(np.dot(expected_w, r[t]))
            assert abs(r_p_t - result["portfolio_returns"][t]) < 1e-12, (
                f"day {t}: r_p mismatch (shift direction wrong)"
            )
            # Drift by day t's return → day t+1's weights. A rebalance decided
            # at day t's close resets to 1/5 instead, still past-only for t+1.
            drifted = expected_w * (1.0 + r[t]) / (1.0 + r_p_t)
            expected_w = (
                np.full(n_assets, 1.0 / n_assets) if t in rebalance_si else drifted
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
