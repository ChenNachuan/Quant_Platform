"""
Tests for engine/pipeline.py — pure-logic components only.
VectorBT and execution.risk_control are mocked at import time.
"""
import sys
import types
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

# --- mock heavy deps before importing pipeline ---
_vbt_mock = types.ModuleType("vectorbt")
_vbt_mock.Portfolio = MagicMock()
sys.modules.setdefault("vectorbt", _vbt_mock)

_risk_mod = types.ModuleType("execution.risk_control")
_risk_mod.RiskControl = MagicMock()
sys.modules.setdefault("execution", types.ModuleType("execution"))
sys.modules.setdefault("execution.risk_control", _risk_mod)

from engine.pipeline import (  # noqa: E402
    EqualWeightPortfolio,
    QuantilePortfolio,
    DefaultRiskFilter,
    SimulationExecutionHandler,
)


# ── fixtures ──────────────────────────────────────────────────

@pytest.fixture
def signals():
    """2 dates × 3 stocks, all non-NaN."""
    dates = pd.date_range("2024-01-01", periods=2)
    return pd.DataFrame(
        [[0.5, 0.3, 0.2], [0.1, 0.6, 0.3]],
        index=dates,
        columns=["A", "B", "C"],
    )


@pytest.fixture
def signals_with_nan():
    dates = pd.date_range("2024-01-01", periods=2)
    return pd.DataFrame(
        [[0.5, np.nan, 0.2], [np.nan, 0.6, 0.3]],
        index=dates,
        columns=["A", "B", "C"],
    )


# ── EqualWeightPortfolio ──────────────────────────────────────

class TestEqualWeightPortfolio:
    def test_weights_sum_to_one(self, signals):
        w = EqualWeightPortfolio().create_portfolio(signals, kdata=None)
        row_sums = w.sum(axis=1)
        assert (row_sums - 1.0).abs().max() < 1e-10

    def test_nan_excluded_from_weight(self, signals_with_nan):
        w = EqualWeightPortfolio().create_portfolio(signals_with_nan, kdata=None)
        # row 0: A=0.5, C=0.2 → 2 valid → each 0.5; B=NaN → weight 0
        assert w.loc[w.index[0], "B"] == pytest.approx(0.0)
        assert w.loc[w.index[0], "A"] == pytest.approx(0.5)

    def test_no_nan_in_output(self, signals_with_nan):
        w = EqualWeightPortfolio().create_portfolio(signals_with_nan, kdata=None)
        assert not w.isna().any().any()


# ── QuantilePortfolio ─────────────────────────────────────────

class TestQuantilePortfolio:
    def test_returns_dict(self, signals):
        result = QuantilePortfolio(n_quantiles=2).create_portfolio(signals, kdata=None)
        assert isinstance(result, dict)

    def test_correct_number_of_groups(self, signals):
        result = QuantilePortfolio(n_quantiles=2).create_portfolio(signals, kdata=None)
        assert set(result.keys()) == {1, 2}

    def test_group_weights_sum_to_one_or_zero(self, signals):
        result = QuantilePortfolio(n_quantiles=2).create_portfolio(signals, kdata=None)
        for w in result.values():
            row_sums = w.sum(axis=1)
            assert ((row_sums - 1.0).abs() < 1e-10).all() or (row_sums == 0.0).all()


# ── DefaultRiskFilter ─────────────────────────────────────────

class TestDefaultRiskFilter:
    def test_clips_above_max(self, signals):
        rf = DefaultRiskFilter(max_weight=0.4)
        w = EqualWeightPortfolio().create_portfolio(signals, kdata=None)
        safe = rf.apply_risk_rules(w)
        assert safe.max().max() <= 0.4 + 1e-10

    def test_does_not_increase_weights(self, signals):
        w = EqualWeightPortfolio().create_portfolio(signals, kdata=None)
        safe = DefaultRiskFilter(max_weight=1.0).apply_risk_rules(w)
        assert (safe <= w + 1e-10).all().all()


# ── SimulationExecutionHandler ────────────────────────────────

class TestSimulationExecutionHandler:
    def test_passthrough(self, signals):
        w = EqualWeightPortfolio().create_portfolio(signals, kdata=None)
        result = SimulationExecutionHandler().execute(w, kdata=pd.DataFrame())
        pd.testing.assert_frame_equal(result, w)
