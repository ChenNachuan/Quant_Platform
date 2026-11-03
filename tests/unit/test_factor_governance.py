"""Tests for research.factor_governance — admission gate, exit line,
lifecycle transitions, and IC evaluation (S11-T2 iteration 1, ac-1..ac-3).

Covers per-condition admission boundary behaviour (strict >, boundary fails),
the D1 exit-line cascade (absolute line first, zero-std t suppressed), and
the D3 lifecycle state machine (two legal transitions, all else rejected).
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from research.factor_governance import (
    CORR_MAX,
    EXIT_IC_THRESHOLD,
    EXIT_RATIO,
    EXIT_T_THRESHOLD,
    GATE_IC_MIN,
    GATE_IR_MIN,
    GATE_T_BASE,
    META_FIELDS,
    WINDOW_12M,
    check_admission,
    check_factor_health,
    evaluate_factor,
    list_factor_meta,
    transition_lifecycle,
)


# ---------------------------------------------------------------------------
# AC1 — admission gate: per-condition, strict inequalities
# ---------------------------------------------------------------------------


class TestAdmissionGate:
    """AC1: ic>0.04 / ir>0.5 / t>2 / max_corr<0.5, all strict."""

    def test_all_conditions_pass(self):
        metrics = {"ic_mean": 0.05, "ic_ir": 0.6, "ic_t": 3.0}
        passed, detail = check_admission(metrics, max_corr=0.3)
        assert passed is True
        for key in ("ic", "ir", "t", "corr"):
            assert detail[key]["passed"] is True, key

    def test_ic_boundary_fails(self):
        """ic_mean == 0.04 must NOT pass (strict >)."""
        metrics = {"ic_mean": 0.04, "ic_ir": 0.6, "ic_t": 3.0}
        passed, detail = check_admission(metrics, max_corr=0.3)
        assert passed is False
        assert detail["ic"]["passed"] is False
        assert detail["ic"]["value"] == 0.04
        assert detail["ic"]["threshold"] == GATE_IC_MIN

    def test_ir_boundary_fails(self):
        """ic_ir == 0.5 must NOT pass (strict >)."""
        metrics = {"ic_mean": 0.05, "ic_ir": 0.5, "ic_t": 3.0}
        passed, detail = check_admission(metrics, max_corr=0.3)
        assert passed is False
        assert detail["ir"]["passed"] is False
        assert detail["ir"]["threshold"] == GATE_IR_MIN

    def test_t_boundary_fails(self):
        """ic_t == 2.0 must NOT pass (strict >)."""
        metrics = {"ic_mean": 0.05, "ic_ir": 0.6, "ic_t": 2.0}
        passed, detail = check_admission(metrics, max_corr=0.3)
        assert passed is False
        assert detail["t"]["passed"] is False
        assert detail["t"]["threshold"] == GATE_T_BASE

    def test_corr_boundary_fails(self):
        """max_corr == 0.5 must NOT pass (strict <)."""
        metrics = {"ic_mean": 0.05, "ic_ir": 0.6, "ic_t": 3.0}
        passed, detail = check_admission(metrics, max_corr=0.5)
        assert passed is False
        assert detail["corr"]["passed"] is False
        assert detail["corr"]["threshold"] == CORR_MAX

    def test_detail_shape(self):
        """Each condition carries {value, threshold, passed}."""
        metrics = {"ic_mean": 0.05, "ic_ir": 0.6, "ic_t": 3.0}
        _, detail = check_admission(metrics, max_corr=0.3)
        assert set(detail.keys()) == {"ic", "ir", "t", "corr"}
        for key in ("ic", "ir", "t", "corr"):
            assert set(detail[key].keys()) == {"value", "threshold", "passed"}

    def test_single_failure_masks_pass(self):
        """One failing condition flips the overall verdict."""
        metrics = {"ic_mean": 0.05, "ic_ir": 0.6, "ic_t": 3.0}
        passed, detail = check_admission(metrics, max_corr=0.6)
        assert passed is False
        assert detail["corr"]["passed"] is False
        assert detail["ic"]["passed"] is True


# ---------------------------------------------------------------------------
# AC2 — exit line: ic_mean < 0.03 or trailing-36m t < 1.5
# ---------------------------------------------------------------------------


class TestExitLine:
    """AC2: D1 cascade absolute exit line, strict <, zero-std suppressed."""

    def test_ic_mean_below_threshold_exits(self):
        """mean IC < 0.03 fires exit."""
        ic = pd.Series([0.02] * 300)
        assert check_factor_health(ic) == "exit"

    def test_ic_mean_at_boundary_not_exit(self):
        """mean IC == 0.03 does NOT fire (strict <)."""
        ic = pd.Series([0.03] * 300)
        # zero-std → t trigger suppressed; recent_6m == mean → healthy
        assert check_factor_health(ic) == "healthy"

    def test_rolling_t_below_threshold_exits(self):
        """trailing-36m t < 1.5 fires exit even when mean IC is fine.

        The series must not also trip the relative-decay exit — otherwise
        the case passes with the t trigger disabled and the ac-2 rolling-t
        line is unpinned.  Decay is held off: recent_6m == recent_12m ==
        mean, so the t trigger is the only exit route.
        """
        # 756 obs of (0.97, -0.87) pairs: mean 0.05, std ~0.92
        # → t = mean/std*sqrt(756) ≈ 1.493 < 1.5.
        ic = pd.Series([0.97, -0.87] * 378)
        window = ic.tail(756)
        w_mean, w_std = float(window.mean()), float(window.std())
        t_val = w_mean / w_std * math.sqrt(len(window))
        mean = float(ic.mean())
        assert mean >= EXIT_IC_THRESHOLD  # IC line does not fire
        assert 1.4 < t_val < EXIT_T_THRESHOLD  # t line fires, near boundary
        assert (
            float(ic.tail(WINDOW_12M).mean()) >= EXIT_RATIO * mean
        )  # decay line cannot fire
        assert check_factor_health(ic) == "exit"

    def test_zero_std_window_does_not_fire_t(self):
        """Zero-std trailing window must not trigger the t exit."""
        ic = pd.Series([0.05] * 300)
        # mean 0.05 > 0.03; std 0 → t suppressed; recent_6m > 0.7*mean → healthy
        assert check_factor_health(ic) == "healthy"

    def test_relative_decay_exit(self):
        """recent_12m < 0.3 * mean_ic fires exit when absolute lines pass."""
        # 600 obs at 0.1, then 256 obs at 0.01.
        # full mean ≈ 0.073 > 0.03; trailing-756 t high enough;
        # recent_6m ≈ 0.01 < 0.5*mean → falls through to relative decay.
        ic = pd.Series([0.1] * 600 + [0.01] * 256)
        assert check_factor_health(ic) == "exit"

    def test_cascade_order_absolute_before_relative(self):
        """Absolute exit line is checked before relative decay."""
        # mean ≈ 0.014 (below 0.03) AND recent_12m < 0.3*mean.
        # Returns "exit" from the absolute line; cascade order means the
        # decay branch is never reached.
        ic = pd.Series([0.02] * 600 + [0.001] * 256)
        assert float(ic.mean()) < EXIT_IC_THRESHOLD  # absolute line fires
        assert check_factor_health(ic) == "exit"

    def test_boundary_t_not_exit(self):
        """t > EXIT_T_THRESHOLD must NOT fire the exit trigger."""
        ic = pd.Series([0.05] * 100 + [0.05, 0.06] * 378)
        window = ic.tail(756)
        w_mean, w_std = float(window.mean()), float(window.std())
        t_val = w_mean / w_std * math.sqrt(len(window))
        assert t_val > EXIT_T_THRESHOLD  # well above boundary
        assert check_factor_health(ic) == "healthy"


# ---------------------------------------------------------------------------
# AC3 — lifecycle: draft→production→deprecated legal, all else rejected
# ---------------------------------------------------------------------------


class TestLifecycle:
    """AC3: D3 state machine — exactly two legal transitions."""

    def test_draft_to_production(self):
        assert transition_lifecycle("draft", "production") == "production"

    def test_production_to_deprecated(self):
        assert transition_lifecycle("production", "deprecated") == "deprecated"

    def test_self_transition_rejected(self):
        for status in ("draft", "production", "deprecated"):
            with pytest.raises(ValueError, match="illegal lifecycle"):
                transition_lifecycle(status, status)

    def test_reverse_transition_rejected(self):
        with pytest.raises(ValueError):
            transition_lifecycle("production", "draft")
        with pytest.raises(ValueError):
            transition_lifecycle("deprecated", "production")

    def test_draft_to_deprecated_rejected(self):
        """draft→deprecated is NOT a legal shortcut."""
        with pytest.raises(ValueError):
            transition_lifecycle("draft", "deprecated")

    def test_legal_transitions_constant(self):
        """_LIFECYCLE_LEGAL has exactly the two permitted pairs."""
        from research.factor_governance import _LIFECYCLE_LEGAL

        assert _LIFECYCLE_LEGAL == {
            ("draft", "production"),
            ("production", "deprecated"),
        }


# ---------------------------------------------------------------------------
# evaluate_factor (D5)
# ---------------------------------------------------------------------------


class TestEvaluateFactor:
    """D5: 5-key dict, rounding, zero-std IR."""

    @staticmethod
    def _make_data():
        """3 dates × 3 tickers; known Spearman IC per date."""
        dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
        tickers = ["A", "B", "C"]
        rows_fv, rows_ret = [], []
        for d in dates:
            for i, t in enumerate(tickers):
                rows_fv.append({"date": d, "ticker": t, "test_factor": float(i + 1)})
                # date 1&2: positive rank correlation; date 3: negative
                fwd = float(i + 1) if d != "2024-01-03" else float(3 - i)
                rows_ret.append({"date": d, "ticker": t, "fwd_return": fwd})
        fv = pd.DataFrame(rows_fv)
        ret = pd.DataFrame(rows_ret)
        return fv, ret

    def test_output_keys(self):
        fv, ret = self._make_data()
        result = evaluate_factor(fv, ret, "test_factor")
        assert set(result.keys()) == {
            "ic_mean",
            "ic_ir",
            "ic_t",
            "n_periods",
            "coverage",
        }

    def test_values(self):
        fv, ret = self._make_data()
        result = evaluate_factor(fv, ret, "test_factor")
        # ic_series = [1.0, 1.0, -1.0]
        assert result["n_periods"] == 3
        assert result["ic_mean"] == round(1.0 / 3.0, 4)  # 0.3333
        assert result["ic_ir"] == pytest.approx(round(math.sqrt(3) / 6, 3), abs=1e-3)
        assert result["ic_t"] == 0.5
        assert result["coverage"] == 1.0

    def test_zero_std_ir(self):
        """Constant IC across dates → std=0 → ic_ir=0.0."""
        dates = ["2024-01-01", "2024-01-02"]
        tickers = ["A", "B", "C"]
        rows_fv, rows_ret = [], []
        for d in dates:
            for i, t in enumerate(tickers):
                rows_fv.append({"date": d, "ticker": t, "test_factor": float(i + 1)})
                rows_ret.append({"date": d, "ticker": t, "fwd_return": float(i + 1)})
        fv = pd.DataFrame(rows_fv)
        ret = pd.DataFrame(rows_ret)
        result = evaluate_factor(fv, ret, "test_factor")
        assert result["ic_ir"] == 0.0
        assert result["ic_t"] == 0.0

    def test_n_periods_excludes_nan_ic(self):
        """Dates that produce NaN IC must not inflate n_periods.

        Date 1: 3 tickers → well-defined Spearman IC.  Date 2: 1 ticker
        → Spearman undefined (NaN).  n_periods must be 1, not 2.
        """
        rows_fv = [
            {"date": "2024-01-01", "ticker": "A", "test_factor": 1.0},
            {"date": "2024-01-01", "ticker": "B", "test_factor": 2.0},
            {"date": "2024-01-01", "ticker": "C", "test_factor": 3.0},
            {"date": "2024-01-02", "ticker": "A", "test_factor": 1.0},
        ]
        rows_ret = [
            {"date": "2024-01-01", "ticker": "A", "fwd_return": 0.01},
            {"date": "2024-01-01", "ticker": "B", "fwd_return": 0.02},
            {"date": "2024-01-01", "ticker": "C", "fwd_return": 0.03},
            {"date": "2024-01-02", "ticker": "A", "fwd_return": 0.01},
        ]
        fv = pd.DataFrame(rows_fv)
        ret = pd.DataFrame(rows_ret)
        result = evaluate_factor(fv, ret, "test_factor")
        assert result["n_periods"] == 1, "NaN-IC date must not count toward n_periods"


# ---------------------------------------------------------------------------
# list_factor_meta
# ---------------------------------------------------------------------------


class TestListFactorMeta:
    """META_FIELDS extraction from FactorRegistry."""

    def test_returns_list_of_dicts(self):
        result = list_factor_meta()
        assert isinstance(result, list)
        for entry in result:
            assert isinstance(entry, dict)
            assert set(entry.keys()) == set(META_FIELDS)

    def test_meta_fields_nine(self):
        assert len(META_FIELDS) == 9
        assert META_FIELDS == [
            "name",
            "version",
            "description",
            "category",
            "frequency",
            "asset_types",
            "depends_on",
            "status",
            "created_at",
        ]
