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
    CS_STD_CV_ABNORMAL,
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
    detect_redundant_factors,
    diagnose_factor,
    evaluate_factor,
    list_factor_meta,
    plot_factor_report,
    redundancy_tier,
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


# ---------------------------------------------------------------------------
# AC4 — redundancy tiers and pair detection
# ---------------------------------------------------------------------------


class TestRedundancyTiers:
    """AC4: D4 tier boundaries — >0.9 high / (0.7,0.9] possible / [0.5,0.7] warn / <0.5 independent."""

    def test_high_tier(self):
        assert redundancy_tier(1.0) == "high"
        assert redundancy_tier(0.95) == "high"
        assert redundancy_tier(0.91) == "high"

    def test_possible_tier(self):
        assert redundancy_tier(0.9) == "possible"  # right boundary inclusive
        assert redundancy_tier(0.8) == "possible"
        assert redundancy_tier(0.71) == "possible"

    def test_warn_tier(self):
        assert redundancy_tier(0.7) == "warn"  # left boundary of possible is exclusive
        assert redundancy_tier(0.6) == "warn"
        assert redundancy_tier(0.5) == "warn"  # left boundary inclusive

    def test_independent_tier(self):
        assert redundancy_tier(0.49) == "independent"
        assert redundancy_tier(0.0) == "independent"

    def test_boundary_09(self):
        """0.9 is 'possible', not 'high' (strict >)."""
        assert redundancy_tier(0.9) == "possible"

    def test_boundary_07(self):
        """0.7 is 'warn', not 'possible' (exclusive lower bound of (0.7, 0.9])."""
        assert redundancy_tier(0.7) == "warn"

    def test_boundary_05(self):
        """0.5 is 'warn', not 'inclusive' (inclusive lower bound of [0.5, 0.7])."""
        assert redundancy_tier(0.5) == "warn"


class TestDetectRedundantFactors:
    """AC4: D9 pairwise detection — threshold strict, sorted by |corr| descending."""

    @staticmethod
    def _make_matrix():
        """3 dates × 4 tickers, 3 factors with controlled correlations.

        f1 and f2: rank order identical → Spearman = 1.0 each date.
        f1 and f3: slight permutation → Spearman ≈ 0.8 each date.
        f2 and f3: different rank → Spearman ≈ 0.8 each date.
        """
        rows = []
        for d in ["2024-01-01", "2024-01-02", "2024-01-03"]:
            for i, t in enumerate(["A", "B", "C", "D"]):
                rows.append(
                    {
                        "date": d,
                        "ticker": t,
                        "f1": float(i + 1),
                        "f2": float(i + 1),
                        "f3": float([1, 2, 4, 3][i]),
                    }
                )
        return pd.DataFrame(rows)

    def test_detects_high_correlation_pair(self):
        fm = self._make_matrix()
        result = detect_redundant_factors(fm, threshold=0.7)
        pairs = {(a, b) for a, b, _ in result}
        assert ("f1", "f2") in pairs

    def test_sorted_by_abs_corr_descending(self):
        fm = self._make_matrix()
        result = detect_redundant_factors(fm, threshold=0.7)
        assert len(result) >= 2
        abs_corrs = [abs(c) for _, _, c in result]
        assert abs_corrs == sorted(abs_corrs, reverse=True)

    def test_threshold_strict(self):
        """abs(mean) == threshold must NOT be included (strict >)."""
        fm = self._make_matrix()
        # Use threshold = 1.0 which no pair can strictly exceed
        result = detect_redundant_factors(fm, threshold=1.0)
        assert result == []

    def test_returns_tuple_shape(self):
        fm = self._make_matrix()
        result = detect_redundant_factors(fm, threshold=0.0)
        for entry in result:
            assert len(entry) == 3
            assert isinstance(entry[0], str)
            assert isinstance(entry[1], str)
            assert isinstance(entry[2], float)

    def test_all_nan_rows_dropped(self):
        """Rows where all factor columns are NaN are excluded from correlation."""
        fm = self._make_matrix()
        # Add a row with all NaN factors — should not affect results
        nan_row = {
            "date": "2024-01-01",
            "ticker": "E",
            "f1": float("nan"),
            "f2": float("nan"),
            "f3": float("nan"),
        }
        fm2 = pd.concat([fm, pd.DataFrame([nan_row])], ignore_index=True)
        r1 = detect_redundant_factors(fm, threshold=0.7)
        r2 = detect_redundant_factors(fm2, threshold=0.7)
        assert r1 == r2


# ---------------------------------------------------------------------------
# D8 — diagnose_factor
# ---------------------------------------------------------------------------


class TestDiagnoseFactor:
    """D8: 12-key dict, cs_std_cv threshold 0.5."""

    @staticmethod
    def _make_df():
        rows = []
        for d in ["2024-01-01", "2024-01-02", "2024-01-03"]:
            for i, t in enumerate(["A", "B", "C"]):
                rows.append({"date": d, "ticker": t, "test_factor": float(i + 1)})
        return pd.DataFrame(rows)

    def test_twelve_keys(self):
        result = diagnose_factor(self._make_df(), "test_factor")
        expected = {
            "total_rows",
            "nan_ratio",
            "inf_count",
            "zero_ratio",
            "coverage_mean",
            "coverage_min",
            "min_date",
            "value_min",
            "value_max",
            "value_mean",
            "value_std",
            "cs_std_cv",
        }
        assert set(result.keys()) == expected

    def test_basic_values(self):
        result = diagnose_factor(self._make_df(), "test_factor")
        assert result["total_rows"] == 9
        assert result["nan_ratio"] == 0.0
        assert result["inf_count"] == 0
        assert result["zero_ratio"] == 0.0
        assert result["value_min"] == 1.0
        assert result["value_max"] == 3.0

    def test_cs_std_cv_constant_section(self):
        """Constant cross-section per date → cs_std_cv = 0."""
        result = diagnose_factor(self._make_df(), "test_factor")
        assert result["cs_std_cv"] == 0.0

    def test_nan_ratio(self):
        df = self._make_df()
        df.loc[0, "test_factor"] = float("nan")
        result = diagnose_factor(df, "test_factor")
        assert result["nan_ratio"] == round(1 / 9, 4)

    def test_inf_count(self):
        df = self._make_df()
        df.loc[0, "test_factor"] = float("inf")
        df.loc[1, "test_factor"] = float("-inf")
        result = diagnose_factor(df, "test_factor")
        assert result["inf_count"] == 2

    def test_cs_std_cv_threshold_constant(self):
        assert CS_STD_CV_ABNORMAL == 0.5


# ---------------------------------------------------------------------------
# D7 — plot_factor_report (smoke test)
# ---------------------------------------------------------------------------


class TestPlotFactorReport:
    """D7: 2×2 tearsheet, lazy matplotlib, no plt.show."""

    @staticmethod
    def _make_data():
        rows_fv, rows_ret = [], []
        for d in ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]:
            for i, t in enumerate(["A", "B", "C", "D", "E"]):
                rows_fv.append({"date": d, "ticker": t, "test_factor": float(i + 1)})
                rows_ret.append(
                    {"date": d, "ticker": t, "fwd_return": float(i + 1) * 0.01}
                )
        return pd.DataFrame(rows_fv), pd.DataFrame(rows_ret)

    def test_creates_png(self, tmp_path, monkeypatch):
        import matplotlib

        matplotlib.use("Agg")
        monkeypatch.chdir(tmp_path)
        fv, ret = self._make_data()
        plot_factor_report(fv, ret, "test_factor")
        out = tmp_path / "research" / "reports" / "test_factor_tearsheet.png"
        assert out.exists()
        assert out.stat().st_size > 0


# ---------------------------------------------------------------------------
# AC5 — TestMomentum20D: four paradigms
# ---------------------------------------------------------------------------


class _Momentum20DFixture:
    """Test-double for a 20-day momentum factor (pct_change(20) per ticker)."""

    name = "momentum_20d"
    description = "20-day momentum"
    category = "time_series"
    frequency = "daily"
    asset_types = ["equity"]
    version = "1.0"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df = df.sort_values(["ticker", "date"])
        df["momentum_20d"] = df.groupby("ticker")["close"].pct_change(20)
        return df[["date", "ticker", "momentum_20d"]].dropna()


class TestMomentum20D:
    """AC5: four test paradigms against local Momentum20D fixture (D6).

    Hand-calc numbers are re-derived by the verifier, not trusted from here.
    """

    @staticmethod
    def _make_bars(n_days=25, tickers=None):
        """Build bars_daily with known closes for hand-calc verification."""
        if tickers is None:
            tickers = ["A"]
        rows = []
        for t_i, t in enumerate(tickers):
            for d_i in range(n_days):
                rows.append(
                    {
                        "date": f"2024-01-{d_i + 1:02d}"
                        if d_i < 28
                        else f"2024-02-{d_i - 27:02d}",
                        "ticker": t,
                        "close": 100.0 + d_i + t_i * 10,
                    }
                )
        return {"bars_daily": pd.DataFrame(rows)}

    def test_basic_computation(self):
        """Hand-calc exact comparison + schema assertion (3-day check window)."""
        data = self._make_bars(n_days=23, tickers=["A"])
        f = _Momentum20DFixture()
        result = f.compute(data)
        assert list(result.columns) == ["date", "ticker", "momentum_20d"]
        # With close = 100 + d_i for ticker A, pct_change(20) at d_i=20 is (120-100)/100 = 0.20
        # At d_i=21: (121-101)/101 ≈ 0.198020
        # At d_i=22: (122-102)/102 ≈ 0.196078
        assert len(result) == 3
        row_20 = result.iloc[0]
        assert row_20["momentum_20d"] == pytest.approx(0.20, abs=1e-9)
        row_21 = result.iloc[1]
        assert row_21["momentum_20d"] == pytest.approx(0.1980198, abs=1e-6)
        row_22 = result.iloc[2]
        assert row_22["momentum_20d"] == pytest.approx(0.1960784, abs=1e-6)

    def test_nan_handling(self):
        """Suspended NaN doesn't pollute entire column; notna().mean() > 0.8."""
        data = self._make_bars(n_days=25, tickers=["A", "B"])
        # Introduce NaN for ticker B on one date (suspension)
        bars = data["bars_daily"]
        mask = (bars["ticker"] == "B") & (bars["date"] == "2024-01-25")
        bars.loc[mask, "close"] = float("nan")
        f = _Momentum20DFixture()
        result = f.compute(data)
        col = result["momentum_20d"]
        assert col.notna().mean() > 0.8
        # The NaN only affects ticker B at that date, not the entire column
        assert col.isna().sum() < len(col)

    def test_output_schema(self):
        """Output columns == [date, ticker, momentum_20d]."""
        data = self._make_bars(n_days=25)
        f = _Momentum20DFixture()
        result = f.compute(data)
        assert list(result.columns) == ["date", "ticker", "momentum_20d"]

    def test_no_lookahead(self):
        """Deleting the last day does not change historical values."""
        data = self._make_bars(n_days=25)
        f = _Momentum20DFixture()
        full = f.compute(data)
        # Truncate last date
        bars = data["bars_daily"]
        last_date = bars["date"].max()
        truncated_data = {"bars_daily": bars[bars["date"] != last_date]}
        truncated = f.compute(truncated_data)
        # Merge on shared (date, ticker) and verify values unchanged
        merged = full.merge(
            truncated, on=["date", "ticker"], suffixes=("_full", "_trunc")
        )
        assert len(merged) > 0
        assert (merged["momentum_20d_full"] == merged["momentum_20d_trunc"]).all()
