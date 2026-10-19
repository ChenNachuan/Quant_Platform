"""Tests for research.factor_combiner — combination, preprocess, evaluation.

Covers every DoD row of S6-T1:

* equal-weight baseline runs first and is exactly the mean of the z-scored
  factors (``TestEqualWeight``)
* ICIR weighting truncates a negative IC to 0 and returns weights that sum to
  1 (``TestIcIrWeights``)
* weights use only IC strictly before the date being scored — a test built so
  that swapping ``<`` for ``<=`` fails it (``TestLookahead``)
* preprocess is winsorize(3 MAD) → cross-sectional z-score → neutralize, each
  step ``groupby(date)``, with a full-sample counter-example per step
  (``TestPreprocess`` / ``TestFullSampleIsLookahead``)
* the 8-metric table, the ``factor_contribution`` breakdown and
  ``data/signals/{name}/eval_report.json`` with ``config_snapshot`` in
  ``path@hash`` form (``TestEvaluate`` / ``TestEvalReport``)
"""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
import pytest

from core.config import config_snapshot
from research.factor_combiner import (
    DEFAULT_LOOKBACK,
    EVAL_METRIC_KEYS,
    WEIGHT_METHODS,
    FactorCombiner,
    factor_contribution,
    preprocess,
    write_eval_report,
)
from research.post_processor import MAD_SCALE, neutralize


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _frame(
    dates_values: dict[str, list[list[float]]], cols: tuple[str, ...] = ("f1",)
) -> pd.DataFrame:
    """Build ``[date, ticker, {cols…}]`` from ``{date: [[row values], …]}``.

    Each row is a list of per-column values, so a single-column frame is
    ``{"2024-01-02": [[1.0], [2.0]]}``.
    """
    rows = []
    for date, payload in dates_values.items():
        for i, values in enumerate(payload):
            row = {"date": date, "ticker": f"t{i:03d}"}
            for col, v in zip(cols, values):
                row[col] = float(v)
            rows.append(row)
    return pd.DataFrame(rows)


def _score_frame(values: list[float], date: str = "2024-01-02") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [date] * len(values),
            "ticker": [f"t{i:03d}" for i in range(len(values))],
            "combined_score": [float(v) for v in values],
        }
    )


def _ic_frame(rows: dict[str, dict[str, float]]) -> pd.DataFrame:
    """Build wide ``[date, f1, f2, …]`` IC history from ``{date: {factor: ic}}``."""
    return pd.DataFrame([{"date": date, **factors} for date, factors in rows.items()])


def _long_ic(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["date", "factor_name", "ic"])


def _weights_frame(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["date", "ticker", "weight"])


def _ret_frame(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["date", "ticker", "ret"])


def _portfolio_ret(rows: list[tuple[str, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["date", "ret"])


# ---------------------------------------------------------------------------
# equal-weight baseline
# ---------------------------------------------------------------------------


class TestEqualWeight:
    def test_default_method_is_equal_weight(self):
        assert FactorCombiner().method == "equal_weight"
        assert "equal_weight" in WEIGHT_METHODS

    def test_score_is_mean_of_factor_zscores(self):
        fs = _frame(
            {"2024-01-02": [[1.0, 0.0], [0.0, 1.0], [-1.0, -1.0]]},
            cols=("f1", "f2"),
        )
        out = FactorCombiner().combine(fs)

        assert list(out.columns) == ["date", "ticker", "combined_score"]
        assert out["combined_score"].tolist() == pytest.approx([0.5, 0.5, -1.0])

    def test_equal_weights_sum_to_one(self):
        w = FactorCombiner()._equal_weights(["f1", "f2", "f3"])
        assert sum(w.values()) == pytest.approx(1.0)
        assert w == {
            "f1": pytest.approx(1 / 3),
            "f2": pytest.approx(1 / 3),
            "f3": pytest.approx(1 / 3),
        }

    def test_two_factor_weighted_sum_matches_hand_calc(self):
        fs = _frame({"2024-01-02": [[2.0, -2.0], [0.0, 4.0]]}, cols=("f1", "f2"))
        out = FactorCombiner().combine(fs)
        # (2 + -2) / 2 = 0 ; (0 + 4) / 2 = 2
        assert out["combined_score"].tolist() == pytest.approx([0.0, 2.0])

    def test_missing_factor_value_renormalises_over_available(self):
        fs = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 2,
                "ticker": ["a", "b"],
                "f1": [2.0, np.nan],
                "f2": [0.0, 4.0],
            }
        )
        out = FactorCombiner().combine(fs)
        # a: both present → (2+0)/2 = 1.  b: only f2 → 4 (weight renormalised).
        assert out["combined_score"].tolist() == pytest.approx([1.0, 4.0])

    def test_weighted_method_without_ic_history_is_refused(self):
        fs = _frame({"2024-01-02": [[1.0]]}, cols=("f1",))
        with pytest.raises(ValueError, match="requires ic_history"):
            FactorCombiner(method="ic_ir_weight").combine(fs)

    def test_misnamed_ic_history_is_refused_not_silently_equal_weight(self):
        """A rename must raise, not quietly fall back to the baseline.

        Non-matching factor columns assign 0 to every factor and
        ``_normalise`` falls back to equal weights — exactly the degradation
        the ``None`` guard's docstring names.  The caller asked for ICIR and
        must be told the history does not match, not handed the baseline.
        """
        fs = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 2,
                "ticker": ["a", "b"],
                "f1": [1.0, -1.0],
                "f2": [1.0, -1.0],
            }
        )
        ic = _ic_frame(
            {
                "2024-01-02": {"g1": 0.05, "g2": -0.02},
                "2024-01-03": {"g1": 0.05, "g2": -0.02},
            }
        )
        with pytest.raises(ValueError, match="shares no factor columns"):
            FactorCombiner(method="ic_ir_weight", lookback=1).combine(fs, ic)

    def test_unknown_method_is_refused(self):
        with pytest.raises(ValueError, match="method must be one of"):
            FactorCombiner(method="pca")

    def test_non_positive_lookback_is_refused(self):
        with pytest.raises(ValueError, match="lookback must be positive"):
            FactorCombiner(lookback=0)

    def test_default_lookback_is_wired_to_the_constant(self):
        """The constructor default must be ``DEFAULT_LOOKBACK``, not a literal.

        A hard-coded ``60`` means changing the constant silently diverges from
        the actual default.  The signature reads the constant so they cannot.
        """
        import inspect

        source = inspect.getsource(FactorCombiner.__init__)
        assert "DEFAULT_LOOKBACK" in source
        assert "lookback: int = 60" not in source
        assert FactorCombiner().lookback == DEFAULT_LOOKBACK


# ---------------------------------------------------------------------------
# ICIR weighting
# ---------------------------------------------------------------------------


class TestIcIrWeights:
    def test_negative_ic_is_truncated_to_zero(self):
        ic = _ic_frame(
            {
                "2024-01-02": {"f1": 0.04, "f2": -0.03},
                "2024-01-03": {"f1": 0.05, "f2": -0.02},
            }
        )
        w = FactorCombiner(method="ic_ir_weight", lookback=2)._ic_ir_weights(
            ic, ["f1", "f2"]
        )

        assert w["f2"] == 0.0
        assert w["f1"] == pytest.approx(1.0)
        assert sum(w.values()) == pytest.approx(1.0)

    def test_negative_mean_ic_is_truncated_for_ic_weight(self):
        ic = _ic_frame(
            {
                "2024-01-02": {"f1": 0.02, "f2": -0.01},
                "2024-01-03": {"f1": 0.02, "f2": -0.01},
            }
        )
        w = FactorCombiner(method="ic_weight", lookback=2)._ic_weights(ic, ["f1", "f2"])

        assert w["f2"] == 0.0
        assert w["f1"] == pytest.approx(1.0)

    def test_weights_sum_to_one_with_mixed_signs(self):
        ic = _ic_frame(
            {
                "2024-01-02": {"f1": 0.02, "f2": -0.05, "f3": 0.01},
                "2024-01-03": {"f1": 0.04, "f2": -0.01, "f3": 0.03},
                "2024-01-04": {"f1": 0.06, "f2": -0.02, "f3": 0.02},
            }
        )
        for method, fn in (
            (
                "ic_ir_weight",
                FactorCombiner(method="ic_ir_weight", lookback=3)._ic_ir_weights,
            ),
            ("ic_weight", FactorCombiner(method="ic_weight", lookback=3)._ic_weights),
        ):
            w = fn(ic, ["f1", "f2", "f3"])
            assert sum(w.values()) == pytest.approx(1.0), method
            assert w["f2"] == 0.0, method
            assert all(v >= 0 for v in w.values()), method

    def test_all_negative_ic_falls_back_to_equal_weights(self):
        ic = _ic_frame(
            {
                "2024-01-02": {"f1": -0.02, "f2": -0.03},
                "2024-01-03": {"f1": -0.01, "f2": -0.04},
            }
        )
        w = FactorCombiner(method="ic_ir_weight", lookback=2)._ic_ir_weights(
            ic, ["f1", "f2"]
        )
        assert w == {"f1": 0.5, "f2": 0.5}

    def test_icir_penalises_unstable_factor(self):
        # f1: steady +0.03 (zero σ → the best ICIR there is).  f2: same mean,
        # violent swings → a much smaller ICIR.
        ic = _ic_frame(
            {
                "2024-01-02": {"f1": 0.03, "f2": 0.20},
                "2024-01-03": {"f1": 0.03, "f2": -0.14},
                "2024-01-04": {"f1": 0.03, "f2": 0.20},
                "2024-01-05": {"f1": 0.03, "f2": -0.14},
            }
        )
        w = FactorCombiner(method="ic_ir_weight", lookback=4)._ic_ir_weights(
            ic, ["f1", "f2"]
        )
        assert w["f1"] > w["f2"]

    def test_steady_positive_ic_takes_the_book(self):
        """Zero σ with a positive mean is +∞ ICIR, not weight 0."""
        ic = _ic_frame(
            {
                "2024-01-02": {"f1": 0.03, "f2": 0.10},
                "2024-01-03": {"f1": 0.03, "f2": -0.04},
            }
        )
        w = FactorCombiner(method="ic_ir_weight", lookback=2)._ic_ir_weights(
            ic, ["f1", "f2"]
        )
        assert w["f1"] == pytest.approx(1.0)
        assert w["f2"] == pytest.approx(0.0)

    def test_missing_factor_column_gets_zero(self):
        ic = _ic_frame(
            {
                "2024-01-02": {"f1": 0.04},
                "2024-01-03": {"f1": 0.06},
            }
        )
        w = FactorCombiner(method="ic_ir_weight", lookback=2)._ic_ir_weights(
            ic, ["f1", "f2"]
        )
        assert w["f2"] == 0.0
        assert w["f1"] == pytest.approx(1.0)

    def test_combine_uses_weights_in_the_output(self):
        dates = pd.date_range("2023-06-01", periods=40, freq="B")
        ic = pd.DataFrame(
            {
                "date": dates,
                "f1": np.full(40, 0.05),
                "f2": np.full(40, -0.02),
            }
        )
        fs = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 2,
                "ticker": ["a", "b"],
                "f1": [1.0, -1.0],
                "f2": [-1.0, 1.0],
            }
        )
        out = FactorCombiner(method="ic_ir_weight", lookback=10).combine(fs, ic)
        # f2 is dead (negative IC) so the score is f1 alone.
        assert out["combined_score"].tolist() == pytest.approx([1.0, -1.0])

    def test_lookback_gates_the_weighted_path(self):
        # 3 IC rows with lookback=60 → warm-up, equal weights, not ICIR.
        ic = _ic_frame(
            {
                "2024-01-02": {"f1": 0.05, "f2": -0.05},
                "2024-01-03": {"f1": 0.05, "f2": -0.05},
                "2024-01-04": {"f1": 0.05, "f2": -0.05},
            }
        )
        fs = pd.DataFrame(
            {
                "date": ["2024-01-05"] * 2,
                "ticker": ["a", "b"],
                "f1": [1.0, -1.0],
                "f2": [1.0, -1.0],
            }
        )
        assert DEFAULT_LOOKBACK == 60
        out = FactorCombiner(method="ic_ir_weight").combine(fs, ic)
        # Equal-weight fallback: (f1+f2)/2, so both rows land on the mean.
        assert out["combined_score"].tolist() == pytest.approx([1.0, -1.0])

        # Drop the warm-up and ICIR engages: f2 is dead (negative IC), so the
        # score is f1 alone.  Same numbers here only because f1 == f2 on these
        # rows — the weight dict is what differs from the fallback above.
        w = FactorCombiner(method="ic_ir_weight", lookback=2)._ic_ir_weights(
            ic, ["f1", "f2"]
        )
        assert w["f2"] == 0.0

    def test_lookback_is_per_factor_not_row_count(self):
        """Ragged IC history: a factor with few ICs must not get a weight.

        80 rows in the wide frame open the coarse gate, but f3 only ever
        recorded 2 ICs (the rest NaN).  ``DEFAULT_LOOKBACK``'s rationale is
        "an ICIR estimated on a handful of dates is noise"; the gate has to
        apply that per factor, not to the frame's row count.
        """
        dates = pd.date_range("2023-01-02", periods=80, freq="B")
        ic = pd.DataFrame(
            {
                "date": dates,
                "f1": np.full(80, 0.05),
                "f2": np.full(80, 0.03),
                "f3": [np.nan] * 78 + [0.90, 0.90],  # 2 ICs only
            }
        )
        assert len(ic) > DEFAULT_LOOKBACK  # coarse gate opens

        for method in ("ic_weight", "ic_ir_weight"):
            w = (
                FactorCombiner(method=method)._ic_weights(ic, ["f1", "f2", "f3"])
                if method == "ic_weight"
                else FactorCombiner(method=method)._ic_ir_weights(
                    ic, ["f1", "f2", "f3"]
                )
            )
            assert w["f3"] == 0.0, method
            assert sum(w.values()) == pytest.approx(1.0), method

        # With enough ICs the same factor engages.
        ic_full = ic.copy()
        ic_full["f3"] = np.full(80, 0.90)
        w_full = FactorCombiner(method="ic_ir_weight")._ic_ir_weights(
            ic_full, ["f1", "f2", "f3"]
        )
        assert w_full["f3"] > 0.0


# ---------------------------------------------------------------------------
# look-ahead: IC strictly before t
# ---------------------------------------------------------------------------


class TestLookahead:
    def test_current_period_ic_must_not_set_current_weights(self):
        """The contract: weights at *t* use IC with ``date < t`` only.

        This test is the 'using current-period IC must fail' case.  It builds
        a history whose IC at the scored date is extreme — if the slice were
        ``date <= t`` that one row would take the whole weight — and asserts
        the implementation ignores it.  Widening the slice in
        ``FactorCombiner._weight_schedule`` flips this test to red.
        """
        # 60 quiet days of history, then one explosive IC on the score date.
        quiet = pd.date_range("2023-01-02", periods=60, freq="B")
        ic = pd.DataFrame(
            {
                "date": list(quiet) + [pd.Timestamp("2024-01-02")],
                "f1": [0.02] * 60 + [5.0],
                "f2": [0.02] * 60 + [-5.0],
            }
        )
        fs = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 2,
                "ticker": ["a", "b"],
                "f1": [1.0, -1.0],
                "f2": [1.0, -1.0],
            }
        )
        combiner = FactorCombiner(method="ic_weight", lookback=10)

        legal = combiner._ic_weights(ic[ic["date"] < "2024-01-02"], ["f1", "f2"])
        lookahead = combiner._ic_weights(ic[ic["date"] <= "2024-01-02"], ["f1", "f2"])

        # The two slices must disagree, or the test is not biting.
        assert legal != lookahead

        schedule = combiner._weight_schedule(fs, ic, ["f1", "f2"])
        _, used = schedule[0]
        assert used == legal
        assert used != lookahead

    def test_score_date_ic_change_does_not_move_the_score(self):
        quiet = pd.date_range("2023-01-02", periods=60, freq="B")
        base = pd.DataFrame(
            {
                "date": list(quiet) + [pd.Timestamp("2024-01-02")],
                "f1": [0.03] * 60 + [0.03],
                "f2": [0.01] * 60 + [0.01],
            }
        )
        poisoned = base.copy()
        poisoned.loc[poisoned["date"] == "2024-01-02", ["f1", "f2"]] = [50.0, -50.0]

        fs = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 2,
                "ticker": ["a", "b"],
                "f1": [1.0, 0.0],
                "f2": [0.0, 1.0],
            }
        )
        combiner = FactorCombiner(method="ic_ir_weight", lookback=10)
        assert combiner.combine(fs, base)["combined_score"].tolist() == pytest.approx(
            combiner.combine(fs, poisoned)["combined_score"].tolist()
        )

    def test_monthly_update_uses_prior_month_ic_only(self):
        """Month M scores with IC before M's first date — not IC from inside M."""
        ic = pd.DataFrame(
            {
                "date": [
                    pd.Timestamp("2023-12-28"),
                    pd.Timestamp("2024-01-15"),
                    pd.Timestamp("2024-01-31"),
                ],
                "f1": [0.01, 0.01, 0.09],
                "f2": [0.09, 0.09, 0.01],
            }
        )
        fs = pd.DataFrame(
            {
                "date": [
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-01-02"),
                    pd.Timestamp("2024-02-01"),
                    pd.Timestamp("2024-02-01"),
                ],
                "ticker": ["a", "b", "a", "b"],
                "f1": [1.0, 0.0, 1.0, 0.0],
                "f2": [0.0, 1.0, 0.0, 1.0],
            }
        )
        combiner = FactorCombiner(method="ic_weight", lookback=1)
        schedule = combiner._weight_schedule(fs, ic, ["f1", "f2"])
        jan_w, feb_w = schedule[0][1], schedule[1][1]

        # January only saw the 2023-12-28 row (f2 leads).  February saw the
        # two January rows as well, so f1's mean IC is now higher.
        assert jan_w["f2"] > jan_w["f1"]
        assert feb_w["f1"] > jan_w["f1"]

    def test_weights_are_constant_within_a_month(self):
        ic = pd.DataFrame(
            {
                "date": pd.date_range("2023-11-01", periods=30, freq="B"),
                "f1": np.linspace(0.01, 0.04, 30),
                "f2": np.full(30, 0.02),
            }
        )
        fs = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2024-01-02"] * 2 + ["2024-01-25"] * 2 + ["2024-01-31"] * 2
                ),
                "ticker": ["a", "b"] * 3,
                "f1": [1.0, -1.0] * 3,
                "f2": [1.0, -1.0] * 3,
            }
        )
        combiner = FactorCombiner(method="ic_weight", lookback=5)
        schedule = combiner._weight_schedule(fs, ic, ["f1", "f2"])
        assert len(schedule) == 1


# ---------------------------------------------------------------------------
# preprocess
# ---------------------------------------------------------------------------


class TestPreprocess:
    def test_pipeline_order_is_winsorize_zscore_neutralize(self):
        rng = np.random.default_rng(7)
        n = 24
        frame = pd.DataFrame(
            {
                "date": ["2024-01-02"] * n,
                "ticker": [f"t{i:03d}" for i in range(n)],
                "f": rng.normal(0.0, 1.0, n),
                "industry": ["A"] * 12 + ["B"] * 12,
                "log_market_cap": np.linspace(20.0, 24.0, n),
            }
        )
        frame.loc[0, "f"] = 1e6  # an outlier the MAD clip must tame

        # Without neutralize the z-score leaves mean 0 / std 1 exactly.
        z_only = preprocess(frame, "f", neutralize=False)
        assert z_only["f"].mean() == pytest.approx(0.0, abs=1e-9)
        assert z_only["f"].std(ddof=1) == pytest.approx(1.0, rel=1e-9)

        out = preprocess(frame, "f")
        # The OLS residual keeps the intercept's mean-0 property and is
        # orthogonal to size and industry; its std is < 1 because the
        # regression took those exposures out.
        assert out["f"].mean() == pytest.approx(0.0, abs=1e-9)
        assert float(
            np.corrcoef(out["f"], out["log_market_cap"])[0, 1]
        ) == pytest.approx(0.0, abs=1e-9)
        ind_b = (frame["industry"] == "B").astype(float)
        assert float(np.corrcoef(out["f"], ind_b)[0, 1]) == pytest.approx(0.0, abs=1e-9)
        assert out["f"].std(ddof=1) < 1.0

    def test_winsorize_uses_three_mad(self):
        """The clip band is exactly ``median ± 3 × 1.4826 × MAD``."""
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 1000.0]
        frame = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 11,
                "ticker": [f"t{i:02d}" for i in range(11)],
                "f": values,
            }
        )
        out = preprocess(frame, "f", neutralize=False)

        med = frame["f"].median()
        mad = (frame["f"] - med).abs().median()
        upper = med + 3 * MAD_SCALE * mad
        assert frame["f"].max() > upper  # the outlier is outside the band

        # preprocess winsorizes then z-scores.  Recover the winsorized value
        # from the preserved rank of the top name: it must sit on the bound.
        z = out["f"]
        top = z.idxmax()
        # z = (w - mean_w) / std_w  →  w = z·std_w + mean_w.  Rebuild the
        # winsorized section by un-shifting with the known clip: every value
        # ≤ upper is unchanged, the top is exactly upper.
        w = frame["f"].clip(upper=upper)
        expected = (w - w.mean()) / w.std()
        assert z.tolist() == pytest.approx(expected.tolist())
        assert float(w.iloc[top]) == pytest.approx(upper)

    def test_neutralize_is_optional_for_etf_pool(self):
        rng = np.random.default_rng(3)
        n = 16
        frame = pd.DataFrame(
            {
                "date": ["2024-01-02"] * n,
                "ticker": [f"t{i:03d}" for i in range(n)],
                "f": rng.normal(size=n),
                "industry": ["A"] * 8 + ["B"] * 8,
                "log_market_cap": np.linspace(20.0, 23.0, n),
            }
        )
        with_n = preprocess(frame, "f", neutralize=True)
        without = preprocess(frame, "f", neutralize=False)
        assert not with_n["f"].equals(without["f"])

    def test_output_is_a_copy_and_leaves_input_alone(self):
        frame = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 4,
                "ticker": ["a", "b", "c", "d"],
                "f": [1.0, 2.0, 3.0, 4.0],
            }
        )
        before = frame.copy()
        out = preprocess(frame, "f", neutralize=False)
        assert frame.equals(before)
        assert out is not frame

    def test_null_date_rows_keep_their_value(self):
        frame = pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-02", pd.NaT],
                "ticker": ["a", "b", "c"],
                "f": [1.0, 3.0, 7.0],
            }
        )
        out = preprocess(frame, "f", neutralize=False)
        assert out.loc[2, "f"] == 7.0

    def test_missing_factor_col_is_refused(self):
        frame = pd.DataFrame({"date": ["2024-01-02"], "ticker": ["a"], "g": [1.0]})
        with pytest.raises(ValueError, match="factor_col"):
            preprocess(frame, "f")

    def test_each_step_is_groupby_date(self):
        """Changing one date's inputs must not move another date's output."""
        base = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 6 + ["2024-01-03"] * 6,
                "ticker": [f"t{i:03d}" for i in range(6)] * 2,
                "f": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0] * 2,
                "industry": ["A", "A", "A", "B", "B", "B"] * 2,
                "log_market_cap": list(np.linspace(20.0, 23.0, 6)) * 2,
            }
        )
        perturbed = base.copy()
        # Blow up every value on the second date only.
        perturbed.loc[perturbed["date"] == "2024-01-03", "f"] = (
            perturbed.loc[perturbed["date"] == "2024-01-03", "f"] * 1000 + 500
        )

        out_base = preprocess(base, "f")
        out_pert = preprocess(perturbed, "f")

        first = out_base["date"] == "2024-01-02"
        assert out_base.loc[first, "f"].tolist() == pytest.approx(
            out_pert.loc[first, "f"].tolist()
        )

    def test_neutralize_with_nonstandard_date_col_is_refused(self):
        """``neutralize`` is hard-wired to ``"date"``; a custom date_col must raise.

        Forwarding ``date_col="trade_date"`` would KeyError inside
        ``research.post_processor.neutralize``, or worse, fit against a stale
        ``"date"`` column the caller did not mean.
        """
        frame = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 12,
                "ticker": [f"t{i:03d}" for i in range(12)],
                "f": list(np.linspace(1.0, 12.0, 12)),
                "industry": ["A"] * 6 + ["B"] * 6,
                "log_market_cap": list(np.linspace(20.0, 23.0, 12)),
            }
        )
        with pytest.raises(ValueError, match="requires date_col='date'"):
            preprocess(frame, "f", date_col="trade_date", neutralize=True)
        # neutralize=False with a custom date_col is fine — no neutralize call.
        out = preprocess(frame, "f", date_col="trade_date", neutralize=False)
        assert "trade_date" in out.columns

    def test_exports(self):
        assert callable(preprocess)
        assert callable(factor_contribution)
        assert callable(write_eval_report)


# ---------------------------------------------------------------------------
# full-sample statistics are look-ahead
# ---------------------------------------------------------------------------


class TestFullSampleIsLookahead:
    def test_zscore_uses_per_date_statistics(self):
        """The per-date z-score and the full-sample z-score must differ.

        The implementation uses the former.  Computing μ/σ once over both
        dates would make the second date's high values drag the first date's
        cleaned values up — the failure mode the counter-example pins.
        """
        frame = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 4 + ["2024-01-03"] * 4,
                "ticker": ["a", "b", "c", "d"] * 2,
                "f": [1.0, 2.0, 3.0, 4.0, 100.0, 200.0, 300.0, 400.0],
            }
        )
        out = preprocess(frame, "f", neutralize=False)
        per_date = out.loc[out["date"] == "2024-01-02", "f"].tolist()

        mu, sigma = frame["f"].mean(), frame["f"].std()
        full_sample = (
            (frame.loc[frame["date"] == "2024-01-02", "f"] - mu) / sigma
        ).tolist()

        assert per_date == pytest.approx([-1.161895, -0.387298, 0.387298, 1.161895])
        assert per_date != pytest.approx(full_sample)

    def test_neutralize_fits_one_regression_per_date(self):
        """A pooled regression would let date 2's cross-section set date 1's beta."""
        n = 12
        frame = pd.DataFrame(
            {
                "date": ["2024-01-02"] * n + ["2024-01-03"] * n,
                "ticker": [f"t{i:03d}" for i in range(n)] * 2,
                "industry": (["A"] * 6 + ["B"] * 6) * 2,
                "log_market_cap": list(np.linspace(20.0, 23.0, n)) * 2,
            }
        )
        # Date 1: f is pure size.  Date 2: f is pure noise.
        size = np.linspace(20.0, 23.0, n)
        frame.loc[frame["date"] == "2024-01-02", "f"] = 3.0 * size
        frame.loc[frame["date"] == "2024-01-03", "f"] = np.linspace(-1.0, 1.0, n)

        out = neutralize(frame, "f")
        first = out.loc[out["date"] == "2024-01-02", "f"]
        assert float(np.abs(first).max()) == pytest.approx(0.0, abs=1e-9)


# ---------------------------------------------------------------------------
# factor_contribution
# ---------------------------------------------------------------------------


class TestFactorContribution:
    def _inputs(self):
        weights = _weights_frame(
            [
                ("2024-01-02", "a", 0.5),
                ("2024-01-02", "b", 0.5),
                ("2024-01-03", "a", 0.5),
                ("2024-01-03", "b", 0.5),
            ]
        )
        # Book-level exposure is Σ_j w_j z_j = 0.5·1 + 0.5·0.5 = 0.75 on both
        # factors — long the factors, so a doubled factor return doubles its
        # contribution share.
        exposures = pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
                "ticker": ["a", "b", "a", "b"],
                "f1": [1.0, 0.5, 1.0, 0.5],
                "f2": [1.0, 0.5, 1.0, 0.5],
            }
        )
        factor_returns = pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
                "factor_name": ["f1", "f2", "f1", "f2"],
                "factor_return": [0.02, 0.01, 0.02, 0.01],
            }
        )
        return weights, factor_returns, exposures

    def test_shares_sum_to_one_in_absolute_value(self):
        w, r, e = self._inputs()
        shares = factor_contribution(w, r, e)
        assert float(shares.abs().sum()) == pytest.approx(1.0)

    def test_stronger_factor_takes_the_larger_share(self):
        w, r, e = self._inputs()
        shares = factor_contribution(w, r, e)
        # Identical exposures, f1's return is double f2's.
        assert shares["f1"] == pytest.approx(2 / 3)
        assert shares["f2"] == pytest.approx(1 / 3)

    def test_unknown_factor_name_is_refused(self):
        w, r, e = self._inputs()
        r = r.copy()
        r["factor_name"] = r["factor_name"].replace({"f2": "f9"})
        with pytest.raises(ValueError, match="not in factor_exposures"):
            factor_contribution(w, r, e)

    def test_orphan_exposure_column_is_refused(self):
        """A factor in exposures but absent from returns must raise.

        The inner merge would silently drop the orphan and renormalise the
        remaining shares to sum to 1 — inflating them, so the "strongest
        factor > 70%" fake-diversification check reads wrong numbers.
        """
        w, r, e = self._inputs()
        e = e.copy()
        e["f3"] = [1.0, 0.5, 1.0, 0.5]  # exposure with no factor_returns rows
        with pytest.raises(ValueError, match="no factor_returns rows"):
            factor_contribution(w, r, e)

    def test_missing_columns_are_refused(self):
        w, r, e = self._inputs()
        with pytest.raises(ValueError, match="weights is missing"):
            factor_contribution(w.drop(columns=["weight"]), r, e)


# ---------------------------------------------------------------------------
# evaluate + eval_report.json
# ---------------------------------------------------------------------------


def _eval_inputs():
    """A small, fully-specified book so every metric is computable by hand."""
    # score and forward return share the same rank order, per date.
    combined = pd.DataFrame(
        {
            "date": ["2024-01-02"] * 4,
            "ticker": ["t000", "t001", "t002", "t003"],
            "combined_score": [1.0, -1.0, 0.5, -0.5],
        }
    )
    forward = _ret_frame(
        [
            ("2024-01-02", "t000", 0.02),
            ("2024-01-02", "t001", -0.02),
            ("2024-01-02", "t002", 0.01),
            ("2024-01-02", "t003", -0.01),
        ]
    )
    # Two months of portfolio returns so turnover has a pair of month-ends.
    port = _portfolio_ret(
        [
            ("2024-01-02", 0.01),
            ("2024-01-03", -0.005),
            ("2024-01-04", 0.002),
            ("2024-02-01", 0.004),
            ("2024-02-02", 0.001),
        ]
    )
    weights = _weights_frame(
        [
            ("2024-01-02", "t000", 0.5),
            ("2024-01-02", "t001", 0.5),
            ("2024-02-01", "t000", 0.25),
            ("2024-02-01", "t001", 0.75),
        ]
    )
    exposures = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-02", "2024-02-01", "2024-02-01"],
            "ticker": ["t000", "t001", "t000", "t001"],
            "f1": [1.0, 0.5, 1.0, 0.5],
            "f2": [1.0, 0.5, 1.0, 0.5],
        }
    )
    factor_returns = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-02", "2024-02-01", "2024-02-01"],
            "factor_name": ["f1", "f2", "f1", "f2"],
            "factor_return": [0.02, 0.01, 0.02, 0.01],
        }
    )
    return combined, forward, port, weights, exposures, factor_returns


class TestEvaluate:
    def test_returns_all_eight_metric_keys(self):
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        assert tuple(metrics.keys()) == EVAL_METRIC_KEYS
        assert len(EVAL_METRIC_KEYS) == 8

    def test_ic_metrics_match_hand_calc(self):
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        # score [1,-1,0.5,-0.5] vs ret [0.02,-0.02,0.01,-0.01] — ranks agree
        # exactly (t000 > t002 > t003 > t001), so Spearman is 1.  One date
        # gives no IC dispersion, so ic_ir is not finite.
        assert metrics["ic_mean"] == pytest.approx(1.0)
        assert math.isnan(metrics["ic_ir"])

    def test_portfolio_metrics_are_finite(self):
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        for key in ("ann_return", "ann_sharpe", "max_drawdown", "calmar"):
            assert np.isfinite(metrics[key]), key
        assert metrics["max_drawdown"] <= 0.0
        # One month-pair: |Δw|/2 between (0.5, 0.5) and (0.25, 0.75)
        # = (|−0.25| + |+0.25|) / 2 = 0.25.
        assert metrics["turnover_monthly"] == pytest.approx(0.25)

    def test_factor_contribution_is_the_breakdown(self):
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        contrib = metrics["factor_contribution"]
        assert set(contrib) == {"f1", "f2"}
        assert contrib["f1"] == pytest.approx(2 / 3)
        assert contrib["f2"] == pytest.approx(1 / 3)

    def test_missing_weights_is_refused(self):
        combined, forward, port, _w, exposures, f_ret = _eval_inputs()
        with pytest.raises(ValueError, match="required"):
            FactorCombiner().evaluate(
                combined,
                forward,
                port,
                factor_exposures=exposures,
                factor_returns=f_ret,
            )

    def test_ann_return_formula(self):
        combined, forward, _p, weights, exposures, f_ret = _eval_inputs()
        port = _portfolio_ret([("2024-01-02", 0.01), ("2024-01-03", 0.01)])
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        expected = (1.01**2) ** (252 / 2) - 1
        assert metrics["ann_return"] == pytest.approx(expected)

    def test_max_drawdown_counts_first_period_loss(self):
        """A loss in the first period must show as a drawdown.

        ``nav.cummax()`` without seeding at 1.0 starts at the post-loss NAV,
        so the drawdown from initial capital is invisible and the metric
        under-reports (and with it, calmar).
        """
        from research.factor_combiner import _max_drawdown

        rets = pd.Series([-0.10, 0.05])
        mdd = _max_drawdown(rets)
        assert mdd == pytest.approx(-0.10, abs=1e-12)

        # And through evaluate: the metric table carries the same number.
        combined, forward, _p, weights, exposures, f_ret = _eval_inputs()
        port = _portfolio_ret([("2024-01-02", -0.10), ("2024-01-03", 0.05)])
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        assert metrics["max_drawdown"] == pytest.approx(-0.10, abs=1e-12)


class TestEvalReport:
    def _config_file(self, tmp_path) -> str:
        cfg = tmp_path / "strategy.toml"
        cfg.write_text('name = "mom_vol"\nmethod = "equal_weight"\n', encoding="utf-8")
        return str(cfg)

    def test_report_is_written_under_signals_name(self, tmp_path):
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        cfg = self._config_file(tmp_path)
        path = write_eval_report(
            "mom_vol_composite",
            metrics,
            cfg,
            out_dir=str(tmp_path / "signals"),
            period=("2024-01-01", "2024-02-28"),
            combiner={"method": "equal_weight", "factors": ["f1", "f2"]},
        )

        assert path == tmp_path / "signals" / "mom_vol_composite" / "eval_report.json"
        assert path.is_file()

        report = json.loads(path.read_text(encoding="utf-8"))
        assert report["alpha_name"] == "mom_vol_composite"
        assert report["period"] == ["2024-01-01", "2024-02-28"]
        assert set(report["metrics"]) == set(EVAL_METRIC_KEYS)
        assert report["combiner"]["method"] == "equal_weight"
        assert isinstance(report["walk_forward_folds"], list)

    def test_config_snapshot_is_path_at_hash(self, tmp_path):
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        cfg = self._config_file(tmp_path)
        path = write_eval_report(
            "mom_vol_composite",
            metrics,
            cfg,
            out_dir=str(tmp_path / "signals"),
        )
        report = json.loads(path.read_text(encoding="utf-8"))

        snapshot = report["config_snapshot"]
        assert isinstance(snapshot, str)
        assert "@" in snapshot
        file_part, digest = snapshot.rsplit("@", 1)
        assert file_part == cfg
        assert snapshot == config_snapshot(cfg)
        assert len(digest) == 64  # sha-256 hex

    def test_metrics_are_json_native(self, tmp_path):
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        cfg = self._config_file(tmp_path)
        path = write_eval_report(
            "mom_vol_composite",
            metrics,
            cfg,
            out_dir=str(tmp_path / "signals"),
        )
        text = path.read_text(encoding="utf-8")
        # Round-trip must not need a custom encoder, and must not emit NaN.
        report = json.loads(text)
        assert "NaN" not in text
        assert report["metrics"]["ic_ir"] is None  # single-date IC series

    def test_numpy_types_in_period_folds_and_combiner_are_serialised(self, tmp_path):
        """``period`` / folds / combiner routinely hold numpy scalars and Timestamps.

        ``json.dump`` raises ``TypeError`` on those — the report is then never
        written and the run record is lost.  Every field goes through
        ``_jsonable``.
        """
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        cfg = self._config_file(tmp_path)
        path = write_eval_report(
            "mom_vol_composite",
            metrics,
            cfg,
            out_dir=str(tmp_path / "signals"),
            period=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-28")),
            walk_forward_folds=[
                {"train": pd.Timestamp("2023-01-01"), "n": np.int64(5)}
            ],
            combiner={"lookback": np.int64(60), "factors": ["f1", "f2"]},
        )
        report = json.loads(path.read_text(encoding="utf-8"))
        assert report["period"][0] == "2024-01-01T00:00:00"
        assert report["walk_forward_folds"][0]["n"] == 5
        assert report["combiner"]["lookback"] == 60

    def test_incomplete_metrics_are_refused(self, tmp_path):
        cfg = self._config_file(tmp_path)
        with pytest.raises(ValueError, match="metrics is missing"):
            write_eval_report(
                "mom_vol_composite",
                {"ic_mean": 0.05},
                cfg,
                out_dir=str(tmp_path / "signals"),
            )

    def test_empty_name_is_refused(self, tmp_path):
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        with pytest.raises(ValueError, match="name must be non-empty"):
            write_eval_report("", metrics, self._config_file(tmp_path))

    def test_env_contents_never_land_in_the_report(self, tmp_path):
        """Red line 8: .env is never read, never printed, never snapshotted."""
        combined, forward, port, weights, exposures, f_ret = _eval_inputs()
        metrics = FactorCombiner().evaluate(
            combined,
            forward,
            port,
            weights=weights,
            factor_exposures=exposures,
            factor_returns=f_ret,
        )
        cfg = self._config_file(tmp_path)
        path = write_eval_report(
            "mom_vol_composite",
            metrics,
            cfg,
            out_dir=str(tmp_path / "signals"),
        )
        text = path.read_text(encoding="utf-8")
        assert ".env" not in text
        for key in ("api_key", "password", "token", "aws_secret"):
            assert key not in text.lower()
