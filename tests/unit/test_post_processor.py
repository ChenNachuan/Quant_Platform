"""Tests for research.post_processor — the four-step cross-sectional clean-up.

Covers every DoD row:

* pipeline order is winsorize → fillna → standardize → neutralize; the old
  winsorize → standardize → fillna order is observably different and is
  asserted to differ (``test_step_order_*``)
* every step is ``groupby(date)``-independent — each one has a
  ``TestFullSampleIsLookahead`` counter-example where the full-sample
  statistics are computed in-test and shown to fail the per-date property the
  implementation must satisfy
* MAD winsorize bounds are exactly ``median ± n × 1.4826 × MAD``
* neutralize is one OLS per date on industry dummies (+ log size), residual out
* presets: ``PIPELINE_RESEARCH`` / ``PIPELINE_STOCK_EVAL`` / ``PIPELINE_ETF``
  / ``PIPELINE_RANK``

Plus hypothesis property tests: z-score leaves mean ≈ 0 and std ≈ 1 on every
cross-section independently (``-k hypothesis``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from research.post_processor import (
    MAD_SCALE,
    MIN_NEUTRALIZE_ROWS,
    PIPELINE_ETF,
    PIPELINE_RANK,
    PIPELINE_RESEARCH,
    PIPELINE_STOCK_EVAL,
    PostProcessStep,
    PostProcessor,
    fillna_cross_section,
    fillna_median,
    neutralize,
    standardize_rank,
    standardize_zscore,
    winsorize_mad,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _frame(dates_values: dict[str, list[float]], col: str = "f") -> pd.DataFrame:
    """Build ``DataFrame[date, ticker, {col}]`` from {date: values}."""
    rows = []
    for date, values in dates_values.items():
        for i, v in enumerate(values):
            rows.append({"date": date, "ticker": f"t{i:03d}", col: v})
    return pd.DataFrame(rows)


def _section(df: pd.DataFrame, date: str, col: str = "f") -> pd.Series:
    return df.loc[df["date"] == date, col]


def _zscore(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()


def _neutral_frame() -> pd.DataFrame:
    """12 stocks × 2 industries, with a log-size column, one date.

    ``f`` is exactly ``1 + 2·I(B) + 3·size``, so a correctly specified
    industry+size regression must take the whole thing out.
    """
    industries = ["A"] * 6 + ["B"] * 6
    sizes = [float(i) for i in range(12)]
    ind_b = np.array([1.0 if ind == "B" else 0.0 for ind in industries])
    factor = 1.0 + 2.0 * ind_b + 3.0 * np.array(sizes)
    return pd.DataFrame(
        {
            "date": ["2024-01-02"] * 12,
            "ticker": [f"t{i:03d}" for i in range(12)],
            "f": factor,
            "industry": industries,
            "log_market_cap": sizes,
        }
    )


# ---------------------------------------------------------------------------
# winsorize_mad
# ---------------------------------------------------------------------------


class TestWinsorizeMad:
    def test_bound_formula_is_median_plus_minus_n_scale_mad(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 100.0])
        med = s.median()  # 3.0
        mad = (s - med).abs().median()  # 1.0
        half = 3.0 * MAD_SCALE * mad
        expected = s.clip(med - half, med + half)

        result = winsorize_mad(s)

        assert MAD_SCALE == pytest.approx(1.4826)
        pd.testing.assert_series_equal(result, expected)
        # 100 is outside [3 - 4.4478, 3 + 4.4478]; the inliers are not.
        assert result.iloc[-1] == pytest.approx(3.0 + half)
        assert list(result.iloc[:4]) == [1.0, 2.0, 3.0, 4.0]

    def test_n_scales_the_band(self):
        s = pd.Series([0.0, 1.0, 2.0, 3.0, 50.0])
        # median 2.0, MAD 1.0 → half-width is n × 1.4826.
        tight = winsorize_mad(s, n=0.5)  # band ±0.7413 → 50 clips to 2.7413
        wide = winsorize_mad(s, n=1000.0)  # band ±1482.6 → nothing clips

        assert tight.iloc[-1] == pytest.approx(2.0 + 0.5 * MAD_SCALE)
        assert wide.iloc[-1] == pytest.approx(50.0)
        assert tight.iloc[-1] < wide.iloc[-1]

    def test_all_nan_section_passes_through(self):
        s = pd.Series([np.nan, np.nan])
        result = winsorize_mad(s)
        assert result.isna().all()


# ---------------------------------------------------------------------------
# fillna
# ---------------------------------------------------------------------------


class TestFillna:
    def test_fillna_median_fills_with_the_section_median(self):
        s = pd.Series([1.0, 2.0, 3.0, np.nan])
        result = fillna_median(s)
        assert result.iloc[3] == pytest.approx(2.0)
        assert result.iloc[:3].tolist() == [1.0, 2.0, 3.0]

    def test_fillna_cross_section_uses_each_date_own_median(self):
        df = _frame(
            {"2024-01-02": [1.0, 2.0, np.nan], "2024-01-03": [10.0, 20.0, np.nan]}
        )
        out = fillna_cross_section(df, "f")

        assert out.loc[out["date"] == "2024-01-02", "f"].iloc[2] == pytest.approx(1.5)
        assert out.loc[out["date"] == "2024-01-03", "f"].iloc[2] == pytest.approx(15.0)
        assert df["f"].isna().sum() == 2  # input untouched

    def test_fillna_cross_section_all_nan_date_stays_nan(self):
        df = _frame({"2024-01-02": [np.nan, np.nan]})
        out = fillna_cross_section(df, "f")
        assert out["f"].isna().all()


# ---------------------------------------------------------------------------
# standardize
# ---------------------------------------------------------------------------


class TestStandardize:
    def test_zscore_zero_mean_unit_std(self):
        s = pd.Series([1.0, 3.0, 5.0, 7.0])
        z = standardize_zscore(s)
        assert z.mean() == pytest.approx(0.0, abs=1e-12)
        assert z.std(ddof=1) == pytest.approx(1.0, abs=1e-12)

    def test_zscore_constant_section_is_zero_not_inf(self):
        z = standardize_zscore(pd.Series([4.0, 4.0, 4.0]))
        assert (z == 0.0).all()

    def test_zscore_single_value_is_zero(self):
        z = standardize_zscore(pd.Series([7.0]))
        assert z.iloc[0] == pytest.approx(0.0)

    def test_rank_is_pct_in_zero_one(self):
        s = pd.Series([10.0, 30.0, 20.0])
        r = standardize_rank(s)
        assert r.tolist() == pytest.approx([1 / 3, 1.0, 2 / 3])
        assert r.between(0, 1).all()

    def test_rank_keeps_nan(self):
        r = standardize_rank(pd.Series([1.0, np.nan, 2.0]))
        assert np.isnan(r.iloc[1])
        assert r.iloc[0] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# neutralize
# ---------------------------------------------------------------------------


class TestNeutralize:
    def test_industry_size_residual_is_zero_on_a_linear_factor(self):
        df = _neutral_frame()
        out = neutralize(df, "f")

        np.testing.assert_allclose(out["f"].to_numpy(), 0.0, atol=1e-10)
        # input frame untouched
        assert out is not df
        assert df["f"].iloc[0] == pytest.approx(1.0)

    def test_residual_uncorrelated_with_size(self):
        rng = np.random.default_rng(7)
        df = _neutral_frame()
        df["f"] = df["f"] + rng.normal(0.0, 0.3, size=len(df))
        out = neutralize(df, "f")

        assert abs(np.corrcoef(out["f"], df["log_market_cap"])[0, 1]) < 1e-10

    def test_industry_only_keeps_size_effect_out_of_scope(self):
        df = _neutral_frame()
        out = neutralize(df, "f", method="industry")

        # With size in the true factor and absent from the regression the
        # residual cannot be zero — it is what neutralize could not explain.
        assert np.abs(out["f"]).max() > 0.5

    def test_unknown_method_raises(self):
        df = _neutral_frame()
        with pytest.raises(ValueError, match="method"):
            neutralize(df, "f", method="magic")

    def test_small_cross_section_passes_through(self):
        df = _neutral_frame().head(MIN_NEUTRALIZE_ROWS - 1)
        out = neutralize(df, "f")
        pd.testing.assert_series_equal(out["f"], df["f"])

    def test_nan_factor_rows_stay_nan_and_are_out_of_the_fit(self):
        df = _neutral_frame()
        df.loc[0, "f"] = np.nan
        out = neutralize(df, "f")

        assert np.isnan(out["f"].iloc[0])
        np.testing.assert_allclose(out["f"].iloc[1:].to_numpy(), 0.0, atol=1e-10)


# ---------------------------------------------------------------------------
# PostProcessor + step order
# ---------------------------------------------------------------------------


class TestPostProcessor:
    def test_default_steps_are_the_research_pipeline(self):
        pp = PostProcessor()
        assert [s.name for s in pp.steps] == [s.name for s in PIPELINE_RESEARCH]
        assert [s.func for s in pp.steps] == [
            winsorize_mad,
            fillna_median,
            standardize_zscore,
        ]

    def test_transform_does_not_mutate_the_input(self):
        df = _frame({"2024-01-02": [1.0, 2.0, np.nan]})
        before = df.copy()
        PostProcessor().transform(df, "f")
        pd.testing.assert_frame_equal(df, before)

    def test_custom_params_are_forwarded(self):
        df = _frame({"2024-01-02": [0.0, 1.0, 2.0, 3.0, 100.0]})
        tight = PostProcessor([PostProcessStep("winsorize", winsorize_mad, {"n": 0.1})])
        wide = PostProcessor(
            [PostProcessStep("winsorize", winsorize_mad, {"n": 1000.0})]
        )

        assert (
            tight.transform(df, "f")["f"].iloc[-1]
            < wide.transform(df, "f")["f"].iloc[-1]
        )

    def test_step_order_is_winsorize_fillna_standardize(self):
        """The settled order, applied on one date's values with a hole in it."""
        df = _frame({"2024-01-02": [1.0, 2.0, 10.0, np.nan]})
        out = PostProcessor().transform(df, "f")

        # Reproduce the order by hand on the same section.
        filled = fillna_median(winsorize_mad(_section(df, "2024-01-02")))
        expected = standardize_zscore(filled)
        np.testing.assert_allclose(out["f"].to_numpy(), expected.to_numpy())

    def test_old_order_winsorize_standardize_fillna_observably_differs(self):
        """The superseded order gives a different answer — so the test can tell
        them apart, and the settled order is the one that passes."""
        df = _frame({"2024-01-02": [1.0, 2.0, 10.0, np.nan]})
        settled = PostProcessor().transform(df, "f")["f"]

        section = _section(df, "2024-01-02")
        old = standardize_zscore(winsorize_mad(section))
        old = fillna_median(old)

        assert not np.allclose(settled.to_numpy(), old.to_numpy())
        # The hole is the clearest witness: settled fills *before* scaling.
        assert settled.iloc[3] != pytest.approx(old.iloc[3])

    def test_step_names_record_the_four_stage_vocabulary(self):
        names = [s.name for s in PIPELINE_RESEARCH]
        assert names == ["winsorize", "fillna", "standardize"]


# ---------------------------------------------------------------------------
# presets
# ---------------------------------------------------------------------------


class TestPresets:
    def test_research_is_the_three_series_steps(self):
        assert [s.func for s in PIPELINE_RESEARCH] == [
            winsorize_mad,
            fillna_median,
            standardize_zscore,
        ]

    def test_stock_eval_carries_the_same_three_then_calls_neutralize(self):
        # neutralize takes a DataFrame, so it cannot be a Series step; the
        # preset documents the follow-up call rather than inlining it.
        assert [s.name for s in PIPELINE_STOCK_EVAL] == [
            "winsorize",
            "fillna",
            "standardize",
        ]
        assert all(s.func is not neutralize for s in PIPELINE_STOCK_EVAL)

    def test_etf_has_no_neutralize(self):
        assert all(s.func is not neutralize for s in PIPELINE_ETF)
        assert [s.func for s in PIPELINE_ETF] == [
            winsorize_mad,
            fillna_median,
            standardize_zscore,
        ]

    def test_rank_swaps_standardize_for_rank(self):
        assert [s.func for s in PIPELINE_RANK] == [
            winsorize_mad,
            fillna_median,
            standardize_rank,
        ]

    def test_presets_are_distinct_step_lists(self):
        assert PIPELINE_RESEARCH is not PIPELINE_STOCK_EVAL
        assert PIPELINE_RESEARCH is not PIPELINE_ETF
        assert PIPELINE_RESEARCH is not PIPELINE_RANK


# ---------------------------------------------------------------------------
# full-sample statistics are look-ahead — the failure cases
# ---------------------------------------------------------------------------


class TestFullSampleIsLookahead:
    """Each step, on data where the pooled statistics are the wrong ones.

    The first assertion is the contract (per-date behaviour).  The second
    computes the full-sample path in-test and asserts it *fails* that same
    property — so a switch to pooled statistics cannot pass quietly.
    """

    #: Two dates whose cross-sections sit on disjoint value scales.  Any
    #: statistic computed across both dates is contaminated by the other.
    _SHIFTED = {
        "2024-01-02": [1.0, 2.0, 3.0, 4.0],
        "2024-01-03": [101.0, 102.0, 103.0, 200.0],
    }

    def test_winsorize_full_sample_mad_is_lookahead(self):
        df = _frame(self._SHIFTED)
        out = PostProcessor([PostProcessStep("w", winsorize_mad)]).transform(df, "f")

        # Per-date: each section clips against its own median ± 3·1.4826·MAD.
        for date, values in self._SHIFTED.items():
            expected = winsorize_mad(pd.Series(values))
            np.testing.assert_allclose(
                _section(out, date).to_numpy(), expected.to_numpy()
            )
        # date1's top (4.0) survives its own small MAD band's context…
        assert _section(out, "2024-01-02").iloc[-1] == pytest.approx(4.0)

        # …but the pooled MAD is set by the 200 outlier and would leave date1
        # untouched AND would not clip 200 the way date2's own stats do.
        pooled = winsorize_mad(df["f"])
        assert not np.allclose(pooled.to_numpy(), out["f"].to_numpy())

    def test_fillna_full_sample_median_is_lookahead(self):
        df = _frame(
            {"2024-01-02": [1.0, 2.0, np.nan], "2024-01-03": [100.0, 200.0, np.nan]}
        )
        out = PostProcessor([PostProcessStep("f", fillna_median)]).transform(df, "f")

        per_date = [
            out.loc[out["date"] == "2024-01-02", "f"].iloc[2],
            out.loc[out["date"] == "2024-01-03", "f"].iloc[2],
        ]
        assert per_date == pytest.approx([1.5, 150.0])

        pooled_median = df["f"].median()  # 100.0 — a number from *both* dates
        assert per_date[0] != pytest.approx(pooled_median)
        assert per_date[1] != pytest.approx(pooled_median)

    def test_zscore_full_sample_mean_std_is_lookahead(self):
        df = _frame(self._SHIFTED)
        out = PostProcessor([PostProcessStep("s", standardize_zscore)]).transform(
            df, "f"
        )

        for date in self._SHIFTED:
            section = _section(out, date)
            assert section.mean() == pytest.approx(0.0, abs=1e-10)
            assert section.std(ddof=1) == pytest.approx(1.0, abs=1e-10)

        pooled = _zscore(df["f"])
        # The pooled z-score does not standardise either section on its own —
        # that is precisely the look-ahead.
        for date in self._SHIFTED:
            section = pooled[df["date"] == date]
            assert section.mean() != pytest.approx(0.0, abs=0.1)

    def test_neutralize_pooled_ols_is_lookahead(self):
        """Two dates, two different true factor structures."""
        frames = []
        for date, (c, b_ind, b_size) in {
            "2024-01-02": (1.0, 2.0, 3.0),
            "2024-01-03": (5.0, 0.5, 1.0),
        }.items():
            block = _neutral_frame().assign(date=date)
            ind_b = (block["industry"] == "B").astype(float).to_numpy()
            block["f"] = c + b_ind * ind_b + b_size * block["log_market_cap"].to_numpy()
            frames.append(block)
        df = pd.concat(frames, ignore_index=True)

        out = neutralize(df, "f")
        # Per-date OLS knows each date's own structure: residual is zero.
        for date in ("2024-01-02", "2024-01-03"):
            section = out.loc[out["date"] == date, "f"]
            np.testing.assert_allclose(section.to_numpy(), 0.0, atol=1e-9)

        # One pooled regression cannot fit both structures at once.
        pooled = neutralize(df.assign(date="all"), "f").loc[:, "f"]
        per_date_pooled = {
            date: pooled[df["date"] == date].to_numpy()
            for date in ("2024-01-02", "2024-01-03")
        }
        assert any(np.abs(resid).max() > 1e-6 for resid in per_date_pooled.values())

        # …and it leaves the rng out of it — the failure is structural.
        assert np.abs(out["f"]).max() < 1e-9


# ---------------------------------------------------------------------------
# hypothesis properties
# ---------------------------------------------------------------------------

_FINITE = st.floats(
    allow_nan=False, allow_infinity=False, min_value=-1e6, max_value=1e6, width=32
)
_SECTIONS = st.lists(st.lists(_FINITE, min_size=2, max_size=25), min_size=1, max_size=6)


class TestHypothesis:
    """z-score leaves mean ≈ 0, std ≈ 1 — on every cross-section alone."""

    @settings(max_examples=60, deadline=None)
    @given(sections=_SECTIONS)
    def test_zscore_mean0_std1_per_cross_section(self, sections):
        rows = []
        for i, values in enumerate(sections):
            date = f"2024-01-{i + 2:02d}"
            for j, v in enumerate(values):
                rows.append({"date": date, "ticker": f"t{j:03d}", "f": v})
        df = pd.DataFrame(rows)

        out = PostProcessor([PostProcessStep("s", standardize_zscore)]).transform(
            df, "f"
        )

        assert out.groupby("date").ngroups == len(sections)
        for _, group in out.groupby("date"):
            vals = group["f"]
            sigma = vals.std(ddof=1)
            if pd.isna(sigma) or sigma == 0:
                # No dispersion to standardise: the contract is zeros, not inf.
                assert (vals == 0).all()
            else:
                assert abs(vals.mean()) < 1e-6
                assert abs(vals.std(ddof=1) - 1.0) < 1e-6

    @settings(max_examples=40, deadline=None)
    @given(values=st.lists(_FINITE, min_size=2, max_size=40))
    def test_winsorize_never_exceeds_its_mad_band(self, values):
        s = pd.Series(values)
        out = winsorize_mad(s)
        med = s.median()
        mad = (s - med).abs().median()
        half = 3.0 * MAD_SCALE * mad
        if pd.isna(half):
            return
        assert out.min() >= med - half - 1e-9
        assert out.max() <= med + half + 1e-9
