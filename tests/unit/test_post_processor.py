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

import copy
import pickle
from dataclasses import FrozenInstanceError

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
    SIGMA_RTOL,
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


def _frame_with_null_date(col: str = "f") -> pd.DataFrame:
    """Two dated rows and one whose date is null.

    The null-date row belongs to no ``groupby`` group, so a naive ``transform``
    assignment writes NaN over its raw value.
    """
    return pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-02", pd.NaT],
            "ticker": ["t000", "t001", "t002"],
            col: [1.0, 3.0, 7.0],
        }
    )


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

    def test_zero_mad_section_passes_through(self):
        """Half or more tied at the median → band is a point.

        `clip` would collapse the whole cross-section onto the median; there
        is no dispersion to winsorise, so the section must come back intact.
        The settled formula is unchanged — this is the degenerate-input guard.
        """
        s = pd.Series([1.0, 1.0, 1.0, 1.0, 50.0])
        assert (s - s.median()).abs().median() == 0.0

        result = winsorize_mad(s)

        assert result.tolist() == [1.0, 1.0, 1.0, 1.0, 50.0]
        # …and a non-zero MAD still clips by the same formula.
        assert winsorize_mad(pd.Series([1.0, 2.0, 3.0, 50.0])).iloc[-1] < 50.0

    @pytest.mark.parametrize("n", [0, 0.0, -1.0, -0.0001, float("nan")])
    def test_non_positive_n_is_rejected(self, n):
        """Zero collapses the band to a point; negative inverts its bounds.

        Either way `clip` silently wipes the section — the same degeneracy
        the zero-MAD guard refuses.  `n` arrives through
        `PostProcessStep.params`, so this has to be loud.
        """
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 100.0])

        with pytest.raises(ValueError, match="n must be positive"):
            winsorize_mad(s, n=n)

    def test_n_is_rejected_before_the_band_not_only_on_dispersed_sections(self):
        """An invalid `n` must raise every time, not only when MAD is non-zero.

        Checking after the degenerate early-returns would make the raise
        data-dependent: a flat date would quietly return while a dispersed
        one raised.
        """
        flat = pd.Series([1.0, 1.0, 1.0])
        with pytest.raises(ValueError, match="n must be positive"):
            winsorize_mad(flat, n=0.0)

        all_nan = pd.Series([np.nan, np.nan])
        with pytest.raises(ValueError, match="n must be positive"):
            winsorize_mad(all_nan, n=-1.0)

    def test_bad_n_in_a_pipeline_step_raises_rather_than_wiping(self):
        """The reachable path: `PostProcessStep.params` carries `n`."""
        df = _frame({"2024-01-02": [1.0, 2.0, 3.0, 4.0, 100.0]})
        pp = PostProcessor([PostProcessStep("w", winsorize_mad, {"n": -1.0})])

        with pytest.raises(ValueError, match="n must be positive"):
            pp.transform(df, "f")

    def test_positive_n_still_works(self):
        assert winsorize_mad(pd.Series([1.0, 2.0, 3.0, 50.0]), n=3.0).iloc[-1] < 50.0


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

    def test_fillna_cross_section_keeps_null_date_rows(self):
        """`groupby` drops null-key rows, so `transform` returns NaN there.

        Assigning that back would write NaN over a raw value that was fine —
        silent data loss, and the opposite of what `neutralize` does.
        """
        df = _frame_with_null_date()
        out = fillna_cross_section(df, "f")

        assert out["f"].iloc[2] == pytest.approx(7.0)
        assert out["f"].iloc[:2].tolist() == [1.0, 3.0]


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

    def test_zscore_flat_unrepresentable_section_is_zero_not_minus_one(self):
        """`sigma == 0` does not catch a flat section.

        ``[0.1, 0.1, 0.1]`` sums to a rounded mean, so its std comes out at
        the float rounding level rather than at 0.  Dividing by that hands
        back all -1.0 — a flat factor carrying a rounding-dependent signal —
        instead of the documented zeros.
        """
        s = pd.Series([0.1, 0.1, 0.1])
        assert s.std() != 0.0  # the exact comparison really would miss this

        z = standardize_zscore(s)

        assert (z == 0.0).all()

    def test_zscore_tolerance_is_relative_to_section_scale(self):
        """A large flat section must not be normalised either."""
        z = standardize_zscore(pd.Series([1e8, 1e8, 1e8]))
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

    @pytest.mark.parametrize("bad_size", [np.nan, np.inf, -np.inf])
    def test_non_finite_size_excludes_the_row_and_does_not_abort(self, bad_size):
        """A live factor with a non-finite size must not reach `lstsq`.

        Before the mask covered the design columns this raised
        `LinAlgError: SVD did not converge` and took every clean date in the
        frame with it.  The row keeps its input value; the rest are fitted.
        """
        df = _neutral_frame()
        df.loc[3, "log_market_cap"] = bad_size
        original = df["f"].iloc[3]

        out = neutralize(df, "f")

        assert out["f"].iloc[3] == pytest.approx(original)
        keep = [i for i in range(12) if i != 3]
        np.testing.assert_allclose(out["f"].iloc[keep].to_numpy(), 0.0, atol=1e-10)

    def test_missing_industry_excludes_the_row_from_the_fit(self):
        """`get_dummies` renders a missing industry as the reference bucket.

        Left in the fit, that name is neutralised against the wrong beta.
        """
        df = _neutral_frame()
        df.loc[2, "industry"] = np.nan
        original = df["f"].iloc[2]

        out = neutralize(df, "f")

        assert out["f"].iloc[2] == pytest.approx(original)
        keep = [i for i in range(12) if i != 2]
        np.testing.assert_allclose(out["f"].iloc[keep].to_numpy(), 0.0, atol=1e-10)

    def test_missing_size_ignored_when_method_is_industry_only(self):
        """Size is not a regressor here, so it cannot disqualify a row."""
        df = _neutral_frame()
        df.loc[4, "log_market_cap"] = np.nan

        out = neutralize(df, "f", method="industry")

        assert np.isfinite(out["f"]).all()

    def test_non_unique_index_does_not_raise(self):
        """Index labels are the caller's business; the fit is positional."""
        df = pd.concat([_neutral_frame(), _neutral_frame()], ignore_index=True)
        df.index = [0] * len(df)  # every label duplicated

        out = neutralize(df, "f")

        assert out.index.tolist() == df.index.tolist()
        np.testing.assert_allclose(out["f"].to_numpy(), 0.0, atol=1e-10)

    # Rank boundary: the guard must skip exactly the saturated design and
    # nothing else.  Skipping too little wipes the date; skipping too much
    # passes it through still carrying the industry effect — the same silent
    # failure, in the other direction.

    @staticmethod
    def _saturated_frame(n: int) -> pd.DataFrame:
        """n names across n industries — design rank == n, no residual left."""
        return pd.DataFrame(
            {
                "date": ["2024-01-02"] * n,
                "ticker": [f"t{i:03d}" for i in range(n)],
                "f": [float(2 * i + 1) for i in range(n)],
                "industry": [f"I{i}" for i in range(n)],
                "log_market_cap": [float(i * 0.5) for i in range(n)],
            }
        )

    @staticmethod
    def _near_saturated_frame(n: int, collinear_size: bool = False) -> pd.DataFrame:
        """n names across n-1 industries — design rank == n-1, one dof left.

        The factor is constant within industry, so it sits entirely in the
        column space and a correct fit takes it all out.  If the guard refuses
        the date instead, the industry effect survives into the output.

        ``collinear_size`` keeps the rank at n-1 under ``industry_size``: size
        is set constant within industry, which puts it in span{1, dummies}, so
        it cannot spend the last degree of freedom.  Independent size would
        push the design to rank n and the date would correctly be skipped.
        """
        industries = [f"I{i}" for i in range(n - 1)] + ["I0"]
        offsets = {f"I{i}": 10.0 * i for i in range(n - 1)}
        if collinear_size:
            size_map = {f"I{i}": float(i) for i in range(n - 1)}
            sizes = [size_map[ind] for ind in industries]
        else:
            sizes = [float(i * 0.5) for i in range(n)]
        return pd.DataFrame(
            {
                "date": ["2024-01-02"] * n,
                "ticker": [f"t{i:03d}" for i in range(n)],
                "f": [offsets[ind] for ind in industries],
                "industry": industries,
                "log_market_cap": sizes,
            }
        )

    def test_rank_equals_n_still_skips(self):
        """rank == n interpolates every name → residuals ~0 → the date would
        be wiped to nothing.  It must pass through unchanged."""
        n = MIN_NEUTRALIZE_ROWS + 2
        df = self._saturated_frame(n)

        out = neutralize(df, "f", method="industry")
        pd.testing.assert_series_equal(out["f"], df["f"])

        out = neutralize(df, "f")
        pd.testing.assert_series_equal(out["f"], df["f"])

    @pytest.mark.parametrize(
        ("method", "collinear_size"),
        [
            ("industry", False),
            ("industry_size", True),
        ],
        ids=["industry", "industry_size-collinear-size"],
    )
    def test_rank_equals_n_minus_one_neutralises(self, method, collinear_size):
        """rank == n-1 has one residual dof — it is not a saturated fit.

        Refusing this date would let the factor through still carrying its
        industry effect (Test's 12-name / 11-industry case).  It must be
        cleaned.

        Pinned for both methods: ``industry_size`` is the default and the
        stock-pool path, and it only stays at rank n-1 when size is collinear
        with the dummies — independent size spends the last dof and the date
        is correctly skipped as rank n.
        """
        n = MIN_NEUTRALIZE_ROWS + 2
        df = self._near_saturated_frame(n, collinear_size=collinear_size)

        out = neutralize(df, "f", method=method)

        assert (out["f"] != df["f"]).any()  # the date was not skipped
        np.testing.assert_allclose(out["f"].to_numpy(), 0.0, atol=1e-10)

    def test_low_rank_neutralises(self):
        """A section with room to be cleaned must still be cleaned."""
        df = _neutral_frame()  # 12 names, 2 industries → rank 3

        out = neutralize(df, "f")

        np.testing.assert_allclose(out["f"].to_numpy(), 0.0, atol=1e-10)

    def test_saturation_guard_spares_other_dates(self):
        """One dirty date must not stop the clean ones being neutralised."""
        saturated = self._saturated_frame(MIN_NEUTRALIZE_ROWS + 2)
        clean = _neutral_frame().assign(date="2024-01-03")
        df = pd.concat([saturated, clean], ignore_index=True)

        out = neutralize(df, "f")

        kept = out.loc[out["date"] == "2024-01-02", "f"].to_numpy()
        np.testing.assert_allclose(kept, saturated["f"].to_numpy())
        cleaned = out.loc[out["date"] == "2024-01-03", "f"].to_numpy()
        np.testing.assert_allclose(cleaned, 0.0, atol=1e-10)


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

    def test_transform_keeps_null_date_rows(self):
        """A null-date row is in no group, so no step may overwrite it."""
        df = _frame_with_null_date()
        out = PostProcessor().transform(df, "f")

        assert out["f"].iloc[2] == pytest.approx(7.0)

    def test_null_date_rows_survive_every_entry_point(self):
        """`transform` and `fillna_cross_section` match `neutralize` here.

        All three leave the raw value alone rather than writing NaN over it.
        """
        n = MIN_NEUTRALIZE_ROWS + 2
        df = pd.DataFrame(
            {
                "date": ["2024-01-02"] * n + [pd.NaT],
                "ticker": [f"t{i:03d}" for i in range(n + 1)],
                "f": [float(i) for i in range(n)] + [777.0],
                "industry": ["A", "B"] * (n // 2) + ["A"],
                "log_market_cap": [float(i) for i in range(n + 1)],
            }
        )
        tail = df["f"].iloc[-1]

        for out in (
            PostProcessor().transform(df, "f"),
            fillna_cross_section(df, "f"),
            neutralize(df, "f"),
        ):
            assert out["f"].iloc[-1] == pytest.approx(tail)

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

    def test_mutating_step_params_does_not_pollute_presets(self):
        """The presets are module constants; a reader edits a copy, not them.

        `PostProcessor` copies every step along with its `params`, so tuning
        `n` on one instance cannot reach into `PIPELINE_*` and silently change
        what the next caller gets.
        """
        from_explicit = PostProcessor(PIPELINE_RESEARCH)
        from_default = PostProcessor()

        from_explicit.steps[0].params["n"] = 0.25
        from_default.steps[1].params["how"] = "ffill"

        for preset in (
            PIPELINE_RESEARCH,
            PIPELINE_STOCK_EVAL,
            PIPELINE_ETF,
            PIPELINE_RANK,
        ):
            assert all(step.params == {} for step in preset)

    def test_presets_are_accepted_directly(self):
        """Passing a preset is the natural call and must type-check.

        The signature is fixed at ``list[PostProcessStep] | None``, so the
        presets are ``list`` rather than ``tuple`` — a tuple would be rejected
        by a type checker at exactly the call a caller writes first.  The copy
        in `PostProcessor` is what keeps that from making the constant mutable
        through the instance.
        """
        for preset in (
            PIPELINE_RESEARCH,
            PIPELINE_STOCK_EVAL,
            PIPELINE_ETF,
            PIPELINE_RANK,
        ):
            assert isinstance(preset, list), type(preset)

        pp = PostProcessor(PIPELINE_RESEARCH)

        assert [s.name for s in pp.steps] == [s.name for s in PIPELINE_RESEARCH]
        # …and it is still a copy, not the shared constant.
        assert pp.steps[0] is not PIPELINE_RESEARCH[0]


# ---------------------------------------------------------------------------
# preset constants refuse writes — the constant side of the isolation
# ---------------------------------------------------------------------------


class TestPresetConstantsRefuseWrites:
    """A write to a ``PIPELINE_*`` must not land and must not be silent.

    `PostProcessor` copies every step off whatever list it is handed, so a
    write that reaches a preset reaches every later instance built from it —
    `PIPELINE_RESEARCH[0] = x` and `PIPELINE_RESEARCH.append(x)` used to do
    exactly that.  The instance side was already isolated; this is the
    constant side.

    Every case below asserts the write is **refused** (raises) and, where the
    shape matters, that the next instance still sees the original pipeline.
    """

    ALL = (PIPELINE_RESEARCH, PIPELINE_STOCK_EVAL, PIPELINE_ETF, PIPELINE_RANK)

    def test_index_assignment_is_rejected(self):
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH[0] = PIPELINE_RESEARCH[1]

    def test_slice_assignment_is_rejected(self):
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH[:] = []

    def test_deletion_is_rejected(self):
        with pytest.raises(TypeError):
            del PIPELINE_RESEARCH[0]

    def test_appending_is_rejected(self):
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.append(PIPELINE_RESEARCH[0])

    def test_extending_is_rejected(self):
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.extend([PIPELINE_RESEARCH[0]])

    def test_inserting_is_rejected(self):
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.insert(0, PIPELINE_RESEARCH[0])

    def test_removing_popping_and_clearing_are_rejected(self):
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.remove(PIPELINE_RESEARCH[0])
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.pop(0)
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.clear()

    def test_in_place_concat_and_repeat_are_rejected(self):
        # `preset += …` binds the name, so run it through a local: the object
        # under test is still the shared constant.
        preset = PIPELINE_RESEARCH
        before = list(preset)
        with pytest.raises(TypeError):
            preset += [preset[0]]
        with pytest.raises(TypeError):
            preset *= 2
        assert list(preset) == before

    def test_sorting_and_reversing_are_rejected(self):
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.sort()
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.reverse()

    @pytest.mark.parametrize(
        "attr",
        ["PIPELINE_RESEARCH", "PIPELINE_STOCK_EVAL", "PIPELINE_ETF", "PIPELINE_RANK"],
    )
    def test_every_preset_refuses_writes_not_just_research(self, attr):
        import research.post_processor as mod

        preset = getattr(mod, attr)
        with pytest.raises(TypeError):
            preset.append(preset[0])
        with pytest.raises(TypeError):
            preset[0] = preset[0]

    def test_writing_a_preset_step_params_is_rejected(self):
        """`params` is the other handle on a shared step — same rule."""
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH[0].params["n"] = 99

    def test_rebinding_a_preset_step_field_is_rejected(self):
        with pytest.raises(FrozenInstanceError):
            PIPELINE_RESEARCH[0].func = winsorize_mad
        with pytest.raises(FrozenInstanceError):
            PIPELINE_RESEARCH[0].name = "other"

    def test_a_refused_write_leaves_the_next_instance_untouched(self):
        """The point of the refusal: later instances still see the original."""
        before = [s.name for s in PostProcessor(PIPELINE_RESEARCH).steps]

        with pytest.raises(TypeError):
            PIPELINE_RESEARCH.append(PostProcessStep("extra", winsorize_mad))
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH[0] = PostProcessStep("extra", winsorize_mad)
        with pytest.raises(TypeError):
            PIPELINE_RESEARCH[0].params["n"] = 99

        assert [s.name for s in PostProcessor(PIPELINE_RESEARCH).steps] == before
        assert [s.name for s in PostProcessor().steps] == before
        assert all(step.params == {} for step in PIPELINE_RESEARCH)

    def test_copy_hands_back_a_writable_list(self):
        """The supported way to derive a variant — edit a copy, not the preset.

        The copy is shallow, like `list.copy`: the steps are shared value
        objects, so changing one's `params` means building a new
        `PostProcessStep` and replacing the element.
        """
        variant = PIPELINE_RESEARCH.copy()
        assert type(variant) is list

        variant.append(PostProcessStep("extra", winsorize_mad))
        variant[0] = PostProcessStep("winsorize", winsorize_mad, {"n": 0.25})

        assert len(PIPELINE_RESEARCH) == 3
        assert PIPELINE_RESEARCH[0].params == {}
        # …and a pipeline built from the variant really sees the edit.
        pp = PostProcessor(variant)
        assert [s.name for s in pp.steps] == [
            "winsorize",
            "fillna",
            "standardize",
            "extra",
        ]
        assert pp.steps[0].params == {"n": 0.25}

    def test_copy_deepcopy_and_pickle_hand_back_a_plain_writable_list(self):
        """`copy` / `pickle` rebuild a subclass through the mutators this
        class refuses, so `__reduce__` serialises as a plain list instead of
        raising on the way back.  Needed to ship a preset to a worker.
        """
        for clone in (
            copy.copy(PIPELINE_RESEARCH),
            copy.deepcopy(PIPELINE_RESEARCH),
            pickle.loads(pickle.dumps(PIPELINE_RESEARCH)),
        ):
            assert type(clone) is list
            clone.append(PostProcessStep("extra", winsorize_mad))
            assert len(clone) == 4

        assert len(PIPELINE_RESEARCH) == 3

    def test_preset_step_params_survive_copy_and_pickle(self):
        params = PIPELINE_RESEARCH[0].params
        for clone in (
            copy.copy(params),
            copy.deepcopy(params),
            pickle.loads(pickle.dumps(params)),
        ):
            assert type(clone) is dict
            clone["n"] = 3.0

        assert params == {}


class TestExports:
    def test_public_constants_are_in_all(self):
        """`MIN_NEUTRALIZE_ROWS` is cited in `neutralize`'s docstring and
        imported by callers; it belongs in the module's public surface."""
        import research.post_processor as mod

        for name in ("MAD_SCALE", "MIN_NEUTRALIZE_ROWS", "SIGMA_RTOL"):
            assert name in mod.__all__, name
            assert hasattr(mod, name)


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
        for date, group in out.groupby("date"):
            raw = df.loc[df["date"] == date, "f"].astype("float64")
            mu, sigma = raw.mean(), raw.std()
            vals = group["f"]
            # Judge flatness from the *input*, with the same tolerance the
            # implementation uses — `sigma == 0` is not a reliable test.
            if pd.isna(sigma) or sigma <= SIGMA_RTOL * max(1.0, abs(mu)):
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
        if pd.isna(med) or pd.isna(mad):
            return
        if mad == 0:
            # Degenerate band (a point): the section passes through intact
            # rather than collapsing onto the median.
            assert out.tolist() == s.tolist()
            return
        half = 3.0 * MAD_SCALE * mad
        assert out.min() >= med - half - 1e-9
        assert out.max() <= med + half + 1e-9
