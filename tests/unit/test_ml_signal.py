"""Tests for research.ml_signal — MLSignalAdapter and smooth_signal.

Covers acceptance criteria ac-3 and ac-4 of QUANT-46:

* ``from_scores`` does per-date cross-sectional z-score; zero-std → 0.0
* ``from_probabilities`` clips to [1e-6, 1-1e-6] → logit → cs z-score
* ``smooth_signal`` does per-ticker EWM with halflife default 5
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.ml_signal import (
    DEFAULT_HALFLIFE,
    PROB_CLIP_HIGH,
    PROB_CLIP_LOW,
    MLSignalAdapter,
    smooth_signal,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _score_frame(
    dates: list[str],
    tickers: list[str],
    scores: list[float],
) -> pd.DataFrame:
    """Build a ``[date, ticker, score]`` frame from parallel lists."""
    return pd.DataFrame(
        {"date": dates, "ticker": tickers, "score": [float(s) for s in scores]}
    )


def _prob_frame(
    dates: list[str],
    tickers: list[str],
    probs: list[float],
) -> pd.DataFrame:
    """Build a ``[date, ticker, prob]`` frame from parallel lists."""
    return pd.DataFrame(
        {"date": dates, "ticker": tickers, "prob": [float(p) for p in probs]}
    )


# ---------------------------------------------------------------------------
# from_scores (ac-3)
# ---------------------------------------------------------------------------


class TestFromScores:
    def test_basic_zscore(self):
        """Known input → known z-score output."""
        frame = _score_frame(
            ["2024-01-02"] * 4,
            ["A", "B", "C", "D"],
            [1.0, 2.0, 3.0, 4.0],
        )
        result = MLSignalAdapter.from_scores(frame)
        # mean=2.5, std(ddof=1)=1.291..., z = (x-2.5)/1.291...
        expected = np.array(
            [
                -1.161895003862225,
                -0.3872983346207417,
                0.3872983346207417,
                1.161895003862225,
            ]
        )
        np.testing.assert_allclose(result["score"].values, expected, rtol=1e-8)

    def test_zero_std_returns_zeros(self):
        """A date with zero cross-sectional std → all-zero scores, not NaN/inf."""
        frame = _score_frame(
            ["2024-01-02"] * 3,
            ["A", "B", "C"],
            [2.0, 2.0, 2.0],
        )
        result = MLSignalAdapter.from_scores(frame)
        assert (result["score"] == 0.0).all()
        assert not result["score"].isna().any()

    def test_single_ticker_zero_std(self):
        """Single ticker per date → zero score (no cross-sectional dispersion)."""
        frame = _score_frame(["2024-01-02"], ["A"], [5.0])
        result = MLSignalAdapter.from_scores(frame)
        assert result["score"].iloc[0] == 0.0

    def test_multiple_dates_independent(self):
        """Each date is z-scored independently."""
        frame = _score_frame(
            ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            ["A", "B", "A", "B"],
            [1.0, 3.0, 10.0, 20.0],
        )
        result = MLSignalAdapter.from_scores(frame)
        # Date 1: mean=2, std=1.414..., z = [-0.707, 0.707]
        # Date 2: mean=15, std=7.071..., z = [-0.707, 0.707]
        d1 = result[result["date"] == "2024-01-02"]["score"].values
        d2 = result[result["date"] == "2024-01-03"]["score"].values
        np.testing.assert_allclose(
            d1, [-0.7071067811865476, 0.7071067811865476], rtol=1e-8
        )
        np.testing.assert_allclose(
            d2, [-0.7071067811865476, 0.7071067811865476], rtol=1e-8
        )

    def test_output_columns(self):
        """Output has [date, ticker, score] columns."""
        frame = _score_frame(["2024-01-02"] * 2, ["A", "B"], [1.0, 2.0])
        result = MLSignalAdapter.from_scores(frame)
        assert list(result.columns) == ["date", "ticker", "score"]

    def test_missing_columns_raises(self):
        """Missing required columns → ValueError."""
        frame = pd.DataFrame({"date": ["2024-01-02"], "value": [1.0]})
        with pytest.raises(ValueError, match="missing columns"):
            MLSignalAdapter.from_scores(frame)

    def test_duplicate_rows_raises(self):
        """Duplicate (date, ticker) → ValueError."""
        frame = _score_frame(
            ["2024-01-02", "2024-01-02"],
            ["A", "A"],
            [1.0, 2.0],
        )
        with pytest.raises(ValueError, match="duplicate"):
            MLSignalAdapter.from_scores(frame)

    def test_does_not_mutate_input(self):
        """Input DataFrame is not modified."""
        frame = _score_frame(["2024-01-02"] * 2, ["A", "B"], [1.0, 2.0])
        original = frame.copy()
        MLSignalAdapter.from_scores(frame)
        pd.testing.assert_frame_equal(frame, original)


# ---------------------------------------------------------------------------
# from_probabilities (ac-3)
# ---------------------------------------------------------------------------


class TestFromProbabilities:
    def test_basic_pipeline(self):
        """prob=0.5 → logit(0.5)=0 → z-score of [0, 0] is [0, 0] (zero std)."""
        frame = _prob_frame(["2024-01-02"] * 2, ["A", "B"], [0.5, 0.5])
        result = MLSignalAdapter.from_probabilities(frame)
        assert (result["score"] == 0.0).all()

    def test_logit_then_zscore(self):
        """prob values → logit → cs z-score.  Check the full pipeline."""
        # probs: 0.1, 0.5, 0.9 → logits: -2.197, 0.0, 2.197
        # mean = 0, std = 2.197... → z-scores ≈ [-1.0, 0.0, 1.0] (symmetric)
        frame = _prob_frame(
            ["2024-01-02"] * 3,
            ["A", "B", "C"],
            [0.1, 0.5, 0.9],
        )
        result = MLSignalAdapter.from_probabilities(frame)
        expected = np.array([-1.0, 0.0, 1.0])
        np.testing.assert_allclose(result["score"].values, expected, atol=1e-10)

    def test_clip_extreme_probs(self):
        """prob=0 and prob=1 are clipped to [1e-6, 1-1e-6], no inf/NaN."""
        frame = _prob_frame(
            ["2024-01-02"] * 4,
            ["A", "B", "C", "D"],
            [0.0, 1e-8, 1.0, 0.9999999],
        )
        result = MLSignalAdapter.from_probabilities(frame)
        assert result["score"].notna().all()
        assert np.isfinite(result["score"]).all()

    def test_clip_bounds(self):
        """PROB_CLIP_LOW=1e-6, PROB_CLIP_HIGH=1-1e-6."""
        assert PROB_CLIP_LOW == pytest.approx(1e-6)
        assert PROB_CLIP_HIGH == pytest.approx(1.0 - 1e-6)

    def test_extreme_probs_clipped_to_same_logit(self):
        """prob=0 and prob=1e-8 both clip to 1e-6 → same logit → same z-score."""
        frame = _prob_frame(
            ["2024-01-02"] * 2,
            ["A", "B"],
            [0.0, 1e-8],
        )
        result = MLSignalAdapter.from_probabilities(frame)
        # Both clip to 1e-6 → same logit → zero std → both 0
        assert (result["score"] == 0.0).all()

    def test_zero_std_after_logit(self):
        """All same probability → zero std after logit → all-zero scores."""
        frame = _prob_frame(
            ["2024-01-02"] * 3,
            ["A", "B", "C"],
            [0.3, 0.3, 0.3],
        )
        result = MLSignalAdapter.from_probabilities(frame)
        assert (result["score"] == 0.0).all()

    def test_output_columns(self):
        """Output has [date, ticker, score] (not prob)."""
        frame = _prob_frame(["2024-01-02"] * 2, ["A", "B"], [0.3, 0.7])
        result = MLSignalAdapter.from_probabilities(frame)
        assert list(result.columns) == ["date", "ticker", "score"]

    def test_missing_prob_column_raises(self):
        """Missing 'prob' column → ValueError."""
        frame = pd.DataFrame({"date": ["2024-01-02"], "ticker": ["A"], "p": [0.5]})
        with pytest.raises(ValueError, match="missing columns"):
            MLSignalAdapter.from_probabilities(frame)

    def test_does_not_mutate_input(self):
        """Input DataFrame is not modified."""
        frame = _prob_frame(["2024-01-02"] * 2, ["A", "B"], [0.3, 0.7])
        original = frame.copy()
        MLSignalAdapter.from_probabilities(frame)
        pd.testing.assert_frame_equal(frame, original)


# ---------------------------------------------------------------------------
# smooth_signal (ac-4)
# ---------------------------------------------------------------------------


class TestSmoothSignal:
    def test_default_halflife(self):
        """Default halflife is 5."""
        assert DEFAULT_HALFLIFE == 5

    def test_single_observation_unchanged(self):
        """EWM of a single point returns that point."""
        frame = _score_frame(["2024-01-02"], ["A"], [3.0])
        result = smooth_signal(frame)
        assert result["score"].iloc[0] == pytest.approx(3.0)

    def test_per_ticker_independent(self):
        """Each ticker is smoothed independently."""
        # Ticker A: [1, 2, 3], Ticker B: [10, 20, 30]
        frame = _score_frame(
            ["2024-01-02", "2024-01-03", "2024-01-04"] * 2,
            ["A", "A", "A", "B", "B", "B"],
            [1.0, 2.0, 3.0, 10.0, 20.0, 30.0],
        )
        result = smooth_signal(frame, halflife=2)
        a_scores = result[result["ticker"] == "A"]["score"].values
        b_scores = result[result["ticker"] == "B"]["score"].values
        # EWM of [1,2,3] with halflife=2: first=1, then ewm mean
        assert a_scores[0] == pytest.approx(1.0)
        assert b_scores[0] == pytest.approx(10.0)
        # Values should be between min and max of each ticker
        assert 1.0 <= a_scores[-1] <= 3.0
        assert 10.0 <= b_scores[-1] <= 30.0

    def test_output_shape_matches_input(self):
        """Output has same shape as input."""
        frame = _score_frame(
            ["2024-01-02", "2024-01-03", "2024-01-04"],
            ["A", "A", "A"],
            [1.0, 2.0, 3.0],
        )
        result = smooth_signal(frame)
        assert result.shape == frame.shape
        assert list(result.columns) == list(frame.columns)

    def test_date_order_preserved(self):
        """Output preserves original date order."""
        frame = _score_frame(
            ["2024-01-03", "2024-01-01", "2024-01-02"],
            ["A", "A", "A"],
            [3.0, 1.0, 2.0],
        )
        result = smooth_signal(frame)
        # Same date order as input
        assert list(result["date"]) == list(frame["date"])

    def test_halflife_one_recency_weighted(self):
        """halflife=1 gives more weight to recent observations."""
        frame = _score_frame(
            ["2024-01-01", "2024-01-02", "2024-01-03"],
            ["A", "A", "A"],
            [0.0, 0.0, 10.0],
        )
        result = smooth_signal(frame, halflife=1)
        # The last value should be pulled toward 10 but not equal to it
        assert result["score"].iloc[-1] > 0.0
        assert result["score"].iloc[-1] < 10.0

    def test_halflife_must_be_positive(self):
        """halflife=0 → ValueError."""
        frame = _score_frame(["2024-01-02"], ["A"], [1.0])
        with pytest.raises(ValueError, match="halflife"):
            smooth_signal(frame, halflife=0)

    def test_constant_series_unchanged(self):
        """A constant score series remains constant after smoothing."""
        frame = _score_frame(
            ["2024-01-01", "2024-01-02", "2024-01-03"],
            ["A", "A", "A"],
            [5.0, 5.0, 5.0],
        )
        result = smooth_signal(frame)
        assert all(v == pytest.approx(5.0) for v in result["score"])

    def test_does_not_mutate_input(self):
        """Input DataFrame is not modified."""
        frame = _score_frame(
            ["2024-01-02", "2024-01-03"],
            ["A", "A"],
            [1.0, 2.0],
        )
        original = frame.copy()
        smooth_signal(frame)
        pd.testing.assert_frame_equal(frame, original)
