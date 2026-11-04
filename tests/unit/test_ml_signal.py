"""Tests for research.ml_signal — MLSignalAdapter, smooth_signal, feature versioning.

Covers acceptance criteria ac-2, ac-3, ac-4 of QUANT-46:

* ``from_scores`` does per-date cross-sectional z-score; zero-std → 0.0
* ``from_probabilities`` clips to [1e-6, 1-1e-6] → logit → cs z-score
* ``smooth_signal`` does per-ticker EWM with halflife default 5
* ``features_hash`` change → ``model_id`` different; sha256 over sorted specs + depends_hash
* ``metadata.json`` carries ``model_id`` / ``features_hash``
"""

from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from research.ml_signal import (
    DEFAULT_HALFLIFE,
    PROB_CLIP_HIGH,
    PROB_CLIP_LOW,
    MLSignalAdapter,
    build_feature_matrix,
    features_hash,
    model_id,
    smooth_signal,
    transform_bars,
    write_experiment,
    write_model,
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


# ---------------------------------------------------------------------------
# build_feature_matrix (ac-2)
# ---------------------------------------------------------------------------


class TestBuildFeatureMatrix:
    def test_raw_features(self, tmp_path):
        """source=raw features with pct_change and zscore_cs transforms."""
        bars = pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-03", "2024-01-04"] * 2,
                "ticker": ["A", "A", "A", "B", "B", "B"],
                "close": [10.0, 11.0, 12.0, 20.0, 22.0, 24.0],
            }
        )
        specs = [
            {"source": "raw", "field": "close", "transform": "pct_change_1"},
            {"source": "raw", "field": "close", "transform": "zscore_cs"},
        ]
        result = build_feature_matrix(
            specs, start="2024-01-02", end="2024-01-04", bars=bars
        )
        assert "feat_0" in result.columns
        assert "feat_1" in result.columns
        assert len(result) == 6

    def test_empty_specs_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            build_feature_matrix([], start="2024-01-02", end="2024-01-04")

    def test_raw_requires_bars(self):
        specs = [{"source": "raw", "field": "close", "transform": "zscore_cs"}]
        with pytest.raises(ValueError, match="bars"):
            build_feature_matrix(specs, start="2024-01-02", end="2024-01-04")

    def test_factor_requires_name(self):
        specs = [{"source": "factor", "transform": "zscore_cs"}]
        with pytest.raises(ValueError, match="name"):
            build_feature_matrix(specs, start="2024-01-02", end="2024-01-04")

    def test_unknown_source_raises(self):
        specs = [{"source": "bogus", "field": "close", "transform": "zscore_cs"}]
        with pytest.raises(ValueError, match="unknown source"):
            build_feature_matrix(specs, start="2024-01-02", end="2024-01-04")


# ---------------------------------------------------------------------------
# transform_bars (ac-2)
# ---------------------------------------------------------------------------


class TestTransformBars:
    def _bars(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-03", "2024-01-04"] * 2,
                "ticker": ["A", "A", "A", "B", "B", "B"],
                "close": [10.0, 11.0, 12.0, 20.0, 22.0, 24.0],
            }
        )

    def test_pct_change(self):
        bars = self._bars()
        result = transform_bars(bars, "close", "pct_change_1")
        # Ticker A: [NaN, 0.1, 0.0909...]; Ticker B: [NaN, 0.1, 0.0909...]
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(0.1)
        assert result.iloc[2] == pytest.approx(1 / 11)

    def test_zscore_cs(self):
        bars = self._bars()
        result = transform_bars(bars, "close", "zscore_cs")
        # Date 1: [10, 20] → mean=15, std=7.071 → z=[-0.707, 0.707]
        assert result.iloc[0] == pytest.approx(-0.7071067811865476)
        assert result.iloc[3] == pytest.approx(0.7071067811865476)

    def test_unknown_transform_raises(self):
        bars = self._bars()
        with pytest.raises(ValueError, match="unknown transform"):
            transform_bars(bars, "close", "bogus")

    def test_missing_field_raises(self):
        bars = self._bars()
        with pytest.raises(ValueError, match="missing column"):
            transform_bars(bars, "open", "pct_change_1")

    def test_bad_pct_change_period_raises(self):
        bars = self._bars()
        with pytest.raises(ValueError, match="pct_change"):
            transform_bars(bars, "close", "pct_change_abc")


# ---------------------------------------------------------------------------
# features_hash (ac-2)
# ---------------------------------------------------------------------------


class TestFeaturesHash:
    def _spec(self, **kw) -> dict:
        base = {"source": "raw", "field": "close", "transform": "pct_change_5"}
        base.update(kw)
        return base

    def test_deterministic(self):
        s = [self._spec()]
        assert features_hash(s, []) == features_hash(s, [])

    def test_spec_change_changes_hash(self):
        h1 = features_hash([self._spec()], [])
        h2 = features_hash([self._spec(transform="zscore_cs")], [])
        assert h1 != h2

    def test_dep_hash_change_changes_hash(self):
        s = [self._spec(source="factor", name="momentum")]
        h1 = features_hash(s, ["abc123"])
        h2 = features_hash(s, ["def456"])
        assert h1 != h2

    def test_dep_hash_order_irrelevant(self):
        s = [self._spec(source="factor", name="momentum")]
        assert features_hash(s, ["abc", "def"]) == features_hash(s, ["def", "abc"])

    def test_dep_hash_dedup(self):
        s = [self._spec(source="factor", name="momentum")]
        assert features_hash(s, ["abc", "abc"]) == features_hash(s, ["abc"])

    def test_spec_order_irrelevant(self):
        s1 = [self._spec(), self._spec(field="volume")]
        s2 = [self._spec(field="volume"), self._spec()]
        assert features_hash(s1, []) == features_hash(s2, [])

    def test_sha256_format(self):
        h = features_hash([self._spec()], [])
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_matches_manual_sha256(self):
        """Verify the hash is exactly sha256(canonical_json + "|" + "|".join(sorted(deps)))."""
        spec = {"source": "raw", "field": "close", "transform": "pct_change_5"}
        specs = [spec]
        deps = ["abc123", "def456"]
        canonical = json.dumps(specs, sort_keys=True, separators=(",", ":"))
        raw = canonical + "|" + "|".join(sorted(deps))
        expected = hashlib.sha256(raw.encode()).hexdigest()
        assert features_hash(specs, deps) == expected


# ---------------------------------------------------------------------------
# model_id (ac-2)
# ---------------------------------------------------------------------------


class TestModelId:
    def test_format(self):
        mid = model_id("alpha", 1, 2, "2023-01-01", "2023-12-31", "abc")
        assert mid == "alpha+v1.2(2023-01-01)+2023-12-31+abc"

    def test_features_hash_change_changes_model_id(self):
        m1 = model_id("alpha", 1, 0, "2023-01-01", "2023-12-31", "hash1")
        m2 = model_id("alpha", 1, 0, "2023-01-01", "2023-12-31", "hash2")
        assert m1 != m2

    def test_full_pipeline_hash_change(self):
        """specs change → features_hash change → model_id change (ac-2 assertion)."""
        spec1 = [{"source": "raw", "field": "close", "transform": "pct_change_5"}]
        spec2 = [{"source": "raw", "field": "close", "transform": "zscore_cs"}]
        h1 = features_hash(spec1, [])
        h2 = features_hash(spec2, [])
        m1 = model_id("alpha", 1, 0, "2023-01-01", "2023-12-31", h1)
        m2 = model_id("alpha", 1, 0, "2023-01-01", "2023-12-31", h2)
        assert m1 != m2


# ---------------------------------------------------------------------------
# write_model (ac-2)
# ---------------------------------------------------------------------------


class TestWriteModel:
    def _metadata(self, mid: str, feat_hash: str) -> dict:
        return {
            "model_id": mid,
            "train_end": "2023-12-31",
            "features_hash": feat_hash,
            "metrics": {"sharpe": 1.5},
            "random_seed": 42,
            "deps": ["numpy==1.24"],
            "features": ["feat_0", "feat_1"],
            "date_range": ["2023-01-01", "2023-12-31"],
            "hyperparams": {"lr": 0.01},
        }

    def test_writes_three_files(self, tmp_path):
        spec = [{"source": "raw", "field": "close", "transform": "pct_change_5"}]
        fh = features_hash(spec, [])
        mid = model_id("alpha", 1, 0, "2023-01-01", "2023-12-31", fh)
        out = write_model(
            "alpha",
            1,
            0,
            model="fake_model",
            config={"lr": 0.01},
            metadata=self._metadata(mid, fh),
            base_dir=str(tmp_path),
        )
        assert (out / "model.pkl").exists()
        assert (out / "config.json").exists()
        assert (out / "metadata.json").exists()

    def test_metadata_carries_model_id_and_features_hash(self, tmp_path):
        spec = [{"source": "raw", "field": "close", "transform": "pct_change_5"}]
        fh = features_hash(spec, [])
        mid = model_id("alpha", 1, 0, "2023-01-01", "2023-12-31", fh)
        out = write_model(
            "alpha",
            1,
            0,
            model="fake",
            config={},
            metadata=self._metadata(mid, fh),
            base_dir=str(tmp_path),
        )
        meta = json.loads((out / "metadata.json").read_text())
        assert meta["model_id"] == mid
        assert meta["features_hash"] == fh

    def test_refuses_reject(self, tmp_path):
        with pytest.raises(ValueError, match="REJECT"):
            write_model(
                "alpha",
                1,
                0,
                model="fake",
                config={},
                metadata={},
                verdict="REJECT",
                base_dir=str(tmp_path),
            )
        assert not (tmp_path / "alpha").exists()

    def test_pass_writes(self, tmp_path):
        out = write_model(
            "alpha",
            1,
            0,
            model="fake",
            config={},
            metadata={},
            verdict="PASS",
            base_dir=str(tmp_path),
        )
        assert out.exists()


# ---------------------------------------------------------------------------
# write_experiment (ac-2)
# ---------------------------------------------------------------------------


class TestWriteExperiment:
    def test_writes_two_files(self, tmp_path):
        out = write_experiment(
            "exp_001",
            metrics={"sharpe": 1.5},
            config={"lr": 0.01},
            base_dir=str(tmp_path),
        )
        assert (out / "metrics.json").exists()
        assert (out / "config.json").exists()

    def test_metrics_carries_created_at(self, tmp_path):
        out = write_experiment(
            "exp_001",
            metrics={"sharpe": 1.5},
            config={},
            base_dir=str(tmp_path),
        )
        m = json.loads((out / "metrics.json").read_text())
        assert "created_at" in m
        assert m["sharpe"] == 1.5

    def test_config_carried(self, tmp_path):
        out = write_experiment(
            "exp_001",
            metrics={},
            config={"lr": 0.01},
            base_dir=str(tmp_path),
        )
        c = json.loads((out / "config.json").read_text())
        assert c["lr"] == 0.01
