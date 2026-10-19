"""Tests for research.alpha_signal — AlphaSignal and score → weights.

Covers every acceptance row of S6-T2:

* ``AlphaSignal`` is a frozen dataclass carrying ``frame`` / ``meta`` /
  ``reason`` / ``factor_snapshot`` (``TestAlphaSignalContract``)
* both aggregation paths construct a signal (``TestFromMultiFactor`` /
  ``TestFromMlFactor``)
* ``to_weights(top_k, k)`` puts exactly ``1/k`` on each of the top *k*
  (``TestTopKWeights``)
* ``proportional`` clips negative scores to 0 (``TestProportionalWeights``)
* every book sums to ≤ ``1 + 1e-9`` and holds no negative weight —
  no shorting (``TestHypothesis``, run with ``-k hypothesis``)
* end-of-day Parquet lands under both ``cross/`` and ``timeseries/``
  (``TestWriteSignal``)
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from research.alpha_signal import (
    DEFAULT_K,
    WEIGHT_METHODS,
    AlphaSignal,
    signal_to_weights,
    write_signal,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _combined(values: list[float], date: str = "2024-01-02") -> pd.DataFrame:
    """``[date, ticker, combined_score]`` — FactorCombiner's output shape."""
    return pd.DataFrame(
        {
            "date": [date] * len(values),
            "ticker": [f"t{i:03d}" for i in range(len(values))],
            "combined_score": [float(v) for v in values],
        }
    )


def _ml_scores(values: list[float], date: str = "2024-01-02") -> pd.DataFrame:
    """``[date, ticker, score]`` — MLSignalAdapter's output shape."""
    return pd.DataFrame(
        {
            "date": [date] * len(values),
            "ticker": [f"t{i:03d}" for i in range(len(values))],
            "score": [float(v) for v in values],
        }
    )


def _score_frame(values: list[float], date: str = "2024-01-02") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [date] * len(values),
            "ticker": [f"t{i:03d}" for i in range(len(values))],
            "score": [float(v) for v in values],
        }
    )


# ---------------------------------------------------------------------------
# AlphaSignal contract
# ---------------------------------------------------------------------------


class TestAlphaSignalContract:
    def test_fields_are_the_four_documented_ones(self):
        sig = AlphaSignal(frame=_score_frame([1.0, 2.0]), meta={"strategy_id": "s"})
        assert hasattr(sig, "frame")
        assert hasattr(sig, "meta")
        assert hasattr(sig, "reason")
        assert hasattr(sig, "factor_snapshot")
        assert sig.reason == ""
        assert sig.factor_snapshot is None

    def test_frozen_rejects_field_reassignment(self):
        sig = AlphaSignal(frame=_score_frame([1.0]), meta={})
        with pytest.raises(FrozenInstanceError):
            sig.frame = pd.DataFrame()
        with pytest.raises(FrozenInstanceError):
            sig.meta = {}

    def test_frame_requires_date_ticker_score(self):
        with pytest.raises(ValueError, match="missing columns"):
            AlphaSignal(frame=pd.DataFrame({"date": [1], "ticker": ["a"]}), meta={})

    def test_rejects_duplicate_date_ticker(self):
        frame = pd.DataFrame(
            {"date": ["2024-01-02"] * 2, "ticker": ["a", "a"], "score": [1.0, 2.0]}
        )
        with pytest.raises(ValueError, match="duplicate"):
            AlphaSignal(frame=frame, meta={})

    def test_reason_and_factor_snapshot_accept_values(self):
        snap = {"momentum": {"t000": 1.2}}
        sig = AlphaSignal(
            frame=_score_frame([1.0]),
            meta={"strategy_id": "s"},
            reason="mom_top5 rebalance",
            factor_snapshot=snap,
        )
        assert sig.reason == "mom_top5 rebalance"
        assert sig.factor_snapshot == snap

    def test_caller_frame_mutation_does_not_rewrite_signal(self):
        frame = _score_frame([1.0, 2.0])
        sig = AlphaSignal(frame=frame, meta={"strategy_id": "s"})
        frame.loc[0, "score"] = 999.0
        assert sig.frame.loc[0, "score"] != 999.0

    def test_caller_meta_mutation_does_not_rewrite_signal(self):
        meta = {"strategy_id": "s"}
        sig = AlphaSignal(frame=_score_frame([1.0]), meta=meta)
        meta["strategy_id"] = "hacked"
        assert sig.meta["strategy_id"] == "s"

    def test_caller_factor_snapshot_mutation_does_not_rewrite_signal(self):
        snap = {"momentum": 1.0}
        sig = AlphaSignal(frame=_score_frame([1.0]), meta={}, factor_snapshot=snap)
        snap["momentum"] = 999.0
        assert sig.factor_snapshot["momentum"] == 1.0


# ---------------------------------------------------------------------------
# from_multi_factor
# ---------------------------------------------------------------------------


class TestFromMultiFactor:
    def test_constructs_from_combined_score(self):
        sig = AlphaSignal.from_multi_factor(
            _combined([1.0, 2.0, 3.0, 4.0]), strategy_id="mom_top5"
        )
        assert isinstance(sig, AlphaSignal)
        assert list(sig.frame.columns) == ["date", "ticker", "score"]
        assert sig.meta["strategy_id"] == "mom_top5"

    def test_renames_combined_score_to_score(self):
        sig = AlphaSignal.from_multi_factor(_combined([1.0, 2.0]), strategy_id="s")
        assert "combined_score" not in sig.frame.columns
        assert "score" in sig.frame.columns

    def test_cross_sectional_zscore_per_date(self):
        combined = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 4 + ["2024-01-03"] * 4,
                "ticker": ["a", "b", "c", "d"] * 2,
                "combined_score": [1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0],
            }
        )
        sig = AlphaSignal.from_multi_factor(combined, strategy_id="s")
        for _, group in sig.frame.groupby("date"):
            assert group["score"].mean() == pytest.approx(0.0, abs=1e-10)
            assert group["score"].std(ddof=1) == pytest.approx(1.0, abs=1e-10)

    def test_requires_combined_score_column(self):
        with pytest.raises(ValueError, match="combined_score"):
            AlphaSignal.from_multi_factor(_ml_scores([1.0]), strategy_id="s")

    def test_meta_collects_keyword_arguments(self):
        sig = AlphaSignal.from_multi_factor(
            _combined([1.0, 2.0]),
            strategy_id="mom_top5",
            trade_date="2024-01-02",
        )
        assert sig.meta == {"strategy_id": "mom_top5", "trade_date": "2024-01-02"}

    def test_reason_and_factor_snapshot_are_not_meta(self):
        sig = AlphaSignal.from_multi_factor(
            _combined([1.0, 2.0]),
            strategy_id="s",
            reason="because",
            factor_snapshot={"f1": 0.5},
        )
        assert sig.reason == "because"
        assert sig.factor_snapshot == {"f1": 0.5}
        assert "reason" not in sig.meta
        assert "factor_snapshot" not in sig.meta


# ---------------------------------------------------------------------------
# from_ml_factor
# ---------------------------------------------------------------------------


class TestFromMlFactor:
    def test_constructs_from_ml_scores(self):
        sig = AlphaSignal.from_ml_factor(
            _ml_scores([1.0, 2.0, 3.0]), strategy_id="lgbm_v1"
        )
        assert isinstance(sig, AlphaSignal)
        assert list(sig.frame.columns) == ["date", "ticker", "score"]
        assert sig.meta["strategy_id"] == "lgbm_v1"

    def test_cross_sectional_zscore_per_date(self):
        scores = _ml_scores([1.0, 2.0, 3.0, 4.0])
        sig = AlphaSignal.from_ml_factor(scores, strategy_id="s")
        assert sig.frame["score"].mean() == pytest.approx(0.0, abs=1e-10)
        assert sig.frame["score"].std(ddof=1) == pytest.approx(1.0, abs=1e-10)

    def test_requires_score_column(self):
        with pytest.raises(ValueError, match="missing columns"):
            AlphaSignal.from_ml_factor(_combined([1.0]), strategy_id="s")

    def test_meta_collects_keyword_arguments(self):
        sig = AlphaSignal.from_ml_factor(
            _ml_scores([1.0, 2.0]), strategy_id="lgbm_v1", trade_date="2024-01-02"
        )
        assert sig.meta == {"strategy_id": "lgbm_v1", "trade_date": "2024-01-02"}


# ---------------------------------------------------------------------------
# top_k
# ---------------------------------------------------------------------------


class TestTopKWeights:
    def test_default_method_is_top_k(self):
        assert "top_k" in WEIGHT_METHODS
        frame = _score_frame([3.0, 1.0, 2.0])
        out = signal_to_weights(frame)
        assert len(out) == min(DEFAULT_K, 3)

    def test_k_default_is_20(self):
        frame = _score_frame([float(i) for i in range(25)])
        out = signal_to_weights(frame, method="top_k")
        assert len(out) == 20

    def test_to_weights_equals_one_over_k(self):
        """The DoD line: ``to_weights(top_k, k)`` is exactly ``1/k`` equal weight."""
        scores = [5.0, 1.0, 4.0, 2.0, 3.0]
        sig = AlphaSignal.from_ml_factor(_ml_scores(scores), strategy_id="s")
        out = sig.to_weights(method="top_k", k=3)

        assert len(out) == 3
        assert out["target_weight"].tolist() == [1 / 3, 1 / 3, 1 / 3]
        assert out["target_weight"].sum() == pytest.approx(1.0)
        # Highest scores first: 5.0, 4.0, 3.0 → t000, t002, t004.
        assert set(out["ticker"]) == {"t000", "t002", "t004"}

    def test_top_k_selects_highest_scores(self):
        frame = _score_frame([10.0, -2.0, 7.0, 99.0])
        out = signal_to_weights(frame, method="top_k", k=2)
        assert set(out["ticker"]) == {"t003", "t000"}
        assert out["target_weight"].tolist() == [0.5, 0.5]

    def test_fewer_than_k_names_each_get_one_over_k(self):
        frame = _score_frame([1.0, 2.0])
        out = signal_to_weights(frame, method="top_k", k=5)
        assert len(out) == 2
        assert out["target_weight"].tolist() == [0.2, 0.2]
        assert out["target_weight"].sum() == pytest.approx(0.4)

    def test_dates_are_independent(self):
        frame = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 3 + ["2024-01-03"] * 3,
                "ticker": ["a", "b", "c"] * 2,
                "score": [3.0, 2.0, 1.0, 1.0, 2.0, 3.0],
            }
        )
        out = signal_to_weights(frame, method="top_k", k=2)
        day1 = out.loc[out["date"] == "2024-01-02"]
        day2 = out.loc[out["date"] == "2024-01-03"]
        assert set(day1["ticker"]) == {"a", "b"}
        assert set(day2["ticker"]) == {"b", "c"}
        assert day1["target_weight"].sum() == pytest.approx(1.0)
        assert day2["target_weight"].sum() == pytest.approx(1.0)

    def test_rejects_unknown_method(self):
        with pytest.raises(ValueError, match="method must be one of"):
            signal_to_weights(_score_frame([1.0]), method="martingale")

    def test_rejects_nonpositive_k(self):
        with pytest.raises(ValueError, match="k must be a positive integer"):
            signal_to_weights(_score_frame([1.0]), method="top_k", k=0)

    def test_rejects_fractional_k(self):
        with pytest.raises(ValueError, match="k must be a positive integer"):
            signal_to_weights(_score_frame([1.0]), method="top_k", k=0.5)

    def test_rejects_unexpected_keyword(self):
        with pytest.raises(ValueError, match="unexpected keyword"):
            signal_to_weights(_score_frame([1.0]), method="top_k", min_weight=0.1)

    def test_to_weights_delegates_to_signal_to_weights(self):
        sig = AlphaSignal.from_ml_factor(_ml_scores([3.0, 1.0, 2.0]), strategy_id="s")
        direct = signal_to_weights(sig.frame, method="top_k", k=2)
        delegated = sig.to_weights(method="top_k", k=2)
        pd.testing.assert_frame_equal(direct, delegated)


# ---------------------------------------------------------------------------
# proportional
# ---------------------------------------------------------------------------


class TestProportionalWeights:
    def test_weights_are_proportional_to_positive_scores(self):
        frame = _score_frame([1.0, 3.0, 6.0])
        out = signal_to_weights(frame, method="proportional")
        by_ticker = out.set_index("ticker")["target_weight"]
        assert by_ticker["t000"] == pytest.approx(0.1)
        assert by_ticker["t001"] == pytest.approx(0.3)
        assert by_ticker["t002"] == pytest.approx(0.6)

    def test_negative_score_clipped_to_zero(self):
        """The gap #14 proposal: a negative score holds nothing, short or not."""
        frame = _score_frame([2.0, -5.0, 3.0])
        out = signal_to_weights(frame, method="proportional")

        assert set(out["ticker"]) == {"t000", "t002"}
        assert (out["target_weight"] > 0).all()
        by_ticker = out.set_index("ticker")["target_weight"]
        assert by_ticker["t000"] == pytest.approx(2.0 / 5.0)
        assert by_ticker["t002"] == pytest.approx(3.0 / 5.0)

    def test_all_negative_scores_hold_nothing(self):
        frame = _score_frame([-1.0, -2.0, -0.5])
        out = signal_to_weights(frame, method="proportional")
        assert out.empty
        assert list(out.columns) == ["date", "ticker", "target_weight"]

    def test_zero_dispersion_falls_back_to_equal_weight(self):
        """All-zero scores after z-score are uninformative, not rejected."""
        frame = _score_frame([0.0, 0.0, 0.0])
        out = signal_to_weights(frame, method="proportional")
        assert len(out) == 3
        assert out["target_weight"].tolist() == [1 / 3, 1 / 3, 1 / 3]

    def test_zero_dispersion_from_factory_falls_back(self):
        """A flat section z-scores to all 0.0 — proportional must still invest."""
        sig = AlphaSignal.from_ml_factor(_ml_scores([5.0, 5.0, 5.0]), strategy_id="s")
        assert (sig.frame["score"] == 0.0).all()
        out = sig.to_weights(method="proportional")
        assert len(out) == 3
        assert out["target_weight"].sum() == pytest.approx(1.0)

    def test_zero_and_negative_hold_nothing(self):
        """Zeros are uninformative; negatives are rejected — nothing survives."""
        frame = _score_frame([0.0, -1.0, -2.0])
        out = signal_to_weights(frame, method="proportional")
        assert out.empty

    def test_zero_score_is_not_held(self):
        frame = _score_frame([0.0, 2.0, 0.0])
        out = signal_to_weights(frame, method="proportional")
        assert set(out["ticker"]) == {"t001"}
        assert out["target_weight"].tolist() == [1.0]

    def test_k_is_ignored_for_proportional(self):
        frame = _score_frame([1.0, 1.0, 1.0, 1.0])
        out = signal_to_weights(frame, method="proportional", k=1)
        assert len(out) == 4
        assert out["target_weight"].sum() == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# end-of-day archive
# ---------------------------------------------------------------------------


class TestWriteSignal:
    def _signal_and_weights(self, tmp_path):
        sig = AlphaSignal.from_multi_factor(
            _combined([1.0, 2.0, 3.0, 4.0]),
            strategy_id="mom_top5",
            signal_time="2024-01-02T15:00:00",
            reason="month-end rebalance",
            factor_snapshot={"momentum_20d": 0.5},
        )
        weights = sig.to_weights(method="top_k", k=2)
        return sig, weights

    def test_writes_both_cross_and_timeseries_directories(self, tmp_path):
        sig, weights = self._signal_and_weights(tmp_path)
        paths = write_signal(sig, weights, base_dir=str(tmp_path))

        assert set(paths) == {"cross", "timeseries"}
        assert len(paths["cross"]) == 1
        assert len(paths["timeseries"]) == 1
        assert paths["cross"][0].exists()
        assert paths["timeseries"][0].exists()

    def test_cross_path_structure(self, tmp_path):
        sig, weights = self._signal_and_weights(tmp_path)
        paths = write_signal(sig, weights, base_dir=str(tmp_path))
        cross = paths["cross"][0]

        assert cross.parent.name == "mom_top5"
        assert cross.parent.parent.name == "cross"
        assert cross.name == "2024-01-02.parquet"

    def test_timeseries_path_structure(self, tmp_path):
        sig, weights = self._signal_and_weights(tmp_path)
        paths = write_signal(sig, weights, base_dir=str(tmp_path))
        ts = paths["timeseries"][0]

        assert ts.parent.name == "mom_top5"
        assert ts.parent.parent.name == "timeseries"
        assert ts.name == "2024-01-02.parquet"

    def test_cross_columns_match_the_contract(self, tmp_path):
        sig, weights = self._signal_and_weights(tmp_path)
        paths = write_signal(sig, weights, base_dir=str(tmp_path))
        df = pd.read_parquet(paths["cross"][0])

        assert list(df.columns) == [
            "signal_time",
            "ticker",
            "target_weight",
            "score",
            "strategy_id",
            "trade_date",
        ]
        assert set(df["strategy_id"]) == {"mom_top5"}
        assert set(df["trade_date"].astype(str)) == {"2024-01-02"}

    def test_timeseries_columns_match_the_contract(self, tmp_path):
        sig, weights = self._signal_and_weights(tmp_path)
        paths = write_signal(sig, weights, base_dir=str(tmp_path))
        df = pd.read_parquet(paths["timeseries"][0])

        assert list(df.columns) == [
            "datetime",
            "ticker",
            "direction",
            "strength",
            "quantity",
            "factor_snapshot",
            "reason",
        ]
        assert (df["quantity"] == 0.0).all()
        assert set(df["reason"].astype(str)) == {"month-end rebalance"}

    def test_one_file_per_date(self, tmp_path):
        combined = pd.DataFrame(
            {
                "date": ["2024-01-02"] * 3 + ["2024-01-03"] * 3,
                "ticker": ["a", "b", "c"] * 2,
                "combined_score": [1.0, 2.0, 3.0, 3.0, 2.0, 1.0],
            }
        )
        sig = AlphaSignal.from_multi_factor(combined, strategy_id="mom_top5")
        weights = sig.to_weights(method="top_k", k=1)
        paths = write_signal(sig, weights, base_dir=str(tmp_path))

        assert [p.name for p in paths["cross"]] == [
            "2024-01-02.parquet",
            "2024-01-03.parquet",
        ]
        assert [p.name for p in paths["timeseries"]] == [
            "2024-01-02.parquet",
            "2024-01-03.parquet",
        ]

    def test_requires_strategy_id(self, tmp_path):
        sig = AlphaSignal.from_ml_factor(_ml_scores([1.0, 2.0]))
        weights = sig.to_weights(method="top_k", k=1)
        with pytest.raises(ValueError, match="strategy_id"):
            write_signal(sig, weights, base_dir=str(tmp_path))

    def test_date_type_mismatch_does_not_archive_all_cash(self, tmp_path):
        """A Parquet/DB round-trip changes date dtype; the merge must not miss."""
        import datetime

        sig = AlphaSignal.from_multi_factor(
            _combined([1.0, 2.0, 3.0, 4.0]),
            strategy_id="mom_top5",
        )
        weights = sig.to_weights(method="top_k", k=2)
        # Simulate a round-trip: frame dates stay str, weights dates become date.
        weights["date"] = weights["date"].map(
            lambda d: datetime.date.fromisoformat(str(d))
        )
        paths = write_signal(sig, weights, base_dir=str(tmp_path))
        df = pd.read_parquet(paths["cross"][0])

        assert df["target_weight"].sum() == pytest.approx(1.0)
        assert (df["target_weight"] >= 0).all()


# ---------------------------------------------------------------------------
# hypothesis properties
# ---------------------------------------------------------------------------

_FINITE = st.floats(
    allow_nan=False, allow_infinity=False, min_value=-1e6, max_value=1e6, width=32
)
_SCORES = st.lists(_FINITE, min_size=1, max_size=30)


class TestHypothesis:
    """Every book is investable and long-only: sum ≤ 1, no negative weight."""

    @settings(max_examples=50, deadline=None)
    @given(scores=_SCORES, k=st.integers(min_value=1, max_value=30))
    def test_hypothesis_top_k_sum_bounded_and_nonnegative(self, scores, k):
        out = signal_to_weights(_score_frame(scores), method="top_k", k=k)
        assert out["target_weight"].sum() <= 1 + 1e-9
        assert (out["target_weight"] >= 0).all()

    @settings(max_examples=50, deadline=None)
    @given(scores=_SCORES)
    def test_hypothesis_proportional_sum_bounded_and_nonnegative(self, scores):
        out = signal_to_weights(_score_frame(scores), method="proportional")
        assert out["target_weight"].sum() <= 1 + 1e-9
        assert (out["target_weight"] >= 0).all()

    @settings(max_examples=30, deadline=None)
    @given(
        sections=st.lists(_SCORES, min_size=1, max_size=4),
        k=st.integers(min_value=1, max_value=15),
    )
    def test_hypothesis_per_date_sum_bounded_top_k(self, sections, k):
        rows = []
        for i, values in enumerate(sections):
            date = f"2024-01-{i + 2:02d}"
            for j, v in enumerate(values):
                rows.append({"date": date, "ticker": f"t{j:03d}", "score": v})
        out = signal_to_weights(pd.DataFrame(rows), method="top_k", k=k)
        if out.empty:
            return
        per_date = out.groupby("date")["target_weight"].sum()
        assert (per_date <= 1 + 1e-9).all()
        assert (out["target_weight"] >= 0).all()

    @settings(max_examples=30, deadline=None)
    @given(scores=_SCORES)
    def test_hypothesis_proportional_never_shorts(self, scores):
        """A negative score cannot survive the clip into a negative weight."""
        out = signal_to_weights(_score_frame(scores), method="proportional")
        if out.empty:
            return
        assert (out["target_weight"] > 0).all()
        assert (
            out["target_weight"].sum() == pytest.approx(1.0, abs=1e-9)
            or out["target_weight"].sum() <= 1 + 1e-9
        )
