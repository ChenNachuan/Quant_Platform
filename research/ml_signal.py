"""ML/DL signal governance — adapter, smoothing, feature versioning, ensemble.

Converts raw ML model outputs (scores or probabilities) onto the unified
cross-sectional z-score scale that :class:`AlphaSignal` consumes, and provides
per-ticker exponential smoothing for daily-frequency strategies.  Converges
with the multi-factor path at ``AlphaSignal.from_ml_factor``.

Iteration 1 scope (ac-3, ac-4): :class:`MLSignalAdapter` (``from_scores`` /
``from_probabilities``) and :func:`smooth_signal`.  Feature versioning,
ensemble, and sanity checks arrive in later iterations.

Design pins (PR restates):

* ``from_probabilities`` clips to ``[1e-6, 1-1e-6]`` before logit — a
  probability of exactly 0 or 1 maps to ±∞, and clipping bounds the logit
  to ±≈13.8 without losing monotonicity.
* Zero cross-sectional std → all-zero scores that date (not NaN/inf).
* ``smooth_signal`` uses per-ticker EWM with ``halflife=5`` default; daily
  strategies call it, monthly rebalance does not (call-site policy).

Not this module: factor combination (``factor_combiner``), weight constraint
solving (``PortfolioConstraints``), MLFactor train/predict scheduling
(``factor_runtime``), or alphaType registration (``alpha_type``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from research.post_processor import standardize_zscore

__all__ = [
    "MLSignalAdapter",
    "smooth_signal",
]

#: Probability clip bounds for ``from_probabilities``.  1e-6 keeps the logit
#: finite (|logit| < 14) while preserving rank order.
PROB_CLIP_LOW: float = 1e-6
PROB_CLIP_HIGH: float = 1.0 - 1e-6

#: Default halflife for :func:`smooth_signal` — 5 trading days (one week).
DEFAULT_HALFLIFE: int = 5

#: Column contract every signal frame honours.
REQUIRED_COLS: tuple[str, ...] = ("date", "ticker", "score")


def _validate_frame(frame: pd.DataFrame, *, score_col: str = "score") -> None:
    """Check ``[date, ticker, <score_col>]`` contract; raise on violation."""
    required = ("date", "ticker", score_col)
    missing = [c for c in required if c not in frame.columns]
    if missing:
        raise ValueError(f"frame is missing columns {missing}")
    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("frame has duplicate (date, ticker) rows")


def _cross_zscore(frame: pd.DataFrame, col: str = "score") -> pd.DataFrame:
    """Cross-sectional z-score of *col* — one standardisation per date.

    A date with zero cross-sectional std becomes all-zero scores (handled by
    :func:`standardize_zscore`), not NaN or inf.
    """
    out = frame.copy()
    out[col] = out.groupby("date")[col].transform(standardize_zscore)
    return out


class MLSignalAdapter:
    """Adapt ML model outputs to the unified ``[date, ticker, score]`` scale.

    Two input paths, both producing a per-date cross-sectional z-score:

    * :meth:`from_scores` — for models that emit raw scores / expected returns.
    * :meth:`from_probabilities` — for classifiers that emit probabilities;
      applies clip → logit → z-score.

    The output is ready for ``AlphaSignal.from_ml_factor``.  Expected-return
    outputs take the scores path (the design rejects quantile grouping and
    binary thresholding — both lose information the signal layer needs).
    """

    @staticmethod
    def from_scores(scores: pd.DataFrame) -> pd.DataFrame:
        """Map raw scores to cross-sectional z-scores.

        Parameters
        ----------
        scores : DataFrame
            Must carry ``[date, ticker, score]``.  ``score`` is a raw model
            output (expected return, margin, etc.).

        Returns
        -------
        DataFrame
            Same shape, ``score`` column replaced by per-date cross-sectional
            z-scores.  A date with zero std → all-zero scores.
        """
        _validate_frame(scores)
        frame = scores[list(REQUIRED_COLS)].copy()
        return _cross_zscore(frame)

    @staticmethod
    def from_probabilities(probs: pd.DataFrame) -> pd.DataFrame:
        """Map classifier probabilities to cross-sectional z-scores.

        Pipeline: clip to ``[1e-6, 1-1e-6]`` → ``logit(p) = log(p/(1-p))``
        → per-date cross-sectional z-score.

        Parameters
        ----------
        probs : DataFrame
            Must carry ``[date, ticker, prob]``.  ``prob`` is a classifier
            probability in ``(0, 1)``.

        Returns
        -------
        DataFrame
            ``[date, ticker, score]`` — per-date cross-sectional z-scores of
            the logit-transformed probabilities.
        """
        _validate_frame(probs, score_col="prob")
        frame = probs[["date", "ticker", "prob"]].copy()
        # Clip to keep logit finite; rank order is preserved.
        p = frame["prob"].astype("float64").clip(PROB_CLIP_LOW, PROB_CLIP_HIGH)
        frame["score"] = np.log(p / (1.0 - p))
        frame = frame[["date", "ticker", "score"]]
        return _cross_zscore(frame)


def smooth_signal(
    frame: pd.DataFrame,
    halflife: int = DEFAULT_HALFLIFE,
) -> pd.DataFrame:
    """Per-ticker exponential weighted moving average on the score column.

    Applies ``pandas.ewm(halflife=...)`` independently to each ticker's score
    time series, preserving date order.  Output has the same shape as input.

    Daily-frequency strategies call this; monthly rebalance does not
    (call-site policy, not a flag).

    Parameters
    ----------
    frame : DataFrame
        Must carry ``[date, ticker, score]``.
    halflife : int
        EWM halflife in periods (trading days).  Default 5 (one week).

    Returns
    -------
    DataFrame
        Same shape as *frame*, ``score`` column replaced by the smoothed
        values.  Date order is preserved.
    """
    _validate_frame(frame)
    if halflife <= 0:
        raise ValueError(f"halflife must be positive, got {halflife!r}")

    out = frame.copy()
    # Compute EWM in chronological order per ticker, then restore original row order.
    orig_index = out.index
    sorted_df = out.sort_values(["ticker", "date"])
    smoothed = sorted_df.groupby("ticker")["score"].transform(
        lambda s: s.ewm(halflife=halflife).mean()
    )
    out.loc[sorted_df.index, "score"] = smoothed
    return out.loc[orig_index]
