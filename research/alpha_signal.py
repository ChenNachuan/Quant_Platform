"""Unified signal layer — :class:`AlphaSignal` and score → position weights.

Collapses the combined multi-factor score and the ML score onto one contract
(alpha_signal.md §32, §63.5) and turns that score into unconstrained target
weights.  This is the first artefact of the three-step main chain
(``AlphaSignal → Portfolio → Execution``).

Three responsibilities and nothing else:

1. :class:`AlphaSignal` — a frozen ``frame``/``meta``/``reason``/
   ``factor_snapshot`` bundle whose ``frame`` is ``[date, ticker, score]``
   after a **cross-sectional z-score** (comparable and additive across
   factors).
2. Two aggregation paths — :meth:`AlphaSignal.from_multi_factor`
   (``FactorCombiner`` output ``[date, ticker, combined_score]``) and
   :meth:`AlphaSignal.from_ml_factor` (``MLSignalAdapter`` output
   ``[date, ticker, score]``).
3. :func:`signal_to_weights` — ``top_k`` equal weight (the default, robust and
   parameter-free) or ``proportional`` with **negative scores clipped to 0**
   (A-share cannot short; a negative weight is a directional bet this layer is
   not licensed to make).  End-of-day Parquet archives land under
   ``data_platform/signals/cross/`` and ``data_platform/signals/timeseries/``.

Not this module: factor combination (S6-T1 ``factor_combiner``), weight
constraint solving (the authority is ``PortfolioConstraints``, S7
``portfolio_construction``), time-series single-ticker or intraday signals
(the Tick layer is out of scope), and order generation (S7
``order_generation``).  ``to_weights`` output is an **unconstrained initial
value**; concentration / turnover / capital constraints are solved downstream.

Gap #14 (clip-0 proposal): ``proportional`` truncates negative scores to 0 and
normalises over the remainder.  All-non-positive cross-sections produce no
rows — holding nothing is the honest answer when every name scores below the
line, and a fallback to equal weight would silently re-introduce the names the
score rejected.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from research.post_processor import standardize_zscore

__all__ = [
    "AlphaSignal",
    "DEFAULT_K",
    "WEIGHT_METHODS",
    "signal_to_weights",
    "write_signal",
]

#: ``top_k`` breadth.  20 names is the design default (§63.5); a caller that
#: needs a different book passes ``k=`` and says why in the PR.
DEFAULT_K: int = 20

#: The two methods the design allows.  ``top_k`` is the primary path — equal
#: weight is robust and adds no parameter to overfit; ``proportional`` is the
#: optional score-weighted alternative.
WEIGHT_METHODS: tuple[str, ...] = ("top_k", "proportional")

#: The contract every signal frame honours: one score per (date, ticker).
REQUIRED_FRAME_COLS: tuple[str, ...] = ("date", "ticker", "score")

#: Column contract of the end-of-day cross archive.
CROSS_COLS: tuple[str, ...] = (
    "signal_time",
    "ticker",
    "target_weight",
    "score",
    "strategy_id",
    "trade_date",
)

#: Column contract of the end-of-day timeseries archive.
TIMESERIES_COLS: tuple[str, ...] = (
    "datetime",
    "ticker",
    "direction",
    "strength",
    "quantity",
    "factor_snapshot",
    "reason",
)

#: Tolerance for the ``weights.sum() ≤ 1`` property.  1e-9 absorbs the float
#: error of summing ``1/k`` k times without ever letting a real over-allocation
#: through.
WEIGHT_SUM_TOL: float = 1e-9


# ---------------------------------------------------------------------------
# frame helpers
# ---------------------------------------------------------------------------


def _validate_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Check the ``[date, ticker, score]`` contract; return *frame* unchanged."""
    missing = [c for c in REQUIRED_FRAME_COLS if c not in frame.columns]
    if missing:
        raise ValueError(f"signal frame is missing columns {missing}")
    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("signal frame has duplicate (date, ticker) rows")
    return frame


def _cross_zscore(frame: pd.DataFrame, col: str = "score") -> pd.DataFrame:
    """Cross-sectional z-score of *col* — one standardisation per date.

    A full-sample mean/σ would let tomorrow's names decide today's score; each
    date is standardised alone, so the score is comparable across names without
    borrowing from the future.
    """
    out = frame.copy()
    out[col] = out.groupby("date")[col].transform(standardize_zscore)
    return out


def _empty_weights() -> pd.DataFrame:
    return pd.DataFrame(columns=["date", "ticker", "target_weight"])


def _date_str(value) -> str:
    """Normalise a date cell to the ``YYYY-MM-DD`` used in archive filenames."""
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _jsonable(value):
    """JSON cannot carry numpy scalars or NaN; normalise them."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


# ---------------------------------------------------------------------------
# AlphaSignal
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AlphaSignal:
    """One cross-sectional signal: scores, provenance, and why.

    ``frame`` is ``[date, ticker, score]`` and is expected to already carry the
    cross-sectional z-score (both factories apply one).  ``meta`` carries at
    least ``strategy_id``; ``trade_date`` / ``signal_time`` / ``config_snapshot``
    ride along when the caller has them.  ``reason`` and ``factor_snapshot``
    exist so a signal can say *why* it exists and *which factor values* pushed
    it — a bare number is not an auditable signal.

    Frozen, because a signal is a point-in-time artefact: handing the same
    object to portfolio construction and to the archive must not let one
    mutate what the other reads.  ``eq=False`` — a DataFrame-bearing dataclass
    cannot generate a meaningful ``__eq__``/``__hash__``, and a half-working
    one is worse than identity comparison.
    """

    frame: pd.DataFrame
    meta: dict
    reason: str | dict = ""
    factor_snapshot: dict | None = None

    def __post_init__(self) -> None:
        _validate_frame(self.frame)

    # -- aggregation paths -------------------------------------------------

    @classmethod
    def from_multi_factor(
        cls,
        combined: pd.DataFrame,
        *,
        reason: str | dict = "",
        factor_snapshot: dict | None = None,
        **meta,
    ) -> "AlphaSignal":
        """Build from ``FactorCombiner`` output ``[date, ticker, combined_score]``.

        The combiner returns a weighted mean of already z-scored factors; that
        mean does not itself have σ = 1, so the combined score is z-scored
        again per date before it becomes the signal.  Extra keyword arguments
        land in ``meta`` (``strategy_id=…``, ``trade_date=…``, …).
        """
        if "combined_score" not in combined.columns:
            raise ValueError("combined must carry a 'combined_score' column")
        missing = [c for c in ("date", "ticker") if c not in combined.columns]
        if missing:
            raise ValueError(f"combined is missing columns {missing}")
        renamed = combined.rename(columns={"combined_score": "score"})
        frame = _cross_zscore(renamed[list(REQUIRED_FRAME_COLS)])
        return cls(
            frame=frame,
            meta=meta,
            reason=reason,
            factor_snapshot=factor_snapshot,
        )

    @classmethod
    def from_ml_factor(
        cls,
        ml_scores: pd.DataFrame,
        *,
        reason: str | dict = "",
        factor_snapshot: dict | None = None,
        **meta,
    ) -> "AlphaSignal":
        """Build from ``MLSignalAdapter`` output ``[date, ticker, score]``.

        Applies the same cross-sectional z-score as the multi-factor path —
        the design unifies both onto one score scale (§63.5), and re-scoring
        an already standardised column is a no-op.
        """
        missing = [c for c in REQUIRED_FRAME_COLS if c not in ml_scores.columns]
        if missing:
            raise ValueError(f"ml_scores is missing columns {missing}")
        frame = _cross_zscore(ml_scores[list(REQUIRED_FRAME_COLS)])
        return cls(
            frame=frame,
            meta=meta,
            reason=reason,
            factor_snapshot=factor_snapshot,
        )

    # -- score → weights ---------------------------------------------------

    def to_weights(
        self, method: str = "top_k", k: int = DEFAULT_K, **kw
    ) -> pd.DataFrame:
        """Return ``[date, ticker, target_weight]`` via :func:`signal_to_weights`."""
        return signal_to_weights(self.frame, method=method, k=k, **kw)


# ---------------------------------------------------------------------------
# signal_to_weights
# ---------------------------------------------------------------------------


def signal_to_weights(
    signal_frame: pd.DataFrame,
    method: str = "top_k",
    k: int = DEFAULT_K,
    **kw,
) -> pd.DataFrame:
    """Convert ``[date, ticker, score]`` to ``[date, ticker, target_weight]``.

    Two methods (§63.5):

    * ``top_k`` (default) — the *k* highest scores each get ``1/k``.  Fewer
      than *k* names means every name still gets ``1/k``, so the book is
      ``n/k`` invested and the rest is cash; the sum never exceeds 1.
    * ``proportional`` — ``w_i ∝ max(score_i, 0)``, normalised within the
      date.  Negative scores clip to 0 (A-share cannot short).  A cross-section
      with no positive score produces no rows: holding nothing is the honest
      reading of "every name scores below the line".

    Only rows with positive weight are returned.  The result is the
    **unconstrained** initial book — concentration / turnover / capital limits
    belong to ``PortfolioConstraints`` (S7), not here.

    Raises ``ValueError`` for an unknown *method*, a non-positive *k*, an
    unexpected keyword, or a frame that breaks the ``[date, ticker, score]``
    contract.
    """
    if method not in WEIGHT_METHODS:
        raise ValueError(f"method must be one of {WEIGHT_METHODS}, got {method!r}")
    if kw:
        raise ValueError(f"unexpected keyword arguments: {sorted(kw)}")
    _validate_frame(signal_frame)

    if method == "top_k":
        if not k > 0:
            raise ValueError(f"k must be positive, got {k!r}")
        return _top_k_weights(signal_frame, int(k))
    return _proportional_weights(signal_frame)


def _top_k_weights(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    """Top *k* by score per date, each at ``1/k``.

    Selection is deterministic under ties: higher score first, then ticker
    ascending.  A name with a NaN score cannot be ranked and is skipped.
    """
    valid = frame.dropna(subset=["score"])
    if valid.empty:
        return _empty_weights()
    ranked = valid.sort_values(
        ["date", "score", "ticker"], ascending=[True, False, True]
    )
    selected = ranked.groupby("date", sort=True).head(k)
    out = selected[["date", "ticker"]].copy()
    out["target_weight"] = 1.0 / k
    return out.reset_index(drop=True)


def _proportional_weights(frame: pd.DataFrame) -> pd.DataFrame:
    """``w_i ∝ max(score_i, 0)`` per date, normalised; zeros dropped.

    Clipping before the normalisation is the no-short rule made arithmetic:
    a negative score contributes nothing to the denominator and holds nothing.
    """
    valid = frame.dropna(subset=["score"]).copy()
    if valid.empty:
        return _empty_weights()
    clipped = valid["score"].astype("float64").clip(lower=0.0)
    totals = clipped.groupby(valid["date"]).transform("sum")
    with np.errstate(invalid="ignore"):
        weights = np.where(totals > 0, clipped / totals, 0.0)
    valid["target_weight"] = weights
    out = valid.loc[valid["target_weight"] > 0, ["date", "ticker", "target_weight"]]
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# end-of-day archive
# ---------------------------------------------------------------------------


def write_signal(
    signal: AlphaSignal,
    weights: pd.DataFrame,
    *,
    base_dir: str = "data_platform/signals",
) -> dict[str, list[Path]]:
    """Archive *signal* and *weights* under ``cross/`` and ``timeseries/``.

    One Parquet per trade date present in ``signal.frame``:

    * ``{base_dir}/cross/{strategy_id}/{date}.parquet`` —
      ``signal_time / ticker / target_weight / score / strategy_id / trade_date``
    * ``{base_dir}/timeseries/{strategy_id}/{date}.parquet`` —
      ``datetime / ticker / direction / strength / quantity / factor_snapshot / reason``

    Runtime objects stay in memory (a signal is a middle product); these files
    are the end-of-day archive the review and Journal reconciliation read.
    ``quantity`` is always 0 here — turning a weight into shares is order
    generation (S7), not this layer.

    Returns ``{"cross": [...], "timeseries": [...]}`` with the written paths in
    date order.
    """
    _validate_frame(signal.frame)
    strategy_id = signal.meta.get("strategy_id")
    if not strategy_id:
        raise ValueError("signal.meta must carry a non-empty 'strategy_id'")
    missing = [
        c for c in ("date", "ticker", "target_weight") if c not in weights.columns
    ]
    if missing:
        raise ValueError(f"weights is missing columns {missing}")

    signal_time = signal.meta.get("signal_time") or signal.meta.get("trade_date")
    snapshot_json = (
        _jsonify(signal.factor_snapshot) if signal.factor_snapshot is not None else ""
    )
    reason_text = (
        signal.reason if isinstance(signal.reason, str) else _jsonify(signal.reason)
    )

    written: dict[str, list[Path]] = {"cross": [], "timeseries": []}
    frame = signal.frame
    for date, group in frame.groupby("date", sort=True):
        date_key = _date_str(date)
        day_w = weights.loc[pd.to_datetime(weights["date"]) == pd.to_datetime(date)]

        cross = group.merge(
            day_w[["date", "ticker", "target_weight"]],
            on=["date", "ticker"],
            how="left",
        )
        cross["target_weight"] = cross["target_weight"].fillna(0.0)
        cross["signal_time"] = signal_time if signal_time is not None else date_key
        cross["strategy_id"] = strategy_id
        cross["trade_date"] = date_key

        ts = pd.DataFrame(
            {
                "datetime": signal_time if signal_time is not None else date_key,
                "ticker": group["ticker"].to_numpy(),
                "direction": np.sign(group["score"].to_numpy(dtype="float64")),
                "strength": np.abs(group["score"].to_numpy(dtype="float64")),
                "quantity": 0.0,
                "factor_snapshot": snapshot_json,
                "reason": reason_text,
            }
        )

        cross_path = _archive_path(base_dir, "cross", strategy_id, date_key)
        ts_path = _archive_path(base_dir, "timeseries", strategy_id, date_key)
        cross[list(CROSS_COLS)].to_parquet(cross_path, index=False)
        ts[list(TIMESERIES_COLS)].to_parquet(ts_path, index=False)
        written["cross"].append(cross_path)
        written["timeseries"].append(ts_path)
    return written


def _archive_path(base_dir: str, tree: str, strategy_id: str, date_key: str) -> Path:
    out = Path(base_dir) / tree / str(strategy_id)
    out.mkdir(parents=True, exist_ok=True)
    return out / f"{date_key}.parquet"


def _jsonify(value) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)
