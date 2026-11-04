"""ML/DL signal governance — adapter, smoothing, feature versioning, ensemble.

Converts raw ML model outputs (scores or probabilities) onto the unified
cross-sectional z-score scale that :class:`AlphaSignal` consumes, and provides
per-ticker exponential smoothing for daily-frequency strategies.  Converges
with the multi-factor path at ``AlphaSignal.from_ml_factor``.

Iteration 1 scope (ac-3, ac-4): :class:`MLSignalAdapter` (``from_scores`` /
``from_probabilities``) and :func:`smooth_signal`.
Iteration 2 scope (ac-2): :func:`build_feature_matrix`, :func:`transform_bars`,
:func:`features_hash`, :func:`model_id`, :func:`write_model`,
:func:`write_experiment`.
Iteration 3 scope (ac-1, ac-5): :func:`ensemble_models`,
:func:`sanity_check_ml_model`.

Design pins (PR restates):

* ``from_probabilities`` clips to ``[1e-6, 1-1e-6]`` before logit — a
  probability of exactly 0 or 1 maps to ±∞, and clipping bounds the logit
  to ±≈13.8 without losing monotonicity.
* Zero cross-sectional std → all-zero scores that date (not NaN/inf).
* ``smooth_signal`` uses per-ticker EWM with ``halflife=5`` default; daily
  strategies call it, monthly rebalance does not (call-site policy).
* ``features_hash`` = ``sha256(canonical_specs_json + "|" + "|".join(sorted(dep_hashes)))``
  over feature specs sorted by ``(source, name|field, transform)``.
* ``model_id`` = ``{name}+v{major}.{minor}({train_date})+{train_end}+{features_hash}``.
* ``transform_bars`` implements ``pct_change_N`` + ``zscore_cs`` only.
* ``write_model`` refuses a ``REJECT`` verdict — nothing is written.

Not this module: factor combination (``factor_combiner``), weight constraint
solving (``PortfolioConstraints``), MLFactor train/predict scheduling
(``factor_runtime``), or alphaType registration (``alpha_type``).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from research.alpha_signal import _cross_zscore  # noqa: PLC2701
from research.post_processor import standardize_zscore

__all__ = [
    "MLSignalAdapter",
    "VERDICTS",
    "build_feature_matrix",
    "ensemble_models",
    "features_hash",
    "model_id",
    "sanity_check_ml_model",
    "smooth_signal",
    "transform_bars",
    "write_experiment",
    "write_model",
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


# ---------------------------------------------------------------------------
# transform_bars (D3)
# ---------------------------------------------------------------------------

#: The two transforms V1 implements.  ``pct_change_N`` is N-period return;
#: ``zscore_cs`` is per-date cross-sectional z-score.
TRANSFORMS: tuple[str, ...] = ("pct_change", "zscore_cs")


def transform_bars(
    bars: pd.DataFrame,
    field: str,
    transform: str,
) -> pd.Series:
    """Apply a named transform to a raw bars column.

    Two transforms in V1 (settled default):

    * ``pct_change_N`` — N-period percentage change (e.g. ``pct_change_5``).
    * ``zscore_cs`` — per-date cross-sectional z-score.

    Parameters
    ----------
    bars : DataFrame
        Must carry ``[date, ticker, <field>]``.
    field : str
        Column name to transform (e.g. ``"close"``, ``"volume"``).
    transform : str
        Transform name.  ``pct_change_N`` where N is a positive integer,
        or ``zscore_cs``.

    Returns
    -------
    Series
        Transformed values, same index as *bars*.
    """
    if field not in bars.columns:
        raise ValueError(f"bars is missing column {field!r}")
    if transform == "zscore_cs":
        return bars.groupby("date")[field].transform(standardize_zscore)
    if transform.startswith("pct_change_"):
        n_str = transform.removeprefix("pct_change_")
        try:
            n = int(n_str)
        except ValueError:
            raise ValueError(f"invalid pct_change transform: {transform!r}") from None
        if n <= 0:
            raise ValueError(f"pct_change period must be positive, got {n}")
        return (
            bars.sort_values(["ticker", "date"])
            .groupby("ticker")[field]
            .transform(lambda s: s.pct_change(n))
        )
    raise ValueError(f"unknown transform: {transform!r} (expected one of {TRANSFORMS})")


# ---------------------------------------------------------------------------
# build_feature_matrix (D3)
# ---------------------------------------------------------------------------


def build_feature_matrix(
    specs: list[dict],
    *,
    start,
    end,
    factor_base_dir: str = "data_platform/factors",
    bars: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build the feature matrix from a list of feature specs.

    Each spec is ``{source: "factor"|"raw", name|field: str, transform: str}``:

    * ``source="factor"`` — read the factor table via
      :func:`research.factor_storage.load_factor`, then apply ``transform``.
    * ``source="raw"`` — apply :func:`transform_bars` to *bars*.

    Returns ``[date, ticker, feat_0, feat_1, ...]`` — one column per spec,
    outer-joined on ``(date, ticker)``.

    Parameters
    ----------
    specs : list of dict
        Feature specifications.  Each must carry ``source``, ``transform``,
        and ``name`` (factor) or ``field`` (raw).
    start, end : date-like
        Date range (inclusive).
    factor_base_dir : str
        Base directory for factor tables.
    bars : DataFrame, optional
        Raw bars data for ``source="raw"`` features.  Required if any spec
        has ``source="raw"``.
    """
    if not specs:
        raise ValueError("specs must be a non-empty list")

    from research.factor_storage import load_factor  # local import to avoid cycle

    frames: list[pd.DataFrame] = []
    for i, spec in enumerate(specs):
        source = spec.get("source")
        transform = spec.get("transform")
        if source == "factor":
            name = spec.get("name")
            if not name:
                raise ValueError(f"spec[{i}] source=factor requires 'name'")
            df = load_factor(name, start, end, base_dir=factor_base_dir)
            if df.empty:
                raise ValueError(
                    f"factor {name!r} returned no data for [{start}, {end}]"
                )
            col_name = f"feat_{i}"
            df = df.rename(columns={name: col_name})
            if transform and transform != "none":
                df[col_name] = transform_bars(df, col_name, transform)
            frames.append(df[["date", "ticker", col_name]])
        elif source == "raw":
            field = spec.get("field")
            if not field:
                raise ValueError(f"spec[{i}] source=raw requires 'field'")
            if bars is None:
                raise ValueError("bars is required for source=raw features")
            col_name = f"feat_{i}"
            raw = bars[["date", "ticker", field]].copy()
            raw[col_name] = transform_bars(bars, field, transform)
            frames.append(raw[["date", "ticker", col_name]])
        else:
            raise ValueError(f"spec[{i}] has unknown source: {source!r}")

    # Normalise date to pd.Timestamp before joining — load_factor returns
    # datetime.date (date32) while bars may carry str or Timestamp.  Mixed
    # types silently double rows or raise on merge.
    frames = [f.assign(date=pd.to_datetime(f["date"])) for f in frames]

    # Outer-join on (date, ticker).
    merged = frames[0]
    for f in frames[1:]:
        merged = merged.merge(f, on=["date", "ticker"], how="outer")
    return merged.sort_values(["date", "ticker"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# features_hash (D3)
# ---------------------------------------------------------------------------


def _canonical_spec_key(spec: dict) -> tuple:
    """Sort key for feature specs: ``(source, name|field, transform)``."""
    return (
        spec.get("source", ""),
        spec.get("name", spec.get("field", "")),
        spec.get("transform", ""),
    )


def features_hash(specs: list[dict], dep_hashes: list[str]) -> str:
    """Compute the feature-set fingerprint.

    ``sha256(canonical_specs_json + "|" + "|".join(sorted(dep_hashes)))``
    where ``canonical_specs_json`` is the sorted-keys, compact-separator JSON
    of *specs* sorted by ``(source, name|field, transform)``.

    *source=raw* features contribute only via the specs JSON (field + transform);
    raw data revisions do not invalidate the hash in V1 (known limitation).

    Parameters
    ----------
    specs : list of dict
        Feature specifications.
    dep_hashes : list of str
        ``depends_hash`` values of the source=factor dependencies.

    Returns
    -------
    str
        64-character hex sha256 digest.
    """
    sorted_specs = sorted(specs, key=_canonical_spec_key)
    canonical_json = json.dumps(sorted_specs, sort_keys=True, separators=(",", ":"))
    sorted_deps = sorted(set(dep_hashes))
    raw = canonical_json + "|" + "|".join(sorted_deps)
    return hashlib.sha256(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# model_id (D4)
# ---------------------------------------------------------------------------


def model_id(
    name: str,
    major: int,
    minor: int,
    train_date: str,
    train_end: str,
    feat_hash: str,
) -> str:
    """Build the model identifier.

    Format: ``{name}+v{major}.{minor}({train_date})+{train_end}+{features_hash}``

    * ``train_date`` — training start date (ISO ``YYYY-MM-DD``).
    * ``train_end`` — training end date (ISO ``YYYY-MM-DD``).
    * ``feat_hash`` — output of :func:`features_hash`.

    Any ``features_hash`` change yields a different ``model_id`` — the ac-2
    assertion.
    """
    return f"{name}+v{major}.{minor}({train_date})+{train_end}+{feat_hash}"


# ---------------------------------------------------------------------------
# write_model (D5)
# ---------------------------------------------------------------------------


def write_model(
    name: str,
    major: int,
    minor: int,
    model,
    config: dict,
    metadata: dict,
    *,
    verdict: str = "PASS",
    base_dir: str = "data/models",
) -> Path:
    """Write a model deployment directory.

    Layout: ``{base_dir}/{name}/v{major}_{minor}/`` containing:

    * ``model.pkl`` — the trained model (joblib).
    * ``config.json`` — hyperparameters.
    * ``metadata.json`` — ``model_id`` / ``train_end`` / ``features_hash`` /
      ``metrics`` + reproducibility five: ``random_seed``, dependency
      versions, feature list, date range, hyperparams.

    Refuses a ``REJECT`` verdict — nothing is written (red line).
    ``PASS``/``SUSPECT`` may be written; ``SUSPECT`` is not promoted.

    Returns the deployment directory path.
    """
    if verdict == "REJECT":
        raise ValueError("REJECT models never enter data/models/ (red line)")

    out_dir = Path(base_dir) / name / f"v{major}_{minor}"
    out_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, out_dir / "model.pkl")

    config_path = out_dir / "config.json"
    config_path.write_text(
        json.dumps(config, indent=2, sort_keys=True), encoding="utf-8"
    )

    meta_path = out_dir / "metadata.json"
    meta_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8"
    )

    return out_dir


# ---------------------------------------------------------------------------
# write_experiment (D6)
# ---------------------------------------------------------------------------


def write_experiment(
    exp_id: str,
    metrics: dict,
    config: dict,
    *,
    base_dir: str = "experiments",
) -> Path:
    """Write an experiment record.

    Layout: ``{base_dir}/{exp_id}/`` containing:

    * ``metrics.json`` — metrics + ``created_at`` ISO timestamp.
    * ``config.json`` — run config / hyperparams.

    No MLflow / W&B — plain directories only.
    """
    out_dir = Path(base_dir) / exp_id
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics_payload = {**metrics, "created_at": datetime.now(timezone.utc).isoformat()}
    (out_dir / "metrics.json").write_text(
        json.dumps(metrics_payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    (out_dir / "config.json").write_text(
        json.dumps(config, indent=2, sort_keys=True), encoding="utf-8"
    )

    return out_dir


# ---------------------------------------------------------------------------
# ensemble_models (D7, ac-5)
# ---------------------------------------------------------------------------


def ensemble_models(
    models: list,
    X=None,
    *,
    weights: list[float] | None = None,
) -> pd.DataFrame:
    """Combine multiple model outputs into one signal frame.

    Each element of *models* is a callable ``f(X) -> DataFrame [date, ticker,
    score]``.  Frames are outer-joined on ``[date, ticker]``; the combined
    score is the weighted mean ``Σwᵢsᵢ / Σwᵢ`` over the models that produced
    that cell (skipna — a model that has no row for a key is excluded from
    that key's denominator).

    ``weights=None`` → ``wᵢ=1`` for all (equal-weight default).  Explicit
    weights are taken as given — no normalisation policy beyond the mean.
    Gap #14 (validation ICIR normalisation) is **not decided here**;
    ``weights`` is an interface only.

    Parameters
    ----------
    models : list of callable
        Each ``f(X) -> DataFrame [date, ticker, score]``.
    X : optional
        Passed through to each model callable.
    weights : list of float, optional
        Per-model weights.  Must have the same length as *models*.

    Returns
    -------
    DataFrame
        ``[date, ticker, score]`` — the weighted mean across models.
    """
    if not models:
        raise ValueError("models must be a non-empty list")
    if weights is not None and len(weights) != len(models):
        raise ValueError(
            f"weights length {len(weights)} != models length {len(models)}"
        )

    w = [1.0] * len(models) if weights is None else [float(v) for v in weights]

    # Collect per-model frames.
    frames: list[pd.DataFrame] = []
    for model in models:
        result = model(X) if X is not None else model()
        if not isinstance(result, pd.DataFrame):
            raise TypeError(f"model must return DataFrame, got {type(result).__name__}")
        _validate_frame(result)
        frames.append(result[["date", "ticker", "score"]].copy())

    # Normalise date to pd.Timestamp for consistent joining.
    frames = [f.assign(date=pd.to_datetime(f["date"])) for f in frames]

    # Outer-join all frames on (date, ticker), keeping scores in separate columns.
    merged = frames[0].rename(columns={"score": "score_0"})
    for i, f in enumerate(frames[1:], start=1):
        merged = merged.merge(
            f.rename(columns={"score": f"score_{i}"}),
            on=["date", "ticker"],
            how="outer",
        )

    # Weighted mean with skipna: only models that produced a cell contribute.
    score_cols = [f"score_{i}" for i in range(len(models))]
    w_arr = np.array(w, dtype="float64")

    def _weighted_mean(row) -> float:
        vals = row[score_cols].to_numpy(dtype="float64")
        mask = ~np.isnan(vals)
        if not mask.any():
            return np.nan
        w_masked = w_arr[mask]
        return float(np.dot(vals[mask], w_masked) / w_masked.sum())

    merged["score"] = merged[score_cols].apply(_weighted_mean, axis=1)
    return merged[["date", "ticker", "score"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# sanity_check_ml_model (D8, ac-1)
# ---------------------------------------------------------------------------

#: The three verdicts the sanity check returns.
VERDICTS: tuple[str, ...] = ("PASS", "SUSPECT", "REJECT")


def sanity_check_ml_model(
    test_metrics: dict,
    train_ic: float,
    baseline_sharpe: float,
) -> str:
    """Overfitting three-check gate: PASS | SUSPECT | REJECT.

    Three checks, REJECT dominating SUSPECT:

    1. ``sharpe <= 1.1 * baseline_sharpe`` → **REJECT** (夏普未超等权 10%).
       Boundary: exactly 1.1× is REJECT (未超 = reached-but-not-exceeded).
    2. ``sharpe > 2.0`` → **SUSPECT** (查前视).
       Boundary: strictly above 2.0.
    3. ``ic < 0.5 * train_ic`` → **SUSPECT** (查泄漏).

    If multiple checks trigger, REJECT wins over SUSPECT.  Otherwise PASS.

    Parameters
    ----------
    test_metrics : dict
        Must carry ``sharpe`` and ``ic``.
    train_ic : float
        Training-set IC.
    baseline_sharpe : float
        Equal-weight ensemble's Sharpe (D7).

    Returns
    -------
    str
        ``"PASS"``, ``"SUSPECT"``, or ``"REJECT"``.
    """
    sharpe = test_metrics.get("sharpe")
    ic = test_metrics.get("ic")
    if sharpe is None:
        raise ValueError("test_metrics must carry 'sharpe'")
    if ic is None:
        raise ValueError("test_metrics must carry 'ic'")

    # REJECT: sharpe not over baseline by 10%.
    if sharpe <= 1.1 * baseline_sharpe:
        return "REJECT"

    # SUSPECT checks (REJECT already excluded above).
    if sharpe > 2.0 or ic < 0.5 * train_ic:
        return "SUSPECT"

    return "PASS"
